/*
Copyright 2025 The PDB Operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	availabilityv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
	"github.com/pdb-operator/pdb-operator/internal/cache"
	"github.com/pdb-operator/pdb-operator/internal/events"
	"github.com/pdb-operator/pdb-operator/internal/logging"
	"github.com/pdb-operator/pdb-operator/internal/metrics"
	"github.com/pdb-operator/pdb-operator/internal/tracing"
)

const (
	kindWorkload      = "Workload"
	kindWorkloadLower = "workload"
	kindPodGroup      = "PodGroup"
	// WorkloadAPIGroup is the API group of the upstream Workload API (KEP-4671).
	WorkloadAPIGroup   = "scheduling.k8s.io"
	WorkloadAPIVersion = "v1beta1"
	// PodGroupNameIndex indexes pods by spec.schedulingGroup.podGroupName.
	PodGroupNameIndex = "spec.schedulingGroup.podGroupName"
	// workloadPodsPollDelay retries reconciles that wait on pods appearing.
	workloadPodsPollDelay = 30 * time.Second
)

// WorkloadGVK is the GroupVersionKind of the upstream Workload object.
var WorkloadGVK = schema.GroupVersionKind{Group: WorkloadAPIGroup, Version: WorkloadAPIVersion, Kind: kindWorkload}

// PodGroupGVK is the GroupVersionKind of the upstream PodGroup object.
var PodGroupGVK = schema.GroupVersionKind{Group: WorkloadAPIGroup, Version: WorkloadAPIVersion, Kind: kindPodGroup}

// NewWorkloadObject returns an empty unstructured Workload with its GVK set.
func NewWorkloadObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(WorkloadGVK)
	return u
}

// NewPodGroupObject returns an empty unstructured PodGroup with its GVK set.
func NewPodGroupObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PodGroupGVK)
	return u
}

// gangTemplate is the parsed gang slice of one spec.podGroupTemplates entry.
type gangTemplate struct {
	Name       string
	MinCount   int32
	DisruptAll bool
}

// parseGangTemplates returns the gang templates, total template count, and composite count.
func parseGangTemplates(u *unstructured.Unstructured) (gangs []gangTemplate, total int, composite int) {
	if c, found, err := unstructured.NestedSlice(u.Object, "spec", "compositePodGroupTemplates"); err == nil && found {
		composite = len(c)
	}
	tmpls, found, err := unstructured.NestedSlice(u.Object, "spec", "podGroupTemplates")
	if err != nil || !found {
		return nil, 0, composite
	}
	for _, t := range tmpls {
		tm, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		total++
		name, _, _ := unstructured.NestedString(tm, "name")
		minCount, foundGang, err := unstructured.NestedInt64(tm, "schedulingPolicy", "gang", "minCount")
		if err != nil || !foundGang {
			continue
		}
		_, disruptAll, _ := unstructured.NestedMap(tm, "disruptionMode", "all")
		gangs = append(gangs, gangTemplate{Name: name, MinCount: int32(minCount), DisruptAll: disruptAll})
	}
	return gangs, total, composite
}

// workloadAPIObject adapts an unstructured Workload to WorkloadAccessor.
// Selector and replicas are derived from the observed pods at reconcile time.
type workloadAPIObject struct {
	*unstructured.Unstructured
	selector *metav1.LabelSelector
	pods     int32
}

func (w *workloadAPIObject) GetObject() client.Object { return w.Unstructured }
func (w *workloadAPIObject) GetName() string          { return w.Unstructured.GetName() }
func (w *workloadAPIObject) GetNamespace() string     { return w.Unstructured.GetNamespace() }
func (w *workloadAPIObject) GetAnnotations() map[string]string {
	return w.Unstructured.GetAnnotations()
}
func (w *workloadAPIObject) GetLabels() map[string]string  { return w.Unstructured.GetLabels() }
func (w *workloadAPIObject) GetGeneration() int64          { return w.Unstructured.GetGeneration() }
func (w *workloadAPIObject) DeepCopyObject() client.Object { return w.DeepCopy() }
func (w *workloadAPIObject) Kind() string                  { return kindWorkload }
func (w *workloadAPIObject) KindLower() string             { return kindWorkloadLower }
func (w *workloadAPIObject) GetReplicas() int32            { return w.pods }
func (w *workloadAPIObject) GetSelector() *metav1.LabelSelector {
	return w.selector
}

func (w *workloadAPIObject) GetDeletionTimestamp() *metav1.Time {
	return w.Unstructured.GetDeletionTimestamp()
}

// deriveGroupSelector intersects the labels of all group pods into a candidate selector.
// It returns nil when no candidate exists or the candidate matches pods outside the group.
func deriveGroupSelector(pods []corev1.Pod, allInNamespace []corev1.Pod) map[string]string {
	if len(pods) == 0 {
		return nil
	}
	candidate := map[string]string{}
	for k, v := range pods[0].Labels {
		candidate[k] = v
	}
	for _, p := range pods[1:] {
		for k, v := range candidate {
			if p.Labels[k] != v {
				delete(candidate, k)
			}
		}
	}
	if len(candidate) == 0 {
		return nil
	}
	// exactness: the candidate must select the group pods and nothing else
	member := make(map[types.UID]bool, len(pods))
	for _, p := range pods {
		member[p.UID] = true
	}
	sel := labels.SelectorFromSet(candidate)
	for _, p := range allInNamespace {
		if sel.Matches(labels.Set(p.Labels)) && !member[p.UID] {
			return nil
		}
	}
	return candidate
}

// WorkloadAPIReconciler manages PDBs for upstream Workload objects (scheduling.k8s.io/v1beta1).
type WorkloadAPIReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    k8sevents.EventRecorder
	Events      *events.EventRecorder
	PolicyCache *cache.PolicyCache
	Config      *SharedConfig
	// Clock is injectable for deterministic maintenance-window tests; defaults to real time.
	Clock clock.PassiveClock

	tracker *WorkloadStateTracker
}

func (r *WorkloadAPIReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=workloads;podgroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=workloads,verbs=update;patch
// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=workloads/finalizers,verbs=get;patch;update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile derives gang-aware PDBs from a Workload's pod groups and pods.
func (r *WorkloadAPIReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()

	ctx, span := tracing.ReconcileSpan(ctx, kindWorkloadLower, req.Namespace, req.Name)
	defer span.End()

	reconcileID := kindWorkloadLower + "-" + uuid.New().String()
	correlationID := uuid.New().String()

	span.SetAttributes(
		attribute.String("reconcile.id", reconcileID),
		attribute.String("correlation.id", correlationID),
	)

	logger := logging.CreateUnifiedLogger(ctx,
		"workload-pdb", "workload-controller", WorkloadAPIGroup, kindWorkload,
		kindWorkloadLower, req.Name, req.Namespace, reconcileID, correlationID,
	)

	var reconcileErr error
	defer func() {
		duration := time.Since(startTime)
		metrics.RecordReconciliation(kindWorkloadLower, duration, reconcileErr)
		if reconcileErr != nil {
			tracing.RecordError(span, reconcileErr, "Reconciliation failed")
		}
		result := logging.AuditResultSuccess
		if reconcileErr != nil {
			result = logging.AuditResultFailure
		}
		logger.Audit("RECONCILE", fmt.Sprintf("%s/%s", req.Namespace, req.Name), kindWorkloadLower,
			req.Namespace, req.Name, result, map[string]interface{}{
				"controller": kindWorkloadLower,
				"duration":   time.Since(startTime).String(),
				"durationMs": time.Since(startTime).Milliseconds(),
			})
		logger.Info("Reconciliation completed", map[string]any{"duration": time.Since(startTime).String()})
	}()

	workload := NewWorkloadObject()
	if err := r.Get(ctx, req.NamespacedName, workload); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Workload not found, ignoring since object must be deleted", map[string]any{})
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		logger.Error(err, "Failed to get Workload", map[string]any{})
		return ctrl.Result{}, err
	}

	w := &workloadAPIObject{Unstructured: workload}

	if w.GetDeletionTimestamp() != nil {
		logger.Info("Workload is being deleted", map[string]any{})
		r.tracker.ClearState(w)
		tracing.AddEvent(ctx, "DeletingPDB")
		if err := HandleDeletion(ctx, r.Client, r.Events, w, logger.ToLogr()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	in, done, res, err := r.prepareGangInput(ctx, w, logger)
	if done || err != nil {
		reconcileErr = err
		return res, err
	}

	tracing.AddEvent(ctx, "EvaluatingPolicies")

	config, err := GetAvailabilityConfigWithCache(ctx, r.Client, r.PolicyCache, r.Events, w, logger.ToLogr())
	if err != nil {
		reconcileErr = err
		logger.Error(err, "Failed to get availability configuration", map[string]any{})
		return ctrl.Result{}, err
	}

	if config == nil {
		logger.Info("No availability configuration found, skipping PDB", map[string]any{})
		if r.Events != nil {
			r.Events.Infof(workload, "WorkloadUnmanaged",
				"Workload %s has no availability configuration", w.GetName())
		}
		return ctrl.Result{}, nil
	}

	if !r.applyGangBudget(config, in, w) {
		return ctrl.Result{}, r.cleanupQuietly(ctx, w, logger)
	}

	// add the finalizer before the state-change guard so an out-of-band removal is always restored
	if !controllerutil.ContainsFinalizer(workload, FinalizerPDBCleanup) {
		patch := client.MergeFrom(workload.DeepCopy())
		controllerutil.AddFinalizer(workload, FinalizerPDBCleanup)
		if err := r.Patch(ctx, workload, patch); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to add finalizer", map[string]any{})
			return ctrl.Result{}, err
		}
		logger.Info("Added finalizer for PDB cleanup", map[string]any{})
		return ctrl.Result{Requeue: true}, nil
	}

	stateChanged, err := r.tracker.HasStateChanged(ctx, r.Client, w, config)
	if err != nil {
		logger.Error(err, "Failed to check workload state changes, proceeding with reconciliation", map[string]any{})
		stateChanged = true
	}
	if !stateChanged {
		logger.Info("Skipping reconciliation - no state change detected", map[string]any{
			"availabilityClass": string(config.AvailabilityClass),
			"reason":            "no_state_change",
		})
		return ctrl.Result{}, nil
	}

	if IsInMaintenanceWindow(config, r.now()) {
		logger.Info("In maintenance window, temporarily relaxing PDB", map[string]any{
			"maintenanceWindow": config.MaintenanceWindow,
		})
		if err := RemovePDBTemporarily(ctx, r.Client, w, log.FromContext(ctx)); err != nil {
			reconcileErr = err
			return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// never adopt a PDB owned by another kind
	existingPDB := &policyv1.PodDisruptionBudget{}
	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}
	if err := r.Get(ctx, pdbKey, existingPDB); err == nil {
		if ref := metav1.GetControllerOf(existingPDB); ref != nil && ref.Kind != kindWorkload {
			logger.Info("PDB name is held by another owner, waiting for its controller to release it", map[string]any{
				"pdb": pdbKey.Name, "ownerKind": ref.Kind, "ownerName": ref.Name,
			})
			return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, nil
		}
	}

	result, err := ReconcilePDB(ctx, r.Client, r.Scheme, r.Events, w, config, log.FromContext(ctx))
	if err != nil {
		reconcileErr = err
		logger.Error(err, "PDB reconciliation failed, will retry", map[string]any{})
		return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
	}

	metrics.ManagedWorkloads.WithLabelValues(w.GetNamespace(), string(config.AvailabilityClass)).Set(1)
	metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), true, "managed")
	if err := r.tracker.UpdateState(ctx, r.Client, w, config); err != nil {
		logger.Error(err, "Failed to update workload state cache", map[string]any{})
	}

	logger.Info("Successfully reconciled PDB", map[string]any{
		"availabilityClass": config.AvailabilityClass,
		"source":            config.Source,
		"reconcileID":       reconcileID,
	})

	return applyMaintenanceRequeue(result, config, r.now()), nil
}

// gangReconcileInput is the gang shape Reconcile derives from templates, pod groups, and pods.
type gangReconcileInput struct {
	gang         gangTemplate
	groupCount   int32
	maxGroupSize int32
}

// prepareGangInput parses the gang shape and derives the pod-based selector onto w.
// done=true means Reconcile should return res immediately.
func (r *WorkloadAPIReconciler) prepareGangInput(ctx context.Context, w *workloadAPIObject,
	logger *logging.UnifiedLogger) (in gangReconcileInput, done bool, res ctrl.Result, err error) {
	gangs, total, composite := parseGangTemplates(w.Unstructured)
	if len(gangs) == 0 {
		// basic-only workloads are owned by the pod-owning controller's native path
		logger.Info("No gang pod group templates, skipping", map[string]any{"templates": total})
		return in, true, ctrl.Result{}, r.cleanupQuietly(ctx, w, logger)
	}
	if len(gangs) > 1 || composite > 0 {
		r.warnSkip(w, "Workload %s has multiple or composite gang templates, not yet supported", w.GetName())
		return in, true, ctrl.Result{}, r.cleanupQuietly(ctx, w, logger)
	}
	in.gang = gangs[0]

	groups, groupPods, err := r.collectTemplatePods(ctx, w.GetNamespace(), w.GetName(), in.gang.Name)
	if err != nil {
		logger.Error(err, "Failed to collect pod groups", map[string]any{})
		return in, true, ctrl.Result{}, err
	}

	if len(groupPods) == 0 {
		r.warnSkip(w, "No pods for Workload %s yet; PDB deferred until its pod groups have pods", w.GetName())
		if err := r.cleanupQuietly(ctx, w, logger); err != nil {
			return in, true, ctrl.Result{}, err
		}
		return in, true, ctrl.Result{RequeueAfter: workloadPodsPollDelay}, nil
	}

	for i := range groupPods {
		// pods with a native gang path (today: LWS) already have a group-aware PDB
		if _, owned := groupPods[i].Labels[LWSSetNameLabelKey]; owned {
			logger.Info("Pods are managed by a native gang path, skipping", map[string]any{})
			return in, true, ctrl.Result{}, r.cleanupQuietly(ctx, w, logger)
		}
	}

	allPods := &corev1.PodList{}
	if err := r.List(ctx, allPods, client.InNamespace(w.GetNamespace())); err != nil {
		return in, true, ctrl.Result{}, err
	}
	selectorLabels := deriveGroupSelector(groupPods, allPods.Items)
	if selectorLabels == nil {
		r.warnSkip(w, "No exact label selector derivable for Workload %s pods; cannot create a safe PDB", w.GetName())
		if err := r.cleanupQuietly(ctx, w, logger); err != nil {
			return in, true, ctrl.Result{}, err
		}
		return in, true, ctrl.Result{RequeueAfter: workloadPodsPollDelay}, nil
	}
	w.selector = &metav1.LabelSelector{MatchLabels: selectorLabels}
	w.pods = int32(len(groupPods))

	in.groupCount = int32(len(groups))
	for _, pods := range groups {
		if int32(len(pods)) > in.maxGroupSize {
			in.maxGroupSize = int32(len(pods))
		}
	}

	if in.gang.DisruptAll && in.groupCount < 2 {
		// a single all-mode group has no valid PDB: any budget permanently blocks drains
		r.warnSkip(w,
			"No PDB created for %s: a single pod group with disruptionMode all restarts as a unit, so any PDB would permanently block node drains",
			w.GetName())
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), false, "single_group")
		return in, true, ctrl.Result{}, r.cleanupQuietly(ctx, w, logger)
	}
	return in, false, ctrl.Result{}, nil
}

// applyGangBudget rewrites config.MinAvailable for the gang shape; false means no valid PDB exists.
func (r *WorkloadAPIReconciler) applyGangBudget(config *AvailabilityConfig, in gangReconcileInput, w *workloadAPIObject) bool {
	if in.gang.DisruptAll {
		// the whole group restarts together, so quantize the budget to whole groups
		config.MinAvailable = QuantizeMinAvailableForGroups(config.MinAvailable, in.groupCount, in.maxGroupSize)
		return true
	}
	if in.groupCount != 1 {
		return true
	}
	// a single independently-disrupted gang keeps pod semantics floored at minCount
	floored, ok := floorAtMinCount(config.MinAvailable, in.gang.MinCount, w.pods)
	if !ok {
		r.warnSkip(w,
			"No PDB created for %s: gang minCount %d leaves no pod evictable, so any PDB would permanently block node drains",
			w.GetName(), in.gang.MinCount)
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), false, "min_count_blocks_drains")
		return false
	}
	config.MinAvailable = floored
	return true
}

// floorAtMinCount resolves minAvailable to an absolute pod count no lower than minCount.
// ok is false when the floor leaves no pod evictable.
func floorAtMinCount(minAvailable intstr.IntOrString, minCount, totalPods int32) (intstr.IntOrString, bool) {
	scaled, err := intstr.GetScaledValueFromIntOrPercent(&minAvailable, int(totalPods), true)
	if err != nil {
		scaled = 0
	}
	floored := int32(scaled)
	if minCount > floored {
		floored = minCount
	}
	if floored >= totalPods {
		return minAvailable, false
	}
	return intstr.FromInt32(floored), true
}

// collectTemplatePods returns pods per PodGroup of the template, and all of them flattened.
func (r *WorkloadAPIReconciler) collectTemplatePods(ctx context.Context, namespace, workloadName, templateName string) (map[string][]corev1.Pod, []corev1.Pod, error) {
	pgList := &unstructured.UnstructuredList{}
	pgList.SetGroupVersionKind(PodGroupGVK.GroupVersion().WithKind(kindPodGroup + "List"))
	if err := r.List(ctx, pgList, client.InNamespace(namespace)); err != nil {
		return nil, nil, err
	}

	groups := map[string][]corev1.Pod{}
	var all []corev1.Pod
	for i := range pgList.Items {
		pg := &pgList.Items[i]
		wName, _, _ := unstructured.NestedString(pg.Object, "spec", "workloadRef", "workloadName")
		tName, _, _ := unstructured.NestedString(pg.Object, "spec", "workloadRef", "templateName")
		if wName != workloadName || tName != templateName {
			continue
		}
		podList := &corev1.PodList{}
		if err := r.List(ctx, podList, client.InNamespace(namespace),
			client.MatchingFields{PodGroupNameIndex: pg.GetName()}); err != nil {
			return nil, nil, err
		}
		groups[pg.GetName()] = podList.Items
		all = append(all, podList.Items...)
	}
	return groups, all, nil
}

func (r *WorkloadAPIReconciler) warnSkip(w *workloadAPIObject, format string, args ...interface{}) {
	if r.Events != nil {
		r.Events.Warnf(w.Unstructured, "WorkloadSkipped", format, args...)
	}
}

// cleanupQuietly removes a previously created PDB when the workload no longer qualifies.
func (r *WorkloadAPIReconciler) cleanupQuietly(ctx context.Context, w *workloadAPIObject, logger *logging.UnifiedLogger) error {
	if err := CleanupPDB(ctx, r.Client, r.Events, w, logger.ToLogr()); err != nil {
		logger.Error(err, "Failed to clean up PDB", map[string]any{})
		return err
	}
	return nil
}

// SetupWithManager sets up the Workload API controller with the Manager.
func (r *WorkloadAPIReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := 3
	if r.Config != nil && r.Config.MaxConcurrentReconciles > 0 {
		maxConcurrent = r.Config.MaxConcurrentReconciles
	}
	return r.SetupWithManagerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: maxConcurrent,
	})
}

// SetupWithManagerWithOptions sets up the controller with custom options.
func (r *WorkloadAPIReconciler) SetupWithManagerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	r.tracker = NewWorkloadStateTracker()

	if r.Recorder == nil && mgr != nil {
		r.Recorder = mgr.GetEventRecorder("workload-pdb-controller")
	}
	if r.Events == nil && r.Recorder != nil {
		r.Events = events.NewEventRecorder(r.Recorder)
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, PodGroupNameIndex,
		func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
				return nil
			}
			return []string{*pod.Spec.SchedulingGroup.PodGroupName}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("workload-pdb").
		For(NewWorkloadObject(), builder.WithPredicates(workloadChangePredicate())).
		Watches(NewPodGroupObject(), podGroupToWorkloadHandler()).
		Watches(&corev1.Pod{}, r.podToWorkloadHandler(), builder.WithPredicates(groupPodPredicate())).
		Watches(&policyv1.PodDisruptionBudget{}, workloadPDBHandler(), builder.WithPredicates(managedWorkloadPDBPredicate())).
		Watches(&availabilityv1alpha1.PDBPolicy{}, r.policyToWorkloadsHandler()).
		WithOptions(opts).
		Complete(r)
}

func workloadChangePredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return isWorkloadObject(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !isWorkloadObject(e.ObjectOld) || !isWorkloadObject(e.ObjectNew) {
				return false
			}
			if e.ObjectOld.GetGeneration() == e.ObjectNew.GetGeneration() {
				return e.ObjectOld.GetDeletionTimestamp() == nil && e.ObjectNew.GetDeletionTimestamp() != nil
			}
			return true
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return true },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func podGroupToWorkloadHandler() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil
		}
		wName, _, _ := unstructured.NestedString(u.Object, "spec", "workloadRef", "workloadName")
		if wName == "" {
			return nil
		}
		return []ctrl.Request{{
			NamespacedName: types.NamespacedName{Name: wName, Namespace: u.GetNamespace()},
		}}
	})
}

func (r *WorkloadAPIReconciler) podToWorkloadHandler() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
			return nil
		}
		pg := NewPodGroupObject()
		key := types.NamespacedName{Name: *pod.Spec.SchedulingGroup.PodGroupName, Namespace: pod.Namespace}
		if err := r.Get(ctx, key, pg); err != nil {
			return nil
		}
		wName, _, _ := unstructured.NestedString(pg.Object, "spec", "workloadRef", "workloadName")
		if wName == "" {
			return nil
		}
		return []ctrl.Request{{
			NamespacedName: types.NamespacedName{Name: wName, Namespace: pod.Namespace},
		}}
	})
}

func groupPodPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return podInSchedulingGroup(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !podInSchedulingGroup(e.ObjectNew) {
				return false
			}
			// selector derivation and pod counts only depend on labels and existence
			return !labels.Equals(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels())
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return podInSchedulingGroup(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func workloadPDBHandler() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		pdb, ok := obj.(*policyv1.PodDisruptionBudget)
		if !ok {
			return nil
		}
		if pdbLabels := pdb.GetLabels(); pdbLabels == nil || pdbLabels[LabelManagedBy] != OperatorName {
			return nil
		}
		ownerRef := metav1.GetControllerOf(pdb)
		if ownerRef == nil || ownerRef.Kind != kindWorkload {
			return nil
		}
		return []ctrl.Request{{
			NamespacedName: types.NamespacedName{Name: ownerRef.Name, Namespace: pdb.Namespace},
		}}
	})
}

func managedWorkloadPDBPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			pdb, ok := e.ObjectNew.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			if pdbLabels := pdb.GetLabels(); pdbLabels != nil && pdbLabels[LabelManagedBy] == OperatorName {
				oldPDB := e.ObjectOld.(*policyv1.PodDisruptionBudget)
				return !isPDBUpdateByUs(oldPDB, pdb)
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			pdb, ok := e.Object.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			if pdbLabels := pdb.GetLabels(); pdbLabels != nil {
				return pdbLabels[LabelManagedBy] == OperatorName
			}
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func (r *WorkloadAPIReconciler) policyToWorkloadsHandler() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		policy, ok := obj.(*availabilityv1alpha1.PDBPolicy)
		if !ok {
			return nil
		}
		logger := log.FromContext(ctx)
		wList := &unstructured.UnstructuredList{}
		wList.SetGroupVersionKind(WorkloadGVK.GroupVersion().WithKind(kindWorkload + "List"))
		if err := r.List(ctx, wList); err != nil {
			logger.Error(err, "Failed to list Workloads for policy change")
			return nil
		}
		var requests []ctrl.Request
		for i := range wList.Items {
			if PolicyMatchesWorkload(policy, &workloadAPIObject{Unstructured: &wList.Items[i]}) {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{Name: wList.Items[i].GetName(), Namespace: wList.Items[i].GetNamespace()},
				})
			}
		}
		return requests
	})
}

func isWorkloadObject(obj client.Object) bool {
	u, ok := obj.(*unstructured.Unstructured)
	return ok && u.GroupVersionKind() == WorkloadGVK
}

func podInSchedulingGroup(obj client.Object) bool {
	pod, ok := obj.(*corev1.Pod)
	return ok && pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil
}
