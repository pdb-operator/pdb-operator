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

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	kindLeaderWorkerSet      = "LeaderWorkerSet"
	kindLeaderWorkerSetLower = "leaderworkerset"
	// LWSGroup is the API group of the LeaderWorkerSet CRD (note: labels use a different prefix).
	LWSGroup   = "leaderworkerset.x-k8s.io"
	LWSVersion = "v1"
	// LWSSetNameLabelKey is the label LWS puts on every pod of a set.
	LWSSetNameLabelKey = "leaderworkerset.sigs.k8s.io/name"
)

// LWSGVK is the GroupVersionKind of LeaderWorkerSet.
var LWSGVK = schema.GroupVersionKind{Group: LWSGroup, Version: LWSVersion, Kind: kindLeaderWorkerSet}

// NewLWSObject returns an empty unstructured LeaderWorkerSet with its GVK set.
func NewLWSObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(LWSGVK)
	return u
}

// lwsWorkload adapts an unstructured LeaderWorkerSet to WorkloadAccessor.
// GetReplicas returns the number of groups, not pods.
type lwsWorkload struct{ *unstructured.Unstructured }

func (l *lwsWorkload) GetObject() client.Object          { return l.Unstructured }
func (l *lwsWorkload) GetName() string                   { return l.Unstructured.GetName() }
func (l *lwsWorkload) GetNamespace() string              { return l.Unstructured.GetNamespace() }
func (l *lwsWorkload) GetAnnotations() map[string]string { return l.Unstructured.GetAnnotations() }
func (l *lwsWorkload) GetLabels() map[string]string      { return l.Unstructured.GetLabels() }
func (l *lwsWorkload) GetGeneration() int64              { return l.Unstructured.GetGeneration() }
func (l *lwsWorkload) DeepCopyObject() client.Object     { return l.DeepCopy() }
func (l *lwsWorkload) Kind() string                      { return kindLeaderWorkerSet }
func (l *lwsWorkload) KindLower() string                 { return kindLeaderWorkerSetLower }

func (l *lwsWorkload) GetDeletionTimestamp() *metav1.Time {
	return l.Unstructured.GetDeletionTimestamp()
}

// GetSelector synthesizes the selector; LWS has no spec.selector but labels every pod with the set name.
func (l *lwsWorkload) GetSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{LWSSetNameLabelKey: l.GetName()}}
}

func (l *lwsWorkload) GetReplicas() int32 {
	if v, found, err := unstructured.NestedInt64(l.Object, "spec", "replicas"); err == nil && found {
		return int32(v)
	}
	return 1
}

// GroupSize returns spec.leaderWorkerTemplate.size (pods per group, leader included).
func (l *lwsWorkload) GroupSize() int32 {
	if v, found, err := unstructured.NestedInt64(l.Object, "spec", "leaderWorkerTemplate", "size"); err == nil && found {
		return int32(v)
	}
	return 1
}

// QuantizeMinAvailableForGroups converts a pod-level minAvailable into a whole-group
// integer for gang-restarting workloads: percentages apply to groups (rounded up),
// absolute values round up to whole groups, and the result is clamped to groups-1
// so one group always stays drainable.
func QuantizeMinAvailableForGroups(minAvailable intstr.IntOrString, groups, size int32) intstr.IntOrString {
	if groups < 2 || size <= 1 {
		return minAvailable
	}
	var desired int32
	if minAvailable.Type == intstr.String {
		v, err := intstr.GetScaledValueFromIntOrPercent(&minAvailable, int(groups), true)
		if err != nil {
			return minAvailable
		}
		desired = int32(v)
	} else {
		desired = (minAvailable.IntVal + size - 1) / size
	}
	if desired > groups-1 {
		desired = groups - 1
	}
	if desired < 0 {
		desired = 0
	}
	return intstr.FromInt32(desired * size)
}

// LeaderWorkerSetReconciler reconciles LeaderWorkerSet objects (leaderworkerset.x-k8s.io/v1).
type LeaderWorkerSetReconciler struct {
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

func (r *LeaderWorkerSetReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets/finalizers,verbs=get;patch;update

// Reconcile handles LeaderWorkerSet changes and manages corresponding PDBs.
func (r *LeaderWorkerSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()

	ctx, span := tracing.ReconcileSpan(ctx, kindLeaderWorkerSetLower, req.Namespace, req.Name)
	defer span.End()

	reconcileID := kindLeaderWorkerSetLower + "-" + uuid.New().String()
	correlationID := uuid.New().String()

	span.SetAttributes(
		attribute.String("reconcile.id", reconcileID),
		attribute.String("correlation.id", correlationID),
	)

	logger := logging.CreateUnifiedLogger(ctx,
		"leaderworkerset-pdb", "leaderworkerset-controller", LWSGroup, kindLeaderWorkerSet,
		kindLeaderWorkerSetLower, req.Name, req.Namespace, reconcileID, correlationID,
	)

	var reconcileErr error
	defer func() {
		duration := time.Since(startTime)
		metrics.RecordReconciliation(kindLeaderWorkerSetLower, duration, reconcileErr)
		if reconcileErr != nil {
			tracing.RecordError(span, reconcileErr, "Reconciliation failed")
		}
		result := logging.AuditResultSuccess
		if reconcileErr != nil {
			result = logging.AuditResultFailure
		}
		logger.Audit("RECONCILE", fmt.Sprintf("%s/%s", req.Namespace, req.Name), kindLeaderWorkerSetLower,
			req.Namespace, req.Name, result, map[string]interface{}{
				"controller": kindLeaderWorkerSetLower,
				"duration":   time.Since(startTime).String(),
				"durationMs": time.Since(startTime).Milliseconds(),
			})
		logger.Info("Reconciliation completed", map[string]any{"duration": time.Since(startTime).String()})
	}()

	tracing.AddEvent(ctx, "FetchingLeaderWorkerSet", attribute.String("reconcile.id", reconcileID))

	lws := NewLWSObject()
	if err := r.Get(ctx, req.NamespacedName, lws); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("LeaderWorkerSet not found, ignoring since object must be deleted", map[string]any{})
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		logger.Error(err, "Failed to get LeaderWorkerSet", map[string]any{})
		return ctrl.Result{}, err
	}

	w := &lwsWorkload{lws}

	if w.GetDeletionTimestamp() != nil {
		logger.Info("LeaderWorkerSet is being deleted", map[string]any{})
		r.tracker.ClearState(w)
		tracing.AddEvent(ctx, "DeletingPDB")
		if err := HandleDeletion(ctx, r.Client, r.Events, w, logger.ToLogr()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	groups := w.GetReplicas()
	size := w.GroupSize()

	if groups < 2 {
		// a single gang-restarting group has no valid PDB: any protection permanently blocks drains
		logger.Info("Single group, no PDB possible without blocking node drains", map[string]any{
			"groups": groups, "size": size,
		})
		tracing.AddEvent(ctx, "SkippedPDB",
			attribute.String("reason", "single_group"),
			attribute.Int("groups", int(groups)),
		)
		if err := CleanupPDB(ctx, r.Client, r.Events, w, logger.ToLogr()); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to clean up PDB for single-group LeaderWorkerSet", map[string]any{})
			return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
		}
		if r.Events != nil {
			r.Events.Warnf(lws, "LeaderWorkerSetSkipped",
				"No PDB created for %s: a single group restarts as a unit, so any PDB would permanently block node drains", w.GetName())
		}
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), false, "single_group")
		return ctrl.Result{}, nil
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
		tracing.AddEvent(ctx, "SkippedPDB", attribute.String("reason", "no_availability_configuration"))
		if r.Events != nil {
			r.Events.Infof(lws, "LeaderWorkerSetUnmanaged",
				"LeaderWorkerSet %s has no availability configuration", w.GetName())
		}
		return ctrl.Result{}, nil
	}

	// pods restart in gangs of `size`, so quantize the budget to whole groups
	config.MinAvailable = QuantizeMinAvailableForGroups(config.MinAvailable, groups, size)

	// add the finalizer before the state-change guard so an out-of-band removal is always restored
	if !controllerutil.ContainsFinalizer(lws, FinalizerPDBCleanup) {
		patch := client.MergeFrom(lws.DeepCopy())
		controllerutil.AddFinalizer(lws, FinalizerPDBCleanup)
		if err := r.Patch(ctx, lws, patch); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to add finalizer", map[string]any{})
			return ctrl.Result{}, err
		}
		logger.Info("Added finalizer for PDB cleanup", map[string]any{})
		return ctrl.Result{Requeue: true}, nil
	}

	stateChanged, err := r.tracker.HasStateChanged(ctx, r.Client, w, config)
	if err != nil {
		reconcileErr = err
		logger.Error(err, "Failed to check leaderworkerset state changes, proceeding with reconciliation", map[string]any{})
		stateChanged = true
	}

	if !stateChanged {
		logger.Info("Skipping reconciliation - no state change detected", map[string]any{
			"availabilityClass": string(config.AvailabilityClass),
			"source":            config.Source,
			"policyName":        config.PolicyName,
			"reason":            "no_state_change",
			"optimized":         true,
		})
		tracing.AddEvent(ctx, "SkippedPDB", attribute.String("reason", "no_state_change"))
		return ctrl.Result{}, nil
	}

	logger.Info("Using availability configuration", map[string]any{
		"availabilityClass": string(config.AvailabilityClass),
		"source":            config.Source,
		"policyName":        config.PolicyName,
		"groups":            groups,
		"groupSize":         size,
		"minAvailable":      config.MinAvailable.String(),
	})

	span.SetAttributes(
		attribute.String("config.source", config.Source),
		attribute.String("config.policy_name", config.PolicyName),
	)

	tracing.AddEvent(ctx, "ReconcilingPDB",
		attribute.String("availability_class", string(config.AvailabilityClass)),
		attribute.String("source", config.Source),
	)

	if IsInMaintenanceWindow(config, r.now()) {
		logger.Info("In maintenance window, temporarily relaxing PDB", map[string]any{
			"maintenanceWindow": config.MaintenanceWindow,
		})
		tracing.AddEvent(ctx, "MaintenanceWindowActive",
			attribute.String("maintenance_window", config.MaintenanceWindow),
		)
		if err := RemovePDBTemporarily(ctx, r.Client, w, log.FromContext(ctx)); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to relax PDB for maintenance window", map[string]any{})
			return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// never adopt a PDB owned by another kind (e.g. the leader StatefulSet's, pending cleanup)
	existingPDB := &policyv1.PodDisruptionBudget{}
	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}
	if err := r.Get(ctx, pdbKey, existingPDB); err == nil {
		if ref := metav1.GetControllerOf(existingPDB); ref != nil && ref.Kind != kindLeaderWorkerSet {
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

	metrics.ManagedLeaderWorkerSets.WithLabelValues(
		w.GetNamespace(),
		string(config.AvailabilityClass),
	).Set(1)

	metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), true, "managed")
	if err := r.tracker.UpdateState(ctx, r.Client, w, config); err != nil {
		logger.Error(err, "Failed to update leaderworkerset state cache", map[string]any{})
	}

	logger.Info("Successfully reconciled PDB", map[string]any{
		"availabilityClass": config.AvailabilityClass,
		"source":            config.Source,
		"reconcileID":       reconcileID,
	})

	// wake up proactively when a maintenance window is due to open
	return applyMaintenanceRequeue(result, config, r.now()), nil
}

// SetupWithManager sets up the LeaderWorkerSet controller with the Manager.
func (r *LeaderWorkerSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := 3
	if r.Config != nil && r.Config.MaxConcurrentReconciles > 0 {
		maxConcurrent = r.Config.MaxConcurrentReconciles
	}
	return r.SetupWithManagerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: maxConcurrent,
	})
}

// SetupWithManagerWithOptions sets up the controller with custom options.
func (r *LeaderWorkerSetReconciler) SetupWithManagerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	r.tracker = NewWorkloadStateTracker()

	if r.Recorder == nil && mgr != nil {
		r.Recorder = mgr.GetEventRecorder("leaderworkerset-pdb-controller")
	}
	if r.Events == nil && r.Recorder != nil {
		r.Events = events.NewEventRecorder(r.Recorder)
	}

	lwsPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return isLWSObject(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !isLWSObject(e.ObjectOld) || !isLWSObject(e.ObjectNew) {
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

	pdbPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			pdb, ok := e.ObjectNew.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			if labels := pdb.GetLabels(); labels != nil {
				if labels[LabelManagedBy] == OperatorName {
					oldPDB := e.ObjectOld.(*policyv1.PodDisruptionBudget)
					return !isPDBUpdateByUs(oldPDB, pdb)
				}
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			pdb, ok := e.Object.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			if labels := pdb.GetLabels(); labels != nil {
				return labels[LabelManagedBy] == OperatorName
			}
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}

	pdbToLWSHandler := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		pdb, ok := obj.(*policyv1.PodDisruptionBudget)
		if !ok {
			return nil
		}
		if labels := pdb.GetLabels(); labels == nil || labels[LabelManagedBy] != OperatorName {
			return nil
		}
		// only enqueue for PDBs actually owned by a LeaderWorkerSet, else other kinds double reconcile traffic
		ownerRef := metav1.GetControllerOf(pdb)
		if ownerRef == nil || ownerRef.Kind != kindLeaderWorkerSet {
			return nil
		}
		return []ctrl.Request{{
			NamespacedName: types.NamespacedName{Name: ownerRef.Name, Namespace: pdb.Namespace},
		}}
	})

	policyHandler := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		policy, ok := obj.(*availabilityv1alpha1.PDBPolicy)
		if !ok {
			return nil
		}
		logger := log.FromContext(ctx)
		lwsList := &unstructured.UnstructuredList{}
		lwsList.SetGroupVersionKind(LWSGVK.GroupVersion().WithKind(kindLeaderWorkerSet + "List"))
		if err := r.List(ctx, lwsList); err != nil {
			logger.Error(err, "Failed to list LeaderWorkerSets for policy change")
			return nil
		}
		var requests []ctrl.Request
		for i := range lwsList.Items {
			if PolicyMatchesWorkload(policy, &lwsWorkload{&lwsList.Items[i]}) {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{Name: lwsList.Items[i].GetName(), Namespace: lwsList.Items[i].GetNamespace()},
				})
			}
		}
		logger.V(2).Info("Policy change affects LeaderWorkerSets",
			"policy", policy.Name, "affectedLeaderWorkerSets", len(requests))
		return requests
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("leaderworkerset-pdb").
		For(NewLWSObject(), builder.WithPredicates(lwsPredicate)).
		Watches(&policyv1.PodDisruptionBudget{}, pdbToLWSHandler, builder.WithPredicates(pdbPredicate)).
		Watches(&availabilityv1alpha1.PDBPolicy{}, policyHandler).
		WithOptions(opts).
		Complete(r)
}

func isLWSObject(obj client.Object) bool {
	u, ok := obj.(*unstructured.Unstructured)
	return ok && u.GroupVersionKind() == LWSGVK
}
