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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/go-logr/logr"
	availabilityv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
	"github.com/pdb-operator/pdb-operator/internal/cache"
	"github.com/pdb-operator/pdb-operator/internal/events"
	"github.com/pdb-operator/pdb-operator/internal/logging"
	"github.com/pdb-operator/pdb-operator/internal/metrics"
	"github.com/pdb-operator/pdb-operator/internal/tracing"
)

// StatefulSetReconciler reconciles a StatefulSet object
type StatefulSetReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    k8sevents.EventRecorder
	Events      *events.EventRecorder
	PolicyCache *cache.PolicyCache
	Config      *SharedConfig

	// Change detection to avoid unnecessary reconciliations
	lastStatefulSetState map[types.NamespacedName]string
	mu                   sync.RWMutex
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=availability.pdboperator.io,resources=pdbpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile handles StatefulSet changes and manages corresponding PDBs
func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()

	ctx, span := tracing.ReconcileSpan(ctx, "statefulset", req.Namespace, req.Name)
	defer span.End()

	reconcileID := "statefulset-" + uuid.New().String()
	correlationID := uuid.New().String()

	span.SetAttributes(
		attribute.String("reconcile.id", reconcileID),
		attribute.String("correlation.id", correlationID),
	)

	logger := logging.CreateUnifiedLogger(ctx,
		"statefulset-pdb",
		"statefulset-controller",
		"apps",
		"StatefulSet",
		"statefulset",
		req.Name,
		req.Namespace,
		reconcileID,
		correlationID,
	)

	var reconcileErr error
	defer func() {
		duration := time.Since(startTime)
		metrics.RecordReconciliation("statefulset", duration, reconcileErr)

		if reconcileErr != nil {
			tracing.RecordError(span, reconcileErr, "Reconciliation failed")
		}

		result := logging.AuditResultSuccess
		if reconcileErr != nil {
			result = logging.AuditResultFailure
		}
		logger.Audit(
			"RECONCILE",
			fmt.Sprintf("%s/%s", req.Namespace, req.Name),
			"statefulset",
			req.Namespace,
			req.Name,
			result,
			map[string]interface{}{
				"controller": "statefulset",
				"duration":   time.Since(startTime).String(),
				"durationMs": time.Since(startTime).Milliseconds(),
			},
		)

		logger.Info("Reconciliation completed", map[string]any{
			"duration": time.Since(startTime).String(),
		})
	}()

	tracing.AddEvent(ctx, "FetchingStatefulSet",
		attribute.String("reconcile.id", reconcileID),
	)

	// Fetch the StatefulSet
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, sts); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("StatefulSet not found, ignoring since object must be deleted", map[string]any{})
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		logger.Error(err, "Failed to get StatefulSet", map[string]any{})
		return ctrl.Result{}, err
	}

	// Handle deletion
	if sts.DeletionTimestamp != nil {
		logger.Info("StatefulSet is being deleted", map[string]any{})
		r.clearStatefulSetState(sts)
		tracing.AddEvent(ctx, "DeletingPDB")
		if err := r.handleDeletion(ctx, sts, logger.ToLogr()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check replicas — StatefulSets with fewer than 2 replicas don't need a PDB
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas < 2 {
		logger.Info("Single replica, no PDB required", map[string]any{
			"replicas": replicas,
		})

		tracing.AddEvent(ctx, "SkippedPDB",
			attribute.String("reason", "insufficient_replicas"),
			attribute.Int("replicas", int(replicas)),
		)

		if r.Events != nil {
			r.Events.StatefulSetSkipped(sts, sts.Name, "insufficient replicas")
		}

		metrics.UpdateComplianceStatus(
			sts.Namespace,
			sts.Name,
			false,
			"insufficient_replicas",
		)

		return ctrl.Result{}, nil
	}

	tracing.AddEvent(ctx, "EvaluatingPolicies")

	config, err := r.getAvailabilityConfigWithCache(ctx, sts, logger.ToLogr())
	if err != nil {
		reconcileErr = err
		logger.Error(err, "Failed to get availability configuration", map[string]any{})
		return ctrl.Result{}, err
	}

	if config == nil {
		logger.Info("No availability configuration found, skipping PDB", map[string]any{})

		tracing.AddEvent(ctx, "SkippedPDB",
			attribute.String("reason", "no_availability_configuration"),
		)

		if r.Events != nil {
			r.Events.StatefulSetUnmanaged(sts, sts.Name, "no availability configuration")
		}

		return ctrl.Result{}, nil
	}

	stateChanged, err := r.hasStatefulSetStateChanged(ctx, sts, config)
	if err != nil {
		logger.Error(err, "Failed to check StatefulSet state changes, proceeding with reconciliation", map[string]any{})
		stateChanged = true
	}

	logger.Info("Change detection result", map[string]any{
		"stateChanged": stateChanged,
		"statefulset":  sts.Name,
		"namespace":    sts.Namespace,
		"generation":   sts.Generation,
		"debug":        true,
	})

	pdbName := types.NamespacedName{
		Name:      sts.Name + DefaultPDBSuffix,
		Namespace: sts.Namespace,
	}
	currentPDB := &policyv1.PodDisruptionBudget{}
	pdbExists := r.Get(ctx, pdbName, currentPDB) == nil

	logger.Info("PDB existence check", map[string]any{
		"pdbName":   pdbName.Name,
		"pdbExists": pdbExists,
		"debug":     true,
	})

	if !stateChanged {
		logger.Info("Skipping reconciliation - no state change detected", map[string]any{
			"availabilityClass": string(config.AvailabilityClass),
			"source":            config.Source,
			"policyName":        config.PolicyName,
			"reason":            "no_state_change",
			"optimized":         true,
		})

		tracing.AddEvent(ctx, "SkippedPDB",
			attribute.String("reason", "no_state_change"),
		)

		return ctrl.Result{}, nil
	}

	logger.Info("Using availability configuration", map[string]any{
		"availabilityClass": string(config.AvailabilityClass),
		"source":            config.Source,
		"policyName":        config.PolicyName,
		"stateChanged":      stateChanged,
	})

	span.SetAttributes(
		attribute.String("config.source", config.Source),
		attribute.String("config.policy_name", config.PolicyName),
	)

	tracing.AddEvent(ctx, "ReconcilingPDB",
		attribute.String("availability_class", string(config.AvailabilityClass)),
		attribute.String("source", config.Source),
	)

	// Ensure finalizer is present for cleanup
	if !controllerutil.ContainsFinalizer(sts, FinalizerPDBCleanup) {
		patch := client.MergeFrom(sts.DeepCopy())
		controllerutil.AddFinalizer(sts, FinalizerPDBCleanup)
		if err := r.Patch(ctx, sts, patch); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to add finalizer", map[string]any{})
			return ctrl.Result{}, err
		}
		logger.Info("Added finalizer for PDB cleanup", map[string]any{})
		return ctrl.Result{Requeue: true}, nil
	}

	if r.isInMaintenanceWindow(config, log.FromContext(ctx)) {
		logger.Info("In maintenance window, temporarily relaxing PDB", map[string]any{
			"maintenanceWindow": config.MaintenanceWindow,
		})
		tracing.AddEvent(ctx, "MaintenanceWindowActive",
			attribute.String("maintenance_window", config.MaintenanceWindow),
		)
		if err := r.removePDBTemporarily(ctx, sts, log.FromContext(ctx)); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to relax PDB for maintenance window", map[string]any{})
			return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	result, err := r.reconcilePDB(ctx, sts, config, log.FromContext(ctx))
	if err != nil {
		reconcileErr = err
		logger.Error(err, "PDB reconciliation failed, will retry", map[string]any{})
		return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
	}

	metrics.ManagedDeployments.WithLabelValues(
		sts.Namespace,
		string(config.AvailabilityClass),
	).Set(1)

	metrics.UpdateComplianceStatus(
		sts.Namespace,
		sts.Name,
		true,
		"managed",
	)

	if err := r.updateStatefulSetState(ctx, sts, config); err != nil {
		logger.Error(err, "Failed to update StatefulSet state cache", map[string]any{})
	}

	logger.Info("Successfully reconciled PDB", map[string]any{
		"availabilityClass": config.AvailabilityClass,
		"source":            config.Source,
		"reconcileID":       reconcileID,
	})

	return result, nil
}

// getAvailabilityConfigWithCache gets configuration using annotation and policy sources
func (r *StatefulSetReconciler) getAvailabilityConfigWithCache(ctx context.Context, sts *appsv1.StatefulSet, logger logr.Logger) (*AvailabilityConfig, error) {
	if invalidClass := r.detectInvalidConfiguration(sts); invalidClass != "" {
		err := fmt.Errorf("invalid availability class: %s", invalidClass)
		logger.Error(err, "Invalid configuration detected", "class", invalidClass)

		if r.Events != nil {
			r.Events.InvalidConfiguration(sts, fmt.Sprintf("invalid availability class: %s", invalidClass))
		}

		return nil, err
	}

	annotationConfig := r.getConfigFromAnnotations(sts, logger)
	policyConfig, matchedPolicy, err := r.getConfigFromPolicyWithCacheAndPolicy(ctx, sts, logger)
	if err != nil {
		return nil, err
	}

	finalConfig := r.resolveConfiguration(ctx, sts, annotationConfig, policyConfig, matchedPolicy, logger)
	if finalConfig == nil {
		logger.V(1).Info("No availability configuration found")
		return nil, nil
	}

	return finalConfig, nil
}

// detectInvalidConfiguration checks for invalid availability class annotations
func (r *StatefulSetReconciler) detectInvalidConfiguration(sts *appsv1.StatefulSet) string {
	if sts.Annotations == nil {
		return ""
	}
	if class, exists := sts.Annotations[AnnotationAvailabilityClass]; exists {
		if !r.isValidAvailabilityClass(availabilityv1alpha1.AvailabilityClass(class)) {
			return class
		}
	}
	return ""
}

// getConfigFromAnnotations extracts availability configuration from StatefulSet annotations
func (r *StatefulSetReconciler) getConfigFromAnnotations(sts *appsv1.StatefulSet, logger logr.Logger) *AvailabilityConfig {
	if sts.Annotations == nil {
		return nil
	}

	availabilityClass, exists := sts.Annotations[AnnotationAvailabilityClass]
	if !exists {
		return nil
	}

	odaClass := availabilityv1alpha1.AvailabilityClass(availabilityClass)
	if !r.isValidAvailabilityClass(odaClass) {
		logger.Error(fmt.Errorf("invalid availability class"),
			"Unsupported availability class", "class", availabilityClass)
		return nil
	}

	componentFunction := availabilityv1alpha1.WorkloadFunction(sts.Annotations[AnnotationWorkloadFunction])
	if componentFunction == "" {
		componentFunction = availabilityv1alpha1.CoreFunction
	}

	minAvailable := availabilityv1alpha1.GetMinAvailableForClass(odaClass, componentFunction)

	return &AvailabilityConfig{
		AvailabilityClass: odaClass,
		WorkloadFunction:  componentFunction,
		MinAvailable:      minAvailable,
		Description:       r.getDescriptionForClass(odaClass),
		MaintenanceWindow: sts.Annotations[AnnotationMaintenanceWindow],
		Source:            "oda-annotation",
	}
}

// getConfigFromPolicyWithCacheAndPolicy finds the best matching PDBPolicy for this StatefulSet
func (r *StatefulSetReconciler) getConfigFromPolicyWithCacheAndPolicy(ctx context.Context, sts *appsv1.StatefulSet, logger logr.Logger) (*AvailabilityConfig, *availabilityv1alpha1.PDBPolicy, error) {
	cacheKey := "all-policies"

	var policies []availabilityv1alpha1.PDBPolicy
	if r.PolicyCache != nil {
		if cachedPolicies, found := r.PolicyCache.GetList(cacheKey); found {
			logger.V(2).Info("Using cached policies", "cacheHit", true)
			metrics.IncrementCacheHit("policy")
			policies = cachedPolicies
		} else {
			logger.V(2).Info("Policy cache miss, fetching from API")
			metrics.IncrementCacheMiss("policy")

			policyList := &availabilityv1alpha1.PDBPolicyList{}
			if err := r.List(ctx, policyList); err != nil {
				return nil, nil, err
			}

			policies = policyList.Items
			r.PolicyCache.SetList(cacheKey, policies)
		}
	} else {
		policyList := &availabilityv1alpha1.PDBPolicyList{}
		if err := r.List(ctx, policyList); err != nil {
			return nil, nil, err
		}
		policies = policyList.Items
	}

	var bestMatch *availabilityv1alpha1.PDBPolicy
	var highestPriority int32 = -1
	var matchingPolicies []*availabilityv1alpha1.PDBPolicy

	for i := range policies {
		policy := &policies[i]
		if r.policyMatchesStatefulSet(policy, sts) {
			matchingPolicies = append(matchingPolicies, policy)
			if policy.Spec.Priority > highestPriority {
				bestMatch = policy
				highestPriority = policy.Spec.Priority
			} else if policy.Spec.Priority == highestPriority && bestMatch != nil {
				currentKey := fmt.Sprintf("%s/%s", bestMatch.Namespace, bestMatch.Name)
				candidateKey := fmt.Sprintf("%s/%s", policy.Namespace, policy.Name)
				if candidateKey < currentKey {
					bestMatch = policy
				}
			}
		}
	}

	if bestMatch == nil {
		return nil, nil, nil
	}

	if len(matchingPolicies) > 1 {
		r.logPolicyConflicts(ctx, sts, matchingPolicies, bestMatch, logger)
	}

	logger.Info("Found matching PDBPolicy",
		"policy", bestMatch.Name,
		"priority", bestMatch.Spec.Priority,
		"enforcement", bestMatch.Spec.GetEnforcement(),
		"fromCache", r.PolicyCache != nil,
		"totalMatchingPolicies", len(matchingPolicies))

	componentFunction := r.inferWorkloadFunction(sts)
	minAvailable := availabilityv1alpha1.GetMinAvailableForClass(bestMatch.Spec.AvailabilityClass, componentFunction)

	if bestMatch.Spec.AvailabilityClass == availabilityv1alpha1.Custom && bestMatch.Spec.CustomPDBConfig != nil {
		if bestMatch.Spec.CustomPDBConfig.MinAvailable != nil {
			minAvailable = *bestMatch.Spec.CustomPDBConfig.MinAvailable
		}
	}

	config := &AvailabilityConfig{
		AvailabilityClass: bestMatch.Spec.AvailabilityClass,
		WorkloadFunction:  componentFunction,
		MinAvailable:      minAvailable,
		Description:       r.getDescriptionForClass(bestMatch.Spec.AvailabilityClass),
		Source:            "policy",
		PolicyName:        bestMatch.Name,
		Enforcement:       string(bestMatch.Spec.GetEnforcement()),
	}

	return config, bestMatch, nil
}

// policyMatchesStatefulSet checks if a policy matches a StatefulSet
func (r *StatefulSetReconciler) policyMatchesStatefulSet(policy *availabilityv1alpha1.PDBPolicy, sts *appsv1.StatefulSet) bool {
	selector := policy.Spec.WorkloadSelector

	// Check namespace
	if len(selector.Namespaces) > 0 {
		namespaceMatch := false
		for _, ns := range selector.Namespaces {
			if ns == sts.Namespace {
				namespaceMatch = true
				break
			}
		}
		if !namespaceMatch {
			return false
		}
	}

	// Check workload names
	if len(selector.WorkloadNames) > 0 {
		workloadName := ""
		if sts.Annotations != nil {
			workloadName = sts.Annotations[AnnotationWorkloadName]
		}
		if workloadName == "" {
			workloadName = sts.Name
		}

		nameMatch := false
		for _, name := range selector.WorkloadNames {
			if name == workloadName {
				nameMatch = true
				break
			}
		}
		if !nameMatch {
			return false
		}
	}

	// Check workload functions
	if len(selector.WorkloadFunctions) > 0 {
		stsFunction := availabilityv1alpha1.CoreFunction
		if sts.Annotations != nil {
			if function := sts.Annotations[AnnotationWorkloadFunction]; function != "" {
				stsFunction = availabilityv1alpha1.WorkloadFunction(function)
			}
		}

		functionMatch := false
		for _, function := range selector.WorkloadFunctions {
			if function == stsFunction {
				functionMatch = true
				break
			}
		}
		if !functionMatch {
			return false
		}
	}

	// Check match labels
	if len(selector.MatchLabels) > 0 {
		if sts.Labels == nil {
			return false
		}
		for key, value := range selector.MatchLabels {
			if sts.Labels[key] != value {
				return false
			}
		}
	}

	// Check match expressions
	if len(selector.MatchExpressions) > 0 {
		for _, expr := range selector.MatchExpressions {
			if !r.evaluateMatchExpression(expr, sts.Labels) {
				return false
			}
		}
	}

	return true
}

// evaluateMatchExpression evaluates a single match expression against labels
func (r *StatefulSetReconciler) evaluateMatchExpression(expr metav1.LabelSelectorRequirement, labels map[string]string) bool {
	value, exists := labels[expr.Key]

	switch expr.Operator {
	case metav1.LabelSelectorOpIn:
		if !exists {
			return false
		}
		for _, v := range expr.Values {
			if value == v {
				return true
			}
		}
		return false
	case metav1.LabelSelectorOpNotIn:
		if !exists {
			return true
		}
		for _, v := range expr.Values {
			if value == v {
				return false
			}
		}
		return true
	case metav1.LabelSelectorOpExists:
		return exists
	case metav1.LabelSelectorOpDoesNotExist:
		return !exists
	default:
		return false
	}
}

// resolveConfiguration implements enforcement priority logic for StatefulSets
func (r *StatefulSetReconciler) resolveConfiguration(
	_ context.Context,
	sts *appsv1.StatefulSet,
	annotationConfig *AvailabilityConfig,
	policyConfig *AvailabilityConfig,
	policy *availabilityv1alpha1.PDBPolicy,
	logger logr.Logger,
) *AvailabilityConfig {
	if policyConfig == nil {
		if annotationConfig != nil {
			annotationConfig.Source = "annotation-no-policy"
		}
		return annotationConfig
	}

	enforcement := policy.Spec.GetEnforcement()

	logger.Info("Resolving configuration",
		"hasAnnotation", annotationConfig != nil,
		"hasPolicy", true,
		"enforcement", enforcement,
		"policyClass", policyConfig.AvailabilityClass,
		"annotationClass", func() string {
			if annotationConfig != nil {
				return string(annotationConfig.AvailabilityClass)
			}
			return "none"
		}())

	metrics.RecordEnforcementDecision(string(enforcement), "processing", sts.Namespace)

	switch enforcement {
	case availabilityv1alpha1.EnforcementStrict:
		if annotationConfig != nil {
			r.Events.Warnf(sts, "PolicyEnforced",
				"Annotation override blocked by strict policy %s", policy.Name)
			metrics.RecordOverrideAttempt("blocked", "strict-enforcement", sts.Namespace)
		}
		policyConfig.Source = "policy-strict"
		metrics.RecordEnforcementDecision(string(enforcement), "policy-wins", sts.Namespace)
		return policyConfig

	case availabilityv1alpha1.EnforcementFlexible:
		if annotationConfig == nil {
			policyConfig.Source = "policy-flexible"
			metrics.RecordEnforcementDecision(string(enforcement), "policy-no-annotation", sts.Namespace)
			return policyConfig
		}

		minimumClass := policy.Spec.MinimumClass
		if minimumClass == "" {
			minimumClass = policy.Spec.AvailabilityClass
		}

		comparison := availabilityv1alpha1.CompareAvailabilityClasses(
			annotationConfig.AvailabilityClass, minimumClass)

		if comparison >= 0 {
			metrics.RecordOverrideAttempt("accepted", "meets-minimum", sts.Namespace)
			metrics.RecordEnforcementDecision(string(enforcement), "annotation-accepted", sts.Namespace)
			annotationConfig.Source = "annotation-flexible"
			annotationConfig.PolicyName = policy.Name
			annotationConfig.Enforcement = string(enforcement)
			return annotationConfig
		}

		metrics.RecordOverrideAttempt("rejected", "below-minimum", sts.Namespace)
		metrics.RecordEnforcementDecision(string(enforcement), "minimum-enforced", sts.Namespace)
		policyConfig.AvailabilityClass = minimumClass
		policyConfig.MinAvailable = availabilityv1alpha1.GetMinAvailableForClass(
			minimumClass, policyConfig.WorkloadFunction)
		policyConfig.Source = "policy-flexible-minimum"
		return policyConfig

	case availabilityv1alpha1.EnforcementAdvisory:
		if annotationConfig != nil {
			if policy.Spec.AllowOverride != nil && !*policy.Spec.AllowOverride {
				metrics.RecordOverrideAttempt("blocked", "override-not-allowed", sts.Namespace)
				policyConfig.Source = "policy-advisory-no-override"
				return policyConfig
			}

			if policy.Spec.OverrideRequiresAnnotation != "" {
				if sts.Annotations == nil ||
					sts.Annotations[policy.Spec.OverrideRequiresAnnotation] == "" {
					metrics.RecordOverrideAttempt("blocked", "missing-required-annotation", sts.Namespace)
					policyConfig.Source = "policy-advisory-missing-annotation"
					return policyConfig
				}
			}

			if policy.Spec.OverrideRequiresReason != nil && *policy.Spec.OverrideRequiresReason {
				if sts.Annotations == nil || sts.Annotations[AnnotationOverrideReason] == "" {
					metrics.RecordOverrideAttempt("blocked", "missing-reason", sts.Namespace)
					policyConfig.Source = "policy-advisory-missing-reason"
					return policyConfig
				}
			}

			metrics.RecordOverrideAttempt("accepted", "advisory-mode", sts.Namespace)
			metrics.RecordEnforcementDecision(string(enforcement), "annotation-wins", sts.Namespace)
			annotationConfig.Source = "annotation-advisory"
			annotationConfig.PolicyName = policy.Name
			annotationConfig.Enforcement = string(enforcement)
			return annotationConfig
		}

		policyConfig.Source = "policy-advisory"
		metrics.RecordEnforcementDecision(string(enforcement), "policy-no-annotation", sts.Namespace)
		return policyConfig

	default:
		policyConfig.Source = "policy-default"
		metrics.RecordEnforcementDecision("unknown", "defaulting-to-policy", sts.Namespace)
		return policyConfig
	}
}

// logPolicyConflicts logs when multiple policies match a StatefulSet
func (r *StatefulSetReconciler) logPolicyConflicts(
	_ context.Context,
	sts *appsv1.StatefulSet,
	matchingPolicies []*availabilityv1alpha1.PDBPolicy,
	selected *availabilityv1alpha1.PDBPolicy,
	logger logr.Logger,
) {
	samePriorityCount := 0
	policyNames := make([]string, 0, len(matchingPolicies))
	for _, p := range matchingPolicies {
		policyNames = append(policyNames, fmt.Sprintf("%s/%s", p.Namespace, p.Name))
		if p.Spec.Priority == selected.Spec.Priority {
			samePriorityCount++
		}
	}

	metrics.RecordMultiPolicyMatch(sts.Namespace, len(matchingPolicies))

	if samePriorityCount > 1 {
		metrics.RecordPolicyTieBreak(sts.Namespace, "alphabetical")
		logger.Info("Multiple policies match StatefulSet at same priority, using tie-break",
			"statefulset", sts.Name,
			"matchingPolicies", policyNames,
			"selectedPolicy", fmt.Sprintf("%s/%s", selected.Namespace, selected.Name),
			"priority", selected.Spec.Priority,
			"tieBreakReason", "alphabetical",
		)
		if r.Events != nil {
			r.Events.Warnf(sts, "PolicyConflict",
				"Multiple policies (%d) match at priority %d, selected %s via alphabetical tie-break",
				samePriorityCount, selected.Spec.Priority, selected.Name)
		}
	} else {
		logger.V(1).Info("Multiple policies match StatefulSet, selected by priority",
			"statefulset", sts.Name,
			"matchingPolicies", policyNames,
			"selectedPolicy", fmt.Sprintf("%s/%s", selected.Namespace, selected.Name),
			"selectedPriority", selected.Spec.Priority,
		)
	}
}

// handleDeletion handles the deletion of a StatefulSet
func (r *StatefulSetReconciler) handleDeletion(ctx context.Context, sts *appsv1.StatefulSet, logger logr.Logger) error {
	logger.Info("Handling StatefulSet deletion")

	if err := r.cleanupPDB(ctx, sts, logger); err != nil {
		logger.Error(err, "Failed to cleanup PDB during deletion")
		return err
	}

	if err := r.removeFinalizer(ctx, sts, logger); err != nil {
		logger.Error(err, "Failed to remove finalizer during deletion")
		return err
	}

	return nil
}

// removeFinalizer removes the PDB cleanup finalizer from a StatefulSet
func (r *StatefulSetReconciler) removeFinalizer(ctx context.Context, sts *appsv1.StatefulSet, logger logr.Logger) error {
	if !controllerutil.ContainsFinalizer(sts, FinalizerPDBCleanup) {
		return nil
	}

	patch := client.MergeFrom(sts.DeepCopy())
	controllerutil.RemoveFinalizer(sts, FinalizerPDBCleanup)

	if err := r.Patch(ctx, sts, patch); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		if errors.IsConflict(err) {
			latest := &appsv1.StatefulSet{}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      sts.Name,
				Namespace: sts.Namespace,
			}, latest); err != nil {
				return err
			}
			patch := client.MergeFrom(latest.DeepCopy())
			controllerutil.RemoveFinalizer(latest, FinalizerPDBCleanup)
			return r.Patch(ctx, latest, patch)
		}
		return err
	}

	logger.Info("Successfully removed finalizer", "finalizer", FinalizerPDBCleanup)
	return nil
}

// cleanupPDB removes the PDB associated with a StatefulSet
func (r *StatefulSetReconciler) cleanupPDB(ctx context.Context, sts *appsv1.StatefulSet, logger logr.Logger) error {
	pdbName := types.NamespacedName{
		Name:      sts.Name + DefaultPDBSuffix,
		Namespace: sts.Namespace,
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, pdbName, pdb); err != nil {
		if errors.IsNotFound(err) {
			logger.V(1).Info("PDB doesn't exist, nothing to clean up")
			return nil
		}
		return err
	}

	if ownerRef := metav1.GetControllerOf(pdb); ownerRef != nil &&
		ownerRef.Kind == "StatefulSet" && ownerRef.Name == sts.Name {
		if err := r.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
			return err
		}

		metrics.RecordPDBDeleted(sts.Namespace, "statefulset_deleted")
		r.Events.PDBDeleted(sts, sts.Name, pdbName.Name, "statefulset_deleted")

		metrics.UpdateComplianceStatus(
			sts.Namespace,
			sts.Name,
			true,
			"deleted",
		)

		logger.Info("Successfully deleted PDB", "pdb", pdbName.Name)
	}

	return nil
}

// reconcilePDB creates or updates the PDB for a StatefulSet
func (r *StatefulSetReconciler) reconcilePDB(ctx context.Context, sts *appsv1.StatefulSet, config *AvailabilityConfig, logger logr.Logger) (ctrl.Result, error) {
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas <= 1 {
		logger.V(1).Info("StatefulSet has 1 or fewer replicas, skipping PDB creation",
			"replicas", *sts.Spec.Replicas)

		r.Events.StatefulSetSkipped(sts, sts.Name, "insufficient replicas")
		metrics.UpdateComplianceStatus(sts.Namespace, sts.Name, false, "insufficient_replicas")

		if err := r.cleanupPDB(ctx, sts, logger); err != nil {
			logger.Error(err, "Failed to cleanup PDB for single replica StatefulSet")
		}
		return ctrl.Result{}, nil
	}

	pdbName := types.NamespacedName{
		Name:      sts.Name + DefaultPDBSuffix,
		Namespace: sts.Namespace,
	}

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(ctx, pdbName, pdb)

	if errors.IsNotFound(err) {
		return r.createPDB(ctx, pdbName, config, sts, logger)
	} else if err != nil {
		logger.Error(err, "Failed to get PDB")
		return ctrl.Result{}, err
	}

	return r.updatePDB(ctx, pdb, config, sts, logger)
}

// createPDB creates a new PDB for the StatefulSet
func (r *StatefulSetReconciler) createPDB(ctx context.Context, pdbName types.NamespacedName, config *AvailabilityConfig, sts *appsv1.StatefulSet, _ logr.Logger) (ctrl.Result, error) {
	structuredLogger := logging.NewStructuredLogger(ctx)
	structuredLogger.Info("Creating PDB", map[string]any{
		"name":              pdbName.Name,
		"availabilityClass": config.AvailabilityClass,
		"minAvailable":      config.MinAvailable.String(),
		"source":            config.Source,
		"policy":            config.PolicyName,
	})

	labels := map[string]string{
		LabelManagedBy:         OperatorName,
		LabelAvailabilityClass: string(config.AvailabilityClass),
		LabelWorkloadFunction:  string(config.WorkloadFunction),
	}
	if sts.Annotations != nil && sts.Annotations[AnnotationWorkloadName] != "" {
		labels[LabelWorkload] = sts.Annotations[AnnotationWorkloadName]
	}
	for k, v := range labels {
		if v == "" {
			delete(labels, k)
		}
	}

	annotations := map[string]string{
		AnnotationCreatedBy:         OperatorName,
		AnnotationCreationTime:      time.Now().Format(time.RFC3339),
		AnnotationAvailabilityClass: string(config.AvailabilityClass),
	}
	if config.Description != "" {
		annotations[AnnotationDescription] = config.Description
	}
	if config.PolicyName != "" {
		annotations[AnnotationPolicySource] = config.PolicyName
		annotations[AnnotationEnforcement] = config.Enforcement
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pdbName.Name,
			Namespace:   pdbName.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &config.MinAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: sts.Spec.Selector.MatchLabels,
			},
		},
	}

	// Owner reference points to the StatefulSet
	if err := ctrl.SetControllerReference(sts, pdb, r.Scheme); err != nil {
		structuredLogger.Error(err, "Failed to set controller reference", nil)
		return ctrl.Result{}, err
	}

	if createErr := r.Create(ctx, pdb); createErr != nil {
		structuredLogger.Error(createErr, "Failed to create PDB", nil)

		if err := r.Get(ctx, client.ObjectKeyFromObject(sts), sts); err == nil {
			r.Events.PDBCreationFailed(sts, sts.Name, createErr)
		}

		metrics.UpdateComplianceStatus(sts.Namespace, sts.Name, false, "creation_failed")
		return ctrl.Result{}, createErr
	}

	metrics.RecordPDBCreated(sts.Namespace, string(config.AvailabilityClass), string(config.WorkloadFunction))

	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), sts); err == nil {
		r.Events.PDBCreated(sts, sts.Name, pdbName.Name, string(config.AvailabilityClass), config.MinAvailable.String())
	}

	auditLogger := logging.NewStructuredLogger(ctx)
	auditLogger.AuditStructured(
		"CREATE",
		pdbName.Name,
		"PodDisruptionBudget",
		sts.Namespace,
		sts.Name,
		logging.AuditResultSuccess,
		map[string]interface{}{
			"availabilityClass": config.AvailabilityClass,
			"minAvailable":      config.MinAvailable.String(),
			"source":            config.Source,
			"policy":            config.PolicyName,
			"enforcement":       config.Enforcement,
		},
	)

	metrics.UpdateComplianceStatus(sts.Namespace, sts.Name, true, "created")
	structuredLogger.Info("Successfully created PDB", map[string]any{"name": pdbName.Name})
	return ctrl.Result{}, nil
}

// updatePDB updates an existing PDB if configuration has changed
func (r *StatefulSetReconciler) updatePDB(ctx context.Context, pdb *policyv1.PodDisruptionBudget, config *AvailabilityConfig, sts *appsv1.StatefulSet, _ logr.Logger) (ctrl.Result, error) {
	oldMinAvailable := ""
	if pdb.Spec.MinAvailable != nil {
		oldMinAvailable = pdb.Spec.MinAvailable.String()
	}

	structuredLogger := logging.NewStructuredLogger(ctx)
	structuredLogger.Info("Updating PDB", map[string]any{
		"name":            pdb.Name,
		"oldMinAvailable": oldMinAvailable,
		"newMinAvailable": config.MinAvailable.String(),
	})

	needsUpdate := false
	patch := client.MergeFrom(pdb.DeepCopy())

	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.String() != config.MinAvailable.String() {
		pdb.Spec.MinAvailable = &config.MinAvailable
		needsUpdate = true
	}

	if pdb.Labels == nil {
		pdb.Labels = make(map[string]string)
	}
	if pdb.Labels[LabelAvailabilityClass] != string(config.AvailabilityClass) {
		pdb.Labels[LabelAvailabilityClass] = string(config.AvailabilityClass)
		needsUpdate = true
	}
	if pdb.Labels[LabelWorkloadFunction] != string(config.WorkloadFunction) {
		pdb.Labels[LabelWorkloadFunction] = string(config.WorkloadFunction)
		needsUpdate = true
	}

	if pdb.Annotations == nil {
		pdb.Annotations = make(map[string]string)
	}
	pdb.Annotations[AnnotationLastModified] = time.Now().Format(time.RFC3339)
	pdb.Annotations[AnnotationAvailabilityClass] = string(config.AvailabilityClass)

	if config.PolicyName != "" && pdb.Annotations[AnnotationPolicySource] != config.PolicyName {
		pdb.Annotations[AnnotationPolicySource] = config.PolicyName
		pdb.Annotations[AnnotationEnforcement] = config.Enforcement
		needsUpdate = true
	}

	if pdb.Annotations["pdboperator.io/maintenance-mode"] == "true" {
		delete(pdb.Annotations, "pdboperator.io/maintenance-mode")
		delete(pdb.Annotations, "pdboperator.io/maintenance-start")
		needsUpdate = true
	}

	if !selectorEquals(pdb.Spec.Selector, sts.Spec.Selector) {
		pdb.Spec.Selector = sts.Spec.Selector.DeepCopy()
		needsUpdate = true
	}

	if !needsUpdate {
		structuredLogger.Debug("PDB is up to date, no changes needed", nil)
		return ctrl.Result{}, nil
	}

	if err := r.Patch(ctx, pdb, patch); err != nil {
		structuredLogger.Error(err, "Failed to update PDB", nil)

		if err := r.Get(ctx, client.ObjectKeyFromObject(sts), sts); err == nil {
			r.Events.PDBUpdateFailed(sts, sts.Name, err)
		}

		metrics.UpdateComplianceStatus(sts.Namespace, sts.Name, false, "update_failed")
		return ctrl.Result{}, err
	}

	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), sts); err == nil {
		r.Events.PDBUpdated(sts, sts.Name, pdb.Name, oldMinAvailable, config.MinAvailable.String())
	}

	metrics.RecordPDBUpdated(sts.Namespace, "config_change")

	auditLogger := logging.NewStructuredLogger(ctx)
	auditLogger.AuditStructured(
		"UPDATE",
		pdb.Name,
		"PodDisruptionBudget",
		sts.Namespace,
		sts.Name,
		logging.AuditResultSuccess,
		map[string]interface{}{
			"oldMinAvailable": oldMinAvailable,
			"newMinAvailable": config.MinAvailable.String(),
			"source":          config.Source,
			"policy":          config.PolicyName,
			"enforcement":     config.Enforcement,
		},
	)

	structuredLogger.Info("Successfully updated PDB", map[string]any{"name": pdb.Name})
	return ctrl.Result{}, nil
}

// isInMaintenanceWindow checks if currently in a maintenance window
func (r *StatefulSetReconciler) isInMaintenanceWindow(config *AvailabilityConfig, _ logr.Logger) bool {
	if config.MaintenanceWindow == "" {
		return false
	}

	cacheKey := fmt.Sprintf("maintenance-window-%s", config.MaintenanceWindow)
	if r.PolicyCache != nil {
		if cachedResult, found := r.PolicyCache.GetMaintenanceWindow(cacheKey); found {
			return cachedResult
		}
	}

	parts := strings.Fields(config.MaintenanceWindow)
	if len(parts) < 1 {
		return false
	}

	timeRange := parts[0]
	timezone := "UTC"
	if len(parts) > 1 {
		timezone = parts[1]
	}

	timeParts := strings.Split(timeRange, "-")
	if len(timeParts) != 2 {
		return false
	}

	location, err := time.LoadLocation(timezone)
	if err != nil {
		return false
	}

	now := time.Now().In(location)
	today := now.Format("2006-01-02")

	todayStart, err := time.ParseInLocation("2006-01-02 15:04", fmt.Sprintf("%s %s", today, timeParts[0]), location)
	if err != nil {
		return false
	}

	todayEnd, err := time.ParseInLocation("2006-01-02 15:04", fmt.Sprintf("%s %s", today, timeParts[1]), location)
	if err != nil {
		return false
	}

	var result bool
	if todayEnd.Before(todayStart) {
		result = now.After(todayStart) || now.Before(todayEnd)
	} else {
		result = now.After(todayStart) && now.Before(todayEnd)
	}

	if r.PolicyCache != nil {
		r.PolicyCache.SetMaintenanceWindow(cacheKey, result, 1*time.Minute)
	}

	return result
}

// removePDBTemporarily disables the PDB during a maintenance window
func (r *StatefulSetReconciler) removePDBTemporarily(ctx context.Context, sts *appsv1.StatefulSet, logger logr.Logger) error {
	pdbName := types.NamespacedName{
		Name:      sts.Name + DefaultPDBSuffix,
		Namespace: sts.Namespace,
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, pdbName, pdb); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	patch := client.MergeFrom(pdb.DeepCopy())

	if pdb.Annotations == nil {
		pdb.Annotations = make(map[string]string)
	}
	pdb.Annotations["pdboperator.io/maintenance-mode"] = "true"
	pdb.Annotations["pdboperator.io/maintenance-start"] = time.Now().Format(time.RFC3339)
	pdb.Spec.MinAvailable = &intstr.IntOrString{Type: intstr.Int, IntVal: 0}

	if err := r.Patch(ctx, pdb, patch); err != nil {
		return err
	}

	logger.Info("Temporarily disabled PDB for maintenance window", "pdb", pdbName.Name)
	return nil
}

// Helper functions

func (r *StatefulSetReconciler) isValidAvailabilityClass(class availabilityv1alpha1.AvailabilityClass) bool {
	switch class {
	case availabilityv1alpha1.NonCritical, availabilityv1alpha1.Standard,
		availabilityv1alpha1.HighAvailability, availabilityv1alpha1.MissionCritical,
		availabilityv1alpha1.Custom:
		return true
	default:
		return false
	}
}

func (r *StatefulSetReconciler) getDescriptionForClass(class availabilityv1alpha1.AvailabilityClass) string {
	descriptions := map[availabilityv1alpha1.AvailabilityClass]string{
		availabilityv1alpha1.NonCritical:      "Non-critical apps (batch jobs, testing)",
		availabilityv1alpha1.Standard:         "Typical microservices & APIs",
		availabilityv1alpha1.HighAvailability: "Stateful apps, DBs, Kafka consumers",
		availabilityv1alpha1.MissionCritical:  "Critical services that must not go down",
		availabilityv1alpha1.Custom:           "Custom availability configuration",
	}
	return descriptions[class]
}

func (r *StatefulSetReconciler) inferWorkloadFunction(sts *appsv1.StatefulSet) availabilityv1alpha1.WorkloadFunction {
	if sts.Annotations != nil {
		if function := sts.Annotations[AnnotationWorkloadFunction]; function != "" {
			return availabilityv1alpha1.WorkloadFunction(function)
		}
	}

	name := strings.ToLower(sts.Name)

	securityPatterns := []string{"auth", "security", "identity", "keycloak", "oauth", "jwt", "rbac"}
	for _, pattern := range securityPatterns {
		if strings.Contains(name, pattern) {
			return availabilityv1alpha1.SecurityFunction
		}
	}

	managementPatterns := []string{"operator", "controller", "manager", "webhook", "admission"}
	for _, pattern := range managementPatterns {
		if strings.Contains(name, pattern) {
			return availabilityv1alpha1.ManagementFunction
		}
	}

	return availabilityv1alpha1.CoreFunction
}

// calculateStatefulSetFingerprint calculates a fingerprint of relevant StatefulSet state
func (r *StatefulSetReconciler) calculateStatefulSetFingerprint(
	ctx context.Context,
	sts *appsv1.StatefulSet,
	config *AvailabilityConfig,
) string {
	h := sha256.New()

	_, _ = fmt.Fprintf(h, "generation:%d", sts.Generation)

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	_, _ = fmt.Fprintf(h, "replicas:%d", replicas)

	if sts.Annotations != nil {
		for _, key := range []string{
			AnnotationAvailabilityClass,
			AnnotationMaintenanceWindow,
			AnnotationWorkloadFunction,
			AnnotationOverrideReason,
		} {
			if value, exists := sts.Annotations[key]; exists {
				_, _ = fmt.Fprintf(h, "%s:%s", key, value)
			}
		}
	}

	if sts.Labels != nil {
		for key, value := range sts.Labels {
			_, _ = fmt.Fprintf(h, "label:%s=%s", key, value)
		}
	}

	if sts.Spec.Selector != nil && sts.Spec.Selector.MatchLabels != nil {
		for key, value := range sts.Spec.Selector.MatchLabels {
			_, _ = fmt.Fprintf(h, "selector:%s=%s", key, value)
		}
	}

	if config != nil {
		_, _ = fmt.Fprintf(h, "config:class=%s", config.AvailabilityClass)
		_, _ = fmt.Fprintf(h, "config:source=%s", config.Source)
		_, _ = fmt.Fprintf(h, "config:policy=%s", config.PolicyName)
		_, _ = fmt.Fprintf(h, "config:enforcement=%s", config.Enforcement)
		_, _ = fmt.Fprintf(h, "config:minAvailable=%s", config.MinAvailable.String())
	}

	pdbName := types.NamespacedName{
		Name:      sts.Name + DefaultPDBSuffix,
		Namespace: sts.Namespace,
	}
	currentPDB := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, pdbName, currentPDB); err == nil {
		_, _ = h.Write([]byte("pdb:exists=true"))
		_, _ = fmt.Fprintf(h, "pdb:minAvailable=%s", currentPDB.Spec.MinAvailable.String())
		if currentPDB.Spec.Selector != nil && currentPDB.Spec.Selector.MatchLabels != nil {
			for key, value := range currentPDB.Spec.Selector.MatchLabels {
				_, _ = fmt.Fprintf(h, "pdb:selector:%s=%s", key, value)
			}
		}
	} else {
		_, _ = h.Write([]byte("pdb:exists=false"))
	}

	if config != nil {
		_, _ = fmt.Fprintf(h, "expected:pdb:minAvailable=%s", config.MinAvailable.String())
		if sts.Spec.Selector != nil && sts.Spec.Selector.MatchLabels != nil {
			for key, value := range sts.Spec.Selector.MatchLabels {
				_, _ = fmt.Fprintf(h, "expected:pdb:selector:%s=%s", key, value)
			}
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

// hasStatefulSetStateChanged checks if the StatefulSet state has changed since last reconciliation
func (r *StatefulSetReconciler) hasStatefulSetStateChanged(
	ctx context.Context,
	sts *appsv1.StatefulSet,
	config *AvailabilityConfig,
) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.lastStatefulSetState == nil {
		return true, nil
	}

	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}
	currentFingerprint := r.calculateStatefulSetFingerprint(ctx, sts, config)

	lastFingerprint, exists := r.lastStatefulSetState[key]
	if !exists {
		log.FromContext(ctx).Info("No previous state found, processing",
			"statefulset", sts.Name,
			"namespace", sts.Namespace,
			"currentFingerprint", currentFingerprint[:12]+"...",
			"debug", true)
		return true, nil
	}

	changed := currentFingerprint != lastFingerprint

	log.FromContext(ctx).Info("Fingerprint comparison",
		"statefulset", sts.Name,
		"namespace", sts.Namespace,
		"currentFingerprint", currentFingerprint[:12]+"...",
		"lastFingerprint", lastFingerprint[:12]+"...",
		"changed", changed,
		"debug", true)

	return changed, nil
}

// updateStatefulSetState updates the cached state after a successful reconciliation
func (r *StatefulSetReconciler) updateStatefulSetState(
	ctx context.Context,
	sts *appsv1.StatefulSet,
	config *AvailabilityConfig,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastStatefulSetState == nil {
		r.lastStatefulSetState = make(map[types.NamespacedName]string)
	}

	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}
	r.lastStatefulSetState[key] = r.calculateStatefulSetFingerprint(ctx, sts, config)
	return nil
}

// clearStatefulSetState removes the cached state for a StatefulSet (called on deletion)
func (r *StatefulSetReconciler) clearStatefulSetState(sts *appsv1.StatefulSet) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastStatefulSetState == nil {
		return
	}

	delete(r.lastStatefulSetState, types.NamespacedName{
		Name:      sts.Name,
		Namespace: sts.Namespace,
	})
}

// SetupWithManager sets up the StatefulSet controller with the Manager
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := 3
	if r.Config != nil && r.Config.MaxConcurrentReconciles > 0 {
		maxConcurrent = r.Config.MaxConcurrentReconciles
	}
	return r.SetupWithManagerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: maxConcurrent,
	})
}

// SetupWithManagerWithOptions sets up the controller with custom options
func (r *StatefulSetReconciler) SetupWithManagerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	r.mu.Lock()
	if r.lastStatefulSetState == nil {
		r.lastStatefulSetState = make(map[types.NamespacedName]string)
	}
	r.mu.Unlock()

	if r.Recorder == nil && mgr != nil {
		r.Recorder = mgr.GetEventRecorder("statefulset-pdb-controller")
	}
	if r.Events == nil && r.Recorder != nil {
		r.Events = events.NewEventRecorder(r.Recorder)
	}

	statefulSetPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.(*appsv1.StatefulSet)
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSts, ok := e.ObjectOld.(*appsv1.StatefulSet)
			if !ok {
				return false
			}
			newSts, ok := e.ObjectNew.(*appsv1.StatefulSet)
			if !ok {
				return false
			}
			if oldSts.Generation == newSts.Generation {
				if oldSts.DeletionTimestamp == nil && newSts.DeletionTimestamp != nil {
					return true
				}
				return false
			}
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	pdbPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
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
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	// Maps PDB events back to the owning StatefulSet for reconciliation
	pdbToStatefulSetHandler := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		pdb, ok := obj.(*policyv1.PodDisruptionBudget)
		if !ok {
			return nil
		}

		if labels := pdb.GetLabels(); labels == nil || labels[LabelManagedBy] != OperatorName {
			return nil
		}

		stsName := pdb.Name
		if len(stsName) > len(DefaultPDBSuffix) &&
			stsName[len(stsName)-len(DefaultPDBSuffix):] == DefaultPDBSuffix {
			stsName = stsName[:len(stsName)-len(DefaultPDBSuffix)]
		} else {
			for _, ownerRef := range pdb.GetOwnerReferences() {
				if ownerRef.Kind == "StatefulSet" && ownerRef.Controller != nil && *ownerRef.Controller {
					stsName = ownerRef.Name
					break
				}
			}
		}

		log.Log.Info("Mapping PDB event to StatefulSet reconciliation",
			"pdb", pdb.Name,
			"statefulset", stsName,
			"namespace", pdb.Namespace)

		return []ctrl.Request{
			{
				NamespacedName: types.NamespacedName{
					Name:      stsName,
					Namespace: pdb.Namespace,
				},
			},
		}
	})

	// Maps PDBPolicy changes to affected StatefulSets
	policyHandler := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		policy, ok := obj.(*availabilityv1alpha1.PDBPolicy)
		if !ok {
			return nil
		}

		logger := log.FromContext(ctx)

		stsList := &appsv1.StatefulSetList{}
		if err := r.List(ctx, stsList); err != nil {
			logger.Error(err, "Failed to list StatefulSets for policy change")
			return nil
		}

		var requests []ctrl.Request
		for _, sts := range stsList.Items {
			if r.policyMatchesStatefulSet(policy, &sts) {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      sts.Name,
						Namespace: sts.Namespace,
					},
				})
			}
		}

		logger.V(2).Info("Policy change affects StatefulSets",
			"policy", policy.Name,
			"affectedStatefulSets", len(requests))

		return requests
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("statefulset-pdb").
		For(&appsv1.StatefulSet{}, builder.WithPredicates(statefulSetPredicate)).
		Watches(
			&policyv1.PodDisruptionBudget{},
			pdbToStatefulSetHandler,
			builder.WithPredicates(pdbPredicate),
		).
		Watches(
			&availabilityv1alpha1.PDBPolicy{},
			policyHandler,
		).
		WithOptions(opts).
		Complete(r)
}
