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
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
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
	// availabilityPolicyFinalizer is the finalizer name for PDBPolicy
	availabilityPolicyFinalizer = "pdbpolicy.pdboperator.io/finalizer"
)

// containsString checks if a slice contains a specific string
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// removeString removes a string from a slice
func removeString(slice []string, item string) []string {
	var result []string
	for _, s := range slice {
		if s != item {
			result = append(result, s)
		}
	}
	return result
}

// PDBPolicyReconciler reconciles a PDBPolicy object
type PDBPolicyReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    k8sevents.EventRecorder
	Events      *events.EventRecorder
	PolicyCache *cache.PolicyCache
}

// +kubebuilder:rbac:groups=availability.pdboperator.io,resources=pdbpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=availability.pdboperator.io,resources=pdbpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=availability.pdboperator.io,resources=pdbpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// Reconcile handles PDBPolicy changes
// Reconcile handles PDBPolicy changes
func (r *PDBPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Start timing for metrics
	startTime := time.Now()

	// Generate reconcile ID for this specific reconciliation with controller prefix
	reconcileID := "policy-" + uuid.New().String()

	// Start tracing span
	ctx, span := tracing.ReconcileSpan(ctx, "pdbpolicy", req.Namespace, req.Name)
	defer span.End()

	// Generate IDs
	correlationID := uuid.New().String()

	// Add IDs to span
	span.SetAttributes(
		attribute.String("reconcile.id", reconcileID),
		attribute.String("correlation.id", correlationID),
	)

	// Create unified logger with clean, structured logging
	logger := logging.CreateUnifiedLogger(ctx,
		"pdb-policy",                  // controllerType
		"pdbpolicy-controller",        // controllerName
		"availability.pdboperator.io", // group
		"PDBPolicy",                   // kind
		"pdbpolicy",                   // resourceType
		req.Name,                      // name
		req.Namespace,                 // namespace
		reconcileID,                   // reconcileID
		correlationID,                 // correlationID
	)

	// Ensure we record metrics and audit at the end
	var reconcileErr error
	defer func() {
		duration := time.Since(startTime)
		metrics.RecordReconciliation("pdbpolicy", duration, reconcileErr)

		// Record tracing error if any
		if reconcileErr != nil {
			tracing.RecordError(span, reconcileErr, "Reconciliation failed")
		}

		// Audit the reconciliation using unified logger
		result := logging.AuditResultSuccess
		if reconcileErr != nil {
			result = logging.AuditResultFailure
		}
		logger.Audit(
			"RECONCILE",
			fmt.Sprintf("%s/%s", req.Namespace, req.Name),
			"pdbpolicy",
			req.Namespace,
			req.Name,
			result,
			map[string]interface{}{
				"controller": "pdbpolicy",
				"duration":   duration.String(),
				"durationMs": duration.Milliseconds(),
			},
		)

		logger.Info("Reconciliation completed", map[string]any{
			"duration":    duration.String(),
			"reconcileID": reconcileID,
		})
	}()

	// Add tracing event
	tracing.AddEvent(ctx, "FetchingPolicy",
		attribute.String("reconcile.id", reconcileID),
	)

	// Fetch the PDBPolicy
	policy := &availabilityv1alpha1.PDBPolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		if errors.IsNotFound(err) {
			// Policy was deleted - invalidate cache with improved invalidation
			if r.PolicyCache != nil {
				r.PolicyCache.InvalidatePolicy(req.String())
			}
			logger.Info("PDBPolicy not found, invalidating cache", map[string]any{})

			// Add cache invalidation event
			tracing.AddEvent(ctx, "CacheInvalidated",
				attribute.String("reason", "policy_not_found"),
			)

			return ctrl.Result{}, nil
		}
		reconcileErr = err
		logger.Error(err, "Failed to get PDBPolicy", map[string]any{})
		return ctrl.Result{}, err
	}

	// Add to cache if we have one, and invalidate list caches
	if r.PolicyCache != nil {
		r.PolicyCache.Set(req.String(), policy)
		// Invalidate list caches since a policy changed (but keep the policy we just set)
		r.PolicyCache.Delete("all-policies")

		// Add cache update event
		tracing.AddEvent(ctx, "CacheUpdated",
			attribute.String("cache.operation", "set"),
			attribute.String("cache.key", req.String()),
		)
	}

	// Handle deletion
	if policy.DeletionTimestamp != nil {
		logger.Info("PDBPolicy is being deleted", map[string]any{})
		ctx = logging.WithOperation(ctx, "delete")

		// Add deletion event to span
		tracing.AddEvent(ctx, "DeletingPolicy",
			attribute.Int("affected_workloads", len(policy.Status.AppliedToWorkloads)),
		)

		// Record event
		if r.Events != nil {
			r.Events.PolicyRemoved(policy, policy.Name, len(policy.Status.AppliedToWorkloads))
		}

		// Audit policy removal
		logging.AuditPolicyApplication(ctx, policy.Namespace, policy.Name,
			policy.Status.AppliedToWorkloads, logging.AuditResultSuccess)

		// Remove finalizer if present
		if containsString(policy.Finalizers, availabilityPolicyFinalizer) {
			policy.Finalizers = removeString(policy.Finalizers, availabilityPolicyFinalizer)
			if err := r.Update(ctx, policy); err != nil {
				reconcileErr = err
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !containsString(policy.Finalizers, availabilityPolicyFinalizer) {
		policy.Finalizers = append(policy.Finalizers, availabilityPolicyFinalizer)
		if err := RetryUpdateWithBackoff(ctx, r.Client, policy, DefaultRetryConfig()); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to remove finalizer after retries", map[string]any{
				"error_type": GetErrorType(err),
			})
			return ctrl.Result{}, err
		}

		// Add finalizer event
		tracing.AddEvent(ctx, "FinalizerAdded")
	}

	// Validate policy
	ctx = logging.WithOperation(ctx, "validate")

	// Add validation event
	tracing.AddEvent(ctx, "ValidatingPolicy")

	if err := r.validatePolicy(policy); err != nil {
		// Update status to reflect validation failure
		// Note: PDBPolicyStatus doesn't have Phase/Message fields
		// We'll use conditions instead
		policy.Status.Conditions = append(policy.Status.Conditions, metav1.Condition{
			Type:               "Validated",
			Status:             metav1.ConditionFalse,
			LastTransitionTime: metav1.Now(),
			Reason:             "PolicyInvalid",
			Message:            err.Error(),
			ObservedGeneration: policy.Generation,
		})
		if statusErr := RetryStatusUpdateWithBackoff(ctx, r.Client, policy, DefaultRetryConfig()); statusErr != nil {
			logger.Error(statusErr, "Failed to update policy status after retries", map[string]any{
				"error_type": GetErrorType(statusErr),
			})
		}

		// Record validation failure in span
		span.SetAttributes(
			attribute.String("validation.result", "failed"),
			attribute.String("validation.error", err.Error()),
		)

		reconcileErr = err
		return ctrl.Result{}, err
	}

	// Update policy status (finds matching deployments, updates conditions, persists)
	result, err := r.updateStatus(ctx, policy, logger.ToLogr())
	if err != nil {
		reconcileErr = err
		return result, err
	}

	// Add status update to span
	span.SetAttributes(
		attribute.Int("policy.applied_workloads", len(policy.Status.AppliedToWorkloads)),
		attribute.String("policy.status", "active"),
	)

	// Update metrics
	metrics.AvailabilityPoliciesActive.WithLabelValues(
		policy.Namespace,
	).Set(float64(len(policy.Status.AppliedToWorkloads)))

	logger.WithDetails(logging.Details{
		"componentsAffected": len(policy.Status.AppliedToWorkloads),
		"enforcement":        policy.Spec.Enforcement,
		"reconcile_id":       reconcileID,
	}).Info("Successfully reconciled PDBPolicy", map[string]any{})

	return result, nil
}

// updateStatus updates the PDBPolicy status with current state
func (r *PDBPolicyReconciler) updateStatus(ctx context.Context, policy *availabilityv1alpha1.PDBPolicy, logger logr.Logger) (ctrl.Result, error) {
	ctx = logging.WithOperation(ctx, "status-update")
	done := logging.StartOperation(ctx, "findMatchingDeployments")

	// Find matching deployments and StatefulSets
	oldComponentCount := len(policy.Status.AppliedToWorkloads)
	appliedComponents, err := r.findMatchingDeployments(ctx, policy, logger)
	done()

	if err != nil {
		logger.Error(err, "Failed to find matching deployments")
		return ctrl.Result{}, err
	}

	// Find matching StatefulSets and merge into appliedComponents
	doneStatefulSets := logging.StartOperation(ctx, "findMatchingStatefulSets")
	stsComponents, err := r.findMatchingStatefulSets(ctx, policy, logger)
	doneStatefulSets()

	if err != nil {
		logger.Error(err, "Failed to find matching StatefulSets")
		return ctrl.Result{}, err
	}
	appliedComponents = append(appliedComponents, stsComponents...)

	// Log component matches
	logger.Info("Found matching workloads",
		"count", len(appliedComponents),
		"workloads", appliedComponents)

	// Update status
	policy.Status.AppliedToWorkloads = appliedComponents
	policy.Status.LastAppliedTime = &metav1.Time{Time: time.Now()}
	policy.Status.ObservedGeneration = policy.Generation

	// Update conditions
	r.updateConditions(policy, len(appliedComponents) > 0)

	// Update the status
	if err := RetryStatusUpdateWithBackoff(ctx, r.Client, policy, DefaultRetryConfig()); err != nil {
		logger.Error(err, "Failed to update PDBPolicy status after retries", map[string]any{
			"error_type": GetErrorType(err),
		})
		return ctrl.Result{}, err
	}

	// Record events and metrics
	if r.Events != nil {
		if oldComponentCount != len(appliedComponents) {
			if len(appliedComponents) > oldComponentCount {
				r.Events.PolicyApplied(policy, policy.Name, len(appliedComponents))
			} else if len(appliedComponents) > 0 {
				r.Events.PolicyUpdated(policy, policy.Name, fmt.Sprintf("component count changed from %d to %d", oldComponentCount, len(appliedComponents)))
			}
		}
	}

	// Audit policy application
	logging.AuditPolicyApplication(ctx, policy.Namespace, policy.Name,
		appliedComponents, logging.AuditResultSuccess)

	// Ensure logger has trace fields before logging
	logger = logging.EnsureTraceFields(ctx, logger)
	logger.Info("Updated PDBPolicy status",
		"appliedComponents", len(appliedComponents),
		"policy", policy.Name)

	// Requeue to periodically update status
	return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
}

// findMatchingDeployments finds deployments that match the policy selector
func (r *PDBPolicyReconciler) findMatchingDeployments(ctx context.Context, policy *availabilityv1alpha1.PDBPolicy, logger logr.Logger) ([]string, error) {
	var matchingComponents []string

	// Add tracing
	_, span := tracing.StartSpan(ctx, "FindMatchingDeployments")
	defer span.End()

	// Get list of deployments with batching for large clusters
	deploymentList := &appsv1.DeploymentList{}

	// If specific namespaces are specified, batch the requests
	if len(policy.Spec.WorkloadSelector.Namespaces) > 0 {
		// Batch namespace queries to reduce API calls
		const batchSize = 5
		namespaces := policy.Spec.WorkloadSelector.Namespaces

		for i := 0; i < len(namespaces); i += batchSize {
			end := i + batchSize
			if end > len(namespaces) {
				end = len(namespaces)
			}

			// Process batch of namespaces concurrently
			batch := namespaces[i:end]
			batchResults := make(chan *appsv1.DeploymentList, len(batch))
			batchErrors := make(chan error, len(batch))

			var wg sync.WaitGroup
			for _, ns := range batch {
				wg.Add(1)
				go func(namespace string) {
					defer wg.Done()
					namespacedList := &appsv1.DeploymentList{}
					if err := r.List(ctx, namespacedList, client.InNamespace(namespace)); err != nil {
						batchErrors <- err
						return
					}
					batchResults <- namespacedList
				}(ns)
			}

			// Wait for all goroutines to complete
			go func() {
				wg.Wait()
				close(batchResults)
				close(batchErrors)
			}()

			// Collect results
			for {
				select {
				case err := <-batchErrors:
					if err != nil {
						return nil, err
					}
				case list, ok := <-batchResults:
					if !ok {
						goto done
					}
					deploymentList.Items = append(deploymentList.Items, list.Items...)
				}
			}
		done:
		}
	} else {
		// List all namespaces
		if err := r.List(ctx, deploymentList); err != nil {
			return nil, err
		}
	}

	// Check each deployment against the selector
	matchCount := 0
	for _, deployment := range deploymentList.Items {
		if r.deploymentMatchesSelector(policy.Spec.WorkloadSelector, &deployment) {
			componentName := deployment.Annotations[AnnotationWorkloadName]
			if componentName == "" {
				componentName = deployment.Name
			}
			matchingComponents = append(matchingComponents, fmt.Sprintf("%s/%s", deployment.Namespace, componentName))
			matchCount++

			logger.V(2).Info("Deployment matches policy",
				"deployment", deployment.Name,
				"namespace", deployment.Namespace,
				"component", componentName)
		}
	}

	// Add tracing attributes
	span.SetAttributes(
		attribute.Int("deployments.evaluated", len(deploymentList.Items)),
		attribute.Int("deployments.matched", matchCount),
	)

	return matchingComponents, nil
}

// deploymentMatchesSelector checks if a deployment matches the workload selector
func (r *PDBPolicyReconciler) deploymentMatchesSelector(selector availabilityv1alpha1.WorkloadSelector, deployment *appsv1.Deployment) bool {
	// Check component names
	if len(selector.WorkloadNames) > 0 {
		componentName := deployment.Annotations[AnnotationWorkloadName]
		if componentName == "" {
			componentName = deployment.Name
		}

		nameMatch := false
		for _, name := range selector.WorkloadNames {
			if name == componentName {
				nameMatch = true
				break
			}
		}
		if !nameMatch {
			return false
		}
	}

	// Check component functions
	if len(selector.WorkloadFunctions) > 0 {
		deploymentFunction := availabilityv1alpha1.WorkloadFunction(deployment.Annotations[AnnotationWorkloadFunction])
		if deploymentFunction == "" {
			deploymentFunction = availabilityv1alpha1.CoreFunction // default
		}

		functionMatch := false
		for _, function := range selector.WorkloadFunctions {
			if function == deploymentFunction {
				functionMatch = true
				break
			}
		}
		if !functionMatch {
			return false
		}
	}

	// Check labels
	if len(selector.MatchLabels) > 0 {
		deploymentLabels := deployment.GetLabels()
		if deploymentLabels == nil {
			return false
		}
		for key, value := range selector.MatchLabels {
			if deploymentLabels[key] != value {
				return false
			}
		}
	}

	// Check match expressions
	if len(selector.MatchExpressions) > 0 {
		deploymentLabels := deployment.GetLabels()
		if deploymentLabels == nil {
			return false
		}

		for _, expr := range selector.MatchExpressions {
			if !r.evaluateLabelSelectorRequirement(expr, deploymentLabels) {
				return false
			}
		}
	}

	return true
}

// evaluateLabelSelectorRequirement evaluates a label selector requirement
func (r *PDBPolicyReconciler) evaluateLabelSelectorRequirement(req metav1.LabelSelectorRequirement, labels map[string]string) bool {
	switch req.Operator {
	case metav1.LabelSelectorOpIn:
		labelValue, exists := labels[req.Key]
		if !exists {
			return false
		}
		for _, value := range req.Values {
			if labelValue == value {
				return true
			}
		}
		return false

	case metav1.LabelSelectorOpNotIn:
		labelValue, exists := labels[req.Key]
		if !exists {
			return true
		}
		for _, value := range req.Values {
			if labelValue == value {
				return false
			}
		}
		return true

	case metav1.LabelSelectorOpExists:
		_, exists := labels[req.Key]
		return exists

	case metav1.LabelSelectorOpDoesNotExist:
		_, exists := labels[req.Key]
		return !exists

	default:
		return false
	}
}

// updateConditions updates the policy conditions based on current state
func (r *PDBPolicyReconciler) updateConditions(policy *availabilityv1alpha1.PDBPolicy, hasMatches bool) {
	now := metav1.NewTime(time.Now())

	// Ready condition
	readyCondition := metav1.Condition{
		Type:               "Ready",
		LastTransitionTime: now,
		ObservedGeneration: policy.Generation,
	}

	if hasMatches {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "ComponentsMatched"
		readyCondition.Message = fmt.Sprintf("Policy is applied to %d workloads", len(policy.Status.AppliedToWorkloads))
	} else {
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = "NoComponentsMatched"
		readyCondition.Message = "No components match the policy selector"
	}

	// Update or add the condition
	r.setCondition(&policy.Status.Conditions, readyCondition)

	// Validated condition
	validatedCondition := metav1.Condition{
		Type:               "Validated",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "PolicyValid",
		Message:            "Policy configuration is valid",
		ObservedGeneration: policy.Generation,
	}

	// Validate the policy configuration
	if err := r.validatePolicy(policy); err != nil {
		validatedCondition.Status = metav1.ConditionFalse
		validatedCondition.Reason = "PolicyInvalid"
		validatedCondition.Message = err.Error()
	}

	r.setCondition(&policy.Status.Conditions, validatedCondition)
}

// validatePolicy validates the policy configuration
func (r *PDBPolicyReconciler) validatePolicy(policy *availabilityv1alpha1.PDBPolicy) error {
	// Validate availability class
	switch policy.Spec.AvailabilityClass {
	case availabilityv1alpha1.NonCritical, availabilityv1alpha1.Standard,
		availabilityv1alpha1.HighAvailability, availabilityv1alpha1.MissionCritical:
		// Valid classes
	case availabilityv1alpha1.Custom:
		// Custom class requires custom PDB config
		if policy.Spec.CustomPDBConfig == nil {
			return fmt.Errorf("custom availability class requires customPDBConfig")
		}
		if policy.Spec.CustomPDBConfig.MinAvailable == nil && policy.Spec.CustomPDBConfig.MaxUnavailable == nil {
			return fmt.Errorf("custom PDB config must specify either minAvailable or maxUnavailable")
		}
	default:
		return fmt.Errorf("invalid availability class: %s", policy.Spec.AvailabilityClass)
	}

	// Validate maintenance windows
	for i, window := range policy.Spec.MaintenanceWindows {
		if err := r.validateMaintenanceWindow(window); err != nil {
			return fmt.Errorf("invalid maintenance window %d: %v", i, err)
		}
	}

	// Validate workload selector (at least one selector must be specified)
	selector := policy.Spec.WorkloadSelector
	if len(selector.WorkloadNames) == 0 &&
		len(selector.WorkloadFunctions) == 0 &&
		len(selector.MatchLabels) == 0 &&
		len(selector.MatchExpressions) == 0 {
		return fmt.Errorf("workload selector must specify at least one selection criteria")
	}

	return nil
}

// validateMaintenanceWindow validates a maintenance window configuration
func (r *PDBPolicyReconciler) validateMaintenanceWindow(window availabilityv1alpha1.MaintenanceWindow) error {
	// Validate time format
	if _, err := time.Parse("15:04", window.Start); err != nil {
		return fmt.Errorf("invalid start time format: %s", window.Start)
	}

	if _, err := time.Parse("15:04", window.End); err != nil {
		return fmt.Errorf("invalid end time format: %s", window.End)
	}

	// Validate timezone
	if window.Timezone != "" {
		if _, err := time.LoadLocation(window.Timezone); err != nil {
			return fmt.Errorf("invalid timezone: %s", window.Timezone)
		}
	}

	// Validate days of week
	for _, day := range window.DaysOfWeek {
		if day < 0 || day > 6 {
			return fmt.Errorf("invalid day of week: %d (must be 0-6)", day)
		}
	}

	return nil
}

// setCondition sets or updates a condition in the conditions slice
func (r *PDBPolicyReconciler) setCondition(conditions *[]metav1.Condition, newCondition metav1.Condition) {
	for i, condition := range *conditions {
		if condition.Type == newCondition.Type {
			// Update existing condition only if status changed
			if condition.Status != newCondition.Status || condition.Reason != newCondition.Reason || condition.Message != newCondition.Message {
				(*conditions)[i] = newCondition
			}
			return
		}
	}

	// Add new condition
	*conditions = append(*conditions, newCondition)
}

// GetCacheStats returns cache statistics
func (r *PDBPolicyReconciler) GetCacheStats() cache.CacheStats {
	if r.PolicyCache != nil {
		return r.PolicyCache.GetStats()
	}
	return cache.CacheStats{}
}

// SetupWithManager sets up the controller with the Manager with optimized settings
func (r *PDBPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Create predicate to filter events
	policyPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			// Always process new policies
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Only reconcile if spec changed
			oldPolicy := e.ObjectOld.(*availabilityv1alpha1.PDBPolicy)
			newPolicy := e.ObjectNew.(*availabilityv1alpha1.PDBPolicy)

			// Check if spec changed
			return oldPolicy.Generation != newPolicy.Generation
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Always process deletes
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	// Create event recorder if not already set
	if r.Recorder == nil && mgr != nil {
		r.Recorder = mgr.GetEventRecorder("pdbpolicy-controller")
	}
	if r.Events == nil && r.Recorder != nil {
		r.Events = events.NewEventRecorder(r.Recorder)
	}

	// Build the controller with optimized settings and UNIQUE NAME
	return ctrl.NewControllerManagedBy(mgr).
		Named("pdbpolicy"). // UNIQUE CONTROLLER NAME
		For(&availabilityv1alpha1.PDBPolicy{}, builder.WithPredicates(policyPredicate)).
		Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.findPoliciesForDeployment),
		).
		Watches(
			&appsv1.StatefulSet{},
			handler.EnqueueRequestsFromMapFunc(r.findPoliciesForStatefulSet),
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 3,
		}).
		Complete(r)
}

// findPoliciesForDeployment finds AvailabilityPolicies that might be affected by a Deployment change
func (r *PDBPolicyReconciler) findPoliciesForDeployment(ctx context.Context, obj client.Object) []ctrl.Request {
	deployment, ok := obj.(*appsv1.Deployment)
	if !ok {
		return nil
	}

	// Add context logging
	ctx = logging.WithDeploymentContext(ctx, deployment.Namespace, deployment.Name)
	logger := log.FromContext(ctx)

	// List all AvailabilityPolicies
	policyList := &availabilityv1alpha1.PDBPolicyList{}
	if err := r.List(ctx, policyList); err != nil {
		logger.Error(err, "Failed to list policies for deployment change")
		return nil
	}

	var requests []ctrl.Request
	for _, policy := range policyList.Items {
		if r.deploymentMatchesSelector(policy.Spec.WorkloadSelector, deployment) {
			requests = append(requests, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      policy.Name,
					Namespace: policy.Namespace,
				},
			})

			logger.V(2).Info("Deployment change triggers policy reconciliation",
				"policy", policy.Name,
				"policyNamespace", policy.Namespace)
		}
	}

	return requests
}

// findMatchingStatefulSets finds StatefulSets that match the policy selector
func (r *PDBPolicyReconciler) findMatchingStatefulSets(ctx context.Context, policy *availabilityv1alpha1.PDBPolicy, logger logr.Logger) ([]string, error) {
	var matchingComponents []string

	_, span := tracing.StartSpan(ctx, "FindMatchingStatefulSets")
	defer span.End()

	stsList := &appsv1.StatefulSetList{}

	if len(policy.Spec.WorkloadSelector.Namespaces) > 0 {
		const batchSize = 5
		namespaces := policy.Spec.WorkloadSelector.Namespaces

		for i := 0; i < len(namespaces); i += batchSize {
			end := i + batchSize
			if end > len(namespaces) {
				end = len(namespaces)
			}

			batch := namespaces[i:end]
			batchResults := make(chan *appsv1.StatefulSetList, len(batch))
			batchErrors := make(chan error, len(batch))

			var wg sync.WaitGroup
			for _, ns := range batch {
				wg.Add(1)
				go func(namespace string) {
					defer wg.Done()
					namespacedList := &appsv1.StatefulSetList{}
					if err := r.List(ctx, namespacedList, client.InNamespace(namespace)); err != nil {
						batchErrors <- err
						return
					}
					batchResults <- namespacedList
				}(ns)
			}

			go func() {
				wg.Wait()
				close(batchResults)
				close(batchErrors)
			}()

			for {
				select {
				case err := <-batchErrors:
					if err != nil {
						return nil, err
					}
				case list, ok := <-batchResults:
					if !ok {
						goto done
					}
					stsList.Items = append(stsList.Items, list.Items...)
				}
			}
		done:
		}
	} else {
		if err := r.List(ctx, stsList); err != nil {
			return nil, err
		}
	}

	matchCount := 0
	for _, sts := range stsList.Items {
		if r.statefulSetMatchesSelector(policy.Spec.WorkloadSelector, &sts) {
			workloadName := ""
			if sts.Annotations != nil {
				workloadName = sts.Annotations[AnnotationWorkloadName]
			}
			if workloadName == "" {
				workloadName = sts.Name
			}
			matchingComponents = append(matchingComponents, fmt.Sprintf("%s/%s", sts.Namespace, workloadName))
			matchCount++

			logger.V(2).Info("StatefulSet matches policy",
				"statefulset", sts.Name,
				"namespace", sts.Namespace,
				"component", workloadName)
		}
	}

	span.SetAttributes(
		attribute.Int("statefulsets.evaluated", len(stsList.Items)),
		attribute.Int("statefulsets.matched", matchCount),
	)

	return matchingComponents, nil
}

// statefulSetMatchesSelector checks if a StatefulSet matches the workload selector
func (r *PDBPolicyReconciler) statefulSetMatchesSelector(selector availabilityv1alpha1.WorkloadSelector, sts *appsv1.StatefulSet) bool {
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

	// Check namespaces
	if len(selector.Namespaces) > 0 {
		nsMatch := false
		for _, ns := range selector.Namespaces {
			if ns == sts.Namespace {
				nsMatch = true
				break
			}
		}
		if !nsMatch {
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
	for _, expr := range selector.MatchExpressions {
		if !r.evaluateMatchExpression(expr, sts.Labels) {
			return false
		}
	}

	return true
}

// findPoliciesForStatefulSet finds PDBPolicies that might be affected by a StatefulSet change
func (r *PDBPolicyReconciler) findPoliciesForStatefulSet(ctx context.Context, obj client.Object) []ctrl.Request {
	sts, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		return nil
	}

	logger := log.FromContext(ctx)

	policyList := &availabilityv1alpha1.PDBPolicyList{}
	if err := r.List(ctx, policyList); err != nil {
		logger.Error(err, "Failed to list policies for StatefulSet change")
		return nil
	}

	var requests []ctrl.Request
	for _, policy := range policyList.Items {
		if r.statefulSetMatchesSelector(policy.Spec.WorkloadSelector, sts) {
			requests = append(requests, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      policy.Name,
					Namespace: policy.Namespace,
				},
			})

			logger.V(2).Info("StatefulSet change triggers policy reconciliation",
				"policy", policy.Name,
				"policyNamespace", policy.Namespace)
		}
	}

	return requests
}

// evaluateMatchExpression evaluates a single label selector requirement against a set of labels
func (r *PDBPolicyReconciler) evaluateMatchExpression(expr metav1.LabelSelectorRequirement, labels map[string]string) bool {
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
