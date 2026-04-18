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

	"github.com/go-logr/logr"

	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	availabilityv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
	"github.com/pdb-operator/pdb-operator/internal/cache"
	"github.com/pdb-operator/pdb-operator/internal/events"
	"github.com/pdb-operator/pdb-operator/internal/logging"
	"github.com/pdb-operator/pdb-operator/internal/metrics"
)

// WorkloadAccessor abstracts Deployment/StatefulSet so shared logic
// does not depend on a concrete API type.
type WorkloadAccessor interface {
	GetObject() client.Object
	GetName() string
	GetNamespace() string
	GetAnnotations() map[string]string
	GetLabels() map[string]string
	GetGeneration() int64
	GetReplicas() int32
	GetSelector() *metav1.LabelSelector
	GetDeletionTimestamp() *metav1.Time
	DeepCopyObject() client.Object
	Kind() string
	KindLower() string
}

// deploymentWorkload adapter is prepared for when deployment_controller.go
// is migrated to use workload.go shared logic.
// Uncomment when that migration happens.
//
// type deploymentWorkload struct{ *appsv1.Deployment }

// func (d *deploymentWorkload) GetObject() client.Object           { return d.Deployment }
// func (d *deploymentWorkload) GetName() string                    { return d.Name }
// func (d *deploymentWorkload) GetNamespace() string               { return d.Namespace }
// func (d *deploymentWorkload) GetAnnotations() map[string]string  { return d.Annotations }
// func (d *deploymentWorkload) GetLabels() map[string]string       { return d.Labels }
// func (d *deploymentWorkload) GetGeneration() int64               { return d.Generation }
// func (d *deploymentWorkload) GetSelector() *metav1.LabelSelector { return d.Spec.Selector }
// func (d *deploymentWorkload) GetDeletionTimestamp() *metav1.Time { return d.DeletionTimestamp }
// func (d *deploymentWorkload) DeepCopyObject() client.Object      { return d.Deployment.DeepCopy() }
// func (d *deploymentWorkload) Kind() string                       { return "Deployment" }
// func (d *deploymentWorkload) KindLower() string                  { return "deployment" }
// func (d *deploymentWorkload) GetReplicas() int32 {
// 	if d.Spec.Replicas != nil {
// 		return *d.Spec.Replicas
// 	}
// 	return 1
// }

// statefulSetWorkload is a thin adapter wrapping appsv1.StatefulSet.
type statefulSetWorkload struct{ *appsv1.StatefulSet }

func (s *statefulSetWorkload) GetObject() client.Object           { return s.StatefulSet }
func (s *statefulSetWorkload) GetName() string                    { return s.Name }
func (s *statefulSetWorkload) GetNamespace() string               { return s.Namespace }
func (s *statefulSetWorkload) GetAnnotations() map[string]string  { return s.Annotations }
func (s *statefulSetWorkload) GetLabels() map[string]string       { return s.Labels }
func (s *statefulSetWorkload) GetGeneration() int64               { return s.Generation }
func (s *statefulSetWorkload) GetSelector() *metav1.LabelSelector { return s.Spec.Selector }
func (s *statefulSetWorkload) GetDeletionTimestamp() *metav1.Time { return s.DeletionTimestamp }
func (s *statefulSetWorkload) DeepCopyObject() client.Object      { return s.DeepCopy() }
func (s *statefulSetWorkload) Kind() string                       { return "StatefulSet" }
func (s *statefulSetWorkload) KindLower() string                  { return "statefulset" }
func (s *statefulSetWorkload) GetReplicas() int32 {
	if s.Spec.Replicas != nil {
		return *s.Spec.Replicas
	}
	return 1
}

// WorkloadStateTracker tracks per-workload fingerprints to skip no-op reconciliations.
type WorkloadStateTracker struct {
	mu    sync.RWMutex
	state map[types.NamespacedName]string
}

func NewWorkloadStateTracker() *WorkloadStateTracker {
	return &WorkloadStateTracker{state: make(map[types.NamespacedName]string)}
}

func (t *WorkloadStateTracker) HasStateChanged(ctx context.Context, c client.Client, w WorkloadAccessor, config *AvailabilityConfig) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.state == nil {
		return true
	}
	key := types.NamespacedName{Name: w.GetName(), Namespace: w.GetNamespace()}
	current := calculateWorkloadFingerprint(ctx, c, w, config)
	last, exists := t.state[key]
	return !exists || current != last
}

func (t *WorkloadStateTracker) UpdateState(ctx context.Context, c client.Client, w WorkloadAccessor, config *AvailabilityConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == nil {
		t.state = make(map[types.NamespacedName]string)
	}
	key := types.NamespacedName{Name: w.GetName(), Namespace: w.GetNamespace()}
	t.state[key] = calculateWorkloadFingerprint(ctx, c, w, config)
}

func (t *WorkloadStateTracker) ClearState(w WorkloadAccessor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.state, types.NamespacedName{Name: w.GetName(), Namespace: w.GetNamespace()})
}

func calculateWorkloadFingerprint(ctx context.Context, c client.Client, w WorkloadAccessor, config *AvailabilityConfig) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "generation:%d", w.GetGeneration())
	_, _ = fmt.Fprintf(h, "replicas:%d", w.GetReplicas())

	if annotations := w.GetAnnotations(); annotations != nil {
		for _, key := range []string{
			AnnotationAvailabilityClass,
			AnnotationMaintenanceWindow,
			AnnotationWorkloadFunction,
			AnnotationOverrideReason,
		} {
			if value, exists := annotations[key]; exists {
				_, _ = fmt.Fprintf(h, "%s:%s", key, value)
			}
		}
	}

	for key, value := range w.GetLabels() {
		_, _ = fmt.Fprintf(h, "label:%s=%s", key, value)
	}

	if sel := w.GetSelector(); sel != nil {
		for key, value := range sel.MatchLabels {
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

	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}
	currentPDB := &policyv1.PodDisruptionBudget{}
	if err := c.Get(ctx, pdbKey, currentPDB); err == nil {
		_, _ = h.Write([]byte("pdb:exists=true"))
		_, _ = fmt.Fprintf(h, "pdb:minAvailable=%s", currentPDB.Spec.MinAvailable.String())
		if currentPDB.Spec.Selector != nil {
			for key, value := range currentPDB.Spec.Selector.MatchLabels {
				_, _ = fmt.Fprintf(h, "pdb:selector:%s=%s", key, value)
			}
		}
	} else {
		_, _ = h.Write([]byte("pdb:exists=false"))
	}

	if config != nil {
		_, _ = fmt.Fprintf(h, "expected:pdb:minAvailable=%s", config.MinAvailable.String())
		if sel := w.GetSelector(); sel != nil {
			for key, value := range sel.MatchLabels {
				_, _ = fmt.Fprintf(h, "expected:pdb:selector:%s=%s", key, value)
			}
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

func IsValidAvailabilityClass(class availabilityv1alpha1.AvailabilityClass) bool {
	switch class {
	case availabilityv1alpha1.NonCritical, availabilityv1alpha1.Standard,
		availabilityv1alpha1.HighAvailability, availabilityv1alpha1.MissionCritical,
		availabilityv1alpha1.Custom:
		return true
	default:
		return false
	}
}

func GetDescriptionForClass(class availabilityv1alpha1.AvailabilityClass) string {
	descriptions := map[availabilityv1alpha1.AvailabilityClass]string{
		availabilityv1alpha1.NonCritical:      "Non-critical apps (batch jobs, testing)",
		availabilityv1alpha1.Standard:         "Typical microservices & APIs",
		availabilityv1alpha1.HighAvailability: "Stateful apps, DBs, Kafka consumers",
		availabilityv1alpha1.MissionCritical:  "Critical services that must not go down",
		availabilityv1alpha1.Custom:           "Custom availability configuration",
	}
	return descriptions[class]
}

func EvaluateMatchExpression(expr metav1.LabelSelectorRequirement, labels map[string]string) bool {
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

func InferWorkloadFunction(w WorkloadAccessor) availabilityv1alpha1.WorkloadFunction {
	if annotations := w.GetAnnotations(); annotations != nil {
		if function := annotations[AnnotationWorkloadFunction]; function != "" {
			return availabilityv1alpha1.WorkloadFunction(function)
		}
	}
	name := strings.ToLower(w.GetName())
	for _, pattern := range []string{"auth", "security", "identity", "keycloak", "oauth", "jwt", "rbac"} {
		if strings.Contains(name, pattern) {
			return availabilityv1alpha1.SecurityFunction
		}
	}
	for _, pattern := range []string{"operator", "controller", "manager", "webhook", "admission"} {
		if strings.Contains(name, pattern) {
			return availabilityv1alpha1.ManagementFunction
		}
	}
	return availabilityv1alpha1.CoreFunction
}

func PolicyMatchesWorkload(policy *availabilityv1alpha1.PDBPolicy, w WorkloadAccessor) bool {
	selector := policy.Spec.WorkloadSelector

	if len(selector.Namespaces) > 0 {
		found := false
		for _, ns := range selector.Namespaces {
			if ns == w.GetNamespace() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(selector.WorkloadNames) > 0 {
		workloadName := ""
		if annotations := w.GetAnnotations(); annotations != nil {
			workloadName = annotations[AnnotationWorkloadName]
		}
		if workloadName == "" {
			workloadName = w.GetName()
		}
		found := false
		for _, name := range selector.WorkloadNames {
			if name == workloadName {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(selector.WorkloadFunctions) > 0 {
		wFunction := availabilityv1alpha1.CoreFunction
		if annotations := w.GetAnnotations(); annotations != nil {
			if f := annotations[AnnotationWorkloadFunction]; f != "" {
				wFunction = availabilityv1alpha1.WorkloadFunction(f)
			}
		}
		found := false
		for _, function := range selector.WorkloadFunctions {
			if function == wFunction {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(selector.MatchLabels) > 0 {
		labels := w.GetLabels()
		if labels == nil {
			return false
		}
		for key, value := range selector.MatchLabels {
			if labels[key] != value {
				return false
			}
		}
	}

	for _, expr := range selector.MatchExpressions {
		if !EvaluateMatchExpression(expr, w.GetLabels()) {
			return false
		}
	}

	return true
}

func DetectInvalidConfiguration(w WorkloadAccessor) string {
	if w.GetAnnotations() == nil {
		return ""
	}
	if class, exists := w.GetAnnotations()[AnnotationAvailabilityClass]; exists {
		if !IsValidAvailabilityClass(availabilityv1alpha1.AvailabilityClass(class)) {
			return class
		}
	}
	return ""
}

func GetConfigFromAnnotations(w WorkloadAccessor, logger logr.Logger) *AvailabilityConfig {
	annotations := w.GetAnnotations()
	if annotations == nil {
		return nil
	}
	availabilityClass, exists := annotations[AnnotationAvailabilityClass]
	if !exists {
		return nil
	}
	odaClass := availabilityv1alpha1.AvailabilityClass(availabilityClass)
	if !IsValidAvailabilityClass(odaClass) {
		logger.Error(fmt.Errorf("invalid availability class"), "Unsupported availability class", "class", availabilityClass)
		return nil
	}
	componentFunction := availabilityv1alpha1.WorkloadFunction(annotations[AnnotationWorkloadFunction])
	if componentFunction == "" {
		componentFunction = availabilityv1alpha1.CoreFunction
	}
	return &AvailabilityConfig{
		AvailabilityClass: odaClass,
		WorkloadFunction:  componentFunction,
		MinAvailable:      availabilityv1alpha1.GetMinAvailableForClass(odaClass, componentFunction),
		Description:       GetDescriptionForClass(odaClass),
		MaintenanceWindow: annotations[AnnotationMaintenanceWindow],
		Source:            "oda-annotation",
	}
}

func GetConfigFromPolicyWithCache(ctx context.Context, c client.Client, policyCache *cache.PolicyCache, w WorkloadAccessor, logger logr.Logger) (*AvailabilityConfig, *availabilityv1alpha1.PDBPolicy, error) {
	cacheKey := "all-policies"
	var policies []availabilityv1alpha1.PDBPolicy

	if policyCache != nil {
		if cachedPolicies, found := policyCache.GetList(cacheKey); found {
			logger.V(2).Info("Using cached policies")
			metrics.IncrementCacheHit("policy")
			policies = cachedPolicies
		} else {
			logger.V(2).Info("Policy cache miss, fetching from API")
			metrics.IncrementCacheMiss("policy")
			policyList := &availabilityv1alpha1.PDBPolicyList{}
			if err := c.List(ctx, policyList); err != nil {
				return nil, nil, err
			}
			policies = policyList.Items
			policyCache.SetList(cacheKey, policies)
		}
	} else {
		policyList := &availabilityv1alpha1.PDBPolicyList{}
		if err := c.List(ctx, policyList); err != nil {
			return nil, nil, err
		}
		policies = policyList.Items
	}

	var bestMatch *availabilityv1alpha1.PDBPolicy
	var highestPriority int32 = -1
	var matchingPolicies []*availabilityv1alpha1.PDBPolicy

	for i := range policies {
		policy := &policies[i]
		if PolicyMatchesWorkload(policy, w) {
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
		logWorkloadPolicyConflicts(w, matchingPolicies, bestMatch, logger)
	}

	componentFunction := InferWorkloadFunction(w)
	minAvailable := availabilityv1alpha1.GetMinAvailableForClass(bestMatch.Spec.AvailabilityClass, componentFunction)

	if bestMatch.Spec.AvailabilityClass == availabilityv1alpha1.Custom && bestMatch.Spec.CustomPDBConfig != nil {
		if bestMatch.Spec.CustomPDBConfig.MinAvailable != nil {
			minAvailable = *bestMatch.Spec.CustomPDBConfig.MinAvailable
		}
	}

	return &AvailabilityConfig{
		AvailabilityClass: bestMatch.Spec.AvailabilityClass,
		WorkloadFunction:  componentFunction,
		MinAvailable:      minAvailable,
		Description:       GetDescriptionForClass(bestMatch.Spec.AvailabilityClass),
		Source:            "policy",
		PolicyName:        bestMatch.Name,
		Enforcement:       string(bestMatch.Spec.GetEnforcement()),
	}, bestMatch, nil
}

func logWorkloadPolicyConflicts(w WorkloadAccessor, matchingPolicies []*availabilityv1alpha1.PDBPolicy, selected *availabilityv1alpha1.PDBPolicy, logger logr.Logger) {
	samePriorityCount := 0
	policyNames := make([]string, 0, len(matchingPolicies))
	for _, p := range matchingPolicies {
		policyNames = append(policyNames, fmt.Sprintf("%s/%s", p.Namespace, p.Name))
		if p.Spec.Priority == selected.Spec.Priority {
			samePriorityCount++
		}
	}
	metrics.RecordMultiPolicyMatch(w.GetNamespace(), len(matchingPolicies))
	if samePriorityCount > 1 {
		metrics.RecordPolicyTieBreak(w.GetNamespace(), "alphabetical")
		logger.Info("Multiple policies match at same priority, using tie-break",
			w.KindLower(), w.GetName(),
			"matchingPolicies", policyNames,
			"selectedPolicy", fmt.Sprintf("%s/%s", selected.Namespace, selected.Name),
			"tieBreakReason", "alphabetical",
		)
	} else {
		logger.V(1).Info("Multiple policies match, selected by priority",
			w.KindLower(), w.GetName(),
			"matchingPolicies", policyNames,
			"selectedPolicy", fmt.Sprintf("%s/%s", selected.Namespace, selected.Name),
			"selectedPriority", selected.Spec.Priority,
		)
	}
}

func GetAvailabilityConfigWithCache(ctx context.Context, c client.Client, policyCache *cache.PolicyCache, eventsRecorder *events.EventRecorder, w WorkloadAccessor, logger logr.Logger) (*AvailabilityConfig, error) {
	if invalidClass := DetectInvalidConfiguration(w); invalidClass != "" {
		err := fmt.Errorf("invalid availability class: %s", invalidClass)
		logger.Error(err, "Invalid configuration detected", "class", invalidClass)
		if eventsRecorder != nil {
			eventsRecorder.InvalidConfiguration(w.GetObject(), fmt.Sprintf("invalid availability class: %s", invalidClass))
		}
		return nil, err
	}
	annotationConfig := GetConfigFromAnnotations(w, logger)
	policyConfig, matchedPolicy, err := GetConfigFromPolicyWithCache(ctx, c, policyCache, w, logger)
	if err != nil {
		return nil, err
	}
	return ResolveConfiguration(w, annotationConfig, policyConfig, matchedPolicy, eventsRecorder, logger), nil
}

func ResolveConfiguration(
	w WorkloadAccessor,
	annotationConfig *AvailabilityConfig,
	policyConfig *AvailabilityConfig,
	policy *availabilityv1alpha1.PDBPolicy,
	eventsRecorder *events.EventRecorder,
	logger logr.Logger,
) *AvailabilityConfig {
	if policyConfig == nil {
		if annotationConfig != nil {
			annotationConfig.Source = "annotation-no-policy"
		}
		return annotationConfig
	}

	enforcement := policy.Spec.GetEnforcement()
	metrics.RecordEnforcementDecision(string(enforcement), "processing", w.GetNamespace())

	switch enforcement {
	case availabilityv1alpha1.EnforcementStrict:
		if annotationConfig != nil && eventsRecorder != nil {
			eventsRecorder.Warnf(w.GetObject(), "PolicyEnforced",
				"Annotation override blocked by strict policy %s", policy.Name)
			metrics.RecordOverrideAttempt("blocked", "strict-enforcement", w.GetNamespace())
		}
		policyConfig.Source = "policy-strict"
		metrics.RecordEnforcementDecision(string(enforcement), "policy-wins", w.GetNamespace())
		return policyConfig

	case availabilityv1alpha1.EnforcementFlexible:
		if annotationConfig == nil {
			policyConfig.Source = "policy-flexible"
			metrics.RecordEnforcementDecision(string(enforcement), "policy-no-annotation", w.GetNamespace())
			return policyConfig
		}
		minimumClass := policy.Spec.MinimumClass
		if minimumClass == "" {
			minimumClass = policy.Spec.AvailabilityClass
		}
		if availabilityv1alpha1.CompareAvailabilityClasses(annotationConfig.AvailabilityClass, minimumClass) >= 0 {
			metrics.RecordOverrideAttempt("accepted", "meets-minimum", w.GetNamespace())
			metrics.RecordEnforcementDecision(string(enforcement), "annotation-accepted", w.GetNamespace())
			annotationConfig.Source = "annotation-flexible"
			annotationConfig.PolicyName = policy.Name
			annotationConfig.Enforcement = string(enforcement)
			return annotationConfig
		}
		metrics.RecordOverrideAttempt("rejected", "below-minimum", w.GetNamespace())
		metrics.RecordEnforcementDecision(string(enforcement), "minimum-enforced", w.GetNamespace())
		policyConfig.AvailabilityClass = minimumClass
		policyConfig.MinAvailable = availabilityv1alpha1.GetMinAvailableForClass(minimumClass, policyConfig.WorkloadFunction)
		policyConfig.Source = "policy-flexible-minimum"
		return policyConfig

	case availabilityv1alpha1.EnforcementAdvisory:
		if annotationConfig != nil {
			if policy.Spec.AllowOverride != nil && !*policy.Spec.AllowOverride {
				metrics.RecordOverrideAttempt("blocked", "override-not-allowed", w.GetNamespace())
				policyConfig.Source = "policy-advisory-no-override"
				return policyConfig
			}
			if policy.Spec.OverrideRequiresAnnotation != "" {
				if w.GetAnnotations() == nil || w.GetAnnotations()[policy.Spec.OverrideRequiresAnnotation] == "" {
					metrics.RecordOverrideAttempt("blocked", "missing-required-annotation", w.GetNamespace())
					policyConfig.Source = "policy-advisory-missing-annotation"
					return policyConfig
				}
			}
			if policy.Spec.OverrideRequiresReason != nil && *policy.Spec.OverrideRequiresReason {
				if w.GetAnnotations() == nil || w.GetAnnotations()[AnnotationOverrideReason] == "" {
					metrics.RecordOverrideAttempt("blocked", "missing-reason", w.GetNamespace())
					policyConfig.Source = "policy-advisory-missing-reason"
					return policyConfig
				}
			}
			metrics.RecordOverrideAttempt("accepted", "advisory-mode", w.GetNamespace())
			metrics.RecordEnforcementDecision(string(enforcement), "annotation-wins", w.GetNamespace())
			annotationConfig.Source = "annotation-advisory"
			annotationConfig.PolicyName = policy.Name
			annotationConfig.Enforcement = string(enforcement)
			return annotationConfig
		}
		policyConfig.Source = "policy-advisory"
		metrics.RecordEnforcementDecision(string(enforcement), "policy-no-annotation", w.GetNamespace())
		return policyConfig

	default:
		policyConfig.Source = "policy-default"
		metrics.RecordEnforcementDecision("unknown", "defaulting-to-policy", w.GetNamespace())
		return policyConfig
	}
}

func IsInMaintenanceWindow(config *AvailabilityConfig, policyCache *cache.PolicyCache, _ logr.Logger) bool {
	if config.MaintenanceWindow == "" {
		return false
	}
	cacheKey := fmt.Sprintf("maintenance-window-%s", config.MaintenanceWindow)
	if policyCache != nil {
		if cachedResult, found := policyCache.GetMaintenanceWindow(cacheKey); found {
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
	if policyCache != nil {
		policyCache.SetMaintenanceWindow(cacheKey, result, 1*time.Minute)
	}
	return result
}

func RemovePDBTemporarily(ctx context.Context, c client.Client, w WorkloadAccessor, logger logr.Logger) error {
	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}
	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(ctx, pdbKey, pdb); err != nil {
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
	if err := c.Patch(ctx, pdb, patch); err != nil {
		return err
	}
	logger.Info("Temporarily disabled PDB for maintenance window", "pdb", pdbKey.Name)
	return nil
}

func CreatePDB(ctx context.Context, c client.Client, scheme *runtime.Scheme, eventsRecorder *events.EventRecorder, w WorkloadAccessor, config *AvailabilityConfig) (ctrl.Result, error) {
	structuredLogger := logging.NewStructuredLogger(ctx)
	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}

	labels := map[string]string{
		LabelManagedBy:         OperatorName,
		LabelAvailabilityClass: string(config.AvailabilityClass),
		LabelWorkloadFunction:  string(config.WorkloadFunction),
	}
	if w.GetAnnotations() != nil && w.GetAnnotations()[AnnotationWorkloadName] != "" {
		labels[LabelWorkload] = w.GetAnnotations()[AnnotationWorkloadName]
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
			Name:        pdbKey.Name,
			Namespace:   pdbKey.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &config.MinAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: w.GetSelector().MatchLabels},
		},
	}

	if err := ctrl.SetControllerReference(w.GetObject().(metav1.Object), pdb, scheme); err != nil {
		return ctrl.Result{}, err
	}

	if createErr := c.Create(ctx, pdb); createErr != nil {
		if eventsRecorder != nil {
			eventsRecorder.PDBCreationFailed(w.GetObject(), w.GetName(), createErr)
		}
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), false, "creation_failed")
		return ctrl.Result{}, createErr
	}

	metrics.RecordPDBCreated(w.GetNamespace(), string(config.AvailabilityClass), string(config.WorkloadFunction))
	if eventsRecorder != nil {
		eventsRecorder.PDBCreated(w.GetObject(), w.GetName(), pdbKey.Name, string(config.AvailabilityClass), config.MinAvailable.String())
	}
	logging.NewStructuredLogger(ctx).AuditStructured("CREATE", pdbKey.Name, "PodDisruptionBudget",
		w.GetNamespace(), w.GetName(), logging.AuditResultSuccess,
		map[string]interface{}{
			"availabilityClass": config.AvailabilityClass,
			"minAvailable":      config.MinAvailable.String(),
			"workloadKind":      w.Kind(),
		})
	metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), true, "created")
	structuredLogger.Info("Successfully created PDB", map[string]any{"name": pdbKey.Name})
	return ctrl.Result{}, nil
}

func UpdatePDB(ctx context.Context, c client.Client, eventsRecorder *events.EventRecorder, pdb *policyv1.PodDisruptionBudget, w WorkloadAccessor, config *AvailabilityConfig) (ctrl.Result, error) {
	oldMinAvailable := ""
	if pdb.Spec.MinAvailable != nil {
		oldMinAvailable = pdb.Spec.MinAvailable.String()
	}
	structuredLogger := logging.NewStructuredLogger(ctx)
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
	if !selectorEquals(pdb.Spec.Selector, w.GetSelector()) {
		pdb.Spec.Selector = w.GetSelector().DeepCopy()
		needsUpdate = true
	}
	if !needsUpdate {
		structuredLogger.Debug("PDB is up to date, no changes needed", nil)
		return ctrl.Result{}, nil
	}
	if err := c.Patch(ctx, pdb, patch); err != nil {
		if eventsRecorder != nil {
			eventsRecorder.PDBUpdateFailed(w.GetObject(), w.GetName(), err)
		}
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), false, "update_failed")
		return ctrl.Result{}, err
	}
	if eventsRecorder != nil {
		eventsRecorder.PDBUpdated(w.GetObject(), w.GetName(), pdb.Name, oldMinAvailable, config.MinAvailable.String())
	}
	metrics.RecordPDBUpdated(w.GetNamespace(), "config_change")
	logging.NewStructuredLogger(ctx).AuditStructured("UPDATE", pdb.Name, "PodDisruptionBudget",
		w.GetNamespace(), w.GetName(), logging.AuditResultSuccess,
		map[string]interface{}{
			"oldMinAvailable": oldMinAvailable,
			"newMinAvailable": config.MinAvailable.String(),
			"workloadKind":    w.Kind(),
		})
	structuredLogger.Info("Successfully updated PDB", map[string]any{"name": pdb.Name})
	return ctrl.Result{}, nil
}

func ReconcilePDB(ctx context.Context, c client.Client, scheme *runtime.Scheme, eventsRecorder *events.EventRecorder, w WorkloadAccessor, config *AvailabilityConfig, logger logr.Logger) (ctrl.Result, error) {
	if w.GetReplicas() <= 1 {
		logger.V(1).Info("Workload has 1 or fewer replicas, skipping PDB", "replicas", w.GetReplicas())
		if eventsRecorder != nil {
			eventsRecorder.DeploymentSkipped(w.GetObject(), w.GetName(), "insufficient replicas")
		}
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), false, "insufficient_replicas")
		if err := CleanupPDB(ctx, c, eventsRecorder, w, logger); err != nil {
			logger.Error(err, "Failed to cleanup PDB for single replica workload")
		}
		return ctrl.Result{}, nil
	}
	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}
	pdb := &policyv1.PodDisruptionBudget{}
	err := c.Get(ctx, pdbKey, pdb)
	if errors.IsNotFound(err) {
		return CreatePDB(ctx, c, scheme, eventsRecorder, w, config)
	} else if err != nil {
		logger.Error(err, "Failed to get PDB")
		return ctrl.Result{}, err
	}
	return UpdatePDB(ctx, c, eventsRecorder, pdb, w, config)
}

func CleanupPDB(ctx context.Context, c client.Client, eventsRecorder *events.EventRecorder, w WorkloadAccessor, logger logr.Logger) error {
	pdbKey := types.NamespacedName{Name: w.GetName() + DefaultPDBSuffix, Namespace: w.GetNamespace()}
	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(ctx, pdbKey, pdb); err != nil {
		if errors.IsNotFound(err) {
			logger.V(1).Info("PDB does not exist, nothing to clean up")
			return nil
		}
		return err
	}
	if ownerRef := metav1.GetControllerOf(pdb); ownerRef != nil &&
		ownerRef.Kind == w.Kind() && ownerRef.Name == w.GetName() {
		if err := c.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
			return err
		}
		metrics.RecordPDBDeleted(w.GetNamespace(), w.KindLower()+"_deleted")
		if eventsRecorder != nil {
			eventsRecorder.PDBDeleted(w.GetObject(), w.GetName(), pdbKey.Name, w.KindLower()+"_deleted")
		}
		metrics.UpdateComplianceStatus(w.GetNamespace(), w.GetName(), true, "deleted")
		logger.Info("Successfully deleted PDB", "pdb", pdbKey.Name)
	}
	return nil
}

func HandleDeletion(ctx context.Context, c client.Client, eventsRecorder *events.EventRecorder, w WorkloadAccessor, logger logr.Logger) error {
	logger.Info("Handling workload deletion", "kind", w.Kind())
	if err := CleanupPDB(ctx, c, eventsRecorder, w, logger); err != nil {
		logger.Error(err, "Failed to cleanup PDB during deletion")
		return err
	}
	obj := w.GetObject()
	if !controllerutil.ContainsFinalizer(obj, FinalizerPDBCleanup) {
		return nil
	}
	patch := client.MergeFrom(w.DeepCopyObject())
	controllerutil.RemoveFinalizer(obj, FinalizerPDBCleanup)
	if err := c.Patch(ctx, obj, patch); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		if errors.IsConflict(err) {
			return err
		}
		logger.Error(err, "Failed to remove finalizer")
		return err
	}
	logger.Info("Successfully removed finalizer", "finalizer", FinalizerPDBCleanup)
	return nil
}
