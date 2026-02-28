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

package v1alpha1

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var pdbpolicylog = logf.Log.WithName("pdbpolicy-resource")

// PDBPolicyCustomDefaulter implements admission.Defaulter
type PDBPolicyCustomDefaulter struct{}

// PDBPolicyCustomValidator implements admission.Validator
type PDBPolicyCustomValidator struct{}

var _ admission.Defaulter[*PDBPolicy] = &PDBPolicyCustomDefaulter{}
var _ admission.Validator[*PDBPolicy] = &PDBPolicyCustomValidator{}

// SetupWebhookWithManager sets up the webhook with the Manager.
func (r *PDBPolicy) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, r).
		WithDefaulter(&PDBPolicyCustomDefaulter{}).
		WithValidator(&PDBPolicyCustomValidator{}).
		Complete()
}

// Default implements admission.Defaulter
func (d *PDBPolicyCustomDefaulter) Default(ctx context.Context, r *PDBPolicy) error {
	pdbpolicylog.Info("default", "name", r.Name)

	if r.Spec.Priority == 0 {
		r.Spec.Priority = 50
	}

	if r.Spec.Enforcement == "" {
		r.Spec.Enforcement = EnforcementAdvisory
	}

	for i := range r.Spec.MaintenanceWindows {
		if r.Spec.MaintenanceWindows[i].Timezone == "" {
			r.Spec.MaintenanceWindows[i].Timezone = "UTC"
		}
		if len(r.Spec.MaintenanceWindows[i].DaysOfWeek) == 0 {
			r.Spec.MaintenanceWindows[i].DaysOfWeek = []int{0, 1, 2, 3, 4, 5, 6}
		}
	}

	if r.Spec.EnforceMinReplicas == nil {
		enforce := true
		r.Spec.EnforceMinReplicas = &enforce
	}

	if r.Spec.AllowOverride == nil {
		allowOverride := true
		if r.Spec.Enforcement == EnforcementStrict {
			allowOverride = false
		}
		r.Spec.AllowOverride = &allowOverride
	}

	if r.Spec.OverrideRequiresReason == nil {
		requiresReason := false
		r.Spec.OverrideRequiresReason = &requiresReason
	}

	if r.Spec.AvailabilityClass == Custom && r.Spec.CustomPDBConfig != nil {
		if r.Spec.CustomPDBConfig.UnhealthyPodEvictionPolicy == "" {
			r.Spec.CustomPDBConfig.UnhealthyPodEvictionPolicy = "IfHealthyBudget"
		}
	}

	if r.Spec.Enforcement == EnforcementFlexible && r.Spec.MinimumClass == "" {
		r.Spec.MinimumClass = r.Spec.AvailabilityClass
	}

	return nil
}

// ValidateCreate implements admission.Validator
func (v *PDBPolicyCustomValidator) ValidateCreate(ctx context.Context, r *PDBPolicy) (admission.Warnings, error) {
	pdbpolicylog.Info("validate create", "name", r.Name)

	var allErrs field.ErrorList
	var warnings admission.Warnings

	if err := validateAvailabilityClass(r); err != nil {
		allErrs = append(allErrs, err)
	}

	if err := validateWorkloadSelector(r); err != nil {
		allErrs = append(allErrs, err)
	}

	for i, window := range r.Spec.MaintenanceWindows {
		if errs := validateMaintenanceWindow(window, field.NewPath("spec", "maintenanceWindows").Index(i)); len(errs) > 0 {
			allErrs = append(allErrs, errs...)
		}
	}

	if r.Spec.Priority < 0 || r.Spec.Priority > 1000 {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "priority"),
			r.Spec.Priority,
			"priority must be between 0 and 1000",
		))
	}

	if errs := validateEnforcementConfiguration(r); len(errs) > 0 {
		allErrs = append(allErrs, errs...)
	}

	if r.Spec.Priority == 0 {
		warnings = append(warnings, "Priority is set to 0, which is the lowest priority. Consider setting a higher value if this policy should take precedence.")
	}

	if len(r.Spec.WorkloadSelector.Namespaces) == 0 {
		warnings = append(warnings, "No namespace selector specified. This policy will apply to all namespaces.")
	}

	if r.Spec.Enforcement == EnforcementStrict && r.Spec.AllowOverride != nil && *r.Spec.AllowOverride {
		warnings = append(warnings, "AllowOverride is true but Enforcement is strict. The strict enforcement will ignore this setting.")
	}

	if r.Spec.Enforcement == EnforcementFlexible && r.Spec.MinimumClass == "" {
		warnings = append(warnings, "Flexible enforcement without MinimumClass specified. Will default to the policy's availability class.")
	}

	if len(allErrs) == 0 {
		return warnings, nil
	}

	return warnings, allErrs.ToAggregate()
}

// ValidateUpdate implements admission.Validator
func (v *PDBPolicyCustomValidator) ValidateUpdate(ctx context.Context, oldPolicy, newPolicy *PDBPolicy) (admission.Warnings, error) {
	pdbpolicylog.Info("validate update", "name", newPolicy.Name)

	var warnings admission.Warnings

	if oldPolicy.Spec.AvailabilityClass != newPolicy.Spec.AvailabilityClass {
		warnings = append(warnings, fmt.Sprintf(
			"Changing availability class from %s to %s will affect all matching deployments",
			oldPolicy.Spec.AvailabilityClass, newPolicy.Spec.AvailabilityClass,
		))
	}

	if oldPolicy.Spec.Enforcement != newPolicy.Spec.Enforcement {
		warnings = append(warnings, fmt.Sprintf(
			"Changing enforcement mode from %s to %s may change how annotations are handled",
			oldPolicy.Spec.GetEnforcement(), newPolicy.Spec.GetEnforcement(),
		))
	}

	createWarnings, err := v.ValidateCreate(ctx, newPolicy)
	warnings = append(warnings, createWarnings...)
	return warnings, err
}

// ValidateDelete implements admission.Validator
func (v *PDBPolicyCustomValidator) ValidateDelete(ctx context.Context, r *PDBPolicy) (admission.Warnings, error) {
	pdbpolicylog.Info("validate delete", "name", r.Name)

	warnings := admission.Warnings{
		"Deleting this policy will remove PDB management from all matching deployments. Ensure alternative policies are in place if needed.",
	}

	return warnings, nil
}

// Validation helpers

func validateAvailabilityClass(r *PDBPolicy) *field.Error {
	validClasses := map[AvailabilityClass]bool{
		NonCritical:      true,
		Standard:         true,
		HighAvailability: true,
		MissionCritical:  true,
		Custom:           true,
	}

	if !validClasses[r.Spec.AvailabilityClass] {
		return field.Invalid(
			field.NewPath("spec", "availabilityClass"),
			r.Spec.AvailabilityClass,
			"must be one of: non-critical, standard, high-availability, mission-critical, custom",
		)
	}

	if r.Spec.AvailabilityClass == Custom {
		if r.Spec.CustomPDBConfig == nil {
			return field.Required(
				field.NewPath("spec", "customPDBConfig"),
				"custom availability class requires customPDBConfig",
			)
		}
		if r.Spec.CustomPDBConfig.MinAvailable == nil && r.Spec.CustomPDBConfig.MaxUnavailable == nil {
			return field.Invalid(
				field.NewPath("spec", "customPDBConfig"),
				r.Spec.CustomPDBConfig,
				"must specify either minAvailable or maxUnavailable",
			)
		}
		if r.Spec.CustomPDBConfig.MinAvailable != nil && r.Spec.CustomPDBConfig.MaxUnavailable != nil {
			return field.Invalid(
				field.NewPath("spec", "customPDBConfig"),
				r.Spec.CustomPDBConfig,
				"cannot specify both minAvailable and maxUnavailable",
			)
		}
	}

	return nil
}

func validateWorkloadSelector(r *PDBPolicy) *field.Error {
	selector := r.Spec.WorkloadSelector

	hasSelection := len(selector.WorkloadNames) > 0 ||
		len(selector.WorkloadFunctions) > 0 ||
		len(selector.MatchLabels) > 0 ||
		len(selector.MatchExpressions) > 0

	if !hasSelection {
		return field.Required(
			field.NewPath("spec", "workloadSelector"),
			"must specify at least one selection criteria (workloadNames, workloadFunctions, matchLabels, or matchExpressions)",
		)
	}

	validFunctions := map[WorkloadFunction]bool{
		CoreFunction:       true,
		ManagementFunction: true,
		SecurityFunction:   true,
	}

	for i, function := range selector.WorkloadFunctions {
		if !validFunctions[function] {
			return field.Invalid(
				field.NewPath("spec", "workloadSelector", "workloadFunctions").Index(i),
				function,
				"must be one of: core, management, security",
			)
		}
	}

	return nil
}

func validateMaintenanceWindow(window MaintenanceWindow, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if _, err := time.Parse("15:04", window.Start); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("start"), window.Start, "must be in HH:MM format"))
	}

	if _, err := time.Parse("15:04", window.End); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("end"), window.End, "must be in HH:MM format"))
	}

	if window.Timezone != "" {
		if _, err := time.LoadLocation(window.Timezone); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("timezone"), window.Timezone, fmt.Sprintf("invalid timezone: %v", err)))
		}
	}

	for i, day := range window.DaysOfWeek {
		if day < 0 || day > 6 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("daysOfWeek").Index(i), day, "must be between 0 (Sunday) and 6 (Saturday)"))
		}
	}

	daySet := make(map[int]bool)
	for _, day := range window.DaysOfWeek {
		if daySet[day] {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("daysOfWeek"), day))
		}
		daySet[day] = true
	}

	return allErrs
}

func validateEnforcementConfiguration(r *PDBPolicy) field.ErrorList {
	var allErrs field.ErrorList
	specPath := field.NewPath("spec")

	validModes := map[EnforcementMode]bool{
		EnforcementStrict:   true,
		EnforcementFlexible: true,
		EnforcementAdvisory: true,
	}

	if r.Spec.Enforcement != "" && !validModes[r.Spec.Enforcement] {
		allErrs = append(allErrs, field.Invalid(specPath.Child("enforcement"), r.Spec.Enforcement, "must be one of: strict, flexible, advisory"))
	}

	if r.Spec.Enforcement == EnforcementFlexible && r.Spec.MinimumClass != "" {
		validClasses := map[AvailabilityClass]bool{
			NonCritical: true, Standard: true, HighAvailability: true, MissionCritical: true, Custom: true,
		}
		if !validClasses[r.Spec.MinimumClass] {
			allErrs = append(allErrs, field.Invalid(specPath.Child("minimumClass"), r.Spec.MinimumClass, "must be a valid availability class"))
		}
		if CompareAvailabilityClasses(r.Spec.MinimumClass, r.Spec.AvailabilityClass) > 0 {
			allErrs = append(allErrs, field.Invalid(specPath.Child("minimumClass"), r.Spec.MinimumClass, "minimumClass cannot be higher than availabilityClass"))
		}
	}

	if r.Spec.Enforcement == EnforcementAdvisory {
		if r.Spec.OverrideRequiresAnnotation != "" && r.Spec.AllowOverride != nil && !*r.Spec.AllowOverride {
			allErrs = append(allErrs, field.Invalid(specPath.Child("overrideRequiresAnnotation"), r.Spec.OverrideRequiresAnnotation, "cannot require annotation when AllowOverride is false"))
		}
	}

	if r.Spec.Enforcement != EnforcementAdvisory {
		if r.Spec.OverrideRequiresAnnotation != "" {
			allErrs = append(allErrs, field.Invalid(specPath.Child("overrideRequiresAnnotation"), r.Spec.OverrideRequiresAnnotation, "only applicable for advisory enforcement mode"))
		}
		if r.Spec.OverrideRequiresReason != nil && *r.Spec.OverrideRequiresReason {
			allErrs = append(allErrs, field.Invalid(specPath.Child("overrideRequiresReason"), r.Spec.OverrideRequiresReason, "only applicable for advisory enforcement mode"))
		}
	}

	return allErrs
}

// HasSelectionCriteria checks if the WorkloadSelector has at least one selection criteria defined
func (s *WorkloadSelector) HasSelectionCriteria() bool {
	return len(s.WorkloadNames) > 0 ||
		len(s.WorkloadFunctions) > 0 ||
		len(s.MatchLabels) > 0 ||
		len(s.MatchExpressions) > 0
}
