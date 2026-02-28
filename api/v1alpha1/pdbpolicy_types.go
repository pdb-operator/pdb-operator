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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// AvailabilityClass defines the level of availability required for a workload.
// +kubebuilder:validation:Enum=non-critical;standard;high-availability;mission-critical;custom
type AvailabilityClass string

const (
	// NonCritical allows frequent disruptions (20% minimum available).
	NonCritical AvailabilityClass = "non-critical"
	// Standard allows controlled disruptions (50% minimum available).
	Standard AvailabilityClass = "standard"
	// HighAvailability limits disruptions (75% minimum available).
	HighAvailability AvailabilityClass = "high-availability"
	// MissionCritical allows almost no disruptions (90% minimum available).
	MissionCritical AvailabilityClass = "mission-critical"
	// Custom allows for custom PDB configuration.
	Custom AvailabilityClass = "custom"
)

// EnforcementMode defines how strictly the policy is enforced.
// +kubebuilder:validation:Enum=strict;flexible;advisory
type EnforcementMode string

const (
	// EnforcementStrict means policy cannot be overridden by annotations.
	EnforcementStrict EnforcementMode = "strict"
	// EnforcementFlexible allows annotations to increase but not decrease availability.
	EnforcementFlexible EnforcementMode = "flexible"
	// EnforcementAdvisory means annotations can freely override (default behavior).
	EnforcementAdvisory EnforcementMode = "advisory"
)

// WorkloadFunction defines the function of the workload.
// +kubebuilder:validation:Enum=core;management;security
type WorkloadFunction string

const (
	// CoreFunction represents business logic and API workloads.
	CoreFunction WorkloadFunction = "core"
	// ManagementFunction represents operators, controllers, and admin interfaces.
	ManagementFunction WorkloadFunction = "management"
	// SecurityFunction represents authentication, authorization, and security services.
	SecurityFunction WorkloadFunction = "security"
)

// MaintenanceWindow defines a time window when disruptions are allowed.
type MaintenanceWindow struct {
	// Start time in HH:MM format.
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	Start string `json:"start"`

	// End time in HH:MM format.
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	End string `json:"end"`

	// Timezone (defaults to UTC).
	// +kubebuilder:default="UTC"
	// +optional
	Timezone string `json:"timezone,omitempty"`

	// Days of week when maintenance window applies (0=Sunday, 6=Saturday).
	// +optional
	DaysOfWeek []int `json:"daysOfWeek,omitempty"`
}

// PodDisruptionBudgetConfig defines custom PDB configuration.
type PodDisruptionBudgetConfig struct {
	// MinAvailable specifies the minimum number of pods that must be available.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`

	// MaxUnavailable specifies the maximum number of pods that can be unavailable.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// UnhealthyPodEvictionPolicy defines how to deal with unhealthy pods.
	// +kubebuilder:validation:Enum=IfHealthyBudget;AlwaysAllow
	// +kubebuilder:default="IfHealthyBudget"
	// +optional
	UnhealthyPodEvictionPolicy string `json:"unhealthyPodEvictionPolicy,omitempty"`
}

// WorkloadSelector defines how to select workloads for this policy.
type WorkloadSelector struct {
	// MatchLabels selects workloads with matching labels.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`

	// MatchExpressions selects workloads using label expressions.
	// +optional
	MatchExpressions []metav1.LabelSelectorRequirement `json:"matchExpressions,omitempty"`

	// WorkloadNames specifies explicit workload names.
	// +optional
	WorkloadNames []string `json:"workloadNames,omitempty"`

	// WorkloadFunctions specifies workload functions to match.
	// +optional
	WorkloadFunctions []WorkloadFunction `json:"workloadFunctions,omitempty"`

	// Namespaces specifies which namespaces to watch (empty means all).
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`
}

// PDBPolicySpec defines the desired state of PDBPolicy.
type PDBPolicySpec struct {
	// AvailabilityClass defines the level of availability required.
	AvailabilityClass AvailabilityClass `json:"availabilityClass"`

	// WorkloadSelector defines which workloads this policy applies to.
	WorkloadSelector WorkloadSelector `json:"workloadSelector"`

	// MaintenanceWindows defines when disruptions are allowed.
	// +optional
	MaintenanceWindows []MaintenanceWindow `json:"maintenanceWindows,omitempty"`

	// CustomPDBConfig allows custom PDB configuration when AvailabilityClass is "custom".
	// +optional
	CustomPDBConfig *PodDisruptionBudgetConfig `json:"customPDBConfig,omitempty"`

	// EnforceMinReplicas ensures deployments have minimum replicas for PDB to be effective.
	// +kubebuilder:default=true
	// +optional
	EnforceMinReplicas *bool `json:"enforceMinReplicas,omitempty"`

	// Priority defines the priority of this policy (higher number = higher priority).
	// +kubebuilder:default=0
	// +optional
	Priority int32 `json:"priority,omitempty"`

	// Enforcement defines how strictly this policy is enforced.
	// +kubebuilder:default=advisory
	// +optional
	Enforcement EnforcementMode `json:"enforcement,omitempty"`

	// MinimumClass defines the minimum availability class allowed when enforcement is flexible.
	// +optional
	MinimumClass AvailabilityClass `json:"minimumClass,omitempty"`

	// AllowOverride allows annotations to override when true (only for advisory mode).
	// +kubebuilder:default=true
	// +optional
	AllowOverride *bool `json:"allowOverride,omitempty"`

	// OverrideRequiresAnnotation specifies an annotation that must be present to allow override.
	// +optional
	OverrideRequiresAnnotation string `json:"overrideRequiresAnnotation,omitempty"`

	// OverrideRequiresReason requires a reason annotation when overriding.
	// +kubebuilder:default=false
	// +optional
	OverrideRequiresReason *bool `json:"overrideRequiresReason,omitempty"`
}

// GetEnforcement returns the enforcement mode with default handling.
func (spec *PDBPolicySpec) GetEnforcement() EnforcementMode {
	if spec.Enforcement == "" {
		return EnforcementAdvisory
	}
	return spec.Enforcement
}

// PDBPolicyStatus defines the observed state of PDBPolicy.
type PDBPolicyStatus struct {
	// Conditions represent the latest available observations of the policy's current state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// AppliedToWorkloads lists the workloads this policy is currently applied to.
	// +optional
	AppliedToWorkloads []string `json:"appliedToWorkloads,omitempty"`

	// PDBsManaged lists the PDBs currently managed by this policy.
	// +optional
	PDBsManaged []string `json:"pdbsManaged,omitempty"`

	// LastAppliedTime indicates when this policy was last successfully applied.
	// +optional
	LastAppliedTime *metav1.Time `json:"lastAppliedTime,omitempty"`

	// ObservedGeneration reflects the generation of the most recently observed PDBPolicy.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// PDBPolicy is the Schema for the pdbpolicies API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pdbp,categories=pdb-operator
// +kubebuilder:printcolumn:name="Availability Class",type=string,JSONPath=`.spec.availabilityClass`
// +kubebuilder:printcolumn:name="Enforcement",type=string,JSONPath=`.spec.enforcement`
// +kubebuilder:printcolumn:name="Priority",type=integer,JSONPath=`.spec.priority`
// +kubebuilder:printcolumn:name="Workloads",type=integer,JSONPath=`.status.appliedToWorkloads[*]`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PDBPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PDBPolicySpec   `json:"spec,omitempty"`
	Status PDBPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PDBPolicyList contains a list of PDBPolicy.
type PDBPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PDBPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PDBPolicy{}, &PDBPolicyList{})
}

// GetMinAvailableForClass returns the default minAvailable value for a given availability class.
func GetMinAvailableForClass(class AvailabilityClass, function WorkloadFunction) intstr.IntOrString {
	baseValues := map[AvailabilityClass]string{
		NonCritical:      "20%",
		Standard:         "50%",
		HighAvailability: "75%",
		MissionCritical:  "90%",
	}

	baseValue := baseValues[class]
	if baseValue == "" {
		baseValue = "50%" // default to standard
	}

	// Apply function-specific adjustments for security workloads
	if function == SecurityFunction {
		switch class {
		case NonCritical:
			baseValue = "50%" // upgrade non-critical security to standard
		case Standard:
			baseValue = "75%" // upgrade standard security to high-availability
		}
	}

	return intstr.FromString(baseValue)
}

// CompareAvailabilityClasses compares two availability classes.
// Returns: negative if a < b, 0 if a == b, positive if a > b.
func CompareAvailabilityClasses(a, b AvailabilityClass) int {
	order := map[AvailabilityClass]int{
		NonCritical:      1,
		Standard:         2,
		HighAvailability: 3,
		MissionCritical:  4,
		Custom:           5,
	}

	aOrder, aOk := order[a]
	bOrder, bOk := order[b]

	if !aOk {
		aOrder = 0
	}
	if !bOk {
		bOrder = 0
	}

	return aOrder - bOrder
}
