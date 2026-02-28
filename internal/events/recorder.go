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

package events

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

const (
	// Event reasons for PDB lifecycle
	ReasonPDBCreated        = "PDBCreated"
	ReasonPDBUpdated        = "PDBUpdated"
	ReasonPDBDeleted        = "PDBDeleted"
	ReasonPDBCreationFailed = "PDBCreationFailed"
	ReasonPDBUpdateFailed   = "PDBUpdateFailed"
	ReasonPDBDeletionFailed = "PDBDeletionFailed"

	// Event reasons for configuration
	ReasonInvalidConfig      = "InvalidConfiguration"
	ReasonConfigurationError = "ConfigurationError"
	ReasonMaintenanceWindow  = "MaintenanceWindowActive"

	// Event reasons for policies
	ReasonPolicyApplied   = "PolicyApplied"
	ReasonPolicyRemoved   = "PolicyRemoved"
	ReasonPolicyUpdated   = "PolicyUpdated"
	ReasonPolicyValidated = "PolicyValidated"
	ReasonPolicyInvalid   = "PolicyInvalid"

	// Event reasons for deployments
	ReasonDeploymentManaged   = "DeploymentManaged"
	ReasonDeploymentUnmanaged = "DeploymentUnmanaged"
	ReasonDeploymentSkipped   = "DeploymentSkipped"

	// Event reasons for compliance
	ReasonComplianceAchieved = "ComplianceAchieved"
	ReasonComplianceLost     = "ComplianceLost"
)

// EventRecorder wraps the Kubernetes event recorder with domain-specific methods
type EventRecorder struct {
	recorder record.EventRecorder
}

// NewEventRecorder creates a new event recorder
func NewEventRecorder(recorder record.EventRecorder) *EventRecorder {
	return &EventRecorder{recorder: recorder}
}

// PDB Lifecycle Events

func (e *EventRecorder) PDBCreated(obj runtime.Object, deployment, pdbName, availabilityClass string, minAvailable string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPDBCreated,
		"Created PodDisruptionBudget %s for deployment %s (class: %s, minAvailable: %s)",
		pdbName, deployment, availabilityClass, minAvailable)
}

func (e *EventRecorder) PDBUpdated(obj runtime.Object, deployment, pdbName, oldMinAvailable, newMinAvailable string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPDBUpdated,
		"Updated PodDisruptionBudget %s for deployment %s (minAvailable: %s -> %s)",
		pdbName, deployment, oldMinAvailable, newMinAvailable)
}

func (e *EventRecorder) PDBDeleted(obj runtime.Object, deployment, pdbName, reason string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPDBDeleted,
		"Deleted PodDisruptionBudget %s for deployment %s (reason: %s)",
		pdbName, deployment, reason)
}

func (e *EventRecorder) PDBCreationFailed(obj runtime.Object, deployment string, err error) {
	if obj != nil {
		e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonPDBCreationFailed,
			"Failed to create PodDisruptionBudget for deployment %s: %v", deployment, err)
	}
}

func (e *EventRecorder) PDBUpdateFailed(obj runtime.Object, deployment string, err error) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonPDBUpdateFailed,
		"Failed to update PodDisruptionBudget for deployment %s: %v", deployment, err)
}

func (e *EventRecorder) PDBDeletionFailed(obj runtime.Object, deployment string, err error) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonPDBDeletionFailed,
		"Failed to delete PodDisruptionBudget for deployment %s: %v", deployment, err)
}

// Configuration Events

func (e *EventRecorder) InvalidConfiguration(obj runtime.Object, reason string) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonInvalidConfig,
		"Invalid configuration: %s", reason)
}

func (e *EventRecorder) ConfigurationError(obj runtime.Object, configType string, err error) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonConfigurationError,
		"Configuration error for %s: %v", configType, err)
}

func (e *EventRecorder) MaintenanceWindowActive(obj runtime.Object, deployment, window string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonMaintenanceWindow,
		"Deployment %s is in maintenance window (%s), PDB enforcement suspended",
		deployment, window)
}

// Policy Events

func (e *EventRecorder) PolicyApplied(obj runtime.Object, policyName string, workloadsCount int) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPolicyApplied,
		"PDBPolicy %s applied to %d workloads", policyName, workloadsCount)
}

func (e *EventRecorder) PolicyRemoved(obj runtime.Object, policyName string, workloadsCount int) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPolicyRemoved,
		"PDBPolicy %s removed from %d workloads", policyName, workloadsCount)
}

func (e *EventRecorder) PolicyUpdated(obj runtime.Object, policyName string, changes string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPolicyUpdated,
		"PDBPolicy %s updated: %s", policyName, changes)
}

func (e *EventRecorder) PolicyValidated(obj runtime.Object, policyName string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonPolicyValidated,
		"PDBPolicy %s validated successfully", policyName)
}

func (e *EventRecorder) PolicyInvalid(obj runtime.Object, policyName string, reason string) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonPolicyInvalid,
		"PDBPolicy %s validation failed: %s", policyName, reason)
}

// Deployment Events

func (e *EventRecorder) DeploymentManaged(obj runtime.Object, deployment string, source string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonDeploymentManaged,
		"Deployment %s is now managed for PDB (source: %s)", deployment, source)
}

func (e *EventRecorder) DeploymentUnmanaged(obj runtime.Object, deployment string, reason string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonDeploymentUnmanaged,
		"Deployment %s is no longer managed for PDB: %s", deployment, reason)
}

func (e *EventRecorder) DeploymentSkipped(obj runtime.Object, deployment string, reason string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonDeploymentSkipped,
		"Deployment %s skipped for PDB management: %s", deployment, reason)
}

// Compliance Events

func (e *EventRecorder) ComplianceAchieved(obj runtime.Object, deployment string, details string) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, ReasonComplianceAchieved,
		"Deployment %s achieved PDB compliance: %s", deployment, details)
}

func (e *EventRecorder) ComplianceLost(obj runtime.Object, deployment string, reason string) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, ReasonComplianceLost,
		"Deployment %s lost PDB compliance: %s", deployment, reason)
}

// Generic event helpers

func (e *EventRecorder) Info(obj runtime.Object, reason, message string) {
	e.recorder.Event(obj, corev1.EventTypeNormal, reason, message)
}

func (e *EventRecorder) Warn(obj runtime.Object, reason, message string) {
	e.recorder.Event(obj, corev1.EventTypeWarning, reason, message)
}

func (e *EventRecorder) Error(obj runtime.Object, reason string, err error) {
	if obj == nil {
		return
	}
	e.recorder.Eventf(obj, corev1.EventTypeWarning, reason, "Error: %v", err)
}

func (e *EventRecorder) SafeEventf(obj runtime.Object, eventType string, reason, format string, args ...interface{}) {
	if obj == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			_ = r
		}
	}()

	e.recorder.Eventf(obj, eventType, reason, format, args...)
}

func (e *EventRecorder) Infof(obj runtime.Object, reason, format string, args ...interface{}) {
	e.recorder.Eventf(obj, corev1.EventTypeNormal, reason, format, args...)
}

func (e *EventRecorder) Warnf(obj runtime.Object, reason, format string, args ...interface{}) {
	e.recorder.Eventf(obj, corev1.EventTypeWarning, reason, format, args...)
}
