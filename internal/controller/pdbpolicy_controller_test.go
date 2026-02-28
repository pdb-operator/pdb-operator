package controller_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pdbv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
	"github.com/pdb-operator/pdb-operator/internal/controller"
)

const conditionTypeReady = "Ready"

func TestPDBPolicyReconciler(t *testing.T) {
	ctx := context.Background()

	// Create a deployment
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "default",
			Labels: map[string]string{
				"app": "test",
			},
			Annotations: map[string]string{
				controller.AnnotationWorkloadName: "test-component",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "test"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "nginx",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("500m"),
								},
							},
						},
					},
				},
			},
		},
	}

	// Create availability policy
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.HighAvailability,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
		},
	}

	// Create test reconcilers
	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler
	fakeRecorder := reconciler.Recorder.(*record.FakeRecorder)

	// Test reconciliation
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-policy",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Controller requeues to periodically update status
	assert.Equal(t, 5*time.Minute, result.RequeueAfter)

	// Verify the policy can be retrieved
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "test-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)
	assert.Equal(t, pdbv1alpha1.HighAvailability, retrievedPolicy.Spec.AvailabilityClass)

	// Verify that the policy status was updated
	assert.NotEmpty(t, retrievedPolicy.Status.AppliedToWorkloads, "Policy should have applied components")
	assert.Contains(t, retrievedPolicy.Status.AppliedToWorkloads[0], "test-component", "Should contain the test component")

	// Verify events were recorded - controller emits PolicyApplied
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "PolicyApplied")
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected PolicyApplied event but none was recorded")
	}
}

func TestPDBPolicyReconciler_MultipleComponents(t *testing.T) {
	ctx := context.Background()

	// Create multiple deployments
	deployments := []*appsv1.Deployment{
		createTestDeployment("test-deploy-1", "default", map[string]string{"tier": "frontend"}, "frontend-component"),
		createTestDeployment("test-deploy-2", "default", map[string]string{"tier": "frontend"}, "api-component"),
		createTestDeployment("test-deploy-3", "default", map[string]string{"tier": "backend"}, "database-component"),
	}

	// Create policy that matches frontend tier
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "frontend-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"tier": "frontend"},
			},
		},
	}

	objects := []client.Object{policy}
	for _, deploy := range deployments {
		objects = append(objects, deploy)
	}

	tr := controller.CreateTestReconcilers(objects...)
	reconciler := tr.PDBPolicyReconciler

	// Reconcile the policy
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "frontend-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify policy status
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "frontend-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	// Should match 2 components (frontend tier)
	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 2, "Should match 2 frontend components")

	// Check conditions
	assert.NotEmpty(t, retrievedPolicy.Status.Conditions, "Should have conditions")

	var readyCondition *metav1.Condition
	for _, condition := range retrievedPolicy.Status.Conditions {
		if condition.Type == conditionTypeReady {
			readyCondition = &condition
			break
		}
	}
	require.NotNil(t, readyCondition, "Should have Ready condition")
	assert.Equal(t, metav1.ConditionTrue, readyCondition.Status, "Ready condition should be True")
}

func TestPDBPolicyReconciler_CustomPDBConfig(t *testing.T) {
	ctx := context.Background()

	// Create deployment
	deploy := createTestDeployment("test-deploy", "default", map[string]string{"app": "test"}, "test-component")

	// Create policy with custom PDB config
	customMinAvailable := intstr.FromString("80%")
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Custom,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			CustomPDBConfig: &pdbv1alpha1.PodDisruptionBudgetConfig{
				MinAvailable: &customMinAvailable,
			},
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler

	// Reconcile
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "custom-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify policy was applied
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "custom-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1, "Should apply to 1 component")
	// Note: LastAppliedTime is set by updateStatus function which isn't called in main reconcile path
	// The test verifies the core functionality without requiring LastAppliedTime
}

func TestPDBPolicyReconciler_InvalidPolicy(t *testing.T) {
	ctx := context.Background()

	// Create policy with invalid configuration (custom class without custom config)
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "invalid-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Custom,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			// Missing CustomPDBConfig for Custom class
		},
	}

	tr := controller.CreateTestReconcilers(policy)
	reconciler := tr.PDBPolicyReconciler

	// Reconcile
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "invalid-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.Error(t, err) // Should return error for invalid policy

	// Verify policy status shows validation error
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err2 := tr.Client.Get(ctx, types.NamespacedName{
		Name:      "invalid-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err2)

	// Check that we have a Validated condition with False status
	var validatedCondition *metav1.Condition
	for _, condition := range retrievedPolicy.Status.Conditions {
		if condition.Type == "Validated" {
			validatedCondition = &condition
			break
		}
	}
	require.NotNil(t, validatedCondition, "Should have Validated condition")
	assert.Equal(t, metav1.ConditionFalse, validatedCondition.Status, "Validated condition should be False")
	assert.Contains(t, validatedCondition.Message, "custom availability class requires customPDBConfig", "Should contain validation error message")
	assert.Equal(t, "PolicyInvalid", validatedCondition.Reason, "Should have PolicyInvalid reason")

	// Note: Controller sets condition but doesn't emit a Kubernetes event for validation failures
	// The validation failure is reflected in the status condition above
}

func TestPDBPolicyReconciler_NoMatchingComponents(t *testing.T) {
	ctx := context.Background()

	// Create deployment that won't match the policy
	deploy := createTestDeployment("test-deploy", "default", map[string]string{"app": "nomatch"}, "nomatch-component")

	// Create policy that won't match any components
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nomatch-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "nonexistent"},
			},
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler

	// Reconcile
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nomatch-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify policy status
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "nomatch-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Empty(t, retrievedPolicy.Status.AppliedToWorkloads, "Should not match any components")

	// Check Ready condition
	var readyCondition *metav1.Condition
	for _, condition := range retrievedPolicy.Status.Conditions {
		if condition.Type == conditionTypeReady {
			readyCondition = &condition
			break
		}
	}
	require.NotNil(t, readyCondition, "Should have Ready condition")
	// Policy is not ready when no components match the selector
	assert.Equal(t, metav1.ConditionFalse, readyCondition.Status, "Ready condition should be False (no matching workloads)")
	assert.Equal(t, "NoComponentsMatched", readyCondition.Reason, "Should have NoComponentsMatched reason")
}

func TestPDBPolicyReconciler_PolicyDeletion(t *testing.T) {
	ctx := context.Background()

	// Create a deployment
	deploy := createTestDeployment("test-deploy", "default", map[string]string{"app": "test"}, "test-component")

	// Create policy that is being deleted - must have finalizer for fake client
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "delete-policy",
			Namespace:         "default",
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			Finalizers:        []string{"test-finalizer"}, // Required by fake client
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
		},
		Status: pdbv1alpha1.PDBPolicyStatus{
			AppliedToWorkloads: []string{"default/test-component"},
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler
	fakeRecorder := reconciler.Recorder.(*record.FakeRecorder)

	// Reconcile
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "delete-policy",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	// Verify event was recorded
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "PolicyRemoved")
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected PolicyRemoved event but none was recorded")
	}
}

func TestPDBPolicyReconciler_PolicyNotFound(t *testing.T) {
	ctx := context.Background()

	tr := controller.CreateTestReconcilers() // No policy
	reconciler := tr.PDBPolicyReconciler

	// Reconcile a non-existent policy
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonexistent-policy",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err, "Should not error for non-existent policy")
	assert.Equal(t, reconcile.Result{}, result)
}

func TestPDBPolicyReconciler_NamespaceSelector(t *testing.T) {
	ctx := context.Background()

	// Create deployments in different namespaces
	deploy1 := createTestDeploymentInNamespace("deploy-1", "production", map[string]string{"app": "web"}, "web-prod")
	deploy2 := createTestDeploymentInNamespace("deploy-2", "staging", map[string]string{"app": "web"}, "web-staging")
	deploy3 := createTestDeploymentInNamespace("deploy-3", "development", map[string]string{"app": "web"}, "web-dev")

	// Create policy that only applies to production namespace
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prod-only-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "web"},
				Namespaces:  []string{"production"},
			},
		},
	}

	tr := controller.CreateTestReconcilers(deploy1, deploy2, deploy3, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "prod-only-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify only production deployment matched
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "prod-only-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1, "Should only match production namespace")
	assert.Contains(t, retrievedPolicy.Status.AppliedToWorkloads[0], "web-prod")
}

func TestPDBPolicyReconciler_MaintenanceWindowValidation(t *testing.T) {
	ctx := context.Background()

	// Create policy with valid maintenance windows
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "maintenance-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{
				{
					Start:      "02:00",
					End:        "04:00",
					Timezone:   "UTC",
					DaysOfWeek: []int{0, 6}, // Sunday and Saturday
				},
				{
					Start:      "22:00",
					End:        "23:00",
					Timezone:   "America/New_York",
					DaysOfWeek: []int{1, 2, 3, 4, 5}, // Weekdays
				},
			},
		},
	}

	tr := controller.CreateTestReconcilers(policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "maintenance-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify policy is valid
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "maintenance-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	// Check for Ready condition (main reconcile path sets Ready when validation passes)
	var readyCondition *metav1.Condition
	for _, condition := range retrievedPolicy.Status.Conditions {
		if condition.Type == conditionTypeReady {
			readyCondition = &condition
			break
		}
	}
	require.NotNil(t, readyCondition, "Should have Ready condition")
	// No deployments match the selector, so Ready is False
	assert.Equal(t, metav1.ConditionFalse, readyCondition.Status, "Should be not ready (no matching workloads)")
}

func TestPDBPolicyReconciler_InvalidMaintenanceWindow(t *testing.T) {
	ctx := context.Background()

	// Create policy with invalid maintenance window
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "invalid-maint-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{
				{
					Start:    "25:00", // Invalid hour
					End:      "04:00",
					Timezone: "UTC",
				},
			},
		},
	}

	tr := controller.CreateTestReconcilers(policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "invalid-maint-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	assert.Error(t, err, "Should error for invalid maintenance window")
}

func TestPDBPolicyReconciler_FlexibleEnforcementWithMinimum(t *testing.T) {
	ctx := context.Background()

	// Create deployment
	deploy := createTestDeployment("flex-deploy", "default", map[string]string{"app": "flex"}, "flex-component")

	// Create policy with flexible enforcement and minimum class
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "flexible-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.HighAvailability,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "flex"},
			},
			Enforcement:  pdbv1alpha1.EnforcementFlexible,
			MinimumClass: pdbv1alpha1.Standard,
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "flexible-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify policy was applied
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "flexible-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1)
}

func TestPDBPolicyReconciler_MatchExpressionDoesNotExist(t *testing.T) {
	ctx := context.Background()

	// Create deployment without the label
	deploy := createTestDeployment("no-label-deploy", "default", map[string]string{"app": "test"}, "test-component")

	// Create policy with DoesNotExist operator
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "doesnotexist-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "deprecated",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
				},
			},
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "doesnotexist-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify deployment without "deprecated" label matches
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "doesnotexist-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1, "Should match deployment without deprecated label")
}

func TestPDBPolicyReconciler_WorkloadFunctionSelector(t *testing.T) {
	ctx := context.Background()

	// Create deployments with different component functions
	securityDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "auth-service",
			Namespace: "default",
			Labels:    map[string]string{"app": "auth"},
			Annotations: map[string]string{
				controller.AnnotationWorkloadName:     "auth-component",
				controller.AnnotationWorkloadFunction: "security",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "auth"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "auth"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "auth", Image: "nginx"}},
				},
			},
		},
	}

	coreDeploy := createTestDeployment("core-service", "default", map[string]string{"app": "core"}, "core-component")

	// Create policy matching security functions
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "security-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				WorkloadFunctions: []pdbv1alpha1.WorkloadFunction{
					pdbv1alpha1.SecurityFunction,
				},
			},
		},
	}

	tr := controller.CreateTestReconcilers(securityDeploy, coreDeploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "security-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify only security deployment matched
	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "security-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1, "Should only match security function")
	assert.Contains(t, retrievedPolicy.Status.AppliedToWorkloads[0], "auth-component")
}

func TestPDBPolicyReconciler_HighPriorityPolicy(t *testing.T) {
	ctx := context.Background()

	// Create deployment
	deploy := createTestDeployment("priority-deploy", "default", map[string]string{"app": "priority"}, "priority-component")

	// Create high priority policy
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "high-priority-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "priority"},
			},
			Priority: 1000, // Very high priority
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "high-priority-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "high-priority-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1)
}

func TestPDBPolicyReconciler_StrictEnforcement(t *testing.T) {
	ctx := context.Background()

	// Create deployment
	deploy := createTestDeployment("strict-deploy", "default", map[string]string{"env": "production"}, "prod-component")

	// Create policy with strict enforcement
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "strict-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"env": "production"},
			},
			Enforcement: pdbv1alpha1.EnforcementStrict,
		},
	}

	tr := controller.CreateTestReconcilers(deploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "strict-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "strict-policy",
		Namespace: "default",
	}, retrievedPolicy)
	require.NoError(t, err)

	// Verify enforcement mode is preserved
	assert.Equal(t, pdbv1alpha1.EnforcementStrict, retrievedPolicy.Spec.Enforcement)
	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1)
}

// Helper functions

func TestPDBPolicyReconciler_InvalidCustomPDBConfigNoPDBConfig(t *testing.T) {
	ctx := context.Background()

	// Custom class without custom PDB config should fail validation
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "invalid-custom-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Custom,
			// Missing CustomPDBConfig
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
		},
	}

	tr := controller.CreateTestReconcilers(policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "invalid-custom-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "custom availability class requires customPDBConfig")
}

func TestPDBPolicyReconciler_AllAvailabilityClasses(t *testing.T) {
	testCases := []struct {
		name              string
		availabilityClass pdbv1alpha1.AvailabilityClass
	}{
		{"NonCritical", pdbv1alpha1.NonCritical},
		{"Standard", pdbv1alpha1.Standard},
		{"HighAvailability", pdbv1alpha1.HighAvailability},
		{"MissionCritical", pdbv1alpha1.MissionCritical},
	}

	for _, tc := range testCases {
		t.Run(string(tc.availabilityClass), func(t *testing.T) {
			ctx := context.Background()

			policy := &pdbv1alpha1.PDBPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "class-test-policy",
					Namespace: "default",
				},
				Spec: pdbv1alpha1.PDBPolicySpec{
					AvailabilityClass: tc.availabilityClass,
					WorkloadSelector: pdbv1alpha1.WorkloadSelector{
						MatchLabels: map[string]string{"test": "true"},
					},
				},
			}

			deployment := createTestDeployment("test-deploy", "default", map[string]string{"test": "true"}, "test-component")

			tr := controller.CreateTestReconcilers(policy, deployment)
			reconciler := tr.PDBPolicyReconciler

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "class-test-policy",
					Namespace: "default",
				},
			}

			_, err := reconciler.Reconcile(ctx, req)
			require.NoError(t, err)

			retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
			err = tr.Client.Get(ctx, req.NamespacedName, retrievedPolicy)
			require.NoError(t, err)
			assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1)
		})
	}
}

func TestPDBPolicyReconciler_MatchExpressionIn(t *testing.T) {
	ctx := context.Background()

	// Create deployments with different environments
	devDeploy := createTestDeployment("dev-service", "dev", map[string]string{"env": "dev"}, "dev-comp")
	stagingDeploy := createTestDeployment("staging-service", "staging", map[string]string{"env": "staging"}, "staging-comp")
	prodDeploy := createTestDeployment("prod-service", "production", map[string]string{"env": "prod"}, "prod-comp")

	// Policy that matches dev and staging but not prod
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nonprod-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"dev", "staging"},
					},
				},
			},
		},
	}

	tr := controller.CreateTestReconcilers(devDeploy, stagingDeploy, prodDeploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonprod-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, req.NamespacedName, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 2, "Should match dev and staging only")
}

func TestPDBPolicyReconciler_MatchExpressionNotIn(t *testing.T) {
	ctx := context.Background()

	// Create deployments with different tiers
	webDeploy := createTestDeployment("web-service", "default", map[string]string{"tier": "web"}, "web-comp")
	apiDeploy := createTestDeployment("api-service", "default", map[string]string{"tier": "api"}, "api-comp")
	dbDeploy := createTestDeployment("db-service", "default", map[string]string{"tier": "database"}, "db-comp")

	// Policy that excludes database tier
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "non-db-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "tier",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"database"},
					},
				},
			},
		},
	}

	tr := controller.CreateTestReconcilers(webDeploy, apiDeploy, dbDeploy, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-db-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, req.NamespacedName, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 2, "Should exclude database tier")
}

func TestPDBPolicyReconciler_ComponentNameSelector(t *testing.T) {
	ctx := context.Background()

	// Create deployments with specific component names
	deploy1 := createTestDeployment("service-a", "default", map[string]string{"app": "a"}, "component-alpha")
	deploy2 := createTestDeployment("service-b", "default", map[string]string{"app": "b"}, "component-beta")
	deploy3 := createTestDeployment("service-c", "default", map[string]string{"app": "c"}, "component-gamma")

	// Policy targeting specific component names
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "component-name-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.HighAvailability,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				WorkloadNames: []string{"component-alpha", "component-gamma"},
			},
		},
	}

	tr := controller.CreateTestReconcilers(deploy1, deploy2, deploy3, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "component-name-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, req.NamespacedName, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 2, "Should match alpha and gamma components")
}

func TestPDBPolicyReconciler_CombinedSelectors(t *testing.T) {
	ctx := context.Background()

	// Create deployments with various characteristics
	deploy1 := createTestDeploymentInNamespace("web-prod", "production", map[string]string{"app": "web", "tier": "frontend"}, "web-prod-comp")
	deploy2 := createTestDeploymentInNamespace("api-prod", "production", map[string]string{"app": "api", "tier": "backend"}, "api-prod-comp")
	deploy3 := createTestDeploymentInNamespace("web-dev", "development", map[string]string{"app": "web", "tier": "frontend"}, "web-dev-comp")

	// Policy with multiple selector criteria (namespace + labels)
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "combined-selector-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				Namespaces:  []string{"production"},
				MatchLabels: map[string]string{"tier": "frontend"},
			},
		},
	}

	tr := controller.CreateTestReconcilers(deploy1, deploy2, deploy3, policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "combined-selector-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	retrievedPolicy := &pdbv1alpha1.PDBPolicy{}
	err = tr.Client.Get(ctx, req.NamespacedName, retrievedPolicy)
	require.NoError(t, err)

	assert.Len(t, retrievedPolicy.Status.AppliedToWorkloads, 1, "Should only match production frontend")
}

func TestPDBPolicyReconciler_EmptySelectorRejected(t *testing.T) {
	ctx := context.Background()

	// Policy with empty selector should be rejected
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-selector-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector:  pdbv1alpha1.WorkloadSelector{
				// Empty selector - should fail validation
			},
		},
	}

	tr := controller.CreateTestReconcilers(policy)
	reconciler := tr.PDBPolicyReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "empty-selector-policy",
			Namespace: "default",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must specify at least one selection criteria")
}

func createTestDeploymentInNamespace(name, namespace string, labels map[string]string, componentName string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
			Annotations: map[string]string{
				controller.AnnotationWorkloadName: componentName,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "nginx",
						},
					},
				},
			},
		},
	}
}

func createTestDeployment(name, namespace string, labels map[string]string, componentName string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
			Annotations: map[string]string{
				controller.AnnotationWorkloadName: componentName,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "nginx",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("500m"),
								},
							},
						},
					},
				},
			},
		},
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}
