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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pdbv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
)

// newTestStatefulSet is a helper to build a basic StatefulSet for tests.
//
//nolint:unparam // namespace parameter allows tests to use non-default namespaces
func newTestStatefulSet(name, namespace string, replicas int32, annotations, labels map[string]string) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "app", Image: "nginx"},
					},
				},
			},
		},
	}
	return sts
}

func TestStatefulSetReconciler_AnnotationBasedPDB(t *testing.T) {
	ctx := context.Background()

	sts := newTestStatefulSet("annotation-sts", "default", 3, map[string]string{
		AnnotationAvailabilityClass: "high-availability",
		AnnotationWorkloadFunction:  "core",
		AnnotationWorkloadName:      "my-statefulset",
	}, nil)

	tr := CreateTestReconcilers(sts)
	reconciler := tr.StatefulSetReconciler
	fakeRecorder := reconciler.Recorder.(*k8sevents.FakeRecorder)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "annotation-sts", Namespace: "default"},
	}

	// First reconciliation adds finalizer
	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{Requeue: true}, result)

	updatedSts := &appsv1.StatefulSet{}
	err = tr.Client.Get(ctx, req.NamespacedName, updatedSts)
	require.NoError(t, err)
	assert.Contains(t, updatedSts.Finalizers, FinalizerPDBCleanup, "Finalizer should be added")

	// Second reconciliation creates PDB
	_, err = reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "annotation-sts-pdb",
		Namespace: "default",
	}, pdb)
	assert.NoError(t, err, "PDB should be created")
	assert.Equal(t, "75%", pdb.Spec.MinAvailable.String(), "high-availability should be 75%")
	assert.Equal(t, map[string]string{"app": "annotation-sts"}, pdb.Spec.Selector.MatchLabels)

	// Verify PDB metadata
	assert.Equal(t, OperatorName, pdb.Labels[LabelManagedBy])
	assert.Equal(t, "my-statefulset", pdb.Labels[LabelWorkload])
	assert.Equal(t, "core", pdb.Labels[LabelWorkloadFunction])
	assert.Equal(t, OperatorName, pdb.Annotations[AnnotationCreatedBy])
	assert.Equal(t, "high-availability", pdb.Annotations[AnnotationAvailabilityClass])

	// Verify owner reference points to StatefulSet
	require.Len(t, pdb.OwnerReferences, 1)
	assert.Equal(t, "StatefulSet", pdb.OwnerReferences[0].Kind)
	assert.Equal(t, "annotation-sts", pdb.OwnerReferences[0].Name)

	// Verify PDBCreated event
	var recordedEvents []string
	timeout := time.After(200 * time.Millisecond)
eventLoop:
	for i := 0; i < 2; i++ {
		select {
		case event := <-fakeRecorder.Events:
			recordedEvents = append(recordedEvents, event)
		case <-timeout:
			break eventLoop
		}
	}
	foundPDBCreated := false
	for _, e := range recordedEvents {
		if contains(e, "PDBCreated") {
			foundPDBCreated = true
			break
		}
	}
	assert.True(t, foundPDBCreated, "Should have PDBCreated event")
}

func TestStatefulSetReconciler_PolicyBasedPDB(t *testing.T) {
	ctx := context.Background()

	sts := newTestStatefulSet("policy-sts", "default", 3, map[string]string{
		AnnotationWorkloadName: "mysql-cluster",
	}, map[string]string{
		"tier": "database",
		"app":  "mysql",
	})

	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "database-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.HighAvailability,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"tier": "database"},
			},
			Priority: 10,
		},
	}

	tr := CreateTestReconcilers(sts, policy)
	reconciler := tr.StatefulSetReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "policy-sts", Namespace: "default"},
	}

	// First reconcile: no availability annotation → no PDB (policy alone doesn't trigger without annotation)
	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{Requeue: true}, result)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "policy-sts-pdb",
		Namespace: "default",
	}, pdb)
	assert.True(t, errors.IsNotFound(err), "PDB should not be created without availability annotation")
}

func TestStatefulSetReconciler_SingleReplica(t *testing.T) {
	ctx := context.Background()

	sts := newTestStatefulSet("single-sts", "default", 1, map[string]string{
		AnnotationAvailabilityClass: "standard",
	}, nil)

	tr := CreateTestReconcilers(sts)
	reconciler := tr.StatefulSetReconciler
	fakeRecorder := reconciler.Recorder.(*k8sevents.FakeRecorder)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "single-sts", Namespace: "default"},
	}

	// Single replica: skipped immediately, no finalizer added
	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	// Second reconcile still skips
	result, err = reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	// No PDB created
	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "single-sts-pdb",
		Namespace: "default",
	}, pdb)
	assert.True(t, errors.IsNotFound(err), "PDB should not be created for single replica StatefulSet")

	// StatefulSetSkipped event recorded
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "StatefulSetSkipped")
		assert.Contains(t, event, "insufficient replicas")
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected StatefulSetSkipped event but none was recorded")
	}
}

func TestStatefulSetReconciler_DeletionWithCleanup(t *testing.T) {
	ctx := context.Background()

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "delete-sts",
			Namespace:         "default",
			Finalizers:        []string{FinalizerPDBCleanup},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			UID:               types.UID("test-sts-uid"),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "delete-sts"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "delete-sts"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "app", Image: "nginx"},
					},
				},
			},
		},
	}

	// Pre-create PDB owned by this StatefulSet
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delete-sts-pdb",
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy: OperatorName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "apps/v1",
					Kind:               "StatefulSet",
					Name:               "delete-sts",
					UID:                sts.UID,
					Controller:         &[]bool{true}[0],
					BlockOwnerDeletion: &[]bool{true}[0],
				},
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.String, StrVal: "75%"},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "delete-sts"},
			},
		},
	}

	tr := CreateTestReconcilers(sts, pdb)
	reconciler := tr.StatefulSetReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "delete-sts", Namespace: "default"},
	}

	// Reconcile handles deletion
	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	// PDB should be deleted
	deletedPDB := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "delete-sts-pdb",
		Namespace: "default",
	}, deletedPDB)
	assert.True(t, errors.IsNotFound(err), "PDB should be deleted when StatefulSet is deleted")

	// Finalizer should be removed
	updatedSts := &appsv1.StatefulSet{}
	err = tr.Client.Get(ctx, req.NamespacedName, updatedSts)
	if err == nil {
		assert.NotContains(t, updatedSts.Finalizers, FinalizerPDBCleanup, "Finalizer should be removed")
	}
}

func TestStatefulSetReconciler_NotFound(t *testing.T) {
	ctx := context.Background()

	tr := CreateTestReconcilers() // no objects
	reconciler := tr.StatefulSetReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent-sts", Namespace: "default"},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
}

func TestStatefulSetReconciler_UpdatePDB(t *testing.T) {
	ctx := context.Background()

	sts := newTestStatefulSet("update-sts", "default", 3, map[string]string{
		AnnotationAvailabilityClass: "standard",
	}, nil)

	tr := CreateTestReconcilers(sts)
	reconciler := tr.StatefulSetReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "update-sts", Namespace: "default"},
	}

	// First reconcile: add finalizer
	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Second reconcile: create PDB
	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "update-sts-pdb",
		Namespace: "default",
	}, pdb)
	require.NoError(t, err)
	assert.Equal(t, "50%", pdb.Spec.MinAvailable.String(), "standard class should be 50%")

	// Upgrade availability class
	updatedSts := &appsv1.StatefulSet{}
	err = tr.Client.Get(ctx, req.NamespacedName, updatedSts)
	require.NoError(t, err)
	updatedSts.Annotations[AnnotationAvailabilityClass] = "high-availability"
	updatedSts.Generation++
	err = tr.Client.Update(ctx, updatedSts)
	require.NoError(t, err)

	// Reconcile: PDB should be updated
	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	updatedPDB := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "update-sts-pdb",
		Namespace: "default",
	}, updatedPDB)
	require.NoError(t, err)
	assert.Equal(t, "75%", updatedPDB.Spec.MinAvailable.String(), "PDB should reflect updated availability class")
}

func TestStatefulSetReconciler_MaintenanceWindow(t *testing.T) {
	ctx := context.Background()

	sts := newTestStatefulSet("maintenance-sts", "default", 3, map[string]string{
		AnnotationAvailabilityClass: "standard",
		AnnotationMaintenanceWindow: "02:00-04:00 UTC", // Not active now
	}, nil)

	tr := CreateTestReconcilers(sts)
	reconciler := tr.StatefulSetReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "maintenance-sts", Namespace: "default"},
	}

	// Add finalizer
	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{Requeue: true}, result)

	// Outside maintenance window: PDB created normally
	result, err = reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{
		Name:      "maintenance-sts-pdb",
		Namespace: "default",
	}, pdb)
	assert.NoError(t, err, "PDB should be created outside maintenance window")
}
