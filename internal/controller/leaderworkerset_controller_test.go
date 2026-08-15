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
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pdbv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
)

// newTestLWS builds a basic unstructured LeaderWorkerSet for tests.
//
//nolint:unparam // namespace parameter allows tests to use non-default namespaces
func newTestLWS(name, namespace string, replicas, size int64, annotations, labels map[string]string) *unstructured.Unstructured {
	u := NewLWSObject()
	u.SetName(name)
	u.SetNamespace(namespace)
	u.SetAnnotations(annotations)
	u.SetLabels(labels)
	u.SetGeneration(1)
	_ = unstructured.SetNestedField(u.Object, replicas, "spec", "replicas")
	_ = unstructured.SetNestedField(u.Object, size, "spec", "leaderWorkerTemplate", "size")
	return u
}

func newLWSTestReconciler(tr *TestReconcilers) *LeaderWorkerSetReconciler {
	return &LeaderWorkerSetReconciler{
		Client:   tr.Client,
		Scheme:   tr.Scheme,
		Recorder: tr.StatefulSetReconciler.Recorder,
		Events:   tr.EventRecorder,
		tracker:  NewWorkloadStateTracker(),
	}
}

func TestQuantizeMinAvailableForGroups(t *testing.T) {
	tests := []struct {
		name   string
		in     intstr.IntOrString
		groups int32
		size   int32
		want   string
	}{
		{"percent exact multiple", intstr.FromString("50%"), 4, 8, "16"},
		{"percent clamped to groups-1", intstr.FromString("90%"), 4, 8, "24"},
		{"percent clamped two groups", intstr.FromString("90%"), 2, 8, "8"},
		{"percent rounds up to one group", intstr.FromString("20%"), 4, 8, "8"},
		{"absolute pods rounds up to group", intstr.FromInt32(12), 4, 8, "16"},
		{"absolute pods clamped", intstr.FromInt32(32), 4, 8, "24"},
		{"negative absolute clamps to zero", intstr.FromInt32(-5), 4, 8, "0"},
		{"size one passes through", intstr.FromString("90%"), 4, 1, "90%"},
		{"single group passes through", intstr.FromString("90%"), 1, 8, "90%"},
		{"invalid string passes through", intstr.FromString("garbage"), 4, 8, "garbage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuantizeMinAvailableForGroups(tt.in, tt.groups, tt.size)
			assert.Equal(t, tt.want, got.String())
		})
	}
}

func TestLWSReconciler_PolicyBasedPDB_Quantized(t *testing.T) {
	ctx := context.Background()

	lws := newTestLWS("vllm", "default", 4, 8, nil, map[string]string{"tier": "inference"})
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inference-policy",
			Namespace: "default",
		},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector: pdbv1alpha1.WorkloadSelector{
				MatchLabels: map[string]string{"tier": "inference"},
			},
			Priority: 10,
		},
	}

	tr := CreateTestReconcilers(lws, policy)
	reconciler := newLWSTestReconciler(tr)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "vllm", Namespace: "default"},
	}

	// First reconciliation adds finalizer
	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{Requeue: true}, result)

	updatedLWS := NewLWSObject()
	err = tr.Client.Get(ctx, req.NamespacedName, updatedLWS)
	require.NoError(t, err)
	assert.Contains(t, updatedLWS.GetFinalizers(), FinalizerPDBCleanup, "Finalizer should be added")

	// Second reconciliation creates PDB
	_, err = reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{Name: "vllm-pdb", Namespace: "default"}, pdb)
	require.NoError(t, err, "PDB should be created")

	// 90% of 4 groups rounds up to 4, clamped to 3 groups x 8 pods = 24
	assert.Equal(t, "24", pdb.Spec.MinAvailable.String())
	assert.Equal(t, map[string]string{LWSSetNameLabelKey: "vllm"}, pdb.Spec.Selector.MatchLabels)

	require.Len(t, pdb.OwnerReferences, 1)
	assert.Equal(t, kindLeaderWorkerSet, pdb.OwnerReferences[0].Kind)
	assert.Equal(t, "vllm", pdb.OwnerReferences[0].Name)
	assert.Equal(t, LWSGroup+"/"+LWSVersion, pdb.OwnerReferences[0].APIVersion)
}

func TestLWSReconciler_SingleGroup_NoPDB(t *testing.T) {
	ctx := context.Background()

	lws := newTestLWS("big-model", "default", 1, 8, map[string]string{
		AnnotationAvailabilityClass: "mission-critical",
	}, nil)

	tr := CreateTestReconcilers(lws)
	reconciler := newLWSTestReconciler(tr)
	fakeRecorder := reconciler.Recorder.(*k8sevents.FakeRecorder)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "big-model", Namespace: "default"},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{Name: "big-model-pdb", Namespace: "default"}, pdb)
	assert.True(t, errors.IsNotFound(err), "No PDB should be created for a single group")

	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning")
		assert.Contains(t, event, "LeaderWorkerSetSkipped")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Expected a LeaderWorkerSetSkipped warning event")
	}
}

func TestLWSReconciler_SizeOne_PodLevel(t *testing.T) {
	ctx := context.Background()

	lws := newTestLWS("flat", "default", 3, 1, map[string]string{
		AnnotationAvailabilityClass: "high-availability",
	}, nil)

	tr := CreateTestReconcilers(lws)
	reconciler := newLWSTestReconciler(tr)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "flat", Namespace: "default"},
	}

	_, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	_, err = reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{Name: "flat-pdb", Namespace: "default"}, pdb)
	require.NoError(t, err, "PDB should be created")
	assert.Equal(t, "75%", pdb.Spec.MinAvailable.String(), "size=1 keeps pod-level percentage semantics")
}

func TestLWSReconciler_ForeignOwnedPDB_NotAdopted(t *testing.T) {
	ctx := context.Background()

	lws := newTestLWS("vllm", "default", 4, 8, map[string]string{
		AnnotationAvailabilityClass: "mission-critical",
	}, nil)
	lws.SetFinalizers([]string{FinalizerPDBCleanup})

	minAvailable := intstr.FromString("90%")
	controller := true
	foreignPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vllm-pdb",
			Namespace: "default",
			Labels:    map[string]string{LabelManagedBy: OperatorName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "vllm",
				UID:        "sts-uid",
				Controller: &controller,
			}},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &minAvailable},
	}

	tr := CreateTestReconcilers(lws, foreignPDB)
	reconciler := newLWSTestReconciler(tr)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "vllm", Namespace: "default"},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, DefaultRequeueDelay, result.RequeueAfter, "should wait for the other controller to release the PDB")

	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, tr.Client.Get(ctx, types.NamespacedName{Name: "vllm-pdb", Namespace: "default"}, pdb))
	assert.Equal(t, "90%", pdb.Spec.MinAvailable.String(), "foreign PDB spec must not be touched")
	assert.Equal(t, "StatefulSet", pdb.OwnerReferences[0].Kind, "foreign PDB ownership must not be touched")
}

func TestStatefulSetReconciler_SkipsLWSInternal(t *testing.T) {
	ctx := context.Background()

	sts := newTestStatefulSet("vllm-sim-0", "default", 7, map[string]string{
		AnnotationAvailabilityClass: "mission-critical",
	}, map[string]string{
		LWSSetNameLabelKey: "vllm-sim",
		"tier":             "inference",
	})
	sts.Finalizers = []string{FinalizerPDBCleanup}

	minAvailable := intstr.FromString("90%")
	controller := true
	stalePDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vllm-sim-0-pdb",
			Namespace: "default",
			Labels:    map[string]string{LabelManagedBy: OperatorName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "vllm-sim-0",
				UID:        "sts-uid",
				Controller: &controller,
			}},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &minAvailable},
	}

	tr := CreateTestReconcilers(sts, stalePDB)
	reconciler := tr.StatefulSetReconciler

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "vllm-sim-0", Namespace: "default"},
	}

	result, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, types.NamespacedName{Name: "vllm-sim-0-pdb", Namespace: "default"}, pdb)
	assert.True(t, errors.IsNotFound(err), "stale pod-level PDB must be cleaned up")

	updatedSts := &appsv1.StatefulSet{}
	require.NoError(t, tr.Client.Get(ctx, req.NamespacedName, updatedSts))
	assert.NotContains(t, updatedSts.Finalizers, FinalizerPDBCleanup, "finalizer must be released")
}

func TestLWSReconciler_Deletion_CleansUp(t *testing.T) {
	ctx := context.Background()

	lws := newTestLWS("gone", "default", 4, 8, map[string]string{
		AnnotationAvailabilityClass: "standard",
	}, nil)

	tr := CreateTestReconcilers(lws)
	reconciler := newLWSTestReconciler(tr)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "gone", Namespace: "default"},
	}

	// reconcile twice: finalizer then PDB
	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, tr.Client.Get(ctx, types.NamespacedName{Name: "gone-pdb", Namespace: "default"}, pdb))

	// delete: finalizer keeps the object; reconcile must remove PDB and finalizer
	current := NewLWSObject()
	require.NoError(t, tr.Client.Get(ctx, req.NamespacedName, current))
	require.NoError(t, tr.Client.Delete(ctx, current))

	_, err = reconciler.Reconcile(ctx, req)
	assert.NoError(t, err)

	err = tr.Client.Get(ctx, types.NamespacedName{Name: "gone-pdb", Namespace: "default"}, pdb)
	assert.True(t, errors.IsNotFound(err), "PDB should be deleted with its owner")

	err = tr.Client.Get(ctx, req.NamespacedName, NewLWSObject())
	assert.True(t, errors.IsNotFound(err), "LWS should be gone once the finalizer is removed")
}
