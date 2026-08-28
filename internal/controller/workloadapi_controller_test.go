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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pdbv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
	"github.com/pdb-operator/pdb-operator/internal/events"
)

// newTestWorkload builds an unstructured Workload with one gang template named "workers".
func newTestWorkload(name string, minCount int64, disruptAll bool, annotations, labels map[string]string) *unstructured.Unstructured {
	u := NewWorkloadObject()
	u.SetName(name)
	u.SetNamespace("default")
	u.SetAnnotations(annotations)
	u.SetLabels(labels)
	u.SetGeneration(1)
	tmpl := map[string]interface{}{
		"name": "workers",
		"schedulingPolicy": map[string]interface{}{
			"gang": map[string]interface{}{"minCount": minCount},
		},
	}
	if disruptAll {
		tmpl["disruptionMode"] = map[string]interface{}{"all": map[string]interface{}{}}
	}
	_ = unstructured.SetNestedSlice(u.Object, []interface{}{tmpl}, "spec", "podGroupTemplates")
	return u
}

// newTestPodGroup builds an unstructured PodGroup referencing a workload template.
func newTestPodGroup(name, namespace, workloadName, templateName string) *unstructured.Unstructured {
	u := NewPodGroupObject()
	u.SetName(name)
	u.SetNamespace(namespace)
	_ = unstructured.SetNestedField(u.Object, workloadName, "spec", "workloadRef", "workloadName")
	_ = unstructured.SetNestedField(u.Object, templateName, "spec", "workloadRef", "templateName")
	return u
}

// newTestGroupPod builds a pod that references its PodGroup via spec.schedulingGroup.
func newTestGroupPod(name, namespace, podGroupName string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
			UID:       types.UID(namespace + "/" + name),
		},
		Spec: corev1.PodSpec{
			SchedulingGroup: &corev1.PodSchedulingGroup{PodGroupName: &podGroupName},
			Containers:      []corev1.Container{{Name: "app", Image: "app"}},
		},
	}
}

func newWorkloadAPITestReconciler(objects ...client.Object) *WorkloadAPIReconciler {
	scheme := SetupTestScheme()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&pdbv1alpha1.PDBPolicy{}).
		WithIndex(&corev1.Pod{}, PodGroupNameIndex, func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
				return nil
			}
			return []string{*pod.Spec.SchedulingGroup.PodGroupName}
		}).
		Build()
	fakeRecorder := k8sevents.NewFakeRecorder(100)
	return &WorkloadAPIReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
		Events:   events.NewEventRecorder(fakeRecorder),
		tracker:  NewWorkloadStateTracker(),
	}
}

// gangFixture seeds G pod groups of S pods each for workload w's "workers" template.
func gangFixture(workloadName string, groups, size int, podLabels map[string]string) []client.Object {
	var objs []client.Object
	for g := 0; g < groups; g++ {
		pgName := fmt.Sprintf("%s-workers-%d", workloadName, g)
		objs = append(objs, newTestPodGroup(pgName, "default", workloadName, "workers"))
		for p := 0; p < size; p++ {
			objs = append(objs, newTestGroupPod(fmt.Sprintf("%s-%d", pgName, p), "default", pgName, podLabels))
		}
	}
	return objs
}

// reconcileWorkloadTwice reconciles twice: once for the finalizer requeue, once for the PDB.
func reconcileWorkloadTwice(t *testing.T, r *WorkloadAPIReconciler, name string) reconcile.Result {
	t.Helper()
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	result, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	return result
}

func TestParseGangTemplates(t *testing.T) {
	basic := NewWorkloadObject()
	basic.SetName("basic")
	_ = unstructured.SetNestedSlice(basic.Object, []interface{}{
		map[string]interface{}{
			"name":             "web",
			"schedulingPolicy": map[string]interface{}{"basic": map[string]interface{}{}},
		},
	}, "spec", "podGroupTemplates")

	multi := newTestWorkload("multi", 2, true, nil, nil)
	tmpls, _, _ := unstructured.NestedSlice(multi.Object, "spec", "podGroupTemplates")
	tmpls = append(tmpls, map[string]interface{}{
		"name": "second",
		"schedulingPolicy": map[string]interface{}{
			"gang": map[string]interface{}{"minCount": int64(3)},
		},
	})
	_ = unstructured.SetNestedSlice(multi.Object, tmpls, "spec", "podGroupTemplates")

	composite := newTestWorkload("composite", 2, false, nil, nil)
	_ = unstructured.SetNestedSlice(composite.Object, []interface{}{
		map[string]interface{}{"name": "outer"},
	}, "spec", "compositePodGroupTemplates")

	tests := []struct {
		name          string
		workload      *unstructured.Unstructured
		wantGangs     int
		wantTotal     int
		wantComposite int
	}{
		{"no templates", NewWorkloadObject(), 0, 0, 0},
		{"basic only", basic, 0, 1, 0},
		{"single gang", newTestWorkload("w", 4, true, nil, nil), 1, 1, 0},
		{"two gangs", multi, 2, 2, 0},
		{"composite present", composite, 1, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gangs, total, composite := parseGangTemplates(tt.workload)
			assert.Len(t, gangs, tt.wantGangs)
			assert.Equal(t, tt.wantTotal, total)
			assert.Equal(t, tt.wantComposite, composite)
		})
	}

	gangs, _, _ := parseGangTemplates(newTestWorkload("w", 4, true, nil, nil))
	require.Len(t, gangs, 1)
	assert.Equal(t, "workers", gangs[0].Name)
	assert.Equal(t, int32(4), gangs[0].MinCount)
	assert.True(t, gangs[0].DisruptAll)

	gangs, _, _ = parseGangTemplates(newTestWorkload("w", 4, false, nil, nil))
	require.Len(t, gangs, 1)
	assert.False(t, gangs[0].DisruptAll, "unset disruptionMode defaults to single")
}

func TestDeriveGroupSelector(t *testing.T) {
	pods := func(labels ...map[string]string) []corev1.Pod {
		out := make([]corev1.Pod, len(labels))
		for i, l := range labels {
			out[i] = *newTestGroupPod(fmt.Sprintf("p%d", i), "default", "pg", l)
		}
		return out
	}

	t.Run("common labels form the selector", func(t *testing.T) {
		group := pods(
			map[string]string{"app": "trainer", "role": "leader"},
			map[string]string{"app": "trainer", "role": "worker"},
		)
		got := deriveGroupSelector(group, group)
		assert.Equal(t, map[string]string{"app": "trainer"}, got)
	})

	t.Run("no pods yields nil", func(t *testing.T) {
		assert.Nil(t, deriveGroupSelector(nil, nil))
	})

	t.Run("empty intersection yields nil", func(t *testing.T) {
		group := pods(map[string]string{"a": "1"}, map[string]string{"b": "2"})
		assert.Nil(t, deriveGroupSelector(group, group))
	})

	t.Run("over-matching candidate yields nil", func(t *testing.T) {
		group := pods(map[string]string{"app": "trainer"}, map[string]string{"app": "trainer"})
		stranger := *newTestGroupPod("stranger", "default", "other", map[string]string{"app": "trainer"})
		assert.Nil(t, deriveGroupSelector(group, append(group, stranger)))
	})
}

func TestFloorAtMinCount(t *testing.T) {
	tests := []struct {
		name     string
		in       intstr.IntOrString
		minCount int32
		total    int32
		want     string
		wantOK   bool
	}{
		{"class below floor is raised", intstr.FromString("50%"), 3, 4, "3", true},
		{"class above floor wins", intstr.FromString("75%"), 3, 8, "6", true},
		{"floor equals total blocks drains", intstr.FromString("50%"), 4, 4, "", false},
		{"scaled rounds up to total blocks drains", intstr.FromString("90%"), 3, 8, "", false},
		{"scaled equals total blocks drains", intstr.FromString("100%"), 1, 4, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := floorAtMinCount(tt.in, tt.minCount, tt.total)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got.String())
			}
		})
	}
}

func TestWorkloadAPIReconciler_AllMode_QuantizedPDB(t *testing.T) {
	workload := newTestWorkload("trainer", 2, true, nil, map[string]string{"tier": "training"})
	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "training-policy", Namespace: "default"},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.MissionCritical,
			WorkloadSelector:  pdbv1alpha1.WorkloadSelector{MatchLabels: map[string]string{"tier": "training"}},
			Priority:          10,
		},
	}
	objs := append(gangFixture("trainer", 4, 2, map[string]string{"app": "trainer"}),
		workload, policy)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "trainer")

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "trainer-pdb", Namespace: "default"}, pdb)
	require.NoError(t, err, "PDB should be created")

	// 90% of 4 groups rounds up to 4, clamped to 3 groups x 2 pods = 6
	assert.Equal(t, "6", pdb.Spec.MinAvailable.String())
	assert.Equal(t, map[string]string{"app": "trainer"}, pdb.Spec.Selector.MatchLabels)

	require.Len(t, pdb.OwnerReferences, 1)
	assert.Equal(t, kindWorkload, pdb.OwnerReferences[0].Kind)
	assert.Equal(t, "trainer", pdb.OwnerReferences[0].Name)
	assert.Equal(t, WorkloadAPIGroup+"/"+WorkloadAPIVersion, pdb.OwnerReferences[0].APIVersion)

	updated := NewWorkloadObject()
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "trainer", Namespace: "default"}, updated))
	assert.Contains(t, updated.GetFinalizers(), FinalizerPDBCleanup)
}

func TestWorkloadAPIReconciler_SingleGroupAllMode_NoPDB(t *testing.T) {
	workload := newTestWorkload("big-model", 8, true,
		map[string]string{AnnotationAvailabilityClass: "mission-critical"}, nil)
	objs := append(gangFixture("big-model", 1, 8, map[string]string{"app": "big-model"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "big-model")

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "big-model-pdb", Namespace: "default"}, pdb)
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "no PDB for a single all-mode group")
}

func TestWorkloadAPIReconciler_NoPods_Defers(t *testing.T) {
	workload := newTestWorkload("early", 2, true,
		map[string]string{AnnotationAvailabilityClass: "standard"}, nil)
	pg := newTestPodGroup("early-workers-0", "default", "early", "workers")
	r := newWorkloadAPITestReconciler(workload, pg)

	result := reconcileWorkloadTwice(t, r, "early")
	assert.Equal(t, workloadPodsPollDelay, result.RequeueAfter)

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "early-pdb", Namespace: "default"}, pdb)
	assert.Error(t, err, "no PDB before pods exist")
}

func TestWorkloadAPIReconciler_SingleMode_FloorsAtMinCount(t *testing.T) {
	workload := newTestWorkload("etl", 3, false,
		map[string]string{AnnotationAvailabilityClass: "standard"}, nil)
	objs := append(gangFixture("etl", 1, 6, map[string]string{"app": "etl"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "etl")

	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "etl-pdb", Namespace: "default"}, pdb))
	// standard (50%) of 6 pods = 3, tied with the gang minCount floor of 3
	assert.Equal(t, "3", pdb.Spec.MinAvailable.String())
}

func TestWorkloadAPIReconciler_SingleMode_MinCountBlocksDrains_NoPDB(t *testing.T) {
	workload := newTestWorkload("tight", 4, false,
		map[string]string{AnnotationAvailabilityClass: "non-critical"}, nil)
	objs := append(gangFixture("tight", 1, 4, map[string]string{"app": "tight"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "tight")

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "tight-pdb", Namespace: "default"}, pdb)
	assert.Error(t, err, "minCount == pod count leaves nothing evictable, no PDB")
}

func TestWorkloadAPIReconciler_LWSOwnedPods_Skipped(t *testing.T) {
	workload := newTestWorkload("lws-backed", 2, true,
		map[string]string{AnnotationAvailabilityClass: "standard"}, nil)
	objs := append(gangFixture("lws-backed", 2, 2,
		map[string]string{"app": "lws-backed", LWSSetNameLabelKey: "lws-backed"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "lws-backed")

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "lws-backed-pdb", Namespace: "default"}, pdb)
	assert.Error(t, err, "pods with a native gang path get no Workload API PDB")
}

func TestWorkloadAPIReconciler_MultipleGangTemplates_Skipped(t *testing.T) {
	workload := newTestWorkload("multi", 2, true,
		map[string]string{AnnotationAvailabilityClass: "standard"}, nil)
	tmpls, _, _ := unstructured.NestedSlice(workload.Object, "spec", "podGroupTemplates")
	tmpls = append(tmpls, map[string]interface{}{
		"name": "second",
		"schedulingPolicy": map[string]interface{}{
			"gang": map[string]interface{}{"minCount": int64(2)},
		},
	})
	_ = unstructured.SetNestedSlice(workload.Object, tmpls, "spec", "podGroupTemplates")
	objs := append(gangFixture("multi", 2, 2, map[string]string{"app": "multi"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "multi")

	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "multi-pdb", Namespace: "default"}, pdb)
	assert.Error(t, err, "multi-template workloads are not yet supported")
}

func TestWorkloadAPIReconciler_ScaleToZeroGroups_CleansUpPDB(t *testing.T) {
	workload := newTestWorkload("shrink", 2, true,
		map[string]string{AnnotationAvailabilityClass: "standard"}, nil)
	objs := append(gangFixture("shrink", 4, 2, map[string]string{"app": "shrink"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "shrink")
	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "shrink-pdb", Namespace: "default"}, pdb))

	// simulate all pods disappearing (workload torn down but object kept)
	pods := &corev1.PodList{}
	require.NoError(t, r.List(context.Background(), pods, client.InNamespace("default")))
	for i := range pods.Items {
		require.NoError(t, r.Delete(context.Background(), &pods.Items[i]))
	}

	result := reconcileWorkloadTwice(t, r, "shrink")
	assert.Equal(t, workloadPodsPollDelay, result.RequeueAfter)

	err := r.Get(context.Background(), types.NamespacedName{Name: "shrink-pdb", Namespace: "default"}, pdb)
	assert.Error(t, err, "PDB should be cleaned up when no pods remain")
}

func TestWorkloadAPIReconciler_Deletion_RemovesFinalizerAndPDB(t *testing.T) {
	workload := newTestWorkload("gone", 2, true,
		map[string]string{AnnotationAvailabilityClass: "standard"}, nil)
	objs := append(gangFixture("gone", 2, 2, map[string]string{"app": "gone"}), workload)
	r := newWorkloadAPITestReconciler(objs...)

	reconcileWorkloadTwice(t, r, "gone")
	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "gone-pdb", Namespace: "default"}, pdb))

	// the finalizer keeps the object around with a deletionTimestamp set
	current := NewWorkloadObject()
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "gone", Namespace: "default"}, current))
	require.NoError(t, r.Delete(context.Background(), current))

	_, err := r.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: types.NamespacedName{Name: "gone", Namespace: "default"}})
	require.NoError(t, err)

	err = r.Get(context.Background(), types.NamespacedName{Name: "gone-pdb", Namespace: "default"}, pdb)
	assert.Error(t, err, "PDB should be deleted with its Workload")

	err = r.Get(context.Background(), types.NamespacedName{Name: "gone", Namespace: "default"}, NewWorkloadObject())
	assert.Error(t, err, "Workload should be gone once the finalizer is removed")
}
