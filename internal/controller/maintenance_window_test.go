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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pdbv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
)

// Wednesday 03:00 UTC - fixed reference so the tests are time-invariant.
var refNow = time.Date(2026, 6, 24, 3, 0, 0, 0, time.UTC)

func TestIsInMaintenanceWindow_StructuredAndAnnotation(t *testing.T) {
	today := int(refNow.Weekday())
	otherDay := (today + 1) % 7

	cases := []struct {
		name   string
		config *AvailabilityConfig
		want   bool
	}{
		{
			name:   "policy window active on matching day",
			config: &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "02:00", End: "04:00", Timezone: "UTC", DaysOfWeek: []int{today}}}},
			want:   true,
		},
		{
			name:   "policy window inactive on non-matching day",
			config: &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "02:00", End: "04:00", DaysOfWeek: []int{otherDay}}}},
			want:   false,
		},
		{
			name:   "policy window inactive outside time",
			config: &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "05:00", End: "06:00"}}},
			want:   false,
		},
		{
			name:   "policy window every day when DaysOfWeek empty",
			config: &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "02:00", End: "04:00"}}},
			want:   true,
		},
		{
			name:   "overnight policy window active after midnight",
			config: &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "22:00", End: "04:00"}}},
			want:   true,
		},
		{
			name:   "annotation window still honored",
			config: &AvailabilityConfig{MaintenanceWindow: "02:00-04:00 UTC"},
			want:   true,
		},
		{
			name:   "no window configured",
			config: &AvailabilityConfig{},
			want:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsInMaintenanceWindow(tc.config, refNow))
		})
	}
}

func TestDurationUntilMaintenanceWindow(t *testing.T) {
	// Window later today (05:00-06:00) is 2h away from 03:00.
	cfg := &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "05:00", End: "06:00"}}}
	d, ok := durationUntilMaintenanceWindow(cfg, refNow)
	require.True(t, ok)
	assert.Equal(t, 2*time.Hour, d)

	// A window that already opened earlier today rolls to the next day.
	cfg = &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "02:00", End: "02:30"}}}
	d, ok = durationUntilMaintenanceWindow(cfg, refNow)
	require.True(t, ok)
	assert.Equal(t, 23*time.Hour, d)

	// No window configured.
	_, ok = durationUntilMaintenanceWindow(&AvailabilityConfig{}, refNow)
	assert.False(t, ok)
}

func TestApplyMaintenanceRequeue_CapsAndKeepsSooner(t *testing.T) {
	// Next window is 2h out, capped to the heartbeat.
	cfg := &AvailabilityConfig{MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{{Start: "05:00", End: "06:00"}}}
	got := applyMaintenanceRequeue(reconcile.Result{}, cfg, refNow)
	assert.Equal(t, maxMaintenanceRequeue, got.RequeueAfter)

	// An existing sooner requeue is preserved.
	got = applyMaintenanceRequeue(reconcile.Result{RequeueAfter: time.Second}, cfg, refNow)
	assert.Equal(t, time.Second, got.RequeueAfter)

	// No window leaves the result untouched.
	got = applyMaintenanceRequeue(reconcile.Result{}, &AvailabilityConfig{}, refNow)
	assert.Zero(t, got.RequeueAfter)
}

// TestDeploymentReconciler_PolicyMaintenanceWindow proves a PDBPolicy-level maintenance
// window is now evaluated (previously ignored) and is time-driven via the injected clock.
func TestDeploymentReconciler_PolicyMaintenanceWindow(t *testing.T) {
	ctx := context.Background()

	policy := &pdbv1alpha1.PDBPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "mw-policy", Namespace: "default"},
		Spec: pdbv1alpha1.PDBPolicySpec{
			AvailabilityClass: pdbv1alpha1.Standard,
			WorkloadSelector:  pdbv1alpha1.WorkloadSelector{MatchLabels: map[string]string{"app": "mw"}},
			MaintenanceWindows: []pdbv1alpha1.MaintenanceWindow{
				{Start: "02:00", End: "04:00", Timezone: "UTC"},
			},
		},
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mw",
			Namespace: "default",
			Labels:    map[string]string{"app": "mw"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mw"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mw"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "nginx"}}},
			},
		},
	}

	tr := CreateTestReconcilers(policy, deployment)
	r := tr.DeploymentReconciler
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "mw", Namespace: "default"}}
	pdbKey := types.NamespacedName{Name: "mw-pdb", Namespace: "default"}

	// Clock inside the policy window: PDB must be relaxed, i.e. not enforced.
	r.Clock = clocktesting.NewFakeClock(refNow)
	_, err := r.Reconcile(ctx, req) // adds finalizer
	require.NoError(t, err)
	result, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter, "maintenance branch requeues every minute")

	pdb := &policyv1.PodDisruptionBudget{}
	err = tr.Client.Get(ctx, pdbKey, pdb)
	assert.True(t, apierrors.IsNotFound(err), "no PDB should be enforced during the policy maintenance window")

	// Clock outside the window: PDB is created normally.
	r.Clock = clocktesting.NewFakeClock(time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC))
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, tr.Client.Get(ctx, pdbKey, pdb))
}

// TestHasStateChanged_PropagatesGetError covers the state tracker surfacing a transient
// client error instead of silently treating the PDB as absent.
func TestHasStateChanged_PropagatesGetError(t *testing.T) {
	ctx := context.Background()
	scheme := SetupTestScheme()
	boom := errors.New("api server unavailable")

	c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*policyv1.PodDisruptionBudget); ok {
				return boom
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}).Build()

	tracker := NewWorkloadStateTracker()
	w := &deploymentWorkload{&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "err", Namespace: "default"},
	}}

	changed, err := tracker.HasStateChanged(ctx, c, w, &AvailabilityConfig{})
	require.ErrorIs(t, err, boom)
	assert.False(t, changed)
}
