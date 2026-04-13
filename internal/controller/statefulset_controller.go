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
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

// StatefulSetReconciler reconciles a StatefulSet object.
type StatefulSetReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    k8sevents.EventRecorder
	Events      *events.EventRecorder
	PolicyCache *cache.PolicyCache
	Config      *SharedConfig

	tracker *WorkloadStateTracker
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=availability.pdboperator.io,resources=pdbpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile handles StatefulSet changes and manages corresponding PDBs.
func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()

	ctx, span := tracing.ReconcileSpan(ctx, "statefulset", req.Namespace, req.Name)
	defer span.End()

	reconcileID := "statefulset-" + uuid.New().String()
	correlationID := uuid.New().String()

	span.SetAttributes(
		attribute.String("reconcile.id", reconcileID),
		attribute.String("correlation.id", correlationID),
	)

	logger := logging.CreateUnifiedLogger(ctx,
		"statefulset-pdb", "statefulset-controller", "apps", "StatefulSet",
		"statefulset", req.Name, req.Namespace, reconcileID, correlationID,
	)

	var reconcileErr error
	defer func() {
		duration := time.Since(startTime)
		metrics.RecordReconciliation("statefulset", duration, reconcileErr)
		if reconcileErr != nil {
			tracing.RecordError(span, reconcileErr, "Reconciliation failed")
		}
		result := logging.AuditResultSuccess
		if reconcileErr != nil {
			result = logging.AuditResultFailure
		}
		logger.Audit("RECONCILE", fmt.Sprintf("%s/%s", req.Namespace, req.Name), "statefulset",
			req.Namespace, req.Name, result, map[string]interface{}{
				"controller": "statefulset",
				"duration":   time.Since(startTime).String(),
				"durationMs": time.Since(startTime).Milliseconds(),
			})
		logger.Info("Reconciliation completed", map[string]any{"duration": time.Since(startTime).String()})
	}()

	tracing.AddEvent(ctx, "FetchingStatefulSet", attribute.String("reconcile.id", reconcileID))

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, sts); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("StatefulSet not found, ignoring since object must be deleted", map[string]any{})
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		logger.Error(err, "Failed to get StatefulSet", map[string]any{})
		return ctrl.Result{}, err
	}

	w := &statefulSetWorkload{sts}

	if sts.DeletionTimestamp != nil {
		logger.Info("StatefulSet is being deleted", map[string]any{})
		r.tracker.ClearState(w)
		tracing.AddEvent(ctx, "DeletingPDB")
		if err := HandleDeletion(ctx, r.Client, r.Events, w, logger.ToLogr()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if w.GetReplicas() < 2 {
		logger.Info("Single replica, no PDB required", map[string]any{"replicas": w.GetReplicas()})
		tracing.AddEvent(ctx, "SkippedPDB",
			attribute.String("reason", "insufficient_replicas"),
			attribute.Int("replicas", int(w.GetReplicas())),
		)
		if r.Events != nil {
			r.Events.StatefulSetSkipped(sts, sts.Name, "insufficient replicas")
		}
		metrics.UpdateComplianceStatus(sts.Namespace, sts.Name, false, "insufficient_replicas")
		return ctrl.Result{}, nil
	}

	tracing.AddEvent(ctx, "EvaluatingPolicies")

	config, err := GetAvailabilityConfigWithCache(ctx, r.Client, r.PolicyCache, r.Events, w, logger.ToLogr())
	if err != nil {
		reconcileErr = err
		logger.Error(err, "Failed to get availability configuration", map[string]any{})
		return ctrl.Result{}, err
	}

	if config == nil {
		logger.Info("No availability configuration found, skipping PDB", map[string]any{})
		tracing.AddEvent(ctx, "SkippedPDB", attribute.String("reason", "no_availability_configuration"))
		if r.Events != nil {
			r.Events.StatefulSetUnmanaged(sts, sts.Name, "no availability configuration")
		}
		return ctrl.Result{}, nil
	}

	stateChanged := r.tracker.HasStateChanged(ctx, r.Client, w, config)

	logger.Info("Change detection result", map[string]any{
		"stateChanged": stateChanged,
		"statefulset":  sts.Name,
		"namespace":    sts.Namespace,
		"generation":   sts.Generation,
		"debug":        true,
	})

	pdbName := types.NamespacedName{Name: sts.Name + DefaultPDBSuffix, Namespace: sts.Namespace}
	currentPDB := &policyv1.PodDisruptionBudget{}
	pdbExists := r.Get(ctx, pdbName, currentPDB) == nil
	logger.Info("PDB existence check", map[string]any{
		"pdbName":   pdbName.Name,
		"pdbExists": pdbExists,
		"debug":     true,
	})

	if !stateChanged {
		logger.Info("Skipping reconciliation - no state change detected", map[string]any{
			"availabilityClass": string(config.AvailabilityClass),
			"source":            config.Source,
			"policyName":        config.PolicyName,
			"reason":            "no_state_change",
			"optimized":         true,
		})
		tracing.AddEvent(ctx, "SkippedPDB", attribute.String("reason", "no_state_change"))
		return ctrl.Result{}, nil
	}

	logger.Info("Using availability configuration", map[string]any{
		"availabilityClass": string(config.AvailabilityClass),
		"source":            config.Source,
		"policyName":        config.PolicyName,
		"stateChanged":      stateChanged,
	})

	span.SetAttributes(
		attribute.String("config.source", config.Source),
		attribute.String("config.policy_name", config.PolicyName),
	)

	tracing.AddEvent(ctx, "ReconcilingPDB",
		attribute.String("availability_class", string(config.AvailabilityClass)),
		attribute.String("source", config.Source),
	)

	if !controllerutil.ContainsFinalizer(sts, FinalizerPDBCleanup) {
		patch := client.MergeFrom(sts.DeepCopy())
		controllerutil.AddFinalizer(sts, FinalizerPDBCleanup)
		if err := r.Patch(ctx, sts, patch); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to add finalizer", map[string]any{})
			return ctrl.Result{}, err
		}
		logger.Info("Added finalizer for PDB cleanup", map[string]any{})
		return ctrl.Result{Requeue: true}, nil
	}

	if IsInMaintenanceWindow(config, r.PolicyCache, log.FromContext(ctx)) {
		logger.Info("In maintenance window, temporarily relaxing PDB", map[string]any{
			"maintenanceWindow": config.MaintenanceWindow,
		})
		tracing.AddEvent(ctx, "MaintenanceWindowActive",
			attribute.String("maintenance_window", config.MaintenanceWindow),
		)
		if err := RemovePDBTemporarily(ctx, r.Client, w, log.FromContext(ctx)); err != nil {
			reconcileErr = err
			logger.Error(err, "Failed to relax PDB for maintenance window", map[string]any{})
			return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	result, err := ReconcilePDB(ctx, r.Client, r.Scheme, r.Events, w, config, log.FromContext(ctx))
	if err != nil {
		reconcileErr = err
		logger.Error(err, "PDB reconciliation failed, will retry", map[string]any{})
		return ctrl.Result{RequeueAfter: DefaultRequeueDelay}, err
	}

	metrics.ManagedStatefulSets.WithLabelValues(
		sts.Namespace,
		string(config.AvailabilityClass),
	).Set(1)

	metrics.UpdateComplianceStatus(sts.Namespace, sts.Name, true, "managed")
	r.tracker.UpdateState(ctx, r.Client, w, config)

	logger.Info("Successfully reconciled PDB", map[string]any{
		"availabilityClass": config.AvailabilityClass,
		"source":            config.Source,
		"reconcileID":       reconcileID,
	})

	return result, nil
}

// policyMatchesStatefulSet checks if a policy matches a StatefulSet.
func (r *StatefulSetReconciler) policyMatchesStatefulSet(policy *availabilityv1alpha1.PDBPolicy, sts *appsv1.StatefulSet) bool {
	return PolicyMatchesWorkload(policy, &statefulSetWorkload{sts})
}

// SetupWithManager sets up the StatefulSet controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := 3
	if r.Config != nil && r.Config.MaxConcurrentReconciles > 0 {
		maxConcurrent = r.Config.MaxConcurrentReconciles
	}
	return r.SetupWithManagerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: maxConcurrent,
	})
}

// SetupWithManagerWithOptions sets up the controller with custom options.
func (r *StatefulSetReconciler) SetupWithManagerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	r.tracker = NewWorkloadStateTracker()

	if r.Recorder == nil && mgr != nil {
		r.Recorder = mgr.GetEventRecorder("statefulset-pdb-controller")
	}
	if r.Events == nil && r.Recorder != nil {
		r.Events = events.NewEventRecorder(r.Recorder)
	}

	statefulSetPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			_, ok := e.Object.(*appsv1.StatefulSet)
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSts, ok := e.ObjectOld.(*appsv1.StatefulSet)
			if !ok {
				return false
			}
			newSts, ok := e.ObjectNew.(*appsv1.StatefulSet)
			if !ok {
				return false
			}
			if oldSts.Generation == newSts.Generation {
				if oldSts.DeletionTimestamp == nil && newSts.DeletionTimestamp != nil {
					return true
				}
				return false
			}
			return true
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return true },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}

	pdbPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			pdb, ok := e.ObjectNew.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			if labels := pdb.GetLabels(); labels != nil {
				if labels[LabelManagedBy] == OperatorName {
					oldPDB := e.ObjectOld.(*policyv1.PodDisruptionBudget)
					return !isPDBUpdateByUs(oldPDB, pdb)
				}
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			pdb, ok := e.Object.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			if labels := pdb.GetLabels(); labels != nil {
				return labels[LabelManagedBy] == OperatorName
			}
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}

	pdbToStatefulSetHandler := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		pdb, ok := obj.(*policyv1.PodDisruptionBudget)
		if !ok {
			return nil
		}
		if labels := pdb.GetLabels(); labels == nil || labels[LabelManagedBy] != OperatorName {
			return nil
		}
		stsName := pdb.Name
		if len(stsName) > len(DefaultPDBSuffix) &&
			stsName[len(stsName)-len(DefaultPDBSuffix):] == DefaultPDBSuffix {
			stsName = stsName[:len(stsName)-len(DefaultPDBSuffix)]
		} else {
			for _, ownerRef := range pdb.GetOwnerReferences() {
				if ownerRef.Kind == "StatefulSet" && ownerRef.Controller != nil && *ownerRef.Controller {
					stsName = ownerRef.Name
					break
				}
			}
		}
		log.Log.Info("Mapping PDB event to StatefulSet reconciliation",
			"pdb", pdb.Name, "statefulset", stsName, "namespace", pdb.Namespace)
		return []ctrl.Request{{
			NamespacedName: types.NamespacedName{Name: stsName, Namespace: pdb.Namespace},
		}}
	})

	policyHandler := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		policy, ok := obj.(*availabilityv1alpha1.PDBPolicy)
		if !ok {
			return nil
		}
		logger := log.FromContext(ctx)
		stsList := &appsv1.StatefulSetList{}
		if err := r.List(ctx, stsList); err != nil {
			logger.Error(err, "Failed to list StatefulSets for policy change")
			return nil
		}
		var requests []ctrl.Request
		for _, sts := range stsList.Items {
			if r.policyMatchesStatefulSet(policy, &sts) {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace},
				})
			}
		}
		logger.V(2).Info("Policy change affects StatefulSets",
			"policy", policy.Name, "affectedStatefulSets", len(requests))
		return requests
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("statefulset-pdb").
		For(&appsv1.StatefulSet{}, builder.WithPredicates(statefulSetPredicate)).
		Watches(&policyv1.PodDisruptionBudget{}, pdbToStatefulSetHandler, builder.WithPredicates(pdbPredicate)).
		Watches(&availabilityv1alpha1.PDBPolicy{}, policyHandler).
		WithOptions(opts).
		Complete(r)
}
