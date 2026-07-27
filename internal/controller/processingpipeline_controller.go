/*
Copyright 2026 Guilherme Canella.

Licensed under the MIT License. See the LICENSE file at the
root of this repository for the full license text.
*/

package controller

import (
	"context"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	weirdevv1alpha1 "github.com/guycanella/weir/api/v1alpha1"
	"github.com/guycanella/weir/internal/pipelinespec"
)

// conditionTypeSpecValid is the condition type this reconciler owns: it
// reports whether the CR's spec passed internal/pipelinespec.ValidateSpec.
// See processingpipeline_validation_test.go for the full contract this
// name and the reasons/messages below are pinned against.
const conditionTypeSpecValid = "SpecValid"

const (
	reasonValidSpec   = "ValidSpec"
	reasonInvalidSpec = "InvalidSpec"
)

// toPipelineSpec converts the CRD's wire-shaped spec into
// internal/pipelinespec.Spec, the dependency-free type the pure validation
// core operates on. pipelinespec deliberately avoids importing kubebuilder
// types (see its doc comment), so this conversion lives here instead - a
// straightforward per-field copy with int32 -> int widening.
func toPipelineSpec(spec weirdevv1alpha1.ProcessingPipelineSpec) pipelinespec.Spec {
	return pipelinespec.Spec{
		Source: pipelinespec.Source{
			Bucket: spec.Source.Bucket,
			Prefix: spec.Source.Prefix,
		},
		Worker: pipelinespec.Worker{
			Image:       spec.Worker.Image,
			Concurrency: int(spec.Worker.Concurrency),
		},
		Scaling: pipelinespec.Scaling{
			Min:        int(spec.Scaling.Min),
			Max:        int(spec.Scaling.Max),
			PerReplica: int(spec.Scaling.PerReplica),
		},
	}
}

// ProcessingPipelineReconciler reconciles a ProcessingPipeline object
type ProcessingPipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=weir.dev,resources=processingpipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=weir.dev,resources=processingpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=weir.dev,resources=processingpipelines/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *ProcessingPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var obj weirdevv1alpha1.ProcessingPipeline
	if err := r.Get(ctx, req.NamespacedName, &obj); err != nil {
		// The object can vanish between the watch event and this reconcile
		// (deletion race) - that is routine, not an error worth requeuing.
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	spec := toPipelineSpec(obj.Spec)
	violations := pipelinespec.ValidateSpec(spec)

	var (
		wantPhase           weirdevv1alpha1.ProcessingPipelinePhase
		wantConditionStatus metav1.ConditionStatus
		wantReason          string
		wantMessage         string
	)

	if len(violations) > 0 {
		messages := make([]string, 0, len(violations))
		for _, v := range violations {
			messages = append(messages, v.Error())
		}

		wantPhase = weirdevv1alpha1.ProcessingPipelinePhaseFailed
		wantConditionStatus = metav1.ConditionFalse
		wantReason = reasonInvalidSpec
		wantMessage = strings.Join(messages, "; ")

		log.Info("spec validation failed", "violations", len(violations))
	} else {
		wantPhase = weirdevv1alpha1.ProcessingPipelinePhasePending
		wantConditionStatus = metav1.ConditionTrue
		wantReason = reasonValidSpec
		wantMessage = "spec satisfies all validation rules"
	}

	// Idempotency: meta.SetStatusCondition already compares Status/Reason/
	// Message/ObservedGeneration against the existing condition (preserving
	// LastTransitionTime when Status hasn't changed) and reports whether
	// anything actually changed, so we use that return value directly
	// instead of re-deriving it - reconciling twice with no spec change
	// must perform zero additional status writes.
	changed := meta.SetStatusCondition(&obj.Status.Conditions, metav1.Condition{
		Type:               conditionTypeSpecValid,
		Status:             wantConditionStatus,
		Reason:             wantReason,
		Message:            wantMessage,
		ObservedGeneration: obj.Generation,
	})

	if !changed && obj.Status.Phase == wantPhase {
		return ctrl.Result{}, nil
	}

	obj.Status.Phase = wantPhase

	if err := r.Status().Update(ctx, &obj); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ProcessingPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&weirdevv1alpha1.ProcessingPipeline{}).
		Named("processingpipeline").
		Complete(r)
}
