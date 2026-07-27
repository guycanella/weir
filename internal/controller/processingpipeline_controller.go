/*
Copyright 2026 Guilherme Canella.

Licensed under the MIT License. See the LICENSE file at the
root of this repository for the full license text.
*/

package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	weirdevv1alpha1 "github.com/guycanella/weir/api/v1alpha1"
)

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
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ProcessingPipeline object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *ProcessingPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ProcessingPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&weirdevv1alpha1.ProcessingPipeline{}).
		Named("processingpipeline").
		Complete(r)
}
