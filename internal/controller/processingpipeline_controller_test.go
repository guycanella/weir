/*
Copyright 2026 Guilherme Canella.

Licensed under the MIT License. See the LICENSE file at the
root of this repository for the full license text.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	weirdevv1alpha1 "github.com/guycanella/weir/api/v1alpha1"
)

var _ = Describe("ProcessingPipeline Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		processingpipeline := &weirdevv1alpha1.ProcessingPipeline{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ProcessingPipeline")
			err := k8sClient.Get(ctx, typeNamespacedName, processingpipeline)
			if err != nil && errors.IsNotFound(err) {
				resource := &weirdevv1alpha1.ProcessingPipeline{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: weirdevv1alpha1.ProcessingPipelineSpec{
						Source: weirdevv1alpha1.ProcessingPipelineSource{
							Bucket: "test-bucket",
						},
						Worker: weirdevv1alpha1.ProcessingPipelineWorker{
							Image: "example.com/worker:latest",
						},
						Scaling: weirdevv1alpha1.ProcessingPipelineScaling{
							Min:        0,
							Max:        5,
							PerReplica: 10,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &weirdevv1alpha1.ProcessingPipeline{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ProcessingPipeline")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ProcessingPipelineReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("not marking a valid spec as Failed")
			// The full spec-validation contract (conditions, phases,
			// self-healing, idempotency) lives in
			// processingpipeline_validation_test.go; this baseline case
			// only guards that the happy path stays happy.
			reconciled := &weirdevv1alpha1.ProcessingPipeline{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, reconciled)).To(Succeed())
			Expect(reconciled.Status.Phase).NotTo(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))
		})
	})
})
