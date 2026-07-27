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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	weirdevv1alpha1 "github.com/guycanella/weir/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// The contract these tests pin (WR-015, reconcile-time spec validation)
// ---------------------------------------------------------------------------
//
// Reconcile converts the CR's Spec into an internal/pipelinespec.Spec, runs
// pipelinespec.ValidateSpec, and reports the verdict through status:
//
//	invalid spec -> Status.Phase = "Failed"
//	                condition {Type: "SpecValid", Status: False,
//	                           Reason: "InvalidSpec",
//	                           Message: enumerates every violation,
//	                                    each as "<field>: <message>",
//	                           ObservedGeneration: obj.Generation}
//
//	valid spec   -> Status.Phase = "Pending"
//	                condition {Type: "SpecValid", Status: True,
//	                           Reason: "ValidSpec",
//	                           Message: non-empty,
//	                           ObservedGeneration: obj.Generation}
//
// In both cases Reconcile returns (ctrl.Result{}, nil): a spec that is
// invalid now is still invalid on immediate retry, so returning an error
// would only buy pointless exponential backoff.
//
// Why the condition type is "SpecValid" and not the more familiar "Ready":
// this reconciler provisions nothing yet (no queue, no Deployment - those
// are later phases). A "Ready: True" written by a controller that has not
// made the pipeline ready would be a claim later tasks must walk back, and
// only ever writing "Ready: False" would leave the happy path unobservable.
// "SpecValid" states exactly what this loop knows, and composes: a future
// "Ready" can be derived from SpecValid && Provisioned && ... without
// renaming or reinterpreting anything written here.
//
// Why Phase "Pending" for a valid spec: DOCUMENTATION.md defines Pending as
// "CR accepted, reconciliation not yet started" - which is precisely a
// pipeline whose spec has been accepted but whose infrastructure does not
// exist yet. Provisioning/Running belong to the tasks that actually build
// the infrastructure.
const (
	wantConditionTypeSpecValid = "SpecValid"
	wantReasonValidSpec        = "ValidSpec"
	wantReasonInvalidSpec      = "InvalidSpec"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// statusCountingClient wraps a client.Client and counts writes to the status
// subresource, so an "idempotent reconcile" assertion can be a concrete
// "wrote nothing" rather than the much weaker "returned no error".
type statusCountingClient struct {
	client.Client
	statusWrites int
}

func (c *statusCountingClient) Status() client.SubResourceWriter {
	return &countingSubResourceWriter{SubResourceWriter: c.Client.Status(), owner: c}
}

type countingSubResourceWriter struct {
	client.SubResourceWriter
	owner *statusCountingClient
}

func (w *countingSubResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.owner.statusWrites++
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func (w *countingSubResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	w.owner.statusWrites++
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// newValidationTestPipeline builds an otherwise-valid ProcessingPipeline
// whose scaling numbers the caller chooses, so each test varies exactly the
// one dimension it is about.
func newValidationTestPipeline(name string, min, max, perReplica int32) *weirdevv1alpha1.ProcessingPipeline {
	return &weirdevv1alpha1.ProcessingPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: weirdevv1alpha1.ProcessingPipelineSpec{
			Source: weirdevv1alpha1.ProcessingPipelineSource{
				Bucket: "test-bucket",
			},
			Worker: weirdevv1alpha1.ProcessingPipelineWorker{
				Image: "example.com/worker:latest",
				// Set explicitly (WR-015): ValidateSpec now rejects
				// concurrency <= 0, so leaving this at its zero value would
				// quietly turn every "valid spec" case below into a
				// concurrency test. These specs are about scaling bounds.
				Concurrency: 1,
			},
			Scaling: weirdevv1alpha1.ProcessingPipelineScaling{
				Min:        min,
				Max:        max,
				PerReplica: perReplica,
			},
		},
	}
}

// createForTest persists the pipeline and registers its deletion, so every
// spec gets its own resource and specs cannot leak state into each other.
func createForTest(pipeline *weirdevv1alpha1.ProcessingPipeline) types.NamespacedName {
	GinkgoHelper()

	Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pipeline))).To(Succeed())
	})

	return types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}
}

func fetchPipeline(key types.NamespacedName) *weirdevv1alpha1.ProcessingPipeline {
	GinkgoHelper()

	fetched := &weirdevv1alpha1.ProcessingPipeline{}
	Expect(k8sClient.Get(ctx, key, fetched)).To(Succeed())

	return fetched
}

// reconcileOnce runs a single Reconcile against the given client and asserts
// the "no error, no requeue" half of the contract, returning the result so a
// caller can assert more.
func reconcileOnce(c client.Client, key types.NamespacedName) reconcile.Result {
	GinkgoHelper()

	reconciler := &ProcessingPipelineReconciler{
		Client: c,
		Scheme: k8sClient.Scheme(),
	}

	result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	Expect(err).NotTo(HaveOccurred(),
		"a spec-validation verdict must be reported through status, never returned as an error")

	return result
}

func specValidCondition(pipeline *weirdevv1alpha1.ProcessingPipeline) *metav1.Condition {
	GinkgoHelper()

	condition := meta.FindStatusCondition(pipeline.Status.Conditions, wantConditionTypeSpecValid)
	Expect(condition).NotTo(BeNil(),
		"expected a %q condition on %s, got conditions: %+v",
		wantConditionTypeSpecValid, pipeline.Name, pipeline.Status.Conditions)

	return condition
}

// ---------------------------------------------------------------------------
// specs
// ---------------------------------------------------------------------------

var _ = Describe("ProcessingPipeline spec validation", func() {
	Context("when the spec violates a rule the CRD schema cannot express", func() {
		// min > max is the one pipelinespec.ValidateSpec rule the OpenAPI
		// schema structurally cannot express (it is cross-field), so it is
		// the one invalid spec that actually reaches the reconciler. This
		// is the case reconcile-time validation exists for.
		It("accepts min > max at the API server, proving the reconciler is the only line of defence", func() {
			By("creating a pipeline with min greater than max")
			key := createForTest(newValidationTestPipeline("invalid-min-gt-max-persists", 10, 1, 10))

			By("confirming the API server persisted it verbatim")
			persisted := fetchPipeline(key)
			Expect(persisted.Spec.Scaling.Min).To(BeEquivalentTo(10))
			Expect(persisted.Spec.Scaling.Max).To(BeEquivalentTo(1))
		})

		It("marks the pipeline Failed with an InvalidSpec condition naming the violated field", func() {
			key := createForTest(newValidationTestPipeline("invalid-min-gt-max", 10, 1, 10))

			By("reconciling the invalid resource")
			result := reconcileOnce(k8sClient, key)

			By("not asking for a requeue - the spec will not fix itself on retry")
			Expect(result.IsZero()).To(BeTrue())

			By("reporting the failure through status")
			reconciled := fetchPipeline(key)
			Expect(reconciled.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))

			condition := specValidCondition(reconciled)
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(wantReasonInvalidSpec))
			Expect(condition.ObservedGeneration).To(Equal(reconciled.Generation),
				"the condition must record which generation it judged, or a reader cannot tell a stale verdict from a current one")

			By("naming the offending field and rule in the message")
			Expect(condition.Message).To(ContainSubstring("spec.scaling.min"))
			Expect(condition.Message).To(ContainSubstring("must be less than or equal to spec.scaling.max"))
		})

		It("still fails a spec whose min exceeds max by one", func() {
			// Boundary: the rule is min <= max, so 2/1 is invalid while 1/1
			// (below) is valid. Guards against a > vs >= slip.
			key := createForTest(newValidationTestPipeline("invalid-min-gt-max-by-one", 2, 1, 1))

			reconcileOnce(k8sClient, key)

			reconciled := fetchPipeline(key)
			Expect(reconciled.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))
			Expect(specValidCondition(reconciled).Reason).To(Equal(wantReasonInvalidSpec))
		})
	})

	Context("when the spec satisfies every validation rule", func() {
		It("does not mark the pipeline Failed and records a SpecValid condition", func() {
			key := createForTest(newValidationTestPipeline("valid-scale-to-zero", 0, 5, 10))

			reconcileOnce(k8sClient, key)

			reconciled := fetchPipeline(key)
			Expect(reconciled.Status.Phase).NotTo(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))
			Expect(reconciled.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhasePending),
				"a spec that passed validation but whose infrastructure does not exist yet is Pending")

			condition := specValidCondition(reconciled)
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(wantReasonValidSpec))
			Expect(condition.Message).NotTo(BeEmpty())
			Expect(condition.ObservedGeneration).To(Equal(reconciled.Generation))
		})

		It("accepts min == max, the inclusive edge of the min <= max rule", func() {
			key := createForTest(newValidationTestPipeline("valid-min-equals-max", 3, 3, 1))

			reconcileOnce(k8sClient, key)

			reconciled := fetchPipeline(key)
			Expect(reconciled.Status.Phase).NotTo(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))
			Expect(specValidCondition(reconciled).Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("when a previously invalid spec is corrected", func() {
		// A reconciler that can only ever transition *into* Failed is a real
		// bug: the user's obvious next move after seeing Failed is to edit
		// the spec, and the pipeline must recover rather than stay stuck.
		It("clears the Failed phase and flips the condition back to True", func() {
			key := createForTest(newValidationTestPipeline("self-healing", 10, 1, 10))

			By("reconciling the initially invalid spec")
			reconcileOnce(k8sClient, key)

			failed := fetchPipeline(key)
			Expect(failed.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))
			Expect(specValidCondition(failed).Status).To(Equal(metav1.ConditionFalse))

			By("raising max above min so the spec becomes valid")
			fixed := fetchPipeline(key)
			fixed.Spec.Scaling.Max = 20
			Expect(k8sClient.Update(ctx, fixed)).To(Succeed())

			By("reconciling the corrected spec")
			reconcileOnce(k8sClient, key)

			recovered := fetchPipeline(key)
			Expect(recovered.Status.Phase).NotTo(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed),
				"a corrected spec must not stay stuck in Failed")
			Expect(recovered.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhasePending))

			condition := specValidCondition(recovered)
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(wantReasonValidSpec))
			Expect(condition.Message).NotTo(ContainSubstring("spec.scaling.min"),
				"the stale failure detail must not survive the fix")
			Expect(condition.ObservedGeneration).To(Equal(recovered.Generation),
				"the verdict must be attributed to the corrected generation, not the broken one")
		})
	})

	Context("when reconciling repeatedly with no spec change", func() {
		// Guards the classic hot-loop bug: a reconciler that rewrites status
		// unconditionally bumps resourceVersion, which re-triggers its own
		// watch, which reconciles again, forever.
		It("writes status once for an invalid spec and not again", func() {
			key := createForTest(newValidationTestPipeline("idempotent-invalid", 10, 1, 10))
			counting := &statusCountingClient{Client: k8sClient}

			By("reconciling once")
			reconcileOnce(counting, key)
			Expect(counting.statusWrites).To(BeNumerically(">=", 1),
				"the first reconcile has a verdict to record, so it must write status")

			afterFirst := fetchPipeline(key)
			Expect(afterFirst.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed))
			writesAfterFirst := counting.statusWrites

			By("reconciling again with nothing changed")
			reconcileOnce(counting, key)

			Expect(counting.statusWrites).To(Equal(writesAfterFirst),
				"status already says what the second reconcile would say, so it must not write again")

			afterSecond := fetchPipeline(key)
			Expect(afterSecond.ResourceVersion).To(Equal(afterFirst.ResourceVersion),
				"a no-op reconcile must not bump resourceVersion")
			Expect(specValidCondition(afterSecond).LastTransitionTime).
				To(Equal(specValidCondition(afterFirst).LastTransitionTime),
					"an unchanged condition keeps its original transition time")
		})

		It("writes status once for a valid spec and not again", func() {
			key := createForTest(newValidationTestPipeline("idempotent-valid", 0, 5, 10))
			counting := &statusCountingClient{Client: k8sClient}

			reconcileOnce(counting, key)
			afterFirst := fetchPipeline(key)
			writesAfterFirst := counting.statusWrites
			Expect(writesAfterFirst).To(BeNumerically(">=", 1))

			reconcileOnce(counting, key)

			Expect(counting.statusWrites).To(Equal(writesAfterFirst))
			Expect(fetchPipeline(key).ResourceVersion).To(Equal(afterFirst.ResourceVersion))
		})
	})

	Context("when the ProcessingPipeline no longer exists", func() {
		It("is a no-op rather than an error", func() {
			// Deletion races are routine: the object can vanish between the
			// watch event and the reconcile. NotFound must be swallowed, not
			// requeued with backoff.
			result := reconcileOnce(k8sClient, types.NamespacedName{
				Name:      "does-not-exist",
				Namespace: "default",
			})
			Expect(result.IsZero()).To(BeTrue())
		})
	})

	// These pin WR-012's schema markers as the *first* line of defence and
	// document why the remaining ValidateSpec rules have no reconciler test:
	// the API server rejects them at admission, so they can never reach
	// Reconcile. Deliberately brief - the rules themselves are already
	// covered by internal/pipelinespec's unit tests.
	Context("rules the API server already rejects at admission", func() {
		It("rejects an empty source.bucket", func() {
			pipeline := newValidationTestPipeline("schema-empty-bucket", 0, 5, 10)
			pipeline.Spec.Source.Bucket = ""

			Expect(k8sClient.Create(ctx, pipeline)).NotTo(Succeed())
		})

		It("rejects a whitespace-only worker.image", func() {
			pipeline := newValidationTestPipeline("schema-blank-image", 0, 5, 10)
			pipeline.Spec.Worker.Image = "   "

			Expect(k8sClient.Create(ctx, pipeline)).NotTo(Succeed())
		})

		It("rejects a negative scaling.min", func() {
			pipeline := newValidationTestPipeline("schema-negative-min", -1, 5, 10)

			Expect(k8sClient.Create(ctx, pipeline)).NotTo(Succeed())
		})

		It("rejects a zero scaling.perReplica", func() {
			pipeline := newValidationTestPipeline("schema-zero-per-replica", 0, 5, 0)

			Expect(k8sClient.Create(ctx, pipeline)).NotTo(Succeed())
		})

		It("rejects an explicitly zero worker.concurrency", func() {
			// Has to go through an unstructured object: the typed struct
			// tags concurrency `omitempty`, so a Go client physically
			// cannot transmit 0 - it is dropped and becomes an omission.
			// `kubectl apply` with `concurrency: 0` in the YAML *can*, and
			// that is the path this guards (minimum: 1 on the schema).
			Expect(k8sClient.Create(ctx, zeroConcurrencyPipelineYAML("schema-zero-concurrency"))).
				NotTo(Succeed())
		})
	})

	// -----------------------------------------------------------------------
	// worker.concurrency (WR-015)
	// -----------------------------------------------------------------------
	//
	// internal/pipelinespec.ValidateSpec now rejects concurrency <= 0. That
	// rule closes a real gap - a worker processing zero events in parallel
	// describes nothing - but on its own it breaks a field the CRD advertises
	// as Optional: an omitted concurrency decodes to the Go zero value, and
	// the reconciler would mark the pipeline Failed for not setting a field
	// the user was told they could leave out. So the rule has to arrive with
	// an API-server default.
	//
	// Why the default belongs on the schema and not in toPipelineSpec: a
	// value the API server writes is visible in `kubectl get -o yaml`, so the
	// user can see the concurrency their workers actually run with. A
	// fallback applied inside the reconciler would leave the stored spec
	// disagreeing with the running behaviour, and would also make the
	// pure-core rule permanently unreachable from the CRD path - defence that
	// can never fire is not defence.
	Context("when worker.concurrency is omitted", func() {
		It("has the API server default it, so a valid spec is stored", func() {
			pipeline := newValidationTestPipeline("concurrency-omitted", 0, 5, 10)
			pipeline.Spec.Worker.Concurrency = 0 // omitempty: not sent at all

			key := createForTest(pipeline)

			Expect(fetchPipeline(key).Spec.Worker.Concurrency).To(BeEquivalentTo(1),
				"omitting an optional field must not store a value the validator rejects; "+
					"1 is the conservative default - one event at a time is what a user who said nothing expects, "+
					"and it never invents parallelism they did not ask for")
		})

		It("reconciles to SpecValid instead of Failed", func() {
			pipeline := newValidationTestPipeline("concurrency-omitted-reconcile", 0, 5, 10)
			pipeline.Spec.Worker.Concurrency = 0

			key := createForTest(pipeline)
			reconcileOnce(k8sClient, key)

			reconciled := fetchPipeline(key)
			Expect(reconciled.Status.Phase).NotTo(Equal(weirdevv1alpha1.ProcessingPipelinePhaseFailed),
				"a CR that omits an Optional field must not be marked Failed")
			Expect(reconciled.Status.Phase).To(Equal(weirdevv1alpha1.ProcessingPipelinePhasePending))

			condition := specValidCondition(reconciled)
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(wantReasonValidSpec))
			Expect(condition.Message).NotTo(ContainSubstring("spec.worker.concurrency"))
		})
	})
})

// zeroConcurrencyPipelineYAML builds the object a user would get from
// `kubectl apply` on a manifest with an explicit `concurrency: 0`. It is
// unstructured on purpose: the typed Go struct cannot represent the
// difference between "0" and "absent".
func zeroConcurrencyPipelineYAML(name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"bucket": "test-bucket",
				},
				"worker": map[string]interface{}{
					"image":       "example.com/worker:latest",
					"concurrency": int64(0),
				},
				"scaling": map[string]interface{}{
					"min":        int64(0),
					"max":        int64(5),
					"perReplica": int64(10),
				},
			},
		},
	}
	obj.SetGroupVersionKind(weirdevv1alpha1.GroupVersion.WithKind("ProcessingPipeline"))

	return obj
}
