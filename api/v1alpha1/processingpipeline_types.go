/*
Copyright 2026 Guilherme Canella.

Licensed under the MIT License. See the LICENSE file at the
root of this repository for the full license text.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
//
// The nesting below (source/worker/scaling) intentionally mirrors
// internal/pipelinespec.Spec (WR-009) field-for-field so the reconcile shell
// can convert this CRD type into the pure-core Spec with a straightforward
// per-field copy rather than a reshaping exercise (see WR-012 briefing).
//
// Status.Backlog is an observation the shell polls from SQS, not a spec
// value; the pure decision core (internal/scaling.desiredReplicas) consumes
// a dereferenced, widened plain-int copy of this value, so *int32 here
// (a pointer so nil can distinguish "never observed" from an observed 0 -
// see the field's own doc comment) does not constrain that boundary.
// Status.Replicas mirrors the same pointer shape for the same reason.

// ProcessingPipelineSpec defines the desired state of ProcessingPipeline
type ProcessingPipelineSpec struct {
	// source describes where the pipeline reads its events from.
	// +kubebuilder:validation:Required
	Source ProcessingPipelineSource `json:"source"`

	// worker describes the image that processes each event.
	// +kubebuilder:validation:Required
	Worker ProcessingPipelineWorker `json:"worker"`

	// scaling captures the replica floor/ceiling and the backlog-per-replica
	// ratio used to derive the desired replica count.
	// +kubebuilder:validation:Required
	Scaling ProcessingPipelineScaling `json:"scaling"`
}

// ProcessingPipelineSource describes the S3 bucket (and optional key
// prefix) that produces the events this pipeline processes.
type ProcessingPipelineSource struct {
	// bucket is the name of the S3 bucket event notifications are read
	// from.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`\S`
	Bucket string `json:"bucket"`

	// prefix optionally scopes which object keys trigger processing.
	// +kubebuilder:validation:Optional
	Prefix string `json:"prefix,omitempty"`
}

// ProcessingPipelineWorker describes the container image that processes
// each event and how much work it does concurrently.
type ProcessingPipelineWorker struct {
	// image is the container image run for each worker replica.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`\S`
	Image string `json:"image"`

	// concurrency is the number of events a single worker replica
	// processes in parallel.
	// +kubebuilder:validation:Optional
	Concurrency int32 `json:"concurrency,omitempty"`
}

// ProcessingPipelineScaling captures the replica floor/ceiling and the
// backlog-per-replica ratio the KEDA ScaledObject uses to derive the
// desired replica count from the queue backlog (ADR-002).
type ProcessingPipelineScaling struct {
	// min is the minimum number of worker replicas. 0 enables
	// scale-to-zero.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min"`

	// max is the maximum number of worker replicas.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	Max int32 `json:"max"`

	// perReplica is the target number of backlog messages a single
	// replica should handle before another replica is added.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	PerReplica int32 `json:"perReplica"`
}

// ProcessingPipelinePhase is a high-level summary of where the pipeline is
// in its reconcile lifecycle. It is a coarse signal for `kubectl get`; the
// authoritative, detailed state lives in Conditions.
type ProcessingPipelinePhase string

const (
	// ProcessingPipelinePhasePending means the resource has been created
	// but the controller has not yet completed an initial reconcile.
	ProcessingPipelinePhasePending ProcessingPipelinePhase = "Pending"

	// ProcessingPipelinePhaseProvisioning means the controller is
	// creating the backing queue infrastructure (and other one-time
	// setup) for the pipeline.
	ProcessingPipelinePhaseProvisioning ProcessingPipelinePhase = "Provisioning"

	// ProcessingPipelinePhaseRunning means the pipeline's infrastructure
	// is provisioned and steady-state reconciliation (backlog polling,
	// replica scaling, including scale-to-zero) is underway. A pipeline
	// idled at zero replicas because its backlog is empty is still
	// Running, not Paused - scale-to-zero is a normal, expected steady
	// state for a backlog-driven autoscaler (ADR-002), not a distinct
	// lifecycle phase.
	ProcessingPipelinePhaseRunning ProcessingPipelinePhase = "Running"

	// ProcessingPipelinePhaseFailed means the controller could not
	// converge the pipeline to its desired state (e.g. invalid spec, or
	// an unrecoverable error provisioning infrastructure). Conditions
	// carry the detail.
	ProcessingPipelinePhaseFailed ProcessingPipelinePhase = "Failed"
)

// ProcessingPipelineStatus defines the observed state of ProcessingPipeline.
type ProcessingPipelineStatus struct {
	// phase is a coarse summary of the pipeline's reconcile lifecycle,
	// surfaced as the STATUS printer column.
	// +kubebuilder:validation:Enum=Pending;Provisioning;Running;Failed
	// +optional
	Phase ProcessingPipelinePhase `json:"phase,omitempty"`

	// backlog is the last observed number of undelivered messages on the
	// pipeline's queue, surfaced as the BACKLOG printer column. nil means
	// no observation has been made yet - distinct from an observed 0,
	// which is the pipeline's normal scale-to-zero steady state
	// (ADR-002), not an absent measurement.
	//
	// This is a pointer rather than a plain int32 so the two states don't
	// collapse into each other: encoding/json always serializes a plain
	// int32 (there is no Go zero value that means "unset"), so a status
	// update that only sets Phase and never touches Backlog would still
	// marshal "backlog": 0, falsely reporting an observed empty queue
	// when in truth nothing was ever observed. `omitempty` on a pointer
	// only treats nil as empty - a non-nil pointer to 0 still serializes,
	// so the BACKLOG printer column still shows 0 for genuine
	// scale-to-zero.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Backlog *int32 `json:"backlog,omitempty"`

	// replicas is the last observed number of worker replicas, surfaced
	// as the REPLICAS printer column. See the Backlog field's comment:
	// nil means "never observed", distinct from an observed 0
	// (scale-to-zero), and the same pointer + omitempty reasoning
	// applies.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// conditions represent the current state of the ProcessingPipeline resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Backlog",type=integer,JSONPath=".status.backlog"
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=".status.replicas"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// ProcessingPipeline is the Schema for the processingpipelines API
type ProcessingPipeline struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ProcessingPipeline
	// +required
	Spec ProcessingPipelineSpec `json:"spec"`

	// status defines the observed state of ProcessingPipeline
	// +optional
	Status ProcessingPipelineStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ProcessingPipelineList contains a list of ProcessingPipeline
type ProcessingPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ProcessingPipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ProcessingPipeline{}, &ProcessingPipelineList{})
}
