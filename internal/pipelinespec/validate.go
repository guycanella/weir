// Package pipelinespec holds the pure decision core for validating a
// ProcessingPipeline spec (WR-009): given an in-memory Spec value, report
// every rule it violates. No API machinery, no client, no I/O — the
// admission/reconcile-time wiring is a later shell's job (ADR-003).
package pipelinespec

import (
	"fmt"
	"strings"
)

// Spec mirrors the ProcessingPipeline CR shape described in
// DOCUMENTATION.md §3.4. It deliberately does not import generated
// kubebuilder types so the validation core stays dependency-free and
// testable in isolation; a later shell converts api/v1alpha1 -> this type.
type Spec struct {
	Source  Source
	Worker  Worker
	Scaling Scaling
}

// Source describes where the pipeline reads its events from.
type Source struct {
	Bucket string
	Prefix string
}

// Worker describes the image that processes each event.
type Worker struct {
	Image       string
	Concurrency int
}

// Scaling captures the replica floor/ceiling and the backlog-per-replica
// ratio used by internal/scaling.desiredReplicas.
type Scaling struct {
	Min        int
	Max        int
	PerReplica int
}

// ValidationError mirrors k8s.io/apimachinery field.Error{Field, Detail}
// using stdlib types only, so a later task can convert cleanly without
// introducing a new dependency into this package.
type ValidationError struct {
	Field   string // dotted CR path, e.g. "spec.scaling.perReplica"
	Message string
}

// Error renders both the field and the message so a caller that only logs
// err.Error() still learns which field is at fault.
func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidateSpec checks spec against every rule and returns every violation
// found, in a fixed, deterministic order. nil means valid; callers should use
// `if errs := ValidateSpec(s); errs != nil`. spec is never mutated.
func ValidateSpec(spec Spec) []ValidationError {
	var errs []ValidationError

	if strings.TrimSpace(spec.Source.Bucket) == "" {
		errs = append(errs, ValidationError{
			Field:   "spec.source.bucket",
			Message: "must not be empty",
		})
	}

	if strings.TrimSpace(spec.Worker.Image) == "" {
		errs = append(errs, ValidationError{
			Field:   "spec.worker.image",
			Message: "must not be empty",
		})
	}

	if spec.Worker.Concurrency <= 0 {
		errs = append(errs, ValidationError{
			Field:   "spec.worker.concurrency",
			Message: "must be greater than 0",
		})
	}

	if spec.Scaling.Min < 0 {
		errs = append(errs, ValidationError{
			Field:   "spec.scaling.min",
			Message: "must be greater than or equal to 0",
		})
	}

	if spec.Scaling.Max < 0 {
		errs = append(errs, ValidationError{
			Field:   "spec.scaling.max",
			Message: "must be greater than or equal to 0",
		})
	}

	if spec.Scaling.Min > spec.Scaling.Max {
		errs = append(errs, ValidationError{
			Field:   "spec.scaling.min",
			Message: "must be less than or equal to spec.scaling.max",
		})
	}

	if spec.Scaling.PerReplica <= 0 {
		errs = append(errs, ValidationError{
			Field:   "spec.scaling.perReplica",
			Message: "must be greater than 0",
		})
	}

	return errs
}
