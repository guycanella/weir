package pipelinespec

import (
	"strings"
	"testing"
)

// This file pins the behavior of the pure ProcessingPipeline spec validator
// (WR-009), the functional core of ADR-003: it takes a plain in-memory Spec
// value and returns the complete set of violations. No API machinery, no
// client, no I/O — the admission/reconcile-time wiring is WR-021's shell.
//
// The Spec struct deliberately mirrors the CR shape in DOCUMENTATION.md §3.4
// rather than importing generated kubebuilder types (which do not exist until
// WR-011/012). Keeping the core decoupled from the API types is the point of
// ADR-003; the later shell converts api/v1alpha1 -> pipelinespec.Spec.
//
// # Design decisions this file pins down
//
//  1. ALL violations are reported, not just the first. A human hand-authoring
//     a CR should see every problem in one apply, not discover them one at a
//     time across N edit/reapply cycles. Hence a slice return, never
//     fail-fast.
//
//  2. Errors are STRUCTURED: each carries the dotted CR field path that is at
//     fault (e.g. "spec.scaling.perReplica"), so a caller can attach the error
//     to the right field in a status condition or admission response. This
//     mirrors k8s.io/apimachinery's field.Error{Field, Detail} shape, which
//     is what WR-021 will convert these into — we model it here with stdlib
//     types only so the core package stays dependency-free.
//
//  3. Error order is DETERMINISTIC. Checks run in this fixed sequence, and
//     each violation is appended in that order:
//
//     1. spec.source.bucket      must be non-empty
//     2. spec.worker.image       must be non-empty
//     3. spec.worker.concurrency must be > 0
//     4. spec.scaling.min        must be >= 0
//     5. spec.scaling.max        must be >= 0
//     6. spec.scaling.min        must be <= spec.scaling.max
//     7. spec.scaling.perReplica must be > 0
//
//     Determinism matters twice: identical input must produce byte-identical
//     kubectl output (no map-iteration shuffling), and one field may legitimately
//     carry more than one violation (see the min=-1,max=-5 case below).
//
//     The concurrency check (3) sits with its sibling worker field rather than
//     at the end: the report reads in the same top-to-bottom order as the YAML
//     the user wrote (source -> worker -> scaling), which is the property that
//     makes a multi-violation message scannable. internal/controller's
//     Reconcile joins these messages with "; " in whatever order they arrive,
//     so the position is a readability contract, not a correctness one — which
//     is exactly why it needs a test to stop it drifting silently.
//
//  4. A valid Spec yields nil, not an empty non-nil slice — so callers can use
//     the idiomatic `if errs := ValidateSpec(s); errs != nil` and so the happy
//     path allocates nothing.
//
// # Judgment calls (documented so they are reviewable, not accidental)
//
//   - "non-empty" for bucket and image also rejects WHITESPACE-ONLY strings.
//     This is a reading of the stated rule, not a new rule: an S3 bucket name
//     may not contain spaces at all, and "   " is not a resolvable OCI
//     reference. Accepting them just defers the failure to an opaque runtime
//     ErrImagePull / AWS 400 instead of a clear admission error.
//
//   - NEGATIVE min/max are rejected (checks 3 and 4), which goes one step past
//     the literal Done-when. Rationale: `min <= max` does NOT imply sane
//     values — Spec{Min: -5, Max: -1} satisfies it. That value flows into
//     scaling.desiredReplicas (WR-006), which clamps to Min and would hand the
//     reconciler -5 replicas for a Deployment field that the Kubernetes API
//     defines as a non-negative int32. desiredReplicas' own doc comment names
//     "CR spec validation (WR-009)" as the layer that guarantees its
//     preconditions, so this package is the designated guard. The cost is two
//     `if` statements. max == 0 is explicitly allowed: min=0/max=0 is a
//     coherent "pipeline paused" state, and inventing a `max >= 1` rule would
//     be genuine scope creep.
//
// # Deliberately NOT validated here
//
//   - Full S3 bucket-name syntax (3-63 chars, lowercase, no underscores, not
//     IP-shaped). That is a well-defined AWS rule set worth its own task; the
//     Done-when asks only for "non-empty bucket".
//   - OCI reference syntax / tag-vs-digest policy for worker.image.
//   - An upper bound on spec.worker.concurrency. "Too high" is a resource
//     question (CPU/memory limits, SQS in-flight caps) with no defensible
//     constant; "<= 0" is a meaning question, and meaninglessness is what a
//     validator is for.
//   - spec.source.prefix (optional; empty legitimately means whole-bucket).
//
// # WR-015 addendum: spec.worker.concurrency
//
// WR-009 shipped with no rule for concurrency, and both its own review and
// WR-012's flagged that as a deferred gap: a worker told to process zero (or
// -1) events in parallel does not describe anything a controller could build,
// yet it passed validation silently. The rule added here is the same shape as
// spec.scaling.perReplica — strictly positive, same message — because it is
// the same kind of quantity: a per-replica throughput knob where zero is not
// a smaller amount of work, it is the absence of a meaning.

// Compile-time assertion that ValidationError satisfies error with a value
// receiver, so a []ValidationError element can be used directly as an error.
var _ error = ValidationError{}

// validSpec returns the canonical example CR from DOCUMENTATION.md §3.4.
// Every invalid case below starts from this value and breaks exactly one
// thing, which is what makes "the other rules still pass" true by
// construction rather than by hand-maintained duplication.
func validSpec() Spec {
	return Spec{
		Source: Source{
			Bucket: "uploads",
			Prefix: "raw/",
		},
		Worker: Worker{
			Image:       "ghcr.io/you/transcoder:v1",
			Concurrency: 4,
		},
		Scaling: Scaling{
			Min:        0,
			Max:        20,
			PerReplica: 30,
		},
	}
}

// wantErr is one expected violation. msgContains is optional: every message
// must always be non-empty, and where the message carries load-bearing
// information (a cross-field reference) we assert on a substring rather than
// the exact wording, so rephrasing the copy does not break the test.
type wantErr struct {
	field       string
	msgContains string
}

func TestValidateSpec(t *testing.T) {
	cases := []struct {
		name string
		// mutate breaks the canonical valid spec. nil means "use it as-is".
		mutate func(s *Spec)
		want   []wantErr
	}{
		// ------------------------------------------------------------------
		// Valid specs. Tested at more than one point so "valid" is not
		// pinned by a single lucky example.
		// ------------------------------------------------------------------
		{
			name:   "canonical example CR from the docs is valid",
			mutate: nil,
			want:   nil,
		},
		{
			name: "min equal to max is valid because the bound is inclusive",
			mutate: func(s *Spec) {
				s.Scaling.Min = 5
				s.Scaling.Max = 5
			},
			want: nil,
		},
		{
			name: "min of zero is valid because scale-to-zero is the design (ADR-002)",
			mutate: func(s *Spec) {
				s.Scaling.Min = 0
			},
			want: nil,
		},
		{
			name: "min and max both zero is a valid paused pipeline",
			mutate: func(s *Spec) {
				s.Scaling.Min = 0
				s.Scaling.Max = 0
			},
			want: nil,
		},
		{
			name: "perReplica of one is valid at the boundary just above zero",
			mutate: func(s *Spec) {
				s.Scaling.PerReplica = 1
			},
			want: nil,
		},
		{
			name: "empty prefix is valid and means the whole bucket",
			mutate: func(s *Spec) {
				s.Source.Prefix = ""
			},
			want: nil,
		},
		{
			// WR-009 left concurrency unvalidated and pinned that non-rule
			// here, explicitly so that adding the rule would be a deliberate
			// edit to this case rather than a surprise. This is that edit
			// (WR-015): zero is now a violation, and the boundary immediately
			// above it must stay valid.
			name: "concurrency of one is valid at the boundary just above zero",
			mutate: func(s *Spec) {
				s.Worker.Concurrency = 1
			},
			want: nil,
		},
		{
			name: "large concurrency is valid because the rule has no upper bound",
			mutate: func(s *Spec) {
				s.Worker.Concurrency = 1024
			},
			want: nil,
		},

		// ------------------------------------------------------------------
		// Rule 1: spec.source.bucket must be non-empty.
		// ------------------------------------------------------------------
		{
			name: "empty bucket is rejected",
			mutate: func(s *Spec) {
				s.Source.Bucket = ""
			},
			want: []wantErr{{field: "spec.source.bucket"}},
		},
		{
			name: "whitespace-only bucket is rejected as empty",
			mutate: func(s *Spec) {
				s.Source.Bucket = "   "
			},
			want: []wantErr{{field: "spec.source.bucket"}},
		},

		// ------------------------------------------------------------------
		// Rule 2: spec.worker.image must be set.
		// ------------------------------------------------------------------
		{
			name: "empty image is rejected",
			mutate: func(s *Spec) {
				s.Worker.Image = ""
			},
			want: []wantErr{{field: "spec.worker.image"}},
		},
		{
			name: "whitespace-only image is rejected as unset",
			mutate: func(s *Spec) {
				s.Worker.Image = " \t\n "
			},
			want: []wantErr{{field: "spec.worker.image"}},
		},

		// ------------------------------------------------------------------
		// Rule 3: spec.worker.concurrency must be > 0. The bound is strict,
		// exactly like spec.scaling.perReplica: a replica that processes zero
		// events in parallel is not a slower worker, it is a worker that can
		// never make progress, and the operator would happily run a
		// Deployment of them.
		// ------------------------------------------------------------------
		{
			name: "concurrency of zero is rejected because the bound is strict",
			mutate: func(s *Spec) {
				s.Worker.Concurrency = 0
			},
			want: []wantErr{{
				field:       "spec.worker.concurrency",
				msgContains: "must be greater than 0",
			}},
		},
		{
			name: "negative concurrency is rejected",
			mutate: func(s *Spec) {
				s.Worker.Concurrency = -1
			},
			want: []wantErr{{
				field:       "spec.worker.concurrency",
				msgContains: "must be greater than 0",
			}},
		},
		{
			// Guards against a rule written as `< 0` (a copy of the min/max
			// bound) instead of `<= 0`: that mistake still rejects -1, so a
			// negative-only test would pass a wrong implementation. Only the
			// zero case above distinguishes them, and this one keeps the
			// far-negative end honest.
			name: "large negative concurrency is rejected",
			mutate: func(s *Spec) {
				s.Worker.Concurrency = -1024
			},
			want: []wantErr{{field: "spec.worker.concurrency"}},
		},

		// ------------------------------------------------------------------
		// Rules 4 & 5: replica bounds may not be negative.
		// ------------------------------------------------------------------
		{
			name: "negative min is rejected even though it is below max",
			mutate: func(s *Spec) {
				s.Scaling.Min = -1 // -1 <= 20, so min<=max alone would not catch it
			},
			want: []wantErr{{field: "spec.scaling.min"}},
		},
		{
			// This is the case that justifies checks 3 and 4 existing at all:
			// min <= max holds, yet both bounds are nonsense and would drive a
			// negative Deployment replica count via desiredReplicas.
			name: "both bounds negative is rejected even though min is still below max",
			mutate: func(s *Spec) {
				s.Scaling.Min = -5
				s.Scaling.Max = -1
			},
			want: []wantErr{
				{field: "spec.scaling.min"},
				{field: "spec.scaling.max"},
			},
		},

		// ------------------------------------------------------------------
		// Rule 6: spec.scaling.min must be <= spec.scaling.max.
		// ------------------------------------------------------------------
		{
			name: "min greater than max is rejected",
			mutate: func(s *Spec) {
				s.Scaling.Min = 21
				s.Scaling.Max = 20
			},
			want: []wantErr{{field: "spec.scaling.min", msgContains: "max"}},
		},
		{
			name: "min exceeding max by one is rejected at the boundary",
			mutate: func(s *Spec) {
				s.Scaling.Min = 1
				s.Scaling.Max = 0
			},
			want: []wantErr{{field: "spec.scaling.min", msgContains: "max"}},
		},
		{
			// One field can carry two independent violations, and the
			// cross-field check must still run after the range check. Pins the
			// declared ordering: min(>=0), max(>=0), then min(<=max).
			name: "min both negative and above max reports every violated bound",
			mutate: func(s *Spec) {
				s.Scaling.Min = -1
				s.Scaling.Max = -5
			},
			want: []wantErr{
				{field: "spec.scaling.min"},
				{field: "spec.scaling.max"},
				{field: "spec.scaling.min", msgContains: "max"},
			},
		},

		// ------------------------------------------------------------------
		// Rule 7: spec.scaling.perReplica must be > 0. Zero is a violation,
		// not just negatives — desiredReplicas divides by it.
		// ------------------------------------------------------------------
		{
			name: "perReplica of zero is rejected because the bound is strict",
			mutate: func(s *Spec) {
				s.Scaling.PerReplica = 0
			},
			want: []wantErr{{field: "spec.scaling.perReplica"}},
		},
		{
			name: "negative perReplica is rejected",
			mutate: func(s *Spec) {
				s.Scaling.PerReplica = -30
			},
			want: []wantErr{{field: "spec.scaling.perReplica"}},
		},

		// ------------------------------------------------------------------
		// Multiple simultaneous violations. This is the whole reason the
		// function returns a slice instead of a single error: prove it does
		// not stop at the first problem.
		// ------------------------------------------------------------------
		{
			name: "two violations in different sections are both reported",
			mutate: func(s *Spec) {
				s.Source.Bucket = ""
				s.Scaling.PerReplica = 0
			},
			want: []wantErr{
				{field: "spec.source.bucket"},
				{field: "spec.scaling.perReplica"},
			},
		},
		{
			// Pins the concurrency check's exact position in the fixed order:
			// after the worker.image check, before any scaling check. Picking
			// one violation on either side of it is what makes this an
			// ordering assertion rather than a set-membership one.
			name: "invalid concurrency is reported between the image and scaling violations",
			mutate: func(s *Spec) {
				s.Worker.Image = ""
				s.Worker.Concurrency = 0
				s.Scaling.Min = -1
			},
			want: []wantErr{
				{field: "spec.worker.image"},
				{field: "spec.worker.concurrency"},
				{field: "spec.scaling.min"},
			},
		},
		{
			// The same ordering claim with the neighbouring checks *passing*,
			// so an implementation that appended concurrency last would still
			// be caught: here only bucket precedes it and only perReplica
			// follows it.
			name: "invalid concurrency is reported after bucket and before perReplica",
			mutate: func(s *Spec) {
				s.Source.Bucket = ""
				s.Worker.Concurrency = -2
				s.Scaling.PerReplica = 0
			},
			want: []wantErr{
				{field: "spec.source.bucket"},
				{field: "spec.worker.concurrency"},
				{field: "spec.scaling.perReplica"},
			},
		},
		{
			name: "every rule violated at once is reported in declared order",
			mutate: func(s *Spec) {
				s.Source.Bucket = ""
				s.Worker.Image = ""
				s.Worker.Concurrency = 0
				s.Scaling.Min = 21
				s.Scaling.Max = 20
				s.Scaling.PerReplica = -3
			},
			want: []wantErr{
				{field: "spec.source.bucket"},
				{field: "spec.worker.image"},
				{field: "spec.worker.concurrency"},
				{field: "spec.scaling.min", msgContains: "max"},
				{field: "spec.scaling.perReplica"},
			},
		},
		{
			// The zero-value Spec is what a CR with `spec: {}` deserializes
			// into, so it must produce a helpful report rather than panic or
			// pass. min=0/max=0 are fine; the four required-ish fields are
			// not — concurrency joins them under WR-015, because its zero
			// value is now a violation rather than an accepted default.
			name: "zero-value spec reports every missing requirement",
			mutate: func(s *Spec) {
				*s = Spec{}
			},
			want: []wantErr{
				{field: "spec.source.bucket"},
				{field: "spec.worker.image"},
				{field: "spec.worker.concurrency"},
				{field: "spec.scaling.perReplica"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			if tc.mutate != nil {
				tc.mutate(&spec)
			}

			got := ValidateSpec(spec)
			assertViolations(t, spec, got, tc.want)
		})
	}
}

// TestValidateSpecDoesNotMutateInput guards the "pure function" half of
// ADR-003: the validator must not rewrite (e.g. trim or default) the spec it
// is handed. Silent normalization inside a validator is how a value drifts
// away from what the user actually applied.
func TestValidateSpecDoesNotMutateInput(t *testing.T) {
	spec := validSpec()
	spec.Source.Bucket = "  uploads  "
	before := spec

	_ = ValidateSpec(spec)

	if spec != before {
		t.Errorf("ValidateSpec mutated its input: got %+v, want %+v", spec, before)
	}
}

// TestValidateSpecValidReturnsNilSlice pins nil (not an empty slice) as the
// representation of "valid", so `errs != nil` is a correct emptiness check for
// callers and the happy path allocates nothing.
func TestValidateSpecValidReturnsNilSlice(t *testing.T) {
	if got := ValidateSpec(validSpec()); got != nil {
		t.Errorf("ValidateSpec(valid spec) = %+v, want nil", got)
	}
}

// TestValidateSpecIsDeterministic pins that repeated calls on the same input
// produce the same errors in the same order — no map iteration in the
// implementation.
func TestValidateSpecIsDeterministic(t *testing.T) {
	spec := Spec{} // several violations at once

	first := ValidateSpec(spec)
	for i := 0; i < 10; i++ {
		got := ValidateSpec(spec)
		if len(got) != len(first) {
			t.Fatalf("call %d returned %d errors, first call returned %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("call %d error %d = %+v, first call had %+v", i, j, got[j], first[j])
			}
		}
	}
}

// TestValidationErrorMessage pins that a ValidationError renders both halves
// of itself, so a caller that only logs err.Error() still learns which field
// is at fault.
func TestValidationErrorMessage(t *testing.T) {
	err := ValidationError{
		Field:   "spec.scaling.perReplica",
		Message: "must be greater than 0",
	}

	msg := err.Error()
	if !strings.Contains(msg, err.Field) {
		t.Errorf("Error() = %q, want it to contain the field %q", msg, err.Field)
	}
	if !strings.Contains(msg, err.Message) {
		t.Errorf("Error() = %q, want it to contain the message %q", msg, err.Message)
	}
}

// assertViolations compares the reported violations against the expectation,
// position by position, and always requires a non-empty human-readable
// message. Asserting on field identity (not just a count) is what makes these
// cases meaningful: a validator that reported the right number of wrong
// errors would still pass a length-only check.
func assertViolations(t *testing.T, spec Spec, got []ValidationError, want []wantErr) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("ValidateSpec(%+v) returned %d violations %+v, want %d %+v",
			spec, len(got), got, len(want), want)
	}

	for i, w := range want {
		if got[i].Field != w.field {
			t.Errorf("violation %d: Field = %q, want %q (full result: %+v)",
				i, got[i].Field, w.field, got)
		}
		if strings.TrimSpace(got[i].Message) == "" {
			t.Errorf("violation %d for field %q has an empty Message; it must explain the rule",
				i, got[i].Field)
		}
		if w.msgContains != "" && !strings.Contains(got[i].Message, w.msgContains) {
			t.Errorf("violation %d for field %q: Message = %q, want it to mention %q",
				i, got[i].Field, got[i].Message, w.msgContains)
		}
	}
}
