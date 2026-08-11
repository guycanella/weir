package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/guycanella/weir/internal/awsclient"
)

// This file pins InstrumentProcess (WR-025): one span and one latency
// observation per message, and a return value that is byte-for-byte the
// wrapped function's. See helpers_test.go for the API and the decisions.

// --- construction ---------------------------------------------------------

func TestInstrumentProcessRejectsANilNext(t *testing.T) {
	got, err := InstrumentProcess(nil, Config{})
	if err == nil {
		t.Fatal("InstrumentProcess(nil, ...) returned a nil error, want an error: a nil next would nil-panic on the first delivery, i.e. in production, not at wiring time")
	}
	if got != nil {
		t.Errorf("InstrumentProcess(nil, ...) returned a non-nil ProcessFunc alongside an error; a caller that logs and continues must not end up holding a usable-looking wrapper")
	}
}

// TestInstrumentProcessDefaultsToTheGlobalProviders pins that a zero Config
// is valid and resolves to the OTel globals — that is what production wiring
// (cmd/worker, after Setup) relies on, so it must not be a silent no-op.
func TestInstrumentProcessDefaultsToTheGlobalProviders(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})

	wrapped, err := InstrumentProcess(func(context.Context, awsclient.Message) error { return nil }, Config{})
	if err != nil {
		t.Fatalf("InstrumentProcess with a zero Config: %v", err)
	}
	if err := wrapped(context.Background(), testMessage()); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	if got := len(spans.GetSpans()); got != 1 {
		t.Errorf("recorded %d spans via the global tracer provider, want 1", got)
	}
}

// --- the happy path -------------------------------------------------------

func TestInstrumentProcessRecordsASuccessfulCall(t *testing.T) {
	h := newHarness(t)

	called := 0
	wrapped := h.wrap(func(context.Context, awsclient.Message) error {
		called++
		return nil
	})

	if err := wrapped(context.Background(), testMessage()); err != nil {
		t.Fatalf("wrapped process returned %v, want nil: instrumentation must pass a nil result through unchanged", err)
	}
	if called != 1 {
		t.Fatalf("next called %d times, want exactly 1", called)
	}

	span := h.onlySpan()
	if span.Status.Code == codes.Error {
		t.Errorf("span status = %v (%q), want a non-error status for a successful call", span.Status.Code, span.Status.Description)
	}
	if len(span.Events) != 0 {
		t.Errorf("span recorded %d events on a successful call, want 0 (no exception should be recorded)", len(span.Events))
	}
	if span.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span kind = %v, want %v: this span covers handling a message pulled from a queue", span.SpanKind, trace.SpanKindConsumer)
	}

	if got := h.latencyCount(); got != 1 {
		t.Errorf("latency histogram recorded %d observations, want 1", got)
	}
}

// TestInstrumentProcessRecordsANonNegativeDuration keeps the timing
// assertion to what is actually deterministic: a duration is never negative,
// and it is expressed in seconds so a fast call stays well under a second.
// Asserting an exact value would be a flake.
func TestInstrumentProcessRecordsANonNegativeDuration(t *testing.T) {
	h := newHarness(t)

	wrapped := h.wrap(func(context.Context, awsclient.Message) error { return nil })
	if err := wrapped(context.Background(), testMessage()); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	hist := h.histogram()
	if len(hist.DataPoints) == 0 {
		t.Fatal("latency histogram has no data points")
	}
	for _, dp := range hist.DataPoints {
		if dp.Sum < 0 {
			t.Errorf("latency histogram data point sum = %v, want >= 0", dp.Sum)
		}
		if v, ok := dp.Min.Value(); ok && v < 0 {
			t.Errorf("latency histogram minimum = %v, want >= 0", v)
		}
		if v, ok := dp.Max.Value(); ok && v > 60 {
			t.Errorf("latency histogram maximum = %v, want a value in SECONDS for a no-op call (a millisecond-based unit would show up here)", v)
		}
	}
}

func TestInstrumentProcessAttachesTheMessageIDToTheSpan(t *testing.T) {
	h := newHarness(t)

	msg := testMessage()
	wrapped := h.wrap(func(context.Context, awsclient.Message) error { return nil })
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	span := h.onlySpan()
	got, ok := attrValue(span.Attributes, attrMessageID)
	if !ok {
		t.Fatalf("span has no %q attribute; without a message identifier a trace cannot be tied back to a delivery (attributes: %v)", attrMessageID, span.Attributes)
	}
	if got.AsString() != msg.MessageId {
		t.Errorf("span attribute %q = %q, want %q", attrMessageID, got.AsString(), msg.MessageId)
	}
}

// --- the failure path -----------------------------------------------------

func TestInstrumentProcessReturnsTheExactErrorFromNext(t *testing.T) {
	h := newHarness(t)

	sentinel := errors.New("stub failed")
	wrapped := h.wrap(func(context.Context, awsclient.Message) error {
		return fmt.Errorf("process message: %w", sentinel)
	})

	err := wrapped(context.Background(), testMessage())
	if err == nil {
		t.Fatal("wrapped process returned nil, want the error next returned: worker.Run would delete a message that never succeeded")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("wrapped process returned %v, which does not wrap the sentinel error; instrumentation must not replace or re-wrap next's error", err)
	}
	if got := err.Error(); got != "process message: "+sentinel.Error() {
		t.Errorf("wrapped process error = %q, want it unchanged (%q): callers already log this text", got, "process message: "+sentinel.Error())
	}
}

func TestInstrumentProcessMarksTheSpanFailedAndStillRecordsLatency(t *testing.T) {
	h := newHarness(t)

	failure := errors.New("stub failed")
	wrapped := h.wrap(func(context.Context, awsclient.Message) error { return failure })

	if err := wrapped(context.Background(), testMessage()); !errors.Is(err, failure) {
		t.Fatalf("wrapped process returned %v, want %v", err, failure)
	}

	span := h.onlySpan()
	if span.Status.Code != codes.Error {
		t.Errorf("span status code = %v, want %v for a failed call", span.Status.Code, codes.Error)
	}
	if !strings.Contains(span.Status.Description, failure.Error()) {
		t.Errorf("span status description = %q, want it to contain %q", span.Status.Description, failure.Error())
	}

	// span.RecordError adds an "exception" event; that is what makes the
	// error visible in a trace UI, distinct from the status code.
	var foundException bool
	for _, ev := range span.Events {
		if ev.Name != "exception" {
			continue
		}
		foundException = true
		msg, ok := attrValue(ev.Attributes, "exception.message")
		if !ok || !strings.Contains(msg.AsString(), failure.Error()) {
			t.Errorf("exception event message = %q (present=%v), want it to contain %q", msg.AsString(), ok, failure.Error())
		}
	}
	if !foundException {
		t.Errorf("span recorded no \"exception\" event; span.RecordError(err) was not called (events: %v)", span.Events)
	}

	// Decision 4: a failed message still consumed processing time.
	if got := h.latencyCount(); got != 1 {
		t.Errorf("latency histogram recorded %d observations for a failed call, want 1: excluding failures would bias the latency signal exactly when it matters", got)
	}
}

// TestInstrumentProcessTreatsAContextCanceledResultAsAFailure pins the
// boundary the worker actually hits on shutdown: a ProcessFunc that returns
// ctx.Err() is a failed delivery like any other — no special-casing that
// would make a canceled run look successful in the trace.
func TestInstrumentProcessTreatsAContextCanceledResultAsAFailure(t *testing.T) {
	h := newHarness(t)

	wrapped := h.wrap(func(ctx context.Context, _ awsclient.Message) error { return ctx.Err() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := wrapped(ctx, testMessage()); !errors.Is(err, context.Canceled) {
		t.Fatalf("wrapped process returned %v, want context.Canceled passed through", err)
	}

	if got := h.onlySpan().Status.Code; got != codes.Error {
		t.Errorf("span status code = %v, want %v", got, codes.Error)
	}
	if got := h.latencyCount(); got != 1 {
		t.Errorf("latency histogram recorded %d observations, want 1", got)
	}
}

// --- context propagation --------------------------------------------------

// TestInstrumentProcessPassesTheSpanContextToNext proves the span is
// genuinely attached to the context next receives — not merely created
// alongside it — by matching the in-context span id against the exported
// span. Without this, any child instrumentation inside processing would be
// orphaned from the message's trace.
func TestInstrumentProcessPassesTheSpanContextToNext(t *testing.T) {
	h := newHarness(t)

	var seen trace.SpanContext
	wrapped := h.wrap(func(ctx context.Context, _ awsclient.Message) error {
		seen = startedSpanFrom(ctx).SpanContext()
		return nil
	})

	if err := wrapped(context.Background(), testMessage()); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	if !seen.IsValid() {
		t.Fatal("the context passed to next carries no valid span; next cannot start child spans or correlate logs")
	}
	span := h.onlySpan()
	if seen.SpanID() != span.SpanContext.SpanID() {
		t.Errorf("span id seen by next = %s, want the exported span's id %s", seen.SpanID(), span.SpanContext.SpanID())
	}
	if seen.TraceID() != span.SpanContext.TraceID() {
		t.Errorf("trace id seen by next = %s, want %s", seen.TraceID(), span.SpanContext.TraceID())
	}
}

// TestInstrumentProcessSpanParentsChildSpansStartedByNext is the observable
// consequence of the above: a child span opened inside next is nested under
// the message span, so one trace shows the whole delivery.
func TestInstrumentProcessSpanParentsChildSpansStartedByNext(t *testing.T) {
	h := newHarness(t)

	wrapped := h.wrap(func(ctx context.Context, _ awsclient.Message) error {
		_, child := h.cfg.TracerProvider.Tracer("test").Start(ctx, "child")
		child.End()
		return nil
	})

	if err := wrapped(context.Background(), testMessage()); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	parent := h.onlySpan()
	all := h.spans.GetSpans()
	var child *tracetest.SpanStub
	for i := range all {
		if all[i].Name == "child" {
			child = &all[i]
		}
	}
	if child == nil {
		t.Fatalf("child span was not recorded (spans: %v)", spanNames(all))
	}
	if child.Parent.SpanID() != parent.SpanContext.SpanID() {
		t.Errorf("child span parent = %s, want the message span %s", child.Parent.SpanID(), parent.SpanContext.SpanID())
	}
}

// TestInstrumentProcessHonorsAnIncomingParentSpan pins that the message span
// nests under whatever span the caller already had, rather than always
// starting a fresh root trace.
func TestInstrumentProcessHonorsAnIncomingParentSpan(t *testing.T) {
	h := newHarness(t)

	wrapped := h.wrap(func(context.Context, awsclient.Message) error { return nil })

	ctx, root := h.cfg.TracerProvider.Tracer("test").Start(context.Background(), "root")
	if err := wrapped(ctx, testMessage()); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}
	root.End()

	span := h.onlySpan()
	if span.Parent.SpanID() != root.SpanContext().SpanID() {
		t.Errorf("message span parent = %s, want the incoming root span %s", span.Parent.SpanID(), root.SpanContext().SpanID())
	}
}

// --- repetition -----------------------------------------------------------

// TestInstrumentProcessRecordsOneSpanAndOneObservationPerCall guards the
// classic instrumentation bug of creating the instrument or the span once per
// wrapper instead of once per message.
func TestInstrumentProcessRecordsOneSpanAndOneObservationPerCall(t *testing.T) {
	h := newHarness(t)

	const n = 5
	failEvery := 2 // some succeed, some fail: both must be counted
	wrapped := h.wrap(func(_ context.Context, msg awsclient.Message) error {
		if msg.MessageId == fmt.Sprintf("msg-%d", failEvery) {
			return errors.New("boom")
		}
		return nil
	})

	for i := 0; i < n; i++ {
		msg := testMessage()
		msg.MessageId = fmt.Sprintf("msg-%d", i)
		_ = wrapped(context.Background(), msg)
	}

	if got := len(h.recordedSpans()); got != n {
		t.Errorf("recorded %d spans for %d calls, want %d", got, n, n)
	}
	if got := h.latencyCount(); got != n {
		t.Errorf("latency histogram recorded %d observations for %d calls, want %d", got, n, n)
	}

	// Each span must carry its own message id, not the first one's.
	seen := map[string]bool{}
	for _, s := range h.recordedSpans() {
		v, ok := attrValue(s.Attributes, attrMessageID)
		if !ok {
			t.Fatalf("span %s has no %q attribute", s.SpanContext.SpanID(), attrMessageID)
		}
		seen[v.AsString()] = true
	}
	if len(seen) != n {
		t.Errorf("spans carry %d distinct message ids, want %d (%v)", len(seen), n, seen)
	}
}

// TestInstrumentProcessIsSafeUnderConcurrentCalls matters because
// worker.Worker dispatches Process from one goroutine per message with
// Concurrency > 1; the wrapper must not share mutable per-call state. Run
// this suite with -race for it to be meaningful.
func TestInstrumentProcessIsSafeUnderConcurrentCalls(t *testing.T) {
	h := newHarness(t)

	wrapped := h.wrap(func(context.Context, awsclient.Message) error { return nil })

	const n = 16
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			msg := testMessage()
			msg.MessageId = fmt.Sprintf("msg-%d", i)
			if err := wrapped(context.Background(), msg); err != nil {
				t.Errorf("wrapped process: %v", err)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		<-done
	}

	if got := len(h.recordedSpans()); got != n {
		t.Errorf("recorded %d spans for %d concurrent calls, want %d", got, n, n)
	}
	if got := h.latencyCount(); got != n {
		t.Errorf("latency histogram recorded %d observations for %d concurrent calls, want %d", got, n, n)
	}
}

// --- panics ---------------------------------------------------------------

// TestInstrumentProcessDoesNotSwallowOrLeakOnAPanic pins the hygiene part of
// panic handling. WR-025 does not ask for a worker that SURVIVES a panicking
// processor (worker.Run has no recover today, and adding one would be a
// behavior change belonging to its own task), so the contract is:
//
//   - the panic value propagates unchanged, so the process still fails loudly
//     exactly as it does without instrumentation;
//   - the span is ENDED, not leaked half-open — which an idiomatic
//     `defer span.End()` gives for free.
//
// The trace CONTENT of a panicking call is pinned separately, by
// TestInstrumentProcessMarksTheSpanFailedOnAPanic.
func TestInstrumentProcessDoesNotSwallowOrLeakOnAPanic(t *testing.T) {
	h := newHarness(t)

	panicValue := "stub exploded"
	wrapped := h.wrap(func(context.Context, awsclient.Message) error {
		panic(panicValue)
	})

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("the panic did not propagate; instrumentation must not turn a programming error into a silent success")
			}
			if r != any(panicValue) {
				t.Errorf("recovered %v, want the original panic value %v unchanged", r, panicValue)
			}
		}()
		_ = wrapped(context.Background(), testMessage())
	}()

	// The exporter only ever receives ENDED spans, so a recorded span here is
	// proof the span was closed rather than leaked.
	if got := len(h.recordedSpans()); got != 1 {
		t.Errorf("recorded %d ended spans after a panic, want 1: the span must be ended (e.g. via defer), not leaked", got)
	}
}

// TestInstrumentProcessMarksTheSpanFailedOnAPanic closes the gap the
// propagation test above leaves open: a panicking delivery must not EXPORT as
// a successful one.
//
// A panic unwinds past the `err = next(ctx, msg)` assignment, so the named
// return stays nil and any `if err != nil` guard in the deferred cleanup never
// fires. The span is still ended and the latency still recorded, so the
// exported trace shows a span with an Unset status and no exception event —
// indistinguishable, in a trace UI or an error-rate query, from a message that
// processed cleanly. That is worse than no telemetry: it actively hides the
// crash. So a panicking call must be recorded as a failure, exactly like a
// returned error is: status codes.Error carrying the panic value, plus an
// "exception" event so it surfaces as an error in a trace UI.
//
// The subtests cover both shapes a panic value takes in practice — a plain
// value (which the implementation must render into an error itself) and an
// error — since only the first is at risk of being dropped for want of an
// `error` to hand to RecordError.
func TestInstrumentProcessMarksTheSpanFailedOnAPanic(t *testing.T) {
	tests := []struct {
		name       string
		panicValue any
		wantInDesc string
	}{
		{
			name:       "a non-error panic value",
			panicValue: "stub exploded",
			wantInDesc: "stub exploded",
		},
		{
			name:       "an error panic value",
			panicValue: errors.New("stub exploded as an error"),
			wantInDesc: "stub exploded as an error",
		},
		{
			name:       "a runtime panic",
			panicValue: nil, // signals: trigger a real nil-map write below
			wantInDesc: "assignment to entry in nil map",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)

			wrapped := h.wrap(func(context.Context, awsclient.Message) error {
				if tc.panicValue == nil {
					var m map[string]string
					m["boom"] = "" //nolint:staticcheck // deliberate nil-map write to trigger a real runtime panic, not an explicit panic call
					return nil
				}
				panic(tc.panicValue)
			})

			// The panic must still propagate: recording it in the trace must
			// not be implemented by swallowing it.
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_ = wrapped(context.Background(), testMessage())
			}()
			if recovered == nil {
				t.Fatal("the panic did not propagate; instrumentation must not turn a programming error into a silent success")
			}
			if tc.panicValue != nil && recovered != tc.panicValue {
				t.Errorf("recovered %v, want the original panic value %v unchanged", recovered, tc.panicValue)
			}

			// The exporter only receives ENDED spans, so getting one back is
			// proof the span was closed rather than leaked.
			span := h.onlySpan()

			if span.Status.Code != codes.Error {
				t.Errorf("span status code = %v (description %q), want %v: a panicking delivery exports as a successful span, so a crash is invisible in the trace and in any error-rate query built on span status",
					span.Status.Code, span.Status.Description, codes.Error)
			}
			if !strings.Contains(span.Status.Description, tc.wantInDesc) {
				t.Errorf("span status description = %q, want it to contain the panic value %q: the status must say WHAT crashed, not just that something did",
					span.Status.Description, tc.wantInDesc)
			}

			// span.RecordError adds an "exception" event; that event, not the
			// status code, is what a trace UI renders as an error on the span.
			var foundException bool
			for _, ev := range span.Events {
				if ev.Name != "exception" {
					continue
				}
				foundException = true
				msg, ok := attrValue(ev.Attributes, "exception.message")
				if !ok || !strings.Contains(msg.AsString(), tc.wantInDesc) {
					t.Errorf("exception event message = %q (present=%v), want it to contain the panic value %q", msg.AsString(), ok, tc.wantInDesc)
				}
			}
			if !foundException {
				t.Errorf("span recorded no \"exception\" event after a panic; the panic was never handed to span.RecordError (events: %v)", span.Events)
			}

			// Decision 4 holds for panics too: the crashed delivery still
			// consumed processing time, and dropping the observation would
			// silently truncate the latency signal for the slowest, most
			// broken calls.
			if got := h.latencyCount(); got != 1 {
				t.Errorf("latency histogram recorded %d observations for a panicking call, want 1", got)
			}
		})
	}
}
