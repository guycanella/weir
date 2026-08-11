package telemetry

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/guycanella/weir/internal/awsclient"
)

// This file holds the in-memory OpenTelemetry harness shared by the WR-025
// tests, plus the exact contract those tests pin. Read this first.
//
// The API being pinned (internal/telemetry):
//
//	// Config selects where instrumentation sends its data. Both fields are
//	// optional: a zero Config uses the OTel global providers (what Setup
//	// installs), which is what production wiring wants; tests inject their
//	// own in-memory-backed providers instead so they never touch — or race
//	// on — process-global mutable state.
//	type Config struct {
//		TracerProvider trace.TracerProvider
//		MeterProvider  metric.MeterProvider
//	}
//
//	func InstrumentProcess(next worker.ProcessFunc, cfg Config) (worker.ProcessFunc, error)
//
//	func Setup(ctx context.Context, out io.Writer) (shutdown func(context.Context) error, err error)
//
// Five load-bearing decisions these tests encode:
//
//  1. Instrumentation is TRANSPARENT. The wrapper returns exactly what next
//     returned — same error identity, nil for nil — and propagates a panic
//     unchanged. Observability is a side channel; it must never become a
//     second source of truth about whether a message succeeded, because
//     worker.Run deletes the SQS message based on that return value.
//
//  2. Dependency injection over globals, matching the seam the rest of the
//     project already uses (worker.Worker's SQSClient, processing.Config).
//     InstrumentProcess reads providers from Config, defaulting to the
//     globals — so a test can assert on recorded telemetry without
//     otel.SetTracerProvider and without serialising against other tests.
//
//  3. Construction errors are returned, not swallowed. Building the
//     histogram instrument can fail, and a nil next is a wiring bug; both
//     surface at wiring time (mirroring processing.New) rather than as a
//     nil-panic or a silently un-instrumented worker on first delivery.
//
//  4. Latency is recorded for EVERY call, success or failure. A message that
//     failed still consumed time, and excluding failures would silently bias
//     the latency signal exactly when it matters most.
//
//  5. Setup takes the destination writer explicitly (nil meaning os.Stdout).
//     WR-025's Done-when is "a trace and latency metric are observable
//     locally"; making the sink a parameter is what lets a test PROVE that
//     end to end — exporters flush real span/metric JSON into a buffer — for
//     one parameter of extra API surface.

const (
	// spanName is the span the wrapper opens per message.
	spanName = "weir.worker.process_message"

	// metricName / metricUnit are the processing-latency histogram. Unit is
	// seconds ("s") per OTel convention for durations, not milliseconds.
	metricName = "weir.worker.processing.duration"
	metricUnit = "s"

	// attrMessageID identifies the message on the span, using the OTel
	// messaging semantic-convention key rather than an ad-hoc one.
	attrMessageID = "messaging.message.id"
)

func testMessage() awsclient.Message {
	return awsclient.Message{
		MessageId:     "msg-0001",
		ReceiptHandle: "rh-0001",
		Body:          `{"Records":[]}`,
	}
}

// harness wires an in-memory span exporter and a manual metric reader into a
// Config, so a test can run the wrapped ProcessFunc and then read back
// exactly what was recorded, synchronously and without any exporter, network
// or global provider involved.
type harness struct {
	t      *testing.T
	spans  *tracetest.InMemoryExporter
	reader *sdkmetric.ManualReader
	cfg    Config
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	t.Cleanup(func() {
		ctx := context.Background()
		if err := tp.Shutdown(ctx); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
		if err := mp.Shutdown(ctx); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})

	return &harness{
		t:      t,
		spans:  spans,
		reader: reader,
		cfg:    Config{TracerProvider: tp, MeterProvider: mp},
	}
}

// wrap builds the instrumented ProcessFunc, failing the test if construction
// errors — every test in this package expects a valid wiring except the ones
// that deliberately probe construction failure.
func (h *harness) wrap(next func(context.Context, awsclient.Message) error) func(context.Context, awsclient.Message) error {
	h.t.Helper()
	got, err := InstrumentProcess(next, h.cfg)
	if err != nil {
		h.t.Fatalf("InstrumentProcess returned error: %v", err)
	}
	if got == nil {
		h.t.Fatal("InstrumentProcess returned a nil ProcessFunc with a nil error")
	}
	return got
}

// recordedSpans returns the ended spans named spanName.
func (h *harness) recordedSpans() tracetest.SpanStubs {
	h.t.Helper()
	var out tracetest.SpanStubs
	for _, s := range h.spans.GetSpans() {
		if s.Name == spanName {
			out = append(out, s)
		}
	}
	return out
}

// onlySpan asserts exactly one span named spanName was recorded and returns
// it.
func (h *harness) onlySpan() tracetest.SpanStub {
	h.t.Helper()
	got := h.recordedSpans()
	if len(got) != 1 {
		h.t.Fatalf("recorded %d spans named %q, want exactly 1 (all: %v)", len(got), spanName, spanNames(h.spans.GetSpans()))
	}
	return got[0]
}

// histogram pulls the latency histogram out of the manual reader. It fails
// the test if no metric named metricName exists, or if it is not a
// float64 histogram — an int64 histogram or a counter would not answer
// "how long did processing take".
func (h *harness) histogram() metricdata.Histogram[float64] {
	h.t.Helper()

	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		h.t.Fatalf("collect metrics: %v", err)
	}

	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
			if m.Name != metricName {
				continue
			}
			if m.Unit != metricUnit {
				h.t.Errorf("metric %q unit = %q, want %q", m.Name, m.Unit, metricUnit)
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				h.t.Fatalf("metric %q data is %T, want metricdata.Histogram[float64]", m.Name, m.Data)
			}
			return hist
		}
	}

	h.t.Fatalf("no metric named %q was recorded (recorded: %v)", metricName, names)
	return metricdata.Histogram[float64]{}
}

// latencyCount sums the Count across every data point of the latency
// histogram. Summing (rather than demanding a single data point) is
// deliberate: the implementation is free to attach attributes such as an
// outcome/error dimension, which splits recordings across attribute sets.
// What the tests care about is "how many observations were made", which is
// attribute-independent.
func (h *harness) latencyCount() uint64 {
	h.t.Helper()
	var total uint64
	for _, dp := range h.histogram().DataPoints {
		total += dp.Count
	}
	return total
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

func attrValue(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// safeBuffer is a mutex-guarded buffer: OTel exporters may flush from a
// background goroutine, so a plain bytes.Buffer would be a data race under
// -race.
type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// startedSpanFrom returns the span carried by ctx, for assertions about what
// the wrapped function actually sees.
func startedSpanFrom(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}
