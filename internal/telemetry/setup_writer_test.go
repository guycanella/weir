package telemetry

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/guycanella/weir/internal/awsclient"
)

// This file pins the one thing Setup owns that neither exporter can own for
// itself: the SHARED destination.
//
// Setup hands the same io.Writer to two independent exporters. The trace
// exporter writes synchronously, on whichever worker goroutine finished a
// message; the metric exporter writes from the PeriodicReader's own background
// goroutine. Each exporter serialises its own writes internally, and neither
// knows the other exists — so nothing stops a span document and a metric
// document from being written to the same stream at the same moment. On
// os.Stdout that produces spliced JSON no collector can parse; on any writer
// with internal state (a bytes.Buffer, a bufio.Writer, a rotating file logger)
// it is an outright data race.
//
// Setup is therefore the only place the two can be serialised against each
// other, and it must use ONE shared synchronised wrapper for both exporters —
// wrapping each exporter's writer separately would compile, look right, and
// fix nothing.

// exporterFlusher is the flush capability the SDK meter provider exposes.
// Asserting for it (rather than importing the concrete type) keeps this test
// from caring how Setup builds the provider, while letting the test force
// metric exports to overlap with span exports instead of waiting out the
// PeriodicReader's default interval — no production knob has to be invented to
// make the hazard reproducible.
type exporterFlusher interface {
	ForceFlush(context.Context) error
}

// exportLoad drives concurrent traffic through the instrumented ProcessFunc
// while repeatedly forcing metric exports, so both exporters write to the
// shared destination at once. It returns once every span export has completed
// and the flush loops have stopped.
func exportLoad(t *testing.T, wrapped func(context.Context, awsclient.Message) error) {
	t.Helper()

	flusher, ok := otel.GetMeterProvider().(exporterFlusher)
	if !ok {
		t.Fatalf("the global meter provider (%T) exposes no ForceFlush; this test cannot make metric exports overlap with span exports", otel.GetMeterProvider())
	}

	const (
		writers        = 6
		callsPerWriter = 10
		flushers       = 2
	)

	stopFlushing := make(chan struct{})

	var flushWG sync.WaitGroup
	for i := 0; i < flushers; i++ {
		flushWG.Add(1)
		go func() {
			defer flushWG.Done()
			for {
				select {
				case <-stopFlushing:
					return
				default:
				}
				// Flush errors are not the subject here (a flush may race the
				// shutdown at the end of the test); the writes it triggers are.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = flusher.ForceFlush(ctx)
				cancel()
			}
		}()
	}

	var writeWG sync.WaitGroup
	for i := 0; i < writers; i++ {
		writeWG.Add(1)
		go func(i int) {
			defer writeWG.Done()
			for j := 0; j < callsPerWriter; j++ {
				msg := testMessage()
				msg.MessageId = string(rune('a'+i)) + "-" + string(rune('0'+j))
				if err := wrapped(context.Background(), msg); err != nil {
					t.Errorf("wrapped process: %v", err)
				}
			}
		}(i)
	}

	writeWG.Wait()
	close(stopFlushing)
	flushWG.Wait()
}

// TestSetupSerializesConcurrentExporterWrites is written to be run under
// -race (make test does), where it is the load-bearing assertion: the writer
// handed to Setup is a plain bytes.Buffer, deliberately NOT safe for
// concurrent use, standing in for every real destination that isn't. If Setup
// lets its two exporters write it concurrently, the race detector says so.
//
// The parse check is the same failure seen from the reader's side: whatever
// ends up in the buffer must still be a stream of whole JSON documents.
func TestSetupSerializesConcurrentExporterWrites(t *testing.T) {
	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	// Deliberately unsynchronised: Setup must not require its caller to pass a
	// concurrency-safe writer, because the caller passes os.Stdout, which the
	// caller cannot synchronise on Setup's behalf.
	var out bytes.Buffer

	shutdown, err := Setup(context.Background(), &out)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	wrapped, err := InstrumentProcess(func(context.Context, awsclient.Message) error { return nil }, Config{})
	if err != nil {
		t.Fatalf("InstrumentProcess: %v", err)
	}

	exportLoad(t, wrapped)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	docs := decodeExportedDocs(t, out.String())

	var spans, metrics int
	for _, doc := range docs {
		switch {
		case doc.isSpan():
			spans++
		case doc.isMetric():
			metrics++
		}
	}
	if spans == 0 {
		t.Error("no span documents were exported; the test exercised nothing")
	}
	if metrics == 0 {
		t.Error("no metric documents were exported; span and metric writes never actually overlapped, so this test proves nothing")
	}
}

// overlapWriter records whether two Writes were ever in flight simultaneously.
// It guards its own buffer, so it is safe to hand to an unsynchronised caller
// — its purpose is to REPORT the overlap rather than crash on it, which is
// what turns "the race detector might notice" into a deterministic assertion.
type overlapWriter struct {
	inFlight atomic.Int32
	overlaps atomic.Int64
	writes   atomic.Int64

	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *overlapWriter) Write(p []byte) (int, error) {
	w.writes.Add(1)
	if w.inFlight.Add(1) > 1 {
		w.overlaps.Add(1)
	}
	// Widen the window so an unsynchronised pair of exporters is caught by
	// design rather than by scheduler luck. This cannot produce a false
	// positive: a writer that is properly serialised never has two Writes in
	// flight, however slow each one is.
	time.Sleep(time.Millisecond)

	w.mu.Lock()
	n, err := w.buf.Write(p)
	w.mu.Unlock()

	w.inFlight.Add(-1)
	return n, err
}

func (w *overlapWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestSetupDoesNotLetTwoExportersWriteAtOnce states the requirement directly,
// so a regression fails with a readable message instead of a race-detector
// stack — and, unlike the -race test above, it also catches the plausible
// half-fix of giving each exporter its own synchronised writer, which
// serialises each exporter against itself and nothing against the other.
func TestSetupDoesNotLetTwoExportersWriteAtOnce(t *testing.T) {
	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	var out overlapWriter

	shutdown, err := Setup(context.Background(), &out)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	wrapped, err := InstrumentProcess(func(context.Context, awsclient.Message) error { return nil }, Config{})
	if err != nil {
		t.Fatalf("InstrumentProcess: %v", err)
	}

	exportLoad(t, wrapped)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := out.writes.Load(); got < 2 {
		t.Fatalf("the destination received %d writes; too few for concurrency to be exercised at all", got)
	}
	if got := out.overlaps.Load(); got != 0 {
		t.Errorf("%d of %d writes to the shared destination started while another was still in flight; the trace and metric exporters must share ONE synchronised writer, so a span document and a metric document can never be spliced together",
			got, out.writes.Load())
	}

	// And the output still has to be readable, for the same reason.
	decodeExportedDocs(t, out.String())
}
