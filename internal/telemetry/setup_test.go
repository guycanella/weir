package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/guycanella/weir/internal/awsclient"
)

// This file pins Setup (WR-025): install global tracer/meter providers whose
// data lands somewhere a human can read locally, and hand back a shutdown
// function that flushes before the process exits.
//
// Every test here mutates OTel process globals, so none of them calls
// t.Parallel and each restores the previous providers on cleanup. The
// InstrumentProcess tests are immune to that because they inject providers
// explicitly (decision 2 in helpers_test.go).

// installGlobals runs Setup against an in-test writer and restores whatever
// providers were installed before, returning the buffer and the shutdown
// function.
func installGlobals(t *testing.T) (*safeBuffer, func(context.Context) error) {
	t.Helper()

	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	var out safeBuffer
	shutdown, err := Setup(context.Background(), &out)
	if err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("Setup returned a nil shutdown function with a nil error; the caller has no way to flush pending spans/metrics before exit")
	}
	return &out, shutdown
}

func TestSetupReturnsAShutdownFunctionThatSucceeds(t *testing.T) {
	_, shutdown := installGlobals(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown returned %v, want nil", err)
	}
}

// TestSetupInstallsGlobalProviders pins that Setup actually replaces the
// no-op globals. Without this, InstrumentProcess with a zero Config in
// cmd/worker would compile, run, and record nothing at all.
func TestSetupInstallsGlobalProviders(t *testing.T) {
	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()

	_, shutdown := installGlobals(t)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	if otel.GetTracerProvider() == prevTP {
		t.Error("Setup left the global tracer provider unchanged")
	}
	if otel.GetMeterProvider() == prevMP {
		t.Error("Setup left the global meter provider unchanged")
	}

	// A span from the global tracer must actually be recording — a provider
	// that is installed but sampling nothing is indistinguishable from no-op
	// for WR-025's Done-when.
	_, span := otel.Tracer("test").Start(context.Background(), "probe")
	recording := span.IsRecording()
	span.End()
	if !recording {
		t.Error("a span from the global tracer is not recording; nothing will ever be exported")
	}
}

// TestSetupMakesTracesAndLatencyObservableLocally is the direct test of
// WR-025's Done-when: after Setup, running an instrumented ProcessFunc and
// then shutting down must leave both a span and the latency metric in the
// exporter's output, with no collector or backend involved.
func TestSetupMakesTracesAndLatencyObservableLocally(t *testing.T) {
	out, shutdown := installGlobals(t)

	wrapped, err := InstrumentProcess(func(context.Context, awsclient.Message) error { return nil }, Config{})
	if err != nil {
		t.Fatalf("InstrumentProcess: %v", err)
	}
	msg := testMessage()
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned %v, want nil: pending data must be flushed, not dropped", err)
	}

	got := out.String()
	for _, want := range []string{spanName, msg.MessageId, metricName} {
		if !strings.Contains(got, want) {
			t.Errorf("exported output does not contain %q; a trace and the latency metric must both be observable locally.\n--- output ---\n%s", want, got)
		}
	}
}

// TestSetupIdentifiesTheWorkerServiceOnEveryExport pins resource attribution.
// Telemetry that cannot say WHICH service produced it is not usable in a
// cluster: with no explicit resource the SDK synthesises
// "unknown_service:<binary name>", so the worker's spans and its metrics
// arrive anonymous and indistinguishable from anything else's. Both providers
// need the resource — attaching it to the tracer provider only would leave the
// latency histogram unattributed, which is the half a dashboard queries.
//
// Only service.name is asserted. The SDK's default resource
// (telemetry.sdk.*) must survive too, which is why this checks for their
// presence rather than for an exact attribute set: replacing the default
// resource instead of merging into it would silently drop them.
func TestSetupIdentifiesTheWorkerServiceOnEveryExport(t *testing.T) {
	out, shutdown := installGlobals(t)

	wrapped, err := InstrumentProcess(func(context.Context, awsclient.Message) error { return nil }, Config{})
	if err != nil {
		t.Fatalf("InstrumentProcess: %v", err)
	}
	if err := wrapped(context.Background(), testMessage()); err != nil {
		t.Fatalf("wrapped process: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	docs := decodeExportedDocs(t, out.String())

	var sawSpan, sawMetric bool
	for _, doc := range docs {
		switch {
		case doc.isSpan():
			sawSpan = true
		case doc.isMetric():
			sawMetric = true
		default:
			continue
		}

		got, ok := doc.resourceAttr("service.name")
		if !ok {
			t.Errorf("exported document (span=%v) carries no string service.name resource attribute; every span and metric must name the emitting service (resource: %+v)", doc.isSpan(), doc.Resource)
			continue
		}
		if got != wantServiceName {
			t.Errorf("exported document (span=%v) service.name = %q, want %q", doc.isSpan(), got, wantServiceName)
		}
		if _, ok := doc.resourceAttr("telemetry.sdk.language"); !ok {
			t.Errorf("exported document (span=%v) lost the SDK's default resource attributes; the explicit resource must be MERGED into resource.Default(), not replace it (resource: %+v)", doc.isSpan(), doc.Resource)
		}
	}

	if !sawSpan {
		t.Errorf("no span document was exported, so span resource attribution was never checked\n--- output ---\n%s", out.String())
	}
	if !sawMetric {
		t.Errorf("no metric document was exported, so metric resource attribution was never checked\n--- output ---\n%s", out.String())
	}
}

// TestSetupDefaultsToStdoutWhenGivenANilWriter pins the production default so
// cmd/worker does not have to name os.Stdout. Nothing is recorded here, so
// this does not pollute the test log.
func TestSetupDefaultsToStdoutWhenGivenANilWriter(t *testing.T) {
	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
	})

	shutdown, err := Setup(context.Background(), nil)
	if err != nil {
		t.Fatalf("Setup(ctx, nil) returned error: %v, want a nil writer to mean os.Stdout", err)
	}
	if shutdown == nil {
		t.Fatal("Setup(ctx, nil) returned a nil shutdown function")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown returned %v, want nil", err)
	}
}

// TestSetupShutdownIsSafeToCallTwice matters because the caller defers
// shutdown and may also call it on a graceful-exit path; a second call must
// not panic. Returning an error the second time is acceptable — panicking or
// blocking is not.
func TestSetupShutdownIsSafeToCallTwice(t *testing.T) {
	_, shutdown := installGlobals(t)

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown returned %v, want nil", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("second shutdown panicked: %v", r)
			}
		}()
		_ = shutdown(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("second shutdown did not return; it must not block")
	}
}

// TestSetupShutdownFlushesEvenWithNothingRecorded guards against a shutdown
// that errors on an empty pipeline — the worker may exit before receiving any
// message at all, and that must not turn into a non-zero exit code.
func TestSetupShutdownFlushesEvenWithNothingRecorded(t *testing.T) {
	_, shutdown := installGlobals(t)

	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown with nothing recorded returned %v, want nil", err)
	}
}
