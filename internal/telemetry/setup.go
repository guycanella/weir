package telemetry

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// lockedWriter serialises writes to a shared io.Writer. Setup hands the
// same instance to both the trace and metric exporters, which write from
// different goroutines (the trace exporter synchronously on whichever
// worker goroutine finished a message, the metric exporter from the
// PeriodicReader's own background goroutine) — without a shared lock their
// writes could interleave into a stream no collector can parse, or race
// outright on a destination with internal state.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// Setup installs OpenTelemetry tracer and meter providers backed by stdout
// exporters writing to out (os.Stdout when out is nil), and sets them as the
// OTel global providers so InstrumentProcess with a zero Config picks them
// up. It returns a shutdown function that flushes and shuts down both
// providers, aggregating any errors from either.
//
// The trace pipeline uses a simple (synchronous) span processor rather than
// a batching one, and the metric pipeline a periodic reader, so that a
// single worker invocation's trace and latency observation are actually
// visible in out by the time shutdown returns — this is a local-demo
// wiring, not a production exporter tuned for throughput.
func Setup(ctx context.Context, out io.Writer) (func(context.Context) error, error) {
	if out == nil {
		out = os.Stdout
	}
	out = &lockedWriter{w: out}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("weir-worker"),
	))
	if err != nil {
		return nil, err
	}

	traceExporter, err := stdouttrace.New(stdouttrace.WithWriter(out))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(traceExporter),
		sdktrace.WithResource(res),
	)

	metricExporter, err := stdoutmetric.New(stdoutmetric.WithWriter(out))
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, err
	}
	reader := sdkmetric.NewPeriodicReader(metricExporter)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)

	shutdown := func(ctx context.Context) error {
		var errs []error
		if err := tp.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		if err := mp.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}

	return shutdown, nil
}
