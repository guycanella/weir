// Package telemetry wires OpenTelemetry tracing and metrics around the
// worker's message processing (WR-025). It stays a thin imperative shell
// around worker.ProcessFunc: InstrumentProcess wraps a ProcessFunc with a
// span and a latency observation per call, and Setup installs local
// stdout-backed tracer/meter providers so both are observable without a
// collector or backend.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

// Naming note: spanName, metricName, metricUnit and attrMessageID are
// pinned as literal constants by the test suite in helpers_test.go (a
// non-test file cannot redeclare the same const in this package), so this
// file mirrors those exact literal values under different identifiers.
const (
	instrumentationName = "github.com/guycanella/weir/internal/telemetry"

	processSpanName = "weir.worker.process_message"

	processMetricName        = "weir.worker.processing.duration"
	processMetricUnit        = "s"
	processMetricDescription = "Duration of worker message processing"

	processMessageIDAttr = "messaging.message.id"
)

// Config selects where instrumentation sends its data. Both fields are
// optional: a zero Config resolves to the OTel global tracer/meter
// providers, which is what Setup installs — so production wiring can pass a
// zero Config and pick up whatever Setup configured.
type Config struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// InstrumentProcess wraps next so that every call opens a span named
// "weir.worker.process_message" and records one observation into the
// "weir.worker.processing.duration" latency histogram, in seconds.
// Instrumentation is transparent: the returned ProcessFunc returns exactly
// what next returned (same error identity) and propagates a panic unchanged.
//
// A nil next is a wiring bug and is rejected immediately, rather than
// nil-panicking on first delivery. Building the histogram instrument can
// itself fail; that error is also returned here rather than swallowed.
func InstrumentProcess(next worker.ProcessFunc, cfg Config) (worker.ProcessFunc, error) {
	if next == nil {
		return nil, errors.New("telemetry: InstrumentProcess: next must not be nil")
	}

	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	tracer := tp.Tracer(instrumentationName)
	meter := mp.Meter(instrumentationName)

	histogram, err := meter.Float64Histogram(
		processMetricName,
		metric.WithUnit(processMetricUnit),
		metric.WithDescription(processMetricDescription),
	)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, msg awsclient.Message) (err error) {
		ctx, span := tracer.Start(ctx, processSpanName,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attribute.String(processMessageIDAttr, msg.MessageId)),
		)

		start := time.Now()
		defer func() {
			r := recover()

			elapsed := time.Since(start).Seconds()
			histogram.Record(ctx, elapsed)

			switch {
			case r != nil:
				panicErr := fmt.Errorf("panic: %v", r)
				span.SetStatus(codes.Error, panicErr.Error())
				span.RecordError(panicErr)
			case err != nil:
				span.SetStatus(codes.Error, err.Error())
				span.RecordError(err)
			}
			span.End()

			if r != nil {
				panic(r)
			}
		}()

		err = next(ctx, msg)
		return err
	}, nil
}
