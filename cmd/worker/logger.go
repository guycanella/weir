package main

import (
	"io"
	"log/slog"
	"os"
)

// newLogger builds a structured JSON logger writing to out (os.Stdout when
// out is nil), so log records are machine-parseable rather than slog's
// default key=value text output.
func newLogger(out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}
	return slog.New(slog.NewJSONHandler(out, nil))
}
