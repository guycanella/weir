package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// newLogger builds a structured JSON logger writing to out (os.Stdout when
// out is nil) at the given level, so log records are machine-parseable
// rather than slog's default key=value text output, and verbosity can be
// changed via LOG_LEVEL on a running deployment.
func newLogger(out io.Writer, level slog.Level) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}
	return slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level}))
}

// parseLogLevel reads LOG_LEVEL's raw value, matching case-insensitively
// against the four canonical slog level names after trimming whitespace.
// Parsing is total: unset, blank, or unrecognised input falls back to Info
// rather than failing startup or silencing the logger.
func parseLogLevel(raw string) slog.Level {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	switch raw {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
