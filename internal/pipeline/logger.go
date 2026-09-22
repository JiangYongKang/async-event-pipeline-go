package pipeline

import (
	"log/slog"
	"os"
)

// Logger is the minimal structured logging surface used by the pipeline.
// *slog.Logger satisfies it, so the default is the standard library logger.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// LoggerOrDefault returns the configured logger or a text handler writing to
// stderr, matching the repository convention of stdlib-only diagnostics.
func (c Config) LoggerOrDefault() Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// DiscardLogger returns a logger that emits nothing (used in benchmarks or
// when callers manage logging themselves).
func DiscardLogger() Logger {
	return discardLogger{}
}

type discardLogger struct{}

func (discardLogger) Info(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}
