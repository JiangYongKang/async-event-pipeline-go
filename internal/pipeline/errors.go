package pipeline

import (
	"errors"
	"fmt"
)

// ClassifyError marks a processor error for retry or terminal dead-lettering.

// Retry wraps err as a transient error: the event will be redelivered until
// MaxAttempts is exhausted.
func Retry(err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{kind: kindRetry, err: err}
}

// Retryf is Retry with a formatted message.
func Retryf(format string, args ...any) error {
	return classifiedError{kind: kindRetry, err: fmt.Errorf(format, args...)}
}

// Fatal wraps err as unrecoverable: the event dead-letters immediately.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{kind: kindFatal, err: err}
}

// Fatalf is Fatal with a formatted message.
func Fatalf(format string, args ...any) error {
	return classifiedError{kind: kindFatal, err: fmt.Errorf(format, args...)}
}

type errorKind int

const (
	kindUnknown errorKind = iota
	kindRetry
	kindFatal
)

type classifiedError struct {
	kind errorKind
	err  error
}

func (e classifiedError) Error() string { return e.err.Error() }
func (e classifiedError) Unwrap() error { return e.err }

func classify(err error) errorKind {
	var c classifiedError
	if errors.As(err, &c) {
		return c.kind
	}
	return kindUnknown
}
