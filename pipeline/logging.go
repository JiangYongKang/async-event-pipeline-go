package pipeline

import (
	"io"
	"log"
	"os"
)

// Logger 是管道内部使用的最小日志接口（单测可注入 testing.T）。
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// stdLogger 基于标准库 log 实现。
type stdLogger struct {
	l *log.Logger
}

func (s *stdLogger) Infof(format string, args ...any) {
	s.l.Printf("INFO  "+format, args...)
}
func (s *stdLogger) Warnf(format string, args ...any) {
	s.l.Printf("WARN  "+format, args...)
}
func (s *stdLogger) Errorf(format string, args ...any) {
	s.l.Printf("ERROR "+format, args...)
}

// NewLogger 创建输出到 w、带统一前缀的标准 Logger；w 为 nil 时输出 stderr。
func NewLogger(w io.Writer) Logger {
	if w == nil {
		w = os.Stderr
	}
	return &stdLogger{l: log.New(w, "[pipeline] ", log.LstdFlags|log.Lmicroseconds)}
}

// DiscardLogger 返回丢弃所有输出的 Logger。
func DiscardLogger() Logger { return discardLogger{} }

type discardLogger struct{}

func (discardLogger) Infof(format string, args ...any)  {}
func (discardLogger) Warnf(format string, args ...any)  {}
func (discardLogger) Errorf(format string, args ...any) {}
