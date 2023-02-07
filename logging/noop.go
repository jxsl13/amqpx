package logging

import "io"

var _ Logger = (*NoOpLogger)(nil)

func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

type NoOpLogger struct{}

func (l *NoOpLogger) Debugf(format string, args ...any) {}
func (l *NoOpLogger) Infof(format string, args ...any)  {}
func (l *NoOpLogger) Printf(format string, args ...any) {}
func (l *NoOpLogger) Warnf(format string, args ...any)  {}
func (l *NoOpLogger) Errorf(format string, args ...any) {}
func (l *NoOpLogger) Fatalf(format string, args ...any) {}
func (l *NoOpLogger) Panicf(format string, args ...any) {}

func (l *NoOpLogger) Debug(args ...any) {}
func (l *NoOpLogger) Info(args ...any)  {}
func (l *NoOpLogger) Print(args ...any) {}
func (l *NoOpLogger) Warn(args ...any)  {}
func (l *NoOpLogger) Error(args ...any) {}
func (l *NoOpLogger) Fatal(args ...any) {}
func (l *NoOpLogger) Panic(args ...any) {}

func (l *NoOpLogger) Output() io.Writer     { return io.Discard }
func (l *NoOpLogger) SetOutput(_ io.Writer) {}

func (l *NoOpLogger) WithError(_ error) Logger         { return l }
func (l *NoOpLogger) WithField(_ string, _ any) Logger { return l }
func (l *NoOpLogger) WithFields(_ Fields) Logger       { return l }
