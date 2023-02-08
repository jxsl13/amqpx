package logging

var _ Logger = (*NoOpLogger)(nil)

func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

type NoOpLogger struct{}

func (l *NoOpLogger) Debugf(format string, args ...any) {}
func (l *NoOpLogger) Infof(format string, args ...any)  {}
func (l *NoOpLogger) Warnf(format string, args ...any)  {}
func (l *NoOpLogger) Errorf(format string, args ...any) {}

func (l *NoOpLogger) Debug(args ...any) {}
func (l *NoOpLogger) Info(args ...any)  {}
func (l *NoOpLogger) Warn(args ...any)  {}
func (l *NoOpLogger) Error(args ...any) {}

func (l *NoOpLogger) WithError(_ error) Logger         { return l }
func (l *NoOpLogger) WithField(_ string, _ any) Logger { return l }
func (l *NoOpLogger) WithFields(_ Fields) Logger       { return l }
