package logging

type Fields = map[string]any

type Logger interface {
	BasicLogger

	WithField(key string, value any) Logger
	WithFields(fields Fields) Logger
	WithError(err error) Logger
}

type BasicLogger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)

	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
}
