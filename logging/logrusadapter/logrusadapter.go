package logrusadapter

import (
	"io"

	"github.com/jxsl13/amqpx/logging"
	"github.com/sirupsen/logrus"
)

var (
	_ = (logging.Logger)((*LogrusLoggerWrapper)(nil))
	_ = (logging.Logger)((*LogrusFieldEntryWrapper)(nil))
)

func NewLogrusLoggerWrapper(l *logrus.Logger) *LogrusLoggerWrapper {
	return &LogrusLoggerWrapper{
		Logger: l,
	}
}

func NewLogrusFieldEntryWrapper(e *logrus.Entry) *LogrusFieldEntryWrapper {
	return &LogrusFieldEntryWrapper{
		Entry: e,
	}
}

type LogrusLoggerWrapper struct {
	*logrus.Logger
}

func (l *LogrusLoggerWrapper) Level() logging.Level {
	return logging.Level(l.Logger.Level)
}

func (l *LogrusLoggerWrapper) SetLevel(lvl logging.Level) {
	l.Logger.SetLevel(logrus.Level(lvl))
}

func (l *LogrusLoggerWrapper) WithError(err error) logging.Logger {
	return NewLogrusFieldEntryWrapper(l.Logger.WithError(err))
}

func (l *LogrusLoggerWrapper) WithField(key string, value interface{}) logging.Logger {
	return NewLogrusFieldEntryWrapper(l.Logger.WithField(key, value))
}

func (l *LogrusLoggerWrapper) WithFields(fields map[string]interface{}) logging.Logger {
	return NewLogrusFieldEntryWrapper(l.Logger.WithFields(fields))
}

func (l *LogrusLoggerWrapper) Output() io.Writer {
	return l.Logger.Out
}

func (l *LogrusLoggerWrapper) SetOutput(w io.Writer) {
	l.Logger.Out = w
}

type LogrusFieldEntryWrapper struct {
	*logrus.Entry
}

func (l *LogrusFieldEntryWrapper) Level() logging.Level {
	return logging.Level(l.Logger.Level)
}

func (l *LogrusFieldEntryWrapper) SetLevel(lvl logging.Level) {
	l.Logger.SetLevel(logrus.Level(lvl))
}

func (l *LogrusFieldEntryWrapper) WithError(err error) logging.Logger {
	return NewLogrusFieldEntryWrapper(l.Logger.WithError(err))
}

func (l *LogrusFieldEntryWrapper) WithField(key string, value interface{}) logging.Logger {
	return NewLogrusFieldEntryWrapper(l.Logger.WithField(key, value))
}

func (l *LogrusFieldEntryWrapper) WithFields(fields map[string]interface{}) logging.Logger {
	return NewLogrusFieldEntryWrapper(l.Logger.WithFields(fields))
}

func (l *LogrusFieldEntryWrapper) Output() io.Writer {
	return l.Logger.Out
}

func (l *LogrusFieldEntryWrapper) SetOutput(w io.Writer) {
	l.Logger.Out = w
}
