package logrusadapter

import (
	"io"

	"github.com/jxsl13/amqpx/logging"
	"github.com/sirupsen/logrus"
)

var (
	_ = (logging.Logger)((*LogrusLoggerWrapper)(nil))
	_ = (logging.Logger)((*fieldEntryWrapper)(nil))
)

func New(l *logrus.Logger) *LogrusLoggerWrapper {
	return &LogrusLoggerWrapper{
		Logger: l,
	}
}

func newFieldEntryWrapper(e *logrus.Entry) *fieldEntryWrapper {
	return &fieldEntryWrapper{
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
	return newFieldEntryWrapper(l.Logger.WithError(err))
}

func (l *LogrusLoggerWrapper) WithField(key string, value interface{}) logging.Logger {
	return newFieldEntryWrapper(l.Logger.WithField(key, value))
}

func (l *LogrusLoggerWrapper) WithFields(fields map[string]interface{}) logging.Logger {
	return newFieldEntryWrapper(l.Logger.WithFields(fields))
}

func (l *LogrusLoggerWrapper) Output() io.Writer {
	return l.Logger.Out
}

func (l *LogrusLoggerWrapper) SetOutput(w io.Writer) {
	l.Logger.Out = w
}

type fieldEntryWrapper struct {
	*logrus.Entry
}

func (l *fieldEntryWrapper) Level() logging.Level {
	return logging.Level(l.Logger.Level)
}

func (l *fieldEntryWrapper) SetLevel(lvl logging.Level) {
	l.Logger.SetLevel(logrus.Level(lvl))
}

func (l *fieldEntryWrapper) WithError(err error) logging.Logger {
	return newFieldEntryWrapper(l.Logger.WithError(err))
}

func (l *fieldEntryWrapper) WithField(key string, value interface{}) logging.Logger {
	return newFieldEntryWrapper(l.Logger.WithField(key, value))
}

func (l *fieldEntryWrapper) WithFields(fields map[string]interface{}) logging.Logger {
	return newFieldEntryWrapper(l.Logger.WithFields(fields))
}

func (l *fieldEntryWrapper) Output() io.Writer {
	return l.Logger.Out
}

func (l *fieldEntryWrapper) SetOutput(w io.Writer) {
	l.Logger.Out = w
}
