package logging

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
)

var _ Logger = (*TestLogger)(nil)

func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{
		level:  DebugLevel,
		t:      t,
		fields: map[string]any{},
	}
}

func newTestLoggerWithFields(l *TestLogger, fields Fields) *TestLogger {
	n := &TestLogger{
		level:  l.level,
		t:      l.t,
		fields: make(map[string]any, len(fields)+len(l.fields)),
	}

	for k, v := range l.fields {
		n.fields[k] = v
	}

	for k, v := range fields {
		n.fields[k] = v
	}
	return n
}

// TestLogger is supposed to be used in the Go testing framework.
type TestLogger struct {
	level  Level
	t      *testing.T
	fields map[string]any
}

func (l *TestLogger) Debugf(format string, args ...any) {
	l.t.Helper()
	l.logf(DebugLevel, "DEBUG", format, args...)
}
func (l *TestLogger) Infof(format string, args ...any) {
	l.t.Helper()
	l.logf(InfoLevel, "INFO", format, args...)
}
func (l *TestLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.logf(InfoLevel, "", format, args...)
}
func (l *TestLogger) Warnf(format string, args ...any) {
	l.t.Helper()
	l.logf(WarnLevel, "WARN", format, args...)
}
func (l *TestLogger) Errorf(format string, args ...any) {
	l.t.Helper()
	l.logf(ErrorLevel, "ERROR", format, args...)
}
func (l *TestLogger) Fatalf(format string, args ...any) {
	l.t.Helper()
	l.logf(FatalLevel, "FATAL", format, args...)
	os.Exit(1)
}
func (l *TestLogger) Panicf(format string, args ...any) {
	l.t.Helper()
	l.logf(PanicLevel, "PANIC", format, args...)
	os.Exit(1)
}

func (l *TestLogger) Debug(args ...any) {
	l.t.Helper()
	l.log(DebugLevel, "DEBUG", args...)
}
func (l *TestLogger) Info(args ...any) {
	l.t.Helper()
	l.log(InfoLevel, "INFO", args...)
}

func (l *TestLogger) Print(args ...any) {
	l.t.Helper()
	l.log(InfoLevel, "", args...)
}
func (l *TestLogger) Warn(args ...any) {
	l.t.Helper()
	l.log(WarnLevel, "WARN", args...)
}
func (l *TestLogger) Error(args ...any) {
	l.t.Helper()
	l.log(ErrorLevel, "ERROR", args...)
}
func (l *TestLogger) Fatal(args ...any) {
	l.t.Helper()
	l.log(FatalLevel, "FATAL", args...)
	os.Exit(1)
}
func (l *TestLogger) Panic(args ...any) {
	l.t.Helper()
	l.log(PanicLevel, "PANIC", args...)
}

func (l *TestLogger) Level() Level       { return l.level }
func (l *TestLogger) SetLevel(lvl Level) { l.level = lvl }

func (l *TestLogger) Output() io.Writer     { return io.Discard }
func (l *TestLogger) SetOutput(_ io.Writer) {}

func (l *TestLogger) WithError(err error) Logger {
	l.t.Helper()
	return newTestLoggerWithFields(l, map[string]any{"error": err})
}
func (l *TestLogger) WithField(key string, value any) Logger {
	return newTestLoggerWithFields(l, map[string]any{key: value})
}
func (l *TestLogger) WithFields(fields Fields) Logger {
	return newTestLoggerWithFields(l, fields)
}

func (l *TestLogger) fieldsMsg(level, msg string) string {

	size := len(l.fields) + 1
	if level != "" {
		size += 1
	}
	kv := make([]string, 0, size)
	for k, v := range l.fields {
		kv = append(kv, fmt.Sprintf("%s=%v", k, v))
	}

	prefix := ""
	if level != "" {
		prefix = fmt.Sprintf("level=%s", strings.ToLower(level))
	}

	prefix += fmt.Sprintf(", msg=%s", msg)

	sort.Strings(kv)
	return fmt.Sprintf("%s, %s", prefix, strings.Join(kv, ", "))
}

func (l *TestLogger) logf(reqLevel Level, prefix, format string, args ...any) {
	l.t.Helper()
	if l.level < reqLevel {
		return
	}

	msg := fmt.Sprintf(format, args...)
	if reqLevel <= FatalLevel {
		l.t.Fatal(l.fieldsMsg(prefix, msg))
	} else {
		l.t.Log(l.fieldsMsg(prefix, msg))
	}

}

func (l *TestLogger) log(reqLevel Level, prefix string, args ...any) {
	l.t.Helper()
	if l.level < reqLevel {
		return
	}
	msg := fmt.Sprint(args...)

	if reqLevel <= FatalLevel {
		l.t.Fatal(l.fieldsMsg(prefix, msg))
	} else {
		l.t.Log(l.fieldsMsg(prefix, msg))
	}

}
