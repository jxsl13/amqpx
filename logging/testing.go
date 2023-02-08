package logging

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

var _ Logger = (*TestLogger)(nil)

func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{
		t:      t,
		fields: map[string]any{},
	}
}

func newTestLoggerWithFields(l *TestLogger, fields Fields) *TestLogger {
	n := &TestLogger{
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
	t      *testing.T
	fields map[string]any
}

func (l *TestLogger) Debugf(format string, args ...any) {
	l.t.Helper()
	l.logf("DEBUG", format, args...)
}
func (l *TestLogger) Infof(format string, args ...any) {
	l.t.Helper()
	l.logf("INFO", format, args...)
}
func (l *TestLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.logf("", format, args...)
}
func (l *TestLogger) Warnf(format string, args ...any) {
	l.t.Helper()
	l.logf("WARN", format, args...)
}
func (l *TestLogger) Errorf(format string, args ...any) {
	l.t.Helper()
	l.logf("ERROR", format, args...)
}
func (l *TestLogger) Fatalf(format string, args ...any) {
	l.t.Helper()
	l.logf("FATAL", format, args...)
	l.t.Fail()
}
func (l *TestLogger) Panicf(format string, args ...any) {
	l.t.Helper()
	l.logf("PANIC", format, args...)
	l.t.FailNow()
}

func (l *TestLogger) Debug(args ...any) {
	l.t.Helper()
	l.log("DEBUG", args...)
}
func (l *TestLogger) Info(args ...any) {
	l.t.Helper()
	l.log("INFO", args...)
}

func (l *TestLogger) Print(args ...any) {
	l.t.Helper()
	l.log("", args...)
}
func (l *TestLogger) Warn(args ...any) {
	l.t.Helper()
	l.log("WARN", args...)
}
func (l *TestLogger) Error(args ...any) {
	l.t.Helper()
	l.log("ERROR", args...)
}
func (l *TestLogger) Fatal(args ...any) {
	l.t.Helper()
	l.log("FATAL", args...)
	os.Exit(1)
}
func (l *TestLogger) Panic(args ...any) {
	l.t.Helper()
	l.log("PANIC", args...)
}

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
		prefix = fmt.Sprintf("time=%v level=%s", time.Now(), strings.ToLower(level))
	}

	prefix += fmt.Sprintf(", msg=%s", msg)

	sort.Strings(kv)
	return fmt.Sprintf("%s, %s", prefix, strings.Join(kv, ", "))
}

func (l *TestLogger) logf(prefix, format string, args ...any) {
	l.t.Helper()
	lpref := strings.ToLower(prefix)
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(lpref, "panic") || strings.Contains(lpref, "fatal") {
		l.t.Fatal(l.fieldsMsg(prefix, msg))
	} else {
		l.t.Log(l.fieldsMsg(prefix, msg))

	}

}

func (l *TestLogger) log(prefix string, args ...any) {
	l.t.Helper()
	msg := fmt.Sprint(args...)
	lpref := strings.ToLower(prefix)
	if strings.Contains(lpref, "panic") || strings.Contains(lpref, "fatal") {
		l.t.Fatal(l.fieldsMsg(prefix, msg))
	} else {
		l.t.Log(l.fieldsMsg(prefix, msg))
	}
}
