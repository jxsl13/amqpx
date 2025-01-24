package logging

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
)

var _ Logger = (*TestLogger)(nil)

func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{
		t:      t,
		fields: map[string]any{},
		lvl:    -4, // Debug
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
	mu     sync.Mutex
	lvl    int
}

// SetLevel sets the log level for the logger.
// -8: Trace
// -4: Debug
// 0: Info
// 4: Warn
// 8: Error
// 12: Fatal
// 16: Panic
func (l *TestLogger) SetLevel(lvl int) {
	l.lvl = lvl
}

func (l *TestLogger) Debugf(format string, args ...any) {
	l.t.Helper()
	l.logf("DEBUG", -4, format, args...)
}
func (l *TestLogger) Infof(format string, args ...any) {
	l.t.Helper()
	l.logf("INFO", 0, format, args...)
}
func (l *TestLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.logf("", 0, format, args...)
}
func (l *TestLogger) Warnf(format string, args ...any) {
	l.t.Helper()
	l.logf("WARN", 4, format, args...)
}
func (l *TestLogger) Errorf(format string, args ...any) {
	l.t.Helper()
	l.logf("ERROR", 8, format, args...)
}
func (l *TestLogger) Fatalf(format string, args ...any) {
	l.t.Helper()
	l.logf("FATAL", 12, format, args...)
	l.t.Fail()
}
func (l *TestLogger) Panicf(format string, args ...any) {
	l.t.Helper()
	l.logf("PANIC", 16, format, args...)
	l.t.FailNow()
}

func (l *TestLogger) Debug(args ...any) {
	l.t.Helper()
	l.log("DEBUG", -4, args...)
}
func (l *TestLogger) Info(args ...any) {
	l.t.Helper()
	l.log("INFO", 0, args...)
}

func (l *TestLogger) Print(args ...any) {
	l.t.Helper()
	l.log("", 0, args...)
}
func (l *TestLogger) Warn(args ...any) {
	l.t.Helper()
	l.log("WARN", 4, args...)
}
func (l *TestLogger) Error(args ...any) {
	l.t.Helper()
	l.log("ERROR", 8, args...)
}
func (l *TestLogger) Fatal(args ...any) {
	l.t.Helper()
	l.log("FATAL", 12, args...)
	l.t.Fail()
}
func (l *TestLogger) Panic(args ...any) {
	l.t.Helper()
	l.log("PANIC", 16, args...)
	l.t.FailNow()
}

func (l *TestLogger) WithError(err error) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.t.Helper()
	return newTestLoggerWithFields(l, map[string]any{"error": err})
}
func (l *TestLogger) WithField(key string, value any) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	return newTestLoggerWithFields(l, map[string]any{key: value})
}
func (l *TestLogger) WithFields(fields Fields) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	return newTestLoggerWithFields(l, fields)
}

// logf is use din all xxxf methods to log a message with a format string.
func (l *TestLogger) logf(prefix string, lvl int, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg, ok := l.msgf(prefix, lvl, format, args...)
	if !ok {
		return
	}
	l.t.Helper()
	l.t.Log(msg)
}

// log is used in all xxx methods to log a message without a format string.
func (l *TestLogger) log(prefix string, lvl int, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lvl < l.lvl {
		return
	}
	msg, ok := l.msg(prefix, lvl, "%v", args...)
	if !ok {
		return
	}

	l.t.Helper()
	l.t.Log(msg)
}

func (l *TestLogger) msg(prefix string, lvl int, format string, args ...any) (string, bool) {
	if lvl < l.lvl {
		return "", false
	}
	l.t.Helper()
	arg := fmt.Sprintf(format, args...)

	msg := l.fieldsMsg(prefix, arg)
	return msg, true
}

func (l *TestLogger) msgf(prefix string, lvl int, format string, args ...any) (string, bool) {
	if lvl < l.lvl {
		return "", false
	}
	arg := fmt.Sprintf(format, args...)

	msg := l.fieldsMsg(prefix, arg)
	return msg, true
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
	sort.Strings(kv)

	result := ""
	if level != "" {
		result = fmt.Sprintf("time=%v level=%s", time.Now(), strings.ToLower(level))
	}

	result += fmt.Sprintf(", msg=%s", msg)

	if len(kv) > 0 {
		result += ", " + strings.Join(kv, ", ")
	}

	if l.lvl > -8 {
		return result
	}

	// trace level, add stack trace
	stack := testutils.Stack(5)

	if len(stack) == 0 {
		return result + ", stacktrace=empty"
	}

	var sb strings.Builder
	sb.Grow(len(stack) * 128)

	sb.WriteString(",\nstacktrace=")
	sb.WriteString(stack[0])

	if len(stack) > 1 {
		sb.WriteString("\n")
		for _, line := range stack[1:] {
			sb.WriteString("\t")
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
	}

	result += sb.String()

	return result
}
