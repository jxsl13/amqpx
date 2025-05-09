package testlogger

import (
	"io"
	"testing"
)

var (
	_ io.Writer = (*writer)(nil)
)

type writer struct {
	t *testing.T
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.t.Helper()
	w.t.Log(string(p))
	return len(p), nil
}
