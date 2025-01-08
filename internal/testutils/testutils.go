package testutils

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
)

func FilePath(relative string, up ...int) string {
	offset := 1
	if len(up) > 0 && up[0] > 0 {
		offset = up[0]
	}
	_, file, _, ok := runtime.Caller(offset)
	if !ok {
		panic("failed to get caller")
	}
	if filepath.IsAbs(relative) {
		panic(fmt.Sprintf("%s is an absolute file path, must be relative to the current go source file", relative))
	}
	abs := filepath.Join(filepath.Dir(file), relative)
	return abs
}

func FuncName(up ...int) string {
	offset := 1
	if len(up) > 0 && up[0] > 0 {
		offset = +up[0]
	}
	pc, _, _, ok := runtime.Caller(offset)
	if !ok {
		panic("failed to get caller")
	}

	f := runtime.FuncForPC(pc)
	if f == nil {
		panic("failed to get function name")
	}
	return f.Name()
}

func FileLine(up ...int) string {
	offset := 1
	if len(up) > 0 && up[0] > 0 {
		offset += up[0]
	}
	pc, _, _, ok := runtime.Caller(offset)
	if !ok {
		panic("failed to get caller")
	}

	f := runtime.FuncForPC(pc)
	if f == nil {
		panic("failed to get function name")
	}
	file, line := f.FileLine(pc)
	return fmt.Sprintf("%s:%d", file, line)
}

func CallerFuncName(up ...int) string {
	offset := 2
	if len(up) > 0 && up[0] > 0 {
		offset += up[0]
	}
	pc, _, _, ok := runtime.Caller(offset)
	if !ok {
		panic("failed to get caller")
	}

	f := runtime.FuncForPC(pc)
	if f == nil {
		panic("failed to get function name")
	}
	return f.Name()
}

func CallerFileLine(up ...int) string {
	offset := 2
	if len(up) > 0 && up[0] > 0 {
		offset += up[0]
	}
	pc, _, _, ok := runtime.Caller(offset)
	if !ok {
		panic("failed to get caller")
	}

	f := runtime.FuncForPC(pc)
	if f == nil {
		panic("failed to get function name")
	}
	file, line := f.FileLine(pc)
	return fmt.Sprintf("%s:%d", file, line)
}

var MaxStackDepth = 32

func Stack(up ...int) []string {
	offset := 1
	if len(up) > 0 && up[0] > 0 {
		offset += up[0]
	}

	pc := make([]uintptr, MaxStackDepth)
	n := runtime.Callers(offset, pc)
	if n == 0 {
		// No PCs available. This can happen if the first argument to
		// runtime.Callers is large.
		//
		// Return now to avoid processing the zero Frame that would
		// otherwise be returned by frames.Next below.
		return []string{}
	}

	pc = pc[:n] // pass only valid pcs to runtime.CallersFrames
	frames := runtime.CallersFrames(pc)

	// Loop to get frames.
	// A fixed number of PCs can expand to an indefinite number of Frames.
	stack := make([]string, 0, n)
	for {
		frame, more := frames.Next()

		funcName := frame.Func.Name()
		if strings.HasPrefix(funcName, "testing.tRunner") || strings.HasPrefix(funcName, "runtime.goexit") {
			break
		}

		stack = append(stack, fmt.Sprintf("%s (%s:%d)", funcName, frame.File, frame.Line))

		// Check whether there are more frames to process after this one.
		if !more {
			break
		}
	}

	return stack
}
