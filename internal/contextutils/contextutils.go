package contextutils

import "context"

func ToCancelFunc(err error, ccf context.CancelCauseFunc) context.CancelFunc {
	return func() {
		ccf(err)
	}
}
