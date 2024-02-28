package pool

import "errors"

func flaggable(err error) bool {
	return err != nil && !errors.Is(err, ErrFlowControl) && !errors.Is(err, ErrClosed)
}
