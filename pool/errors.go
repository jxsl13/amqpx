package pool

import "errors"

var (
	ErrInvalidConnectURL = errors.New("invalid connection url")
	ErrConnectionClosed  = errors.New("connection closed")
	ErrConnectionFailed  = errors.New("connection failed")

	ErrPoolInitializationFailed = errors.New("pool initialization failed")
	ErrPoolClosed               = errors.New("pool closed")
)
