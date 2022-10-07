package pool

import (
	"context"
	"net"
	"time"
)

// defaultDial establishes a connection
// it allows to additionally pass a context to the dialer
func defaultDial(ctx context.Context, connectionTimeout time.Duration) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: connectionTimeout}

		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		// Heartbeating hasn't started yet, don't stall forever on a dead server.
		// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
		// the deadline is cleared in openComplete.
		if err := conn.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}
