package amqpx

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/jxsl13/amqpx/pool"
)

type option struct {
	PoolOptions []pool.Option

	PublisherOptions []pool.PublisherOption

	PublisherConnections  int
	PublisherSessions     int
	SubscriberConnections int

	CloseTimeout time.Duration
}

type Option func(*option)

// WithName gives all of your pooled connections a prefix name
func WithName(name string) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithName(name))
	}
}

// WithLogger allows to set a custom logger for the connection AND session pool
func WithLogger(logger *slog.Logger) Option {
	return func(o *option) {
		if logger != nil {
			o.PoolOptions = append(o.PoolOptions, pool.WithLogger(logger))
		}
	}
}

// WithHeartbeatInterval allows to set a custom heartbeat interval, that MUST be >= 1 * time.Second
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithHeartbeatInterval(interval))
	}
}

// WithConnectionTimeout allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func WithConnectionTimeout(timeout time.Duration) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithConnectionTimeout(timeout))
	}
}

// WithTLS allows to configure tls connectivity.
func WithTLS(config *tls.Config) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithTLS(config))
	}
}

// WithBufferCapacity allows to configurethe size of
// the confirmation, error & blocker buffers of all sessions
func WithBufferCapacity(capacity int) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithBufferCapacity(capacity))
	}
}

// WithConfirms requires all messages from sessions to be acked.
// This affects publishers.
func WithConfirms(requirePublishConfirms bool) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithConfirms(requirePublishConfirms))
	}
}

// WithPublisherConnections defines the number of tcp connections of the publisher.
func WithPublisherConnections(connections int) Option {
	if connections < 1 {
		connections = 1
	}
	return func(o *option) {
		o.PublisherConnections = connections
	}
}

// WithPublisherSessions defines the number of multiplexed sessions for all connections.
// Meaning, if you have 1 connection and two sessions, every connectionhas two sessions.
// If you have two connections and two sessions, every connection gets one session.
// Every connection gets a session assigned to it in a round robin manner.
func WithPublisherSessions(sessions int) Option {
	if sessions < 1 {
		sessions = 1
	}
	return func(o *option) {
		o.PublisherSessions = sessions
	}
}

// WithSubscriberConnections defines the number connections all of the consumer
// sessions share.
// Meaning, if you have registered 10 handlers and define 5 connections, every connection
// has two sessions that are multiplexed over it.
// If you have 1 connection, all consumers will derive sessions from that connection in order to consume from the
// specified queue.
// You cannot have less than one connection, nor can you havemore connections than handlers, as there can at most be
// one (tcp) connection with one session per handler.
func WithSubscriberConnections(connections int) Option {
	if connections < 1 {
		connections = 1
	}
	return func(o *option) {
		o.SubscriberConnections = connections
	}
}

// WithPoolOption is a functionthat allows to directly manipulate the options of the underlying pool.
// DO NOT USE this option unless you have read the source code enough in order to understand what configutaion options
// influence what behavior.
// This might make sense if you want to change the pool name prefix or suffix.
func WithPoolOption(po pool.Option) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, po)
	}
}

// WithCloseTimeout affects the duration that the topology deleter functions are allowed to delete topologies.
// This timeout is especially interesting for containerized environments where containers may potentionally be killed after
// a specific timeout. To we want to cancel deletion operations before those hard kill comes into play.
func WithCloseTimeout(timeout time.Duration) Option {
	return func(o *option) {
		if timeout <= 0 {
			o.CloseTimeout = 15 * time.Second
		} else {
			o.CloseTimeout = timeout
		}
	}
}
