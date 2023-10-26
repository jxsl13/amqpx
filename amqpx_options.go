package amqpx

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/jxsl13/amqpx/logging"
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
func WithLogger(logger logging.Logger) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithLogger(logger))
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

// WithContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func WithContext(ctx context.Context) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithContext(ctx))
	}
}

// WithTLS allows to configure tls connectivity.
func WithTLS(config *tls.Config) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithTLS(config))
	}
}

// WithBufferSize allows to configurethe size of
// the confirmation, error & blocker buffers of all sessions
func WithBufferSize(size int) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithBufferSize(size))
	}
}

// WithConfirms requires all messages from sessions to be acked.
// This affects publishers.
func WithConfirms(requirePublishConfirms bool) Option {
	return func(o *option) {
		o.PoolOptions = append(o.PoolOptions, pool.WithConfirms(requirePublishConfirms))
	}
}

// WithConfirmTimeout is the timout before another publishing attempt is tried.
// As the broker did not send any confirmation that our published message arrived.
func WithConfirmTimeout(timeout time.Duration) Option {
	return func(o *option) {
		o.PublisherOptions = append(o.PublisherOptions, pool.PublisherWithConfirmTimeout(timeout))
	}
}

// WithPublishTimeout is the timout that we attempt to try sending our message to the broker
// before aborting. This does not affect the overall time of publishing, as publishing is retried indefinitely.
func WithPublishTimeout(timeout time.Duration) Option {
	return func(o *option) {
		o.PublisherOptions = append(o.PublisherOptions, pool.PublisherWithPublishTimeout(timeout))
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
// If youhave 1 connection, all consumers will derive sessions from that connection in order to consume from the
// specified queue.
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
