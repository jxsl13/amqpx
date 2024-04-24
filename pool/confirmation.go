package pool

import (
	"context"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

// Confirmation is a deferred confirmation of a single publish.
// We use DeferredConfirmation instead of Transactions due to the performance implications of transactions.
// See: https://www.rabbitmq.com/docs/confirms#publisher-confirms
type Confirmation struct {
	dc *amqp091.DeferredConfirmation
	s  *Session
}

// BatchConfirmation is a deferred confirmation of a batch of publishes.
// We use DeferredConfirmation instead of Transactions due to the performance implications of transactions.
// See: https://www.rabbitmq.com/docs/confirms#publisher-confirms
type BatchConfirmation struct {
	dc []*amqp091.DeferredConfirmation
	s  *Session
}

func (c *Confirmation) Session() *Session {
	return c.s
}

func (c *Confirmation) DeferredConfirmation() *amqp091.DeferredConfirmation {
	return c.dc
}

func (bc *BatchConfirmation) Session() *Session {
	return bc.s
}

func (bc *BatchConfirmation) DeferredConfirmations() []*amqp091.DeferredConfirmation {
	return bc.dc
}

// Wait blocks until the server confirms the publish, the channel/connection is closed, the context is cancelled, or an error occurs.
func (c *Confirmation) Wait(ctx context.Context) error {
	select {
	case <-c.dc.Done():
		err := c.s.error()
		if err != nil {
			return fmt.Errorf("await confirm failed: confirms channel closed: %w", err)
		}
		if !c.dc.Acked() {
			return fmt.Errorf("await confirm failed: %w", ErrNack)
		}
		return nil
	case returned, ok := <-c.s.returned:
		if !ok {
			err := c.s.error()
			if err != nil {
				return fmt.Errorf("await confirm failed: returned channel closed: %w", err)
			}
			return fmt.Errorf("await confirm failed: %w", errReturnedClosed)
		}
		return fmt.Errorf("await confirm failed: %w: %s", ErrReturned, returned.ReplyText)
	case blocking, ok := <-c.s.conn.BlockingFlowControl():
		if !ok {
			err := c.s.error()
			if err != nil {
				return fmt.Errorf("await confirm failed: blocking channel closed: %w", err)
			}
			return fmt.Errorf("await confirm failed: %w", errBlockingFlowControlClosed)
		}
		return fmt.Errorf("await confirm failed: %w: %s", ErrBlockingFlowControl, blocking.Reason)
	case <-ctx.Done():
		return fmt.Errorf("await confirm: failed context %w: %w", ErrClosed, ctx.Err())
	case <-c.s.ctx.Done():
		return fmt.Errorf("await confirm failed: session %w: %w", ErrClosed, c.s.ctx.Err())
	}
}

// Wait blocks until the server confirms all publishes, the channel/connection is closed, the context is cancelled, or an error occurs.
func (bc *BatchConfirmation) Wait(ctx context.Context) error {
	for {
		select {
		case returned, ok := <-bc.s.returned:
			if !ok {
				err := bc.s.error()
				if err != nil {
					return fmt.Errorf("await confirm failed: returned channel closed: %w", err)
				}
				return fmt.Errorf("await confirm failed: %w", errReturnedClosed)
			}
			return fmt.Errorf("await confirm failed: %w: %s", ErrReturned, returned.ReplyText)
		case blocking, ok := <-bc.s.conn.BlockingFlowControl():
			if !ok {
				err := bc.s.error()
				if err != nil {
					return fmt.Errorf("await confirm failed: blocking channel closed: %w", err)
				}
				return fmt.Errorf("await confirm failed: %w", errBlockingFlowControlClosed)
			}
			return fmt.Errorf("await confirm failed: %w: %s", ErrBlockingFlowControl, blocking.Reason)
		case <-ctx.Done():
			return fmt.Errorf("await confirm: failed context %w: %w", ErrClosed, ctx.Err())
		case <-bc.s.ctx.Done():
			return fmt.Errorf("await confirm failed: session %w: %w", ErrClosed, bc.s.ctx.Err())
		default:
			err := bc.s.error()
			if err != nil {
				return fmt.Errorf("await confirm failed: confirms channel closed: %w", err)
			}

			for _, dc := range bc.dc {
				if !dc.Acked() {
					continue // not all acked yet, keep waiting
				}
			}
		}

		return nil
	}
}
