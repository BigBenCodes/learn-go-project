package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/store"
)

type OutboxRepository interface {
	PublishOutbox(context.Context, int, func(store.OutboxEvent) error) (int, error)
}

type Publisher interface {
	Publish(context.Context, string, string, []byte) error
}

type Outbox struct {
	repository OutboxRepository
	publisher  Publisher
	metrics    *Metrics
	logger     *slog.Logger
}

func NewOutbox(repository OutboxRepository, publisher Publisher, metrics *Metrics, logger *slog.Logger) *Outbox {
	return &Outbox{repository: repository, publisher: publisher, metrics: metrics, logger: logger}
}

func (o *Outbox) Run(ctx context.Context) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := o.flush(ctx); err != nil {
			o.metrics.OutboxFailures.Inc()
			o.logger.Error("outbox flush failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// flush claims a batch of unpublished rows and publishes them. The claim and
// the published_at writes happen in one store-side transaction so a second
// replica cannot publish the same rows; see store.Postgres.PublishOutbox.
func (o *Outbox) flush(ctx context.Context) error {
	published, err := o.repository.PublishOutbox(ctx, 100, func(event store.OutboxEvent) error {
		return o.publisher.Publish(ctx, event.Topic, event.Key, event.Payload)
	})
	o.metrics.OutboxPublished.Add(float64(published))
	return err
}
