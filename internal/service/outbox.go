package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/store"
)

type OutboxRepository interface {
	FetchOutbox(context.Context, int) ([]store.OutboxEvent, error)
	MarkOutboxPublished(context.Context, int64) error
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

func (o *Outbox) flush(ctx context.Context) error {
	events, err := o.repository.FetchOutbox(ctx, 100)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := o.publisher.Publish(ctx, event.Topic, event.Key, event.Payload); err != nil {
			return err
		}
		if err := o.repository.MarkOutboxPublished(ctx, event.ID); err != nil {
			return err
		}
		o.metrics.OutboxPublished.Inc()
	}
	return nil
}
