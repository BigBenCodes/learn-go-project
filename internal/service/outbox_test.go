package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/bensullivan2002/learn-go-project/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeOutboxRepository mimics store.Postgres.PublishOutbox: it hands each
// claimed row to publish in order and stops at the first failure, reporting how
// many were marked published before that point.
type fakeOutboxRepository struct {
	events    []store.OutboxEvent
	published []int64
}

func (r *fakeOutboxRepository) PublishOutbox(_ context.Context, limit int, publish func(store.OutboxEvent) error) (int, error) {
	count := 0
	for _, event := range r.events {
		if count == limit {
			break
		}
		if err := publish(event); err != nil {
			return count, err
		}
		r.published = append(r.published, event.ID)
		count++
	}
	return count, nil
}

type stubPublisher struct {
	failOn string
	sent   []string
}

func (p *stubPublisher) Publish(_ context.Context, topic, key string, _ []byte) error {
	if key == p.failOn {
		return errors.New("broker unavailable")
	}
	p.sent = append(p.sent, topic+"/"+key)
	return nil
}

func newOutbox(t *testing.T, repository OutboxRepository, publisher Publisher) (*Outbox, *Metrics) {
	t.Helper()
	metrics := NewMetrics(prometheus.NewRegistry())
	return NewOutbox(repository, publisher, metrics, slog.New(slog.NewTextHandler(io.Discard, nil))), metrics
}

func TestOutboxFlushPublishesAndCountsRows(t *testing.T) {
	repository := &fakeOutboxRepository{events: []store.OutboxEvent{
		{ID: 1, Topic: "fraud.assessments.v1", Key: "tx-1", Payload: []byte(`{}`)},
		{ID: 2, Topic: "fraud.assessments.v1", Key: "tx-2", Payload: []byte(`{}`)},
	}}
	publisher := &stubPublisher{}
	outbox, metrics := newOutbox(t, repository, publisher)

	if err := outbox.flush(context.Background()); err != nil {
		t.Fatalf("flush() error = %v", err)
	}
	if got := len(publisher.sent); got != 2 {
		t.Fatalf("published %d events, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.OutboxPublished); got != 2 {
		t.Fatalf("OutboxPublished = %v, want 2", got)
	}
}

// A failing row stops the batch, but the rows published before it must still
// count as published — otherwise one undeliverable event forces every event
// ahead of it to be re-sent on the next tick.
func TestOutboxFlushKeepsProgressBeforeAFailure(t *testing.T) {
	repository := &fakeOutboxRepository{events: []store.OutboxEvent{
		{ID: 1, Topic: "fraud.assessments.v1", Key: "tx-1", Payload: []byte(`{}`)},
		{ID: 2, Topic: "fraud.assessments.v1", Key: "tx-bad", Payload: []byte(`{}`)},
		{ID: 3, Topic: "fraud.assessments.v1", Key: "tx-3", Payload: []byte(`{}`)},
	}}
	publisher := &stubPublisher{failOn: "tx-bad"}
	outbox, metrics := newOutbox(t, repository, publisher)

	err := outbox.flush(context.Background())
	if err == nil {
		t.Fatal("flush() = nil error, want the publish failure surfaced")
	}
	if got := testutil.ToFloat64(metrics.OutboxPublished); got != 1 {
		t.Fatalf("OutboxPublished = %v, want 1 (the row published before the failure)", got)
	}
	if len(repository.published) != 1 || repository.published[0] != 1 {
		t.Fatalf("marked published = %v, want [1]", repository.published)
	}
}
