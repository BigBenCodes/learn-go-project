package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	TransactionsTopic = "transactions.v1"
	LabelsTopic       = "fraud.labels.v1"
	AssessmentsTopic  = "fraud.assessments.v1"
	DeadLetterTopic   = "fraud.dead-letter.v1"
)

// ParseBrokers splits a comma-separated broker list, trimming blanks so a
// trailing comma or a space after one does not reach kgo.SeedBrokers as an
// empty or padded address.
func ParseBrokers(value string) []string {
	var brokers []string
	for _, candidate := range strings.Split(value, ",") {
		if candidate = strings.TrimSpace(candidate); candidate != "" {
			brokers = append(brokers, candidate)
		}
	}
	return brokers
}

type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	err := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}).FirstErr()
	if err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

func (p *Producer) Close() { p.client.Close() }

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

type Handler func(context.Context, []byte) error

type Consumer struct {
	client   *kgo.Client
	producer *Producer
	logger   *slog.Logger
	workers  int
}

func NewConsumer(brokers []string, group, topic string, workers int, producer *Producer, logger *slog.Logger) (*Consumer, error) {
	if workers < 1 {
		workers = 1
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client, producer: producer, logger: logger, workers: workers}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	jobs := make([]chan *kgo.Record, c.workers)
	errCh := make(chan error, c.workers)
	var wg sync.WaitGroup
	for i := range jobs {
		jobs[i] = make(chan *kgo.Record, 32)
		wg.Add(1)
		go func(records <-chan *kgo.Record) {
			defer wg.Done()
			for record := range records {
				if err := c.handleRecord(ctx, record, handler); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}(jobs[i])
	}
	// Close the job channels and wait for the workers to drain before
	// returning. Without the join, Run's caller proceeds to its deferred
	// Consumer.Close()/Producer.Close()/pool.Close() while handlers are still
	// mid-flight, and a late CommitRecords lands on an already-closed client.
	//
	// cancel() has to happen first. On the errCh path the parent context is
	// still live, so a worker retrying a transient failure would never stop
	// and wg.Wait() would block forever.
	defer func() {
		cancel()
		for _, ch := range jobs {
			close(ch)
		}
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		for _, fetchErr := range fetches.Errors() {
			c.logger.Error("kafka fetch failed", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "error", fetchErr.Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			worker := record.Partition % int32(c.workers)
			select {
			case jobs[worker] <- record:
			case err := <-errCh:
				return err
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record, handler Handler) error {
	backoff := 100 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := handler(ctx, record.Value)
		if err == nil {
			if err := c.client.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
				return fmt.Errorf("commit %s partition %d offset %d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			return nil
		}
		var permanent permanentError
		if errors.As(err, &permanent) {
			if dlqErr := c.publishDeadLetter(ctx, record, err); dlqErr != nil {
				return dlqErr
			}
			c.logger.Warn("sent invalid event to dead-letter topic", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
			if err := c.client.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
				return fmt.Errorf("commit dead-lettered record: %w", err)
			}
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		c.logger.Error("event processing failed; retrying", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "backoff", backoff, "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

type deadLetter struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error"`
	Payload   []byte `json:"payload"`
	FailedAt  string `json:"failed_at"`
}

func (c *Consumer) publishDeadLetter(ctx context.Context, record *kgo.Record, processingErr error) error {
	payload, err := json.Marshal(deadLetter{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Error: processingErr.Error(), Payload: record.Value, FailedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	if err := c.producer.Publish(ctx, DeadLetterTopic, string(record.Key), payload); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	return nil
}
