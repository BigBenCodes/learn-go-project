package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/bensullivan2002/learn-go-project/internal/simulator"
	"github.com/bensullivan2002/learn-go-project/internal/stream"
)

type queuedLabel struct {
	label domain.FraudLabel
	due   time.Time
}

// defaultEventStep spaces event timestamps when pacing is disabled (--rate 0).
const defaultEventStep = 10 * time.Millisecond

// eventStep picks the spacing between successive event timestamps. It follows
// the pacing interval when there is one, but must never return zero: every
// history feature in store.Evaluate is computed with `occurred_at < $2`, so a
// run in which all events share one timestamp scores every transaction against
// an empty history — tx_count_10m and spend_1h pinned at 0, new_merchant and
// new_country pinned at 1.
func eventStep(interval time.Duration) time.Duration {
	if interval <= 0 {
		return defaultEventStep
	}
	return interval
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		brokersRaw = flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
		count      = flag.Int("count", 1000, "transactions to emit; 0 runs until interrupted")
		rate       = flag.Float64("rate", 100, "transactions per second; 0 emits without pacing")
		seed       = flag.Int64("seed", 42, "deterministic random seed")
		accounts   = flag.Int("accounts", 200, "number of simulated accounts")
		fraudRate  = flag.Float64("fraud-rate", 0.01, "fraction of fraudulent transactions")
		labelDelay = flag.Duration("label-delay", 5*time.Second, "delay before publishing ground-truth labels")
		startRaw   = flag.String("start-time", "", "optional RFC3339 event start time")
	)
	flag.Parse()
	if *count < 0 || *rate < 0 || *labelDelay < 0 {
		return fmt.Errorf("count, rate, and label-delay cannot be negative")
	}
	start := time.Now().UTC().Truncate(time.Second)
	if *startRaw != "" {
		parsed, err := time.Parse(time.RFC3339, *startRaw)
		if err != nil {
			return fmt.Errorf("parse start-time: %w", err)
		}
		start = parsed.UTC()
	}
	generator, err := simulator.New(*seed, *accounts, *fraudRate)
	if err != nil {
		return err
	}
	producer, err := stream.NewProducer(stream.ParseBrokers(*brokersRaw))
	if err != nil {
		return err
	}
	defer producer.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	labels := make(chan queuedLabel, 256)
	labelErr := make(chan error, 1)
	go func() { labelErr <- publishLabels(ctx, producer, labels) }()

	// stop closes the label queue and waits for publishLabels to finish before
	// returning. Without the wait, the deferred producer.Close() above can fire
	// while that goroutine is still inside producer.Publish.
	stop := func(cause error) error {
		if cause != nil {
			// Cut the publisher short rather than letting a failed run wait out
			// the label delay for every row still queued. On a clean finish we
			// deliberately do not cancel, so those labels still get published.
			cancel()
		}
		close(labels)
		labelResult := <-labelErr
		if cause != nil {
			return cause
		}
		if labelResult != nil && ctx.Err() == nil {
			return labelResult
		}
		return nil
	}
	// failed reports an error from the already-exited publishLabels goroutine.
	// Its result is spent, so stop must not be called on these paths.
	failed := func(err error) error {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	interval := time.Duration(0)
	if *rate > 0 {
		interval = time.Duration(float64(time.Second) / *rate)
	}
	step := eventStep(interval)
	emitted := 0
	for *count == 0 || emitted < *count {
		if emitted > 0 && interval > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
			case err := <-labelErr:
				timer.Stop()
				return failed(err)
			case <-ctx.Done():
				timer.Stop()
				return stop(nil)
			}
		}
		occurredAt := start.Add(time.Duration(emitted) * step)
		tx, label := generator.Next(occurredAt, *labelDelay)
		payload, err := json.Marshal(tx)
		if err != nil {
			return stop(fmt.Errorf("encode transaction %s: %w", tx.TransactionID, err))
		}
		if err := producer.Publish(ctx, stream.TransactionsTopic, tx.AccountID, payload); err != nil {
			return stop(err)
		}
		select {
		case labels <- queuedLabel{label: label, due: time.Now().Add(*labelDelay)}:
		case err := <-labelErr:
			return failed(err)
		case <-ctx.Done():
			return stop(nil)
		}
		emitted++
	}
	if err := stop(nil); err != nil {
		return err
	}
	fmt.Printf("published %d transactions and labels\n", emitted)
	return nil
}

func publishLabels(ctx context.Context, producer *stream.Producer, labels <-chan queuedLabel) error {
	for queued := range labels {
		wait := time.Until(queued.due)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		payload, err := json.Marshal(queued.label)
		if err != nil {
			return fmt.Errorf("encode label %s: %w", queued.label.TransactionID, err)
		}
		if err := producer.Publish(ctx, stream.LabelsTopic, queued.label.TransactionID, payload); err != nil {
			return err
		}
	}
	return nil
}
