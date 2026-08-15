package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
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
	producer, err := stream.NewProducer(strings.Split(*brokersRaw, ","))
	if err != nil {
		return err
	}
	defer producer.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	labels := make(chan queuedLabel, 256)
	labelErr := make(chan error, 1)
	go func() { labelErr <- publishLabels(ctx, producer, labels) }()

	interval := time.Duration(0)
	if *rate > 0 {
		interval = time.Duration(float64(time.Second) / *rate)
	}
	emitted := 0
	for *count == 0 || emitted < *count {
		if emitted > 0 && interval > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
			case err := <-labelErr:
				timer.Stop()
				return err
			case <-ctx.Done():
				timer.Stop()
				close(labels)
				return nil
			}
		}
		occurredAt := start.Add(time.Duration(emitted) * interval)
		tx, label := generator.Next(occurredAt, *labelDelay)
		payload, _ := json.Marshal(tx)
		if err := producer.Publish(ctx, stream.TransactionsTopic, tx.AccountID, payload); err != nil {
			close(labels)
			return err
		}
		select {
		case labels <- queuedLabel{label: label, due: time.Now().Add(*labelDelay)}:
		case err := <-labelErr:
			return err
		case <-ctx.Done():
			close(labels)
			return nil
		}
		emitted++
	}
	close(labels)
	if err := <-labelErr; err != nil && ctx.Err() == nil {
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
		payload, _ := json.Marshal(queued.label)
		if err := producer.Publish(ctx, stream.LabelsTopic, queued.label.TransactionID, payload); err != nil {
			return err
		}
	}
	return nil
}
