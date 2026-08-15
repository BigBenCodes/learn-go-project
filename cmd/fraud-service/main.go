package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/bensullivan2002/learn-go-project/internal/httpapi"
	"github.com/bensullivan2002/learn-go-project/internal/model"
	"github.com/bensullivan2002/learn-go-project/internal/policy"
	"github.com/bensullivan2002/learn-go-project/internal/service"
	"github.com/bensullivan2002/learn-go-project/internal/store"
	"github.com/bensullivan2002/learn-go-project/internal/stream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		brokersRaw  = flag.String("brokers", env("KAFKA_BROKERS", "localhost:19092"), "comma-separated Kafka brokers")
		databaseURL = flag.String("database-url", env("DATABASE_URL", "postgres://fraud:fraud@localhost:5432/fraud?sslmode=disable"), "Postgres URL")
		address     = flag.String("http-address", env("HTTP_ADDRESS", ":8080"), "HTTP listen address")
		modelPath   = flag.String("model", env("MODEL_PATH", "configs/model.json"), "model artifact path")
		workers     = flag.Int("workers", 6, "bounded transaction workers")
		review      = flag.Float64("review-threshold", 0.65, "review score threshold")
		escalate    = flag.Float64("escalate-threshold", 0.85, "escalation score threshold")
		logLevel    = flag.String("log-level", env("LOG_LEVEL", "info"), "debug, info, warn, or error")
	)
	flag.Parse()
	level, err := parseLevel(*logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	repository, err := store.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		return err
	}
	scorer, err := model.Load(*modelPath)
	if err != nil {
		return err
	}
	thresholds, err := policy.New(*review, *escalate)
	if err != nil {
		return err
	}
	brokers := stream.ParseBrokers(*brokersRaw)
	producer, err := stream.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	transactionConsumer, err := stream.NewConsumer(brokers, "fraud-scorer-v1", stream.TransactionsTopic, *workers, producer, logger)
	if err != nil {
		return err
	}
	defer transactionConsumer.Close()
	labelConsumer, err := stream.NewConsumer(brokers, "fraud-label-evaluator-v1", stream.LabelsTopic, 2, producer, logger)
	if err != nil {
		return err
	}
	defer labelConsumer.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := service.NewMetrics(registry)
	processor := service.NewProcessor(repository, scorer, thresholds, metrics, logger)
	outbox := service.NewOutbox(repository, producer, metrics, logger)
	api := httpapi.New(*address, repository, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		func() (domain.PipelineMetrics, error) { return service.PipelineSnapshot(registry) }, logger)

	logger.Info("starting fraud service", "brokers", brokers, "http_address", *address, "model_version", scorer.Version())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return transactionConsumer.Run(groupCtx, processor.HandleTransaction) })
	group.Go(func() error { return labelConsumer.Run(groupCtx, processor.HandleLabel) })
	group.Go(func() error { return outbox.Run(groupCtx) })
	group.Go(func() error {
		// api.Run already maps http.ErrServerClosed to nil.
		if err := api.Run(groupCtx); err != nil {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	})
	return group.Wait()
}

func parseLevel(name string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(name)); err != nil {
		return 0, fmt.Errorf("parse log level %q: %w", name, err)
	}
	return level, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
