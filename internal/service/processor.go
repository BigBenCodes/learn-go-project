package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/bensullivan2002/learn-go-project/internal/features"
	"github.com/bensullivan2002/learn-go-project/internal/policy"
	"github.com/bensullivan2002/learn-go-project/internal/store"
	"github.com/bensullivan2002/learn-go-project/internal/stream"
)

type Scorer interface {
	Version() string
	Score(domain.FeatureVector) (float64, []domain.Signal)
}

type Repository interface {
	Evaluate(context.Context, domain.TransactionCreated, store.Evaluator) (domain.Assessment, bool, error)
	StoreLabel(context.Context, domain.FraudLabel) (bool, error)
}

type Processor struct {
	repository Repository
	model      Scorer
	policy     policy.Thresholds
	metrics    *Metrics
	logger     *slog.Logger
	now        func() time.Time
}

func NewProcessor(repository Repository, scorer Scorer, thresholds policy.Thresholds, metrics *Metrics, logger *slog.Logger) *Processor {
	return &Processor{
		repository: repository, model: scorer, policy: thresholds,
		metrics: metrics, logger: logger, now: time.Now,
	}
}

func (p *Processor) HandleTransaction(ctx context.Context, payload []byte) error {
	started := time.Now()
	var event domain.TransactionCreated
	if err := json.Unmarshal(payload, &event); err != nil {
		p.metrics.Failures.WithLabelValues("transaction").Inc()
		p.logger.Warn("transaction decode failed", "error", err)
		return stream.Permanent(fmt.Errorf("decode transaction: %w", err))
	}
	if err := event.Validate(); err != nil {
		p.metrics.Failures.WithLabelValues("transaction").Inc()
		p.logger.Warn("transaction validation failed", "transaction_id", event.TransactionID, "error", err)
		return stream.Permanent(fmt.Errorf("validate transaction: %w", err))
	}

	assessment, created, err := p.repository.Evaluate(ctx, event, func(history domain.History) (domain.Assessment, error) {
		vector := features.Extract(event, history)
		score, signals := p.model.Score(vector)
		return domain.Assessment{
			SchemaVersion:     domain.SchemaVersion,
			EventID:           "assessment-" + event.TransactionID,
			TransactionID:     event.TransactionID,
			AssessedAt:        p.now().UTC(),
			ModelVersion:      p.model.Version(),
			RiskScore:         score,
			RecommendedAction: p.policy.Decide(score),
			Signals:           signals,
		}, nil
	})
	if err != nil {
		p.metrics.Failures.WithLabelValues("transaction").Inc()
		p.logger.Error("transaction evaluation failed", "transaction_id", event.TransactionID, "error", err)
		// A uniqueness conflict outside the idempotency key is a bad event, not
		// a sick database: retrying it would spin forever and stall the
		// partition, so dead-letter it like a decode failure.
		if store.IsUnprocessable(err) {
			return stream.Permanent(err)
		}
		return err
	}
	if !created {
		p.metrics.Duplicates.WithLabelValues("transaction").Inc()
		p.logger.Info("duplicate transaction ignored", "transaction_id", event.TransactionID)
		return nil
	}
	p.metrics.Processed.WithLabelValues("transaction").Inc()
	p.metrics.Actions.WithLabelValues(string(assessment.RecommendedAction), assessment.ModelVersion).Inc()
	p.metrics.Latency.Observe(time.Since(started).Seconds())
	log := p.logger.Debug
	if assessment.RecommendedAction != domain.ActionNone {
		log = p.logger.Info
	}
	log("transaction assessed",
		"transaction_id", assessment.TransactionID,
		"score", assessment.RiskScore,
		"action", assessment.RecommendedAction,
		"model_version", assessment.ModelVersion,
	)
	return nil
}

func (p *Processor) HandleLabel(ctx context.Context, payload []byte) error {
	var label domain.FraudLabel
	if err := json.Unmarshal(payload, &label); err != nil {
		p.metrics.Failures.WithLabelValues("label").Inc()
		p.logger.Warn("label decode failed", "error", err)
		return stream.Permanent(fmt.Errorf("decode label: %w", err))
	}
	if err := label.Validate(); err != nil {
		p.metrics.Failures.WithLabelValues("label").Inc()
		p.logger.Warn("label validation failed", "transaction_id", label.TransactionID, "error", err)
		return stream.Permanent(fmt.Errorf("validate label: %w", err))
	}
	created, err := p.repository.StoreLabel(ctx, label)
	if err != nil {
		p.metrics.Failures.WithLabelValues("label").Inc()
		p.logger.Error("label storage failed", "transaction_id", label.TransactionID, "error", err)
		if store.IsUnprocessable(err) {
			return stream.Permanent(err)
		}
		return err
	}
	if created {
		p.metrics.Processed.WithLabelValues("label").Inc()
	} else {
		p.metrics.Duplicates.WithLabelValues("label").Inc()
		p.logger.Info("duplicate label ignored", "transaction_id", label.TransactionID)
	}
	return nil
}
