package service

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/bensullivan2002/learn-go-project/internal/model"
	"github.com/bensullivan2002/learn-go-project/internal/policy"
	"github.com/bensullivan2002/learn-go-project/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeRepository struct {
	assessment domain.Assessment
	created    bool
	label      domain.FraudLabel
}

func (r *fakeRepository) Evaluate(_ context.Context, event domain.TransactionCreated, evaluate store.Evaluator) (domain.Assessment, bool, error) {
	assessment, err := evaluate(domain.History{})
	r.assessment = assessment
	return assessment, r.created, err
}

func (r *fakeRepository) StoreLabel(_ context.Context, label domain.FraudLabel) (bool, error) {
	r.label = label
	return true, nil
}

func TestProcessorSeparatesScoringFromPolicy(t *testing.T) {
	repository := &fakeRepository{created: true}
	scorer, _ := model.New(model.Artifact{Version: "v1", Intercept: 1, Weights: map[string]float64{"foreign": 1}})
	thresholds, _ := policy.New(0.65, 0.85)
	processor := NewProcessor(repository, scorer, thresholds, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	processor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	event := domain.TransactionCreated{
		SchemaVersion: 1, EventID: "event", TransactionID: "tx", OccurredAt: time.Now(),
		AccountID: "account", CardID: "card", AmountMinor: 1000, Currency: "GBP",
		Merchant:  domain.Merchant{ID: "merchant", Category: "groceries", Country: "FR"},
		EntryMode: domain.EntryModeChip,
	}
	payload, _ := json.Marshal(event)
	if err := processor.HandleTransaction(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if repository.assessment.RecommendedAction != domain.ActionEscalate {
		t.Fatalf("action = %q, want escalate", repository.assessment.RecommendedAction)
	}
	if repository.assessment.ModelVersion != "v1" {
		t.Fatalf("model version = %q", repository.assessment.ModelVersion)
	}
}

func TestProcessorRejectsMalformedEvent(t *testing.T) {
	repository := &fakeRepository{}
	scorer, _ := model.New(model.Artifact{Version: "v1", Weights: map[string]float64{"foreign": 1}})
	thresholds, _ := policy.New(0.65, 0.85)
	processor := NewProcessor(repository, scorer, thresholds, NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := processor.HandleTransaction(context.Background(), []byte("not-json")); err == nil {
		t.Fatal("expected malformed input to fail")
	}
}
