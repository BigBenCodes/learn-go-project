package domain

import (
	"errors"
	"fmt"
	"time"
)

const SchemaVersion = 1

type EntryMode string

const (
	EntryModeChip        EntryMode = "chip"
	EntryModeContactless EntryMode = "contactless"
	EntryModeEcommerce   EntryMode = "ecommerce"
)

type Action string

const (
	ActionNone     Action = "no_action"
	ActionReview   Action = "review"
	ActionEscalate Action = "escalate"
)

type Merchant struct {
	ID       string `json:"id"`
	Category string `json:"category"`
	Country  string `json:"country"`
}

type TransactionCreated struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	TransactionID string    `json:"transaction_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	AccountID     string    `json:"account_id"`
	CardID        string    `json:"card_id"`
	AmountMinor   int64     `json:"amount_minor"`
	Currency      string    `json:"currency"`
	Merchant      Merchant  `json:"merchant"`
	EntryMode     EntryMode `json:"entry_mode"`
}

func (t TransactionCreated) Validate() error {
	if t.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema version %d", t.SchemaVersion)
	}
	if t.EventID == "" || t.TransactionID == "" || t.AccountID == "" || t.CardID == "" {
		return errors.New("event, transaction, account, and card IDs are required")
	}
	if t.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	if t.AmountMinor <= 0 {
		return errors.New("amount_minor must be positive")
	}
	if t.Currency != "GBP" {
		return fmt.Errorf("unsupported currency %q", t.Currency)
	}
	if t.Merchant.ID == "" || t.Merchant.Category == "" || len(t.Merchant.Country) != 2 {
		return errors.New("merchant id, category, and ISO country are required")
	}
	switch t.EntryMode {
	case EntryModeChip, EntryModeContactless, EntryModeEcommerce:
		return nil
	default:
		return fmt.Errorf("unsupported entry mode %q", t.EntryMode)
	}
}

type FraudLabel struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	TransactionID string    `json:"transaction_id"`
	LabelledAt    time.Time `json:"labelled_at"`
	IsFraud       bool      `json:"is_fraud"`
	FraudType     string    `json:"fraud_type,omitempty"`
}

func (l FraudLabel) Validate() error {
	if l.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema version %d", l.SchemaVersion)
	}
	if l.EventID == "" || l.TransactionID == "" || l.LabelledAt.IsZero() {
		return errors.New("event_id, transaction_id, and labelled_at are required")
	}
	if l.IsFraud && l.FraudType == "" {
		return errors.New("fraud_type is required for fraud labels")
	}
	return nil
}

type History struct {
	TransactionCount10m int64
	SpendMinor1h        int64
	SeenMerchant        bool
	SeenCountry         bool
}

type FeatureVector map[string]float64

type Signal struct {
	Feature      string  `json:"feature"`
	Value        float64 `json:"value"`
	Contribution float64 `json:"contribution"`
}

type Assessment struct {
	SchemaVersion     int       `json:"schema_version"`
	EventID           string    `json:"event_id"`
	TransactionID     string    `json:"transaction_id"`
	AssessedAt        time.Time `json:"assessed_at"`
	ModelVersion      string    `json:"model_version"`
	RiskScore         float64   `json:"risk_score"`
	RecommendedAction Action    `json:"recommended_action"`
	Signals           []Signal  `json:"signals"`
}

type TransactionRecord struct {
	Transaction TransactionCreated `json:"transaction"`
	Assessment  Assessment         `json:"assessment"`
	Label       *FraudLabel        `json:"label,omitempty"`
}

type ModelMetrics struct {
	ModelVersion      string  `json:"model_version,omitempty"`
	TotalAssessments  int64   `json:"total_assessments"`
	Labelled          int64   `json:"labelled"`
	TruePositive      int64   `json:"true_positive"`
	FalsePositive     int64   `json:"false_positive"`
	TrueNegative      int64   `json:"true_negative"`
	FalseNegative     int64   `json:"false_negative"`
	Precision         float64 `json:"precision"`
	Recall            float64 `json:"recall"`
	FalsePositiveRate float64 `json:"false_positive_rate"`
	LabelCoverage     float64 `json:"label_coverage"`
}

// PipelineMetrics summarises processing throughput and latency for the
// event pipeline (Kafka consumers + outbox publisher), as opposed to
// ModelMetrics which scores the fraud model's predictive skill.
type PipelineMetrics struct {
	Processed         map[string]int64 `json:"processed"`
	Duplicates        map[string]int64 `json:"duplicates"`
	Failures          map[string]int64 `json:"failures"`
	Actions           map[string]int64 `json:"actions"`
	LatencyCount      int64            `json:"latency_count"`
	LatencySumSeconds float64          `json:"latency_sum_seconds"`
	LatencyAvgSeconds float64          `json:"latency_avg_seconds"`
	LatencyP50Seconds float64          `json:"latency_p50_seconds"`
	LatencyP95Seconds float64          `json:"latency_p95_seconds"`
	LatencyP99Seconds float64          `json:"latency_p99_seconds"`
	OutboxPublished   int64            `json:"outbox_published"`
	OutboxFailures    int64            `json:"outbox_failures"`
}
