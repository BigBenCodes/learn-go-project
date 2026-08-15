package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_init.sql
var migration string

type Postgres struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("configure postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (s *Postgres) Close() { s.pool.Close() }

func (s *Postgres) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Postgres) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, migration); err != nil {
		return fmt.Errorf("apply database migration: %w", err)
	}
	return nil
}

type Evaluator func(domain.History) (domain.Assessment, error)

func (s *Postgres) Evaluate(ctx context.Context, event domain.TransactionCreated, evaluate Evaluator) (domain.Assessment, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	raw, err := json.Marshal(event)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("encode transaction event: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO transactions
			(transaction_id, event_id, account_id, occurred_at, merchant_id, merchant_country, raw_event)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING`,
		event.TransactionID, event.EventID, event.AccountID, event.OccurredAt,
		event.Merchant.ID, event.Merchant.Country, raw,
	)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("insert transaction: %w", err)
	}
	if tag.RowsAffected() == 0 {
		assessment, err := getAssessment(ctx, tx, event.TransactionID)
		if err != nil {
			return domain.Assessment{}, false, fmt.Errorf("load duplicate assessment: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Assessment{}, false, fmt.Errorf("commit duplicate lookup: %w", err)
		}
		return assessment, false, nil
	}

	var history domain.History
	err = tx.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE occurred_at >= $2::timestamptz - interval '10 minutes' AND occurred_at < $2::timestamptz),
			COALESCE(sum((raw_event->>'amount_minor')::bigint) FILTER (
				WHERE occurred_at >= $2::timestamptz - interval '1 hour' AND occurred_at < $2::timestamptz
			), 0),
			bool_or(merchant_id = $3 AND occurred_at < $2::timestamptz),
			bool_or(merchant_country = $4 AND occurred_at < $2::timestamptz)
		FROM transactions
		WHERE account_id = $1 AND occurred_at >= $2::timestamptz - interval '90 days'`,
		event.AccountID, event.OccurredAt, event.Merchant.ID, event.Merchant.Country,
	).Scan(&history.TransactionCount10m, &history.SpendMinor1h, &history.SeenMerchant, &history.SeenCountry)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("load transaction history: %w", err)
	}

	assessment, err := evaluate(history)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("evaluate transaction: %w", err)
	}
	signals, err := json.Marshal(assessment.Signals)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("encode assessment signals: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO assessments
			(transaction_id, event_id, assessed_at, model_version, risk_score, recommended_action, signals)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		assessment.TransactionID, assessment.EventID, assessment.AssessedAt,
		assessment.ModelVersion, assessment.RiskScore, assessment.RecommendedAction, signals,
	)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("insert assessment: %w", err)
	}
	payload, err := json.Marshal(assessment)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("encode assessment event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (event_id, topic, event_key, payload)
		VALUES ($1, 'fraud.assessments.v1', $2, $3)`,
		assessment.EventID, assessment.TransactionID, payload,
	)
	if err != nil {
		return domain.Assessment{}, false, fmt.Errorf("insert outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Assessment{}, false, fmt.Errorf("commit assessment: %w", err)
	}
	return assessment, true, nil
}

func getAssessment(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, transactionID string) (domain.Assessment, error) {
	var assessment domain.Assessment
	var signals []byte
	err := q.QueryRow(ctx, `
		SELECT event_id, transaction_id, assessed_at, model_version, risk_score, recommended_action, signals
		FROM assessments WHERE transaction_id = $1`, transactionID,
	).Scan(
		&assessment.EventID, &assessment.TransactionID, &assessment.AssessedAt,
		&assessment.ModelVersion, &assessment.RiskScore, &assessment.RecommendedAction, &signals,
	)
	if err != nil {
		return domain.Assessment{}, err
	}
	assessment.SchemaVersion = domain.SchemaVersion
	if err := json.Unmarshal(signals, &assessment.Signals); err != nil {
		return domain.Assessment{}, err
	}
	return assessment, nil
}

func (s *Postgres) StoreLabel(ctx context.Context, label domain.FraudLabel) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO fraud_labels (transaction_id, event_id, labelled_at, is_fraud, fraud_type)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`,
		label.TransactionID, label.EventID, label.LabelledAt, label.IsFraud, label.FraudType,
	)
	if err != nil {
		return false, fmt.Errorf("store fraud label: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

type OutboxEvent struct {
	ID      int64
	EventID string
	Topic   string
	Key     string
	Payload []byte
}

func (s *Postgres) FetchOutbox(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, topic, event_key, payload
		FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox: %w", err)
	}
	defer rows.Close()
	var events []OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventID, &event.Topic, &event.Key, &event.Payload); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Postgres) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

type ListFilter struct {
	Limit      int
	Action     domain.Action
	BeforeTime *time.Time
	BeforeID   string
}

type Page struct {
	Records  []domain.TransactionRecord
	NextTime *time.Time
	NextID   string
}

func (s *Postgres) ListTransactions(ctx context.Context, filter ListFilter) (Page, error) {
	limit := filter.Limit
	if limit < 1 || limit > 100 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT t.raw_event, a.event_id, a.assessed_at, a.model_version, a.risk_score,
			a.recommended_action, a.signals,
			l.event_id, l.labelled_at, l.is_fraud, l.fraud_type
		FROM transactions t
		JOIN assessments a USING (transaction_id)
		LEFT JOIN fraud_labels l USING (transaction_id)
		WHERE ($1 = '' OR a.recommended_action = $1)
		  AND ($2::timestamptz IS NULL OR (t.occurred_at, t.transaction_id) < ($2, $3))
		ORDER BY t.occurred_at DESC, t.transaction_id DESC
		LIMIT $4`, string(filter.Action), filter.BeforeTime, filter.BeforeID, limit+1)
	if err != nil {
		return Page{}, fmt.Errorf("list transactions: %w", err)
	}
	defer rows.Close()
	page := Page{Records: make([]domain.TransactionRecord, 0, limit)}
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return Page{}, err
		}
		page.Records = append(page.Records, record)
	}
	if err := rows.Err(); err != nil {
		return Page{}, fmt.Errorf("iterate transactions: %w", err)
	}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
		last := page.Records[limit-1].Transaction
		page.NextTime = &last.OccurredAt
		page.NextID = last.TransactionID
	}
	return page, nil
}

func (s *Postgres) GetTransaction(ctx context.Context, id string) (domain.TransactionRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT t.raw_event, a.event_id, a.assessed_at, a.model_version, a.risk_score,
			a.recommended_action, a.signals,
			l.event_id, l.labelled_at, l.is_fraud, l.fraud_type
		FROM transactions t
		JOIN assessments a USING (transaction_id)
		LEFT JOIN fraud_labels l USING (transaction_id)
		WHERE t.transaction_id = $1`, id)
	return scanRecord(row)
}

type scanner interface {
	Scan(...any) error
}

func scanRecord(row scanner) (domain.TransactionRecord, error) {
	var record domain.TransactionRecord
	var raw, signals []byte
	var labelEvent, fraudType sql.NullString
	var labelledAt sql.NullTime
	var isFraud sql.NullBool
	err := row.Scan(
		&raw, &record.Assessment.EventID, &record.Assessment.AssessedAt,
		&record.Assessment.ModelVersion, &record.Assessment.RiskScore,
		&record.Assessment.RecommendedAction, &signals,
		&labelEvent, &labelledAt, &isFraud, &fraudType,
	)
	if err != nil {
		return domain.TransactionRecord{}, err
	}
	if err := json.Unmarshal(raw, &record.Transaction); err != nil {
		return domain.TransactionRecord{}, fmt.Errorf("decode stored transaction: %w", err)
	}
	record.Assessment.SchemaVersion = domain.SchemaVersion
	record.Assessment.TransactionID = record.Transaction.TransactionID
	if err := json.Unmarshal(signals, &record.Assessment.Signals); err != nil {
		return domain.TransactionRecord{}, fmt.Errorf("decode stored signals: %w", err)
	}
	if labelEvent.Valid {
		record.Label = &domain.FraudLabel{
			SchemaVersion: domain.SchemaVersion,
			EventID:       labelEvent.String,
			TransactionID: record.Transaction.TransactionID,
			LabelledAt:    labelledAt.Time,
			IsFraud:       isFraud.Bool,
			FraudType:     fraudType.String,
		}
	}
	return record, nil
}

func (s *Postgres) ModelMetrics(ctx context.Context, modelVersion string) (domain.ModelMetrics, error) {
	metrics := domain.ModelMetrics{ModelVersion: modelVersion}
	err := s.pool.QueryRow(ctx, `
		SELECT
			count(*),
			count(l.transaction_id),
			count(*) FILTER (WHERE l.is_fraud AND a.recommended_action <> 'no_action'),
			count(*) FILTER (WHERE NOT l.is_fraud AND a.recommended_action <> 'no_action'),
			count(*) FILTER (WHERE NOT l.is_fraud AND a.recommended_action = 'no_action'),
			count(*) FILTER (WHERE l.is_fraud AND a.recommended_action = 'no_action')
		FROM assessments a
		LEFT JOIN fraud_labels l USING (transaction_id)
		WHERE ($1 = '' OR a.model_version = $1)`, modelVersion,
	).Scan(
		&metrics.TotalAssessments, &metrics.Labelled,
		&metrics.TruePositive, &metrics.FalsePositive,
		&metrics.TrueNegative, &metrics.FalseNegative,
	)
	if err != nil {
		return domain.ModelMetrics{}, fmt.Errorf("calculate model metrics: %w", err)
	}
	metrics.Precision = ratio(metrics.TruePositive, metrics.TruePositive+metrics.FalsePositive)
	metrics.Recall = ratio(metrics.TruePositive, metrics.TruePositive+metrics.FalseNegative)
	metrics.FalsePositiveRate = ratio(metrics.FalsePositive, metrics.FalsePositive+metrics.TrueNegative)
	metrics.LabelCoverage = ratio(metrics.Labelled, metrics.TotalAssessments)
	return metrics, nil
}

func ratio(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
