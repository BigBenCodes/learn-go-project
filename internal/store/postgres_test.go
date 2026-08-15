package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

// fakeRow feeds scanRecord the column values a real query would produce, so
// the scan logic can be exercised without standing up Postgres.
type fakeRow struct{ values []any }

func (f fakeRow) Scan(dest ...any) error {
	if len(dest) != len(f.values) {
		return fmt.Errorf("scan into %d destinations, have %d values", len(dest), len(f.values))
	}
	for i, d := range dest {
		switch target := d.(type) {
		case *time.Time:
			*target = f.values[i].(time.Time)
		case *string:
			*target = f.values[i].(string)
		case *[]byte:
			*target = f.values[i].([]byte)
		case *float64:
			*target = f.values[i].(float64)
		case *domain.Action:
			*target = f.values[i].(domain.Action)
		case *sql.NullString:
			*target = sql.NullString{}
		case *sql.NullTime:
			*target = sql.NullTime{}
		case *sql.NullBool:
			*target = sql.NullBool{}
		default:
			return fmt.Errorf("unexpected destination %T at index %d", d, i)
		}
	}
	return nil
}

// The occurred_at column is timestamptz and holds microseconds; the raw_event
// JSON holds whatever nanosecond precision the producer sent. A cursor built
// from the JSON is then later than the row it names, and
// `(occurred_at, transaction_id) < (cursor)` matches that row again — it
// reappears at the head of every following page.
func TestScanRecordTakesCursorFromColumnNotRawEvent(t *testing.T) {
	stored := time.Date(2026, 1, 2, 3, 4, 5, 333333000, time.UTC)
	fromProducer := time.Date(2026, 1, 2, 3, 4, 5, 333333333, time.UTC)

	raw, err := json.Marshal(domain.TransactionCreated{
		SchemaVersion: domain.SchemaVersion,
		EventID:       "transaction-1",
		TransactionID: "tx-1",
		OccurredAt:    fromProducer,
		AccountID:     "account-1",
		CardID:        "card-1",
		AmountMinor:   1000,
		Currency:      "GBP",
		Merchant:      domain.Merchant{ID: "m", Category: "groceries", Country: "GB"},
		EntryMode:     domain.EntryModeChip,
	})
	if err != nil {
		t.Fatal(err)
	}

	record, key, err := scanRecord(fakeRow{values: []any{
		stored, "tx-1", raw,
		"assessment-tx-1", stored, "v1", 0.5, domain.ActionReview, []byte("[]"),
		nil, nil, nil, nil,
	}})
	if err != nil {
		t.Fatalf("scanRecord() error = %v", err)
	}
	if !key.occurredAt.Equal(stored) {
		t.Errorf("cursor time = %s, want the occurred_at column value %s",
			key.occurredAt.Format(time.RFC3339Nano), stored.Format(time.RFC3339Nano))
	}
	if key.occurredAt.Equal(record.Transaction.OccurredAt) {
		t.Errorf("cursor time came from raw_event (%s); it must come from the occurred_at column",
			record.Transaction.OccurredAt.Format(time.RFC3339Nano))
	}
	if key.transactionID != "tx-1" {
		t.Errorf("cursor id = %q, want %q", key.transactionID, "tx-1")
	}
}

func TestUnprocessableTagsUniqueViolations(t *testing.T) {
	if IsUnprocessable(nil) {
		t.Error("nil error should not be unprocessable")
	}
	plain := fmt.Errorf("connection refused")
	if got := unprocessable(plain); IsUnprocessable(got) {
		t.Error("a transient error must stay retryable, or a down database dead-letters real events")
	}
	tagged := fmt.Errorf("%w: duplicate key", ErrUnprocessable)
	if !IsUnprocessable(fmt.Errorf("insert transaction: %w", tagged)) {
		t.Error("IsUnprocessable must see through wrapping")
	}
}
