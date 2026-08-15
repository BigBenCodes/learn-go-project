package domain

import (
	"testing"
	"time"
)

func validTransaction() TransactionCreated {
	return TransactionCreated{
		SchemaVersion: SchemaVersion,
		EventID:       "transaction-1",
		TransactionID: "tx-1",
		OccurredAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		AccountID:     "account-1",
		CardID:        "card-1",
		AmountMinor:   1250,
		Currency:      "GBP",
		Merchant:      Merchant{ID: "merchant-grocer", Category: "groceries", Country: "GB"},
		EntryMode:     EntryModeChip,
	}
}

// Validate is the gate between "dead-letter this event" and "retry it
// forever", so every rejection here is load-bearing.
func TestTransactionValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*TransactionCreated)
		wantErr bool
	}{
		{"valid", func(*TransactionCreated) {}, false},
		{"wrong schema version", func(tx *TransactionCreated) { tx.SchemaVersion = 2 }, true},
		{"missing event id", func(tx *TransactionCreated) { tx.EventID = "" }, true},
		{"missing transaction id", func(tx *TransactionCreated) { tx.TransactionID = "" }, true},
		{"missing account id", func(tx *TransactionCreated) { tx.AccountID = "" }, true},
		{"missing card id", func(tx *TransactionCreated) { tx.CardID = "" }, true},
		{"zero occurred_at", func(tx *TransactionCreated) { tx.OccurredAt = time.Time{} }, true},
		{"zero amount", func(tx *TransactionCreated) { tx.AmountMinor = 0 }, true},
		{"negative amount", func(tx *TransactionCreated) { tx.AmountMinor = -1 }, true},
		{"unsupported currency", func(tx *TransactionCreated) { tx.Currency = "USD" }, true},
		{"missing merchant id", func(tx *TransactionCreated) { tx.Merchant.ID = "" }, true},
		{"missing merchant category", func(tx *TransactionCreated) { tx.Merchant.Category = "" }, true},
		{"non-ISO country", func(tx *TransactionCreated) { tx.Merchant.Country = "GBR" }, true},
		{"unsupported entry mode", func(tx *TransactionCreated) { tx.EntryMode = "telepathy" }, true},
		{"contactless", func(tx *TransactionCreated) { tx.EntryMode = EntryModeContactless }, false},
		{"ecommerce", func(tx *TransactionCreated) { tx.EntryMode = EntryModeEcommerce }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := validTransaction()
			tt.mutate(&tx)
			if err := tx.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Validate only checks these fields for non-emptiness. That is deliberate, but
// it means every consumer of them — the dashboard above all — must treat them
// as untrusted input rather than as safe display strings.
func TestTransactionValidateAcceptsHostileStrings(t *testing.T) {
	tx := validTransaction()
	tx.Merchant.Category = `<img src=x onerror=alert(1)>`
	if err := tx.Validate(); err != nil {
		t.Fatalf("Validate() rejected a hostile-but-well-formed category (%v); the dashboard escaping test is what guards this", err)
	}
}

func TestFraudLabelValidate(t *testing.T) {
	valid := FraudLabel{
		SchemaVersion: SchemaVersion,
		EventID:       "label-1",
		TransactionID: "tx-1",
		LabelledAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	tests := []struct {
		name    string
		mutate  func(*FraudLabel)
		wantErr bool
	}{
		{"valid legit label", func(*FraudLabel) {}, false},
		{"wrong schema version", func(l *FraudLabel) { l.SchemaVersion = 0 }, true},
		{"missing event id", func(l *FraudLabel) { l.EventID = "" }, true},
		{"missing transaction id", func(l *FraudLabel) { l.TransactionID = "" }, true},
		{"zero labelled_at", func(l *FraudLabel) { l.LabelledAt = time.Time{} }, true},
		{"fraud without type", func(l *FraudLabel) { l.IsFraud = true }, true},
		{"fraud with type", func(l *FraudLabel) { l.IsFraud, l.FraudType = true, "card_not_present" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			label := valid
			tt.mutate(&label)
			if err := label.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
