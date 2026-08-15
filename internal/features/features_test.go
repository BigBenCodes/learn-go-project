package features

import (
	"testing"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

func TestExtract(t *testing.T) {
	tx := domain.TransactionCreated{
		OccurredAt:  time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		AmountMinor: 100_000,
		Merchant:    domain.Merchant{ID: "m", Category: "crypto", Country: "LT"},
		EntryMode:   domain.EntryModeEcommerce,
	}
	history := domain.History{TransactionCount10m: 5, SpendMinor1h: 100_000}
	got := Extract(tx, history)
	for _, feature := range []string{"foreign", "card_not_present", "risky_merchant", "nighttime", "new_merchant", "new_country"} {
		if got[feature] != 1 {
			t.Errorf("%s = %v, want 1", feature, got[feature])
		}
	}
	if got["tx_count_10m"] != 0.5 || got["spend_1h"] != 0.5 {
		t.Fatalf("unexpected velocity features: %#v", got)
	}
}
