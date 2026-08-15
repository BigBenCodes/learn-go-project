package simulator

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

type merchant struct {
	id       string
	category string
	country  string
}

var normalMerchants = []merchant{
	{"merchant-grocer", "groceries", "GB"},
	{"merchant-coffee", "eating_out", "GB"},
	{"merchant-rail", "transport", "GB"},
	{"merchant-books", "retail", "GB"},
	{"merchant-streaming", "entertainment", "GB"},
}

var fraudMerchants = []merchant{
	{"merchant-crypto", "crypto", "LT"},
	{"merchant-casino", "gambling", "MT"},
	{"merchant-transfer", "money_transfer", "RO"},
}

type Generator struct {
	rng       *rand.Rand
	seed      int64
	next      int64
	accounts  int
	fraudRate float64
}

func New(seed int64, accounts int, fraudRate float64) (*Generator, error) {
	if accounts < 1 {
		return nil, fmt.Errorf("accounts must be positive")
	}
	if fraudRate < 0 || fraudRate > 1 {
		return nil, fmt.Errorf("fraud rate must be between 0 and 1")
	}
	return &Generator{
		rng:       rand.New(rand.NewSource(seed)),
		seed:      seed,
		accounts:  accounts,
		fraudRate: fraudRate,
	}, nil
}

func (g *Generator) Next(occurredAt time.Time, labelDelay time.Duration) (domain.TransactionCreated, domain.FraudLabel) {
	g.next++
	sequence := g.next
	accountNumber := g.rng.Intn(g.accounts) + 1
	isFraud := g.rng.Float64() < g.fraudRate

	m := normalMerchants[g.rng.Intn(len(normalMerchants))]
	entryMode := []domain.EntryMode{domain.EntryModeChip, domain.EntryModeContactless}[g.rng.Intn(2)]
	amountMinor := int64(250 + g.rng.Intn(15_000))
	fraudType := ""
	if isFraud {
		m = fraudMerchants[g.rng.Intn(len(fraudMerchants))]
		entryMode = domain.EntryModeEcommerce
		amountMinor = int64(50_000 + g.rng.Intn(150_000))
		fraudType = "card_not_present"
	}

	id := fmt.Sprintf("%d-%09d", g.seed, sequence)
	tx := domain.TransactionCreated{
		SchemaVersion: domain.SchemaVersion,
		EventID:       "transaction-" + id,
		TransactionID: "tx-" + id,
		OccurredAt:    occurredAt.UTC(),
		AccountID:     fmt.Sprintf("account-%04d", accountNumber),
		CardID:        fmt.Sprintf("card-%04d", accountNumber),
		AmountMinor:   amountMinor,
		Currency:      "GBP",
		Merchant: domain.Merchant{
			ID:       m.id,
			Category: m.category,
			Country:  m.country,
		},
		EntryMode: entryMode,
	}
	label := domain.FraudLabel{
		SchemaVersion: domain.SchemaVersion,
		EventID:       "label-" + id,
		TransactionID: tx.TransactionID,
		LabelledAt:    occurredAt.UTC().Add(labelDelay),
		IsFraud:       isFraud,
		FraudType:     fraudType,
	}
	return tx, label
}
