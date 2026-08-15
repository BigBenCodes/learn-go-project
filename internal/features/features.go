package features

import (
	"math"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

var riskyCategories = map[string]bool{
	"crypto":         true,
	"gambling":       true,
	"money_transfer": true,
}

func Extract(tx domain.TransactionCreated, history domain.History) domain.FeatureVector {
	foreign := 0.0
	if tx.Merchant.Country != "GB" {
		foreign = 1
	}
	cardNotPresent := 0.0
	if tx.EntryMode == domain.EntryModeEcommerce {
		cardNotPresent = 1
	}
	riskyMerchant := 0.0
	if riskyCategories[tx.Merchant.Category] {
		riskyMerchant = 1
	}
	nighttime := 0.0
	hour := tx.OccurredAt.UTC().Hour()
	if hour < 6 || hour >= 23 {
		nighttime = 1
	}
	newMerchant := 0.0
	if !history.SeenMerchant {
		newMerchant = 1
	}
	newCountry := 0.0
	if !history.SeenCountry {
		newCountry = 1
	}

	return domain.FeatureVector{
		"log_amount":       math.Log1p(float64(tx.AmountMinor) / 100),
		"foreign":          foreign,
		"card_not_present": cardNotPresent,
		"risky_merchant":   riskyMerchant,
		"nighttime":        nighttime,
		"tx_count_10m":     math.Min(float64(history.TransactionCount10m), 10) / 10,
		"spend_1h":         math.Min(float64(history.SpendMinor1h)/100, 2000) / 2000,
		"new_merchant":     newMerchant,
		"new_country":      newCountry,
	}
}
