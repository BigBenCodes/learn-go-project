package model

import (
	"math"
	"testing"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

func TestScore(t *testing.T) {
	m, err := New(Artifact{Version: "test", Intercept: -1, Weights: map[string]float64{"a": 2, "b": -1}})
	if err != nil {
		t.Fatal(err)
	}
	score, signals := m.Score(domain.FeatureVector{"a": 1, "b": 0.5})
	want := 1 / (1 + math.Exp(-0.5))
	if math.Abs(score-want) > 1e-12 {
		t.Fatalf("score = %f, want %f", score, want)
	}
	if len(signals) != 2 || signals[0].Feature != "a" {
		t.Fatalf("signals not sorted by absolute contribution: %#v", signals)
	}
}
