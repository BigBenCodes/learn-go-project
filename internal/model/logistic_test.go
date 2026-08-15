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

// Weights is a map, so ranging it gives a different order every call. Without
// a tiebreaker, tied contributions (very often 0) would order differently on
// each run and the same input would persist different assessments.signals.
func TestScoreOrdersTiedSignalsDeterministically(t *testing.T) {
	m, err := New(Artifact{Version: "test", Weights: map[string]float64{
		"delta": 1, "alpha": 1, "charlie": 1, "bravo": 1, "echo": 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Every feature except "echo" contributes exactly 0.
	features := domain.FeatureVector{"echo": 1}

	_, first := m.Score(features)
	want := []string{"echo", "alpha", "bravo", "charlie", "delta"}
	for i, feature := range want {
		if first[i].Feature != feature {
			t.Fatalf("signal %d = %q, want %q (ties must break on feature name): %#v",
				i, first[i].Feature, feature, first)
		}
	}
	for range 20 {
		_, next := m.Score(features)
		for i := range next {
			if next[i].Feature != first[i].Feature {
				t.Fatalf("signal order is not stable across calls: %q then %q at index %d",
					first[i].Feature, next[i].Feature, i)
			}
		}
	}
}
