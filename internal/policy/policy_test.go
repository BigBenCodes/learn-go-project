package policy

import (
	"math"
	"testing"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

func TestThresholdBoundaries(t *testing.T) {
	p, err := New(0.65, 0.85)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		score float64
		want  domain.Action
	}{
		{0.649, domain.ActionNone},
		{0.65, domain.ActionReview},
		{0.849, domain.ActionReview},
		{0.85, domain.ActionEscalate},
	}
	for _, tt := range tests {
		if got := p.Decide(tt.score); got != tt.want {
			t.Errorf("Decide(%v) = %q, want %q", tt.score, got, tt.want)
		}
	}
}

func TestNewRejectsInvalidThresholds(t *testing.T) {
	nan := math.NaN()
	tests := []struct {
		name             string
		review, escalate float64
	}{
		{"review below zero", -0.1, 0.85},
		{"escalate above one", 0.65, 1.1},
		{"review above escalate", 0.9, 0.85},
		{"review equals escalate", 0.85, 0.85},
		// NaN compares false against every bound, so it would otherwise slip
		// through and produce a policy that never escalates anything.
		{"NaN review", nan, 0.85},
		{"NaN escalate", 0.65, nan},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.review, tt.escalate); err == nil {
				t.Fatalf("New(%v, %v) = nil error, want rejection", tt.review, tt.escalate)
			}
		})
	}
}
