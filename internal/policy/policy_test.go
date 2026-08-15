package policy

import (
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
