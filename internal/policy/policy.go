package policy

import (
	"errors"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

type Thresholds struct {
	Review   float64
	Escalate float64
}

func New(review, escalate float64) (Thresholds, error) {
	if review < 0 || escalate > 1 || review >= escalate {
		return Thresholds{}, errors.New("thresholds must satisfy 0 <= review < escalate <= 1")
	}
	return Thresholds{Review: review, Escalate: escalate}, nil
}

func (t Thresholds) Decide(score float64) domain.Action {
	if score >= t.Escalate {
		return domain.ActionEscalate
	}
	if score >= t.Review {
		return domain.ActionReview
	}
	return domain.ActionNone
}
