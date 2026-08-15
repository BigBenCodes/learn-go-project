package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

type Artifact struct {
	Version   string             `json:"version"`
	Intercept float64            `json:"intercept"`
	Weights   map[string]float64 `json:"weights"`
}

type Logistic struct {
	artifact Artifact
}

func Load(path string) (*Logistic, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read model artifact: %w", err)
	}
	var artifact Artifact
	if err := json.Unmarshal(b, &artifact); err != nil {
		return nil, fmt.Errorf("decode model artifact: %w", err)
	}
	if artifact.Version == "" || len(artifact.Weights) == 0 {
		return nil, errors.New("model artifact requires a version and weights")
	}
	return &Logistic{artifact: artifact}, nil
}

func New(artifact Artifact) (*Logistic, error) {
	if artifact.Version == "" || len(artifact.Weights) == 0 {
		return nil, errors.New("model artifact requires a version and weights")
	}
	return &Logistic{artifact: artifact}, nil
}

func (m *Logistic) Version() string { return m.artifact.Version }

func (m *Logistic) Score(features domain.FeatureVector) (float64, []domain.Signal) {
	logit := m.artifact.Intercept
	signals := make([]domain.Signal, 0, len(m.artifact.Weights))
	for feature, weight := range m.artifact.Weights {
		value := features[feature]
		contribution := value * weight
		logit += contribution
		signals = append(signals, domain.Signal{Feature: feature, Value: value, Contribution: contribution})
	}
	sort.Slice(signals, func(i, j int) bool {
		return math.Abs(signals[i].Contribution) > math.Abs(signals[j].Contribution)
	})
	return 1 / (1 + math.Exp(-logit)), signals
}
