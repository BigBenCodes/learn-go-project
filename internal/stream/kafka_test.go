package stream

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"single", "localhost:19092", []string{"localhost:19092"}},
		{"multiple", "a:9092,b:9092", []string{"a:9092", "b:9092"}},
		{"spaces after commas", "a:9092, b:9092", []string{"a:9092", "b:9092"}},
		{"trailing comma", "a:9092,", []string{"a:9092"}},
		{"empty", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseBrokers(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseBrokers(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

// handleRecord routes permanent errors to the dead-letter topic and retries
// everything else forever, so the classification is what decides whether a bad
// event is parked or blocks its partition.
func TestPermanentClassification(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must stay nil")
	}
	var permanent permanentError

	wrapped := fmt.Errorf("validate transaction: %w", Permanent(errors.New("bad currency")))
	if !errors.As(wrapped, &permanent) {
		t.Error("a wrapped permanent error must still be detected as permanent")
	}
	if errors.As(errors.New("connection refused"), &permanent) {
		t.Error("a plain error must not be treated as permanent, or transient failures get dead-lettered")
	}
}
