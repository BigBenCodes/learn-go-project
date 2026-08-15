package main

import (
	"testing"
	"time"
)

func TestEventStepNeverCollapsesTimestamps(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{"follows the pacing interval", 10 * time.Millisecond, 10 * time.Millisecond},
		// --rate 0 disables pacing and leaves interval at zero. Reusing it as
		// the timestamp step would stamp every event with the same
		// occurred_at and zero out the velocity features for the whole run.
		{"unpaced runs still advance", 0, defaultEventStep},
		{"negative is treated as unpaced", -time.Second, defaultEventStep},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eventStep(tt.interval); got != tt.want {
				t.Fatalf("eventStep(%v) = %v, want %v", tt.interval, got, tt.want)
			}
		})
	}
}

func TestEventStepProducesDistinctTimestamps(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	step := eventStep(0)
	seen := map[time.Time]bool{}
	for emitted := range 100 {
		at := start.Add(time.Duration(emitted) * step)
		if seen[at] {
			t.Fatalf("timestamp %s repeated at event %d", at, emitted)
		}
		seen[at] = true
	}
}
