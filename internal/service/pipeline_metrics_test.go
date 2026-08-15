package service

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPipelineSnapshot(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	metrics.Processed.WithLabelValues("transaction").Add(3)
	metrics.Duplicates.WithLabelValues("transaction").Inc()
	metrics.Failures.WithLabelValues("label").Inc()
	metrics.Actions.WithLabelValues("review", "v1").Inc()
	metrics.OutboxPublished.Add(2)
	metrics.OutboxFailures.Inc()
	for _, seconds := range []float64{0.01, 0.02, 0.05, 0.1, 0.2} {
		metrics.Latency.Observe(seconds)
	}

	snapshot, err := PipelineSnapshot(registry)
	if err != nil {
		t.Fatalf("PipelineSnapshot() error = %v", err)
	}
	if got := snapshot.Processed["transaction"]; got != 3 {
		t.Errorf("Processed[transaction] = %d, want 3", got)
	}
	if got := snapshot.Duplicates["transaction"]; got != 1 {
		t.Errorf("Duplicates[transaction] = %d, want 1", got)
	}
	if got := snapshot.Failures["label"]; got != 1 {
		t.Errorf("Failures[label] = %d, want 1", got)
	}
	if got := snapshot.Actions["review"]; got != 1 {
		t.Errorf("Actions[review] = %d, want 1", got)
	}
	if snapshot.OutboxPublished != 2 || snapshot.OutboxFailures != 1 {
		t.Errorf("outbox counters = %d/%d, want 2/1", snapshot.OutboxPublished, snapshot.OutboxFailures)
	}
	if snapshot.LatencyCount != 5 {
		t.Errorf("LatencyCount = %d, want 5", snapshot.LatencyCount)
	}
	if snapshot.LatencyAvgSeconds <= 0 {
		t.Errorf("LatencyAvgSeconds = %v, want > 0", snapshot.LatencyAvgSeconds)
	}
	if snapshot.LatencyP50Seconds <= 0 || snapshot.LatencyP99Seconds < snapshot.LatencyP50Seconds {
		t.Errorf("latency percentiles = p50 %v p99 %v, want p99 >= p50 > 0",
			snapshot.LatencyP50Seconds, snapshot.LatencyP99Seconds)
	}
}

func TestPipelineSnapshotEmpty(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewMetrics(registry)

	snapshot, err := PipelineSnapshot(registry)
	if err != nil {
		t.Fatalf("PipelineSnapshot() error = %v", err)
	}
	if snapshot.LatencyCount != 0 || snapshot.LatencyAvgSeconds != 0 || snapshot.LatencyP99Seconds != 0 {
		t.Errorf("empty snapshot latency = %+v, want zero values", snapshot)
	}
}
