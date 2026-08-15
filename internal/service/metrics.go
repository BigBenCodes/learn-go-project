package service

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Processed       *prometheus.CounterVec
	Duplicates      *prometheus.CounterVec
	Failures        *prometheus.CounterVec
	Actions         *prometheus.CounterVec
	Latency         prometheus.Histogram
	OutboxPublished prometheus.Counter
	OutboxFailures  prometheus.Counter
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fraud_events_processed_total", Help: "Successfully processed input events.",
		}, []string{"type"}),
		Duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fraud_duplicate_events_total", Help: "Idempotently ignored input events.",
		}, []string{"type"}),
		Failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fraud_processing_failures_total", Help: "Event processing failures.",
		}, []string{"type"}),
		Actions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fraud_recommended_actions_total", Help: "Recommended fraud actions.",
		}, []string{"action", "model_version"}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "fraud_assessment_duration_seconds", Help: "End-to-end assessment processing duration.",
			Buckets: prometheus.DefBuckets,
		}),
		OutboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fraud_outbox_published_total", Help: "Successfully published outbox events.",
		}),
		OutboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fraud_outbox_failures_total", Help: "Failed outbox publish attempts.",
		}),
	}
	registerer.MustRegister(m.Processed, m.Duplicates, m.Failures, m.Actions, m.Latency, m.OutboxPublished, m.OutboxFailures)
	return m
}
