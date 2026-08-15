package service

import (
	"math"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
)

// PipelineSnapshot reads the counters and histogram registered by NewMetrics
// straight out of the Prometheus registry and summarises them for the JSON
// dashboard API. It reports the same numbers /metrics exposes, just shaped
// for a UI rather than for scraping.
func PipelineSnapshot(gatherer prometheus.Gatherer) (domain.PipelineMetrics, error) {
	families, err := gatherer.Gather()
	if err != nil {
		return domain.PipelineMetrics{}, err
	}
	metrics := domain.PipelineMetrics{
		Processed:  map[string]int64{},
		Duplicates: map[string]int64{},
		Failures:   map[string]int64{},
		Actions:    map[string]int64{},
	}
	for _, family := range families {
		switch family.GetName() {
		case "fraud_events_processed_total":
			sumCounterByLabel(family, "type", metrics.Processed)
		case "fraud_duplicate_events_total":
			sumCounterByLabel(family, "type", metrics.Duplicates)
		case "fraud_processing_failures_total":
			sumCounterByLabel(family, "type", metrics.Failures)
		case "fraud_recommended_actions_total":
			sumCounterByLabel(family, "action", metrics.Actions)
		case "fraud_assessment_duration_seconds":
			populateLatency(family, &metrics)
		case "fraud_outbox_published_total":
			metrics.OutboxPublished = sumCounter(family)
		case "fraud_outbox_failures_total":
			metrics.OutboxFailures = sumCounter(family)
		}
	}
	return metrics, nil
}

func sumCounter(family *dto.MetricFamily) int64 {
	var total int64
	for _, m := range family.GetMetric() {
		total += int64(m.GetCounter().GetValue())
	}
	return total
}

func sumCounterByLabel(family *dto.MetricFamily, label string, into map[string]int64) {
	for _, m := range family.GetMetric() {
		into[labelValue(m, label)] += int64(m.GetCounter().GetValue())
	}
}

func labelValue(m *dto.Metric, name string) string {
	for _, pair := range m.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}

func populateLatency(family *dto.MetricFamily, metrics *domain.PipelineMetrics) {
	for _, m := range family.GetMetric() {
		hist := m.GetHistogram()
		metrics.LatencyCount += int64(hist.GetSampleCount())
		metrics.LatencySumSeconds += hist.GetSampleSum()
	}
	if metrics.LatencyCount > 0 {
		metrics.LatencyAvgSeconds = metrics.LatencySumSeconds / float64(metrics.LatencyCount)
	}
	metrics.LatencyP50Seconds = latencyQuantile(family, 0.50)
	metrics.LatencyP95Seconds = latencyQuantile(family, 0.95)
	metrics.LatencyP99Seconds = latencyQuantile(family, 0.99)
}

// latencyQuantile approximates a quantile from the histogram's cumulative
// buckets via linear interpolation, the same approach Prometheus'
// histogram_quantile() uses.
func latencyQuantile(family *dto.MetricFamily, q float64) float64 {
	cumulative := map[float64]uint64{}
	var count uint64
	for _, m := range family.GetMetric() {
		hist := m.GetHistogram()
		count += hist.GetSampleCount()
		for _, bucket := range hist.GetBucket() {
			cumulative[bucket.GetUpperBound()] += bucket.GetCumulativeCount()
		}
	}
	if count == 0 {
		return 0
	}
	bounds := make([]float64, 0, len(cumulative))
	for bound := range cumulative {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)

	target := q * float64(count)
	var prevBound, prevCount float64
	for _, bound := range bounds {
		bucketCount := float64(cumulative[bound])
		if bucketCount >= target {
			if math.IsInf(bound, 1) {
				return prevBound
			}
			if bucketCount == prevCount {
				return bound
			}
			return prevBound + (bound-prevBound)*(target-prevCount)/(bucketCount-prevCount)
		}
		prevBound, prevCount = bound, bucketCount
	}
	return prevBound
}
