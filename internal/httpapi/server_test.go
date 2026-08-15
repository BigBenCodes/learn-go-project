package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/bensullivan2002/learn-go-project/internal/store"
)

type fakeRepository struct {
	page store.Page
}

func (f fakeRepository) Ping(context.Context) error { return nil }
func (f fakeRepository) ListTransactions(context.Context, store.ListFilter) (store.Page, error) {
	return f.page, nil
}
func (f fakeRepository) GetTransaction(context.Context, string) (domain.TransactionRecord, error) {
	return domain.TransactionRecord{}, nil
}
func (f fakeRepository) ModelMetrics(context.Context, string) (domain.ModelMetrics, error) {
	return domain.ModelMetrics{TotalAssessments: 10}, nil
}

func fakePipelineMetrics() (domain.PipelineMetrics, error) {
	return domain.PipelineMetrics{Processed: map[string]int64{"transaction": 5}}, nil
}

func TestCursorRoundTrip(t *testing.T) {
	wantTime := time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC)
	wantID := "tx-123"
	gotTime, gotID, err := decodeCursor(encodeCursor(wantTime, wantID))
	if err != nil || !gotTime.Equal(wantTime) || gotID != wantID {
		t.Fatalf("cursor round trip = %v %q %v", gotTime, gotID, err)
	}
}

func TestDashboardServesHTML(t *testing.T) {
	server := New(":0", fakeRepository{}, http.NotFoundHandler(), fakePipelineMetrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if ct := response.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Fatalf("content-type = %q, want text/html; charset=utf-8", ct)
	}
	if response.Body.Len() == 0 {
		t.Fatal("dashboard body is empty")
	}
}

func TestPipelineMetricsServesJSON(t *testing.T) {
	server := New(":0", fakeRepository{}, http.NotFoundHandler(), fakePipelineMetrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/v1/pipeline-metrics", nil)
	response := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	var metrics domain.PipelineMetrics
	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if metrics.Processed["transaction"] != 5 {
		t.Fatalf("Processed[transaction] = %d, want 5", metrics.Processed["transaction"])
	}
}

func TestListRejectsInvalidLimit(t *testing.T) {
	server := New(":0", fakeRepository{}, http.NotFoundHandler(), fakePipelineMetrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/v1/transactions?limit=101", nil)
	response := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}
