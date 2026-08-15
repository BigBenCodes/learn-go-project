package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bensullivan2002/learn-go-project/internal/domain"
	"github.com/bensullivan2002/learn-go-project/internal/store"
)

type Repository interface {
	Ping(context.Context) error
	ListTransactions(context.Context, store.ListFilter) (store.Page, error)
	GetTransaction(context.Context, string) (domain.TransactionRecord, error)
	ModelMetrics(context.Context, string) (domain.ModelMetrics, error)
}

type Server struct {
	server     *http.Server
	repository Repository
	logger     *slog.Logger
}

func New(address string, repository Repository, metricsHandler http.Handler, logger *slog.Logger) *Server {
	s := &Server{repository: repository, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /v1/transactions", s.listTransactions)
	mux.HandleFunc("GET /v1/transactions/{id}", s.getTransaction)
	mux.HandleFunc("GET /v1/model-metrics", s.modelMetrics)
	mux.Handle("GET /metrics", metricsHandler)
	s.server = &http.Server{
		Addr: address, Handler: loggingMiddleware(logger, mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	}
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.repository.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) listTransactions(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	action := domain.Action(r.URL.Query().Get("action"))
	if action != "" && action != domain.ActionNone && action != domain.ActionReview && action != domain.ActionEscalate {
		writeError(w, http.StatusBadRequest, "invalid action filter")
		return
	}
	filter := store.ListFilter{Limit: limit, Action: action}
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		when, id, err := decodeCursor(cursor)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		filter.BeforeTime, filter.BeforeID = &when, id
	}
	page, err := s.repository.ListTransactions(r.Context(), filter)
	if err != nil {
		s.logger.Error("list transactions failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list transactions")
		return
	}
	next := ""
	if page.NextTime != nil {
		next = encodeCursor(*page.NextTime, page.NextID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"transactions": page.Records, "next_cursor": next})
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusBadRequest, "invalid transaction id")
		return
	}
	record, err := s.repository.GetTransaction(r.Context(), id)
	if store.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "transaction not found")
		return
	}
	if err != nil {
		s.logger.Error("get transaction failed", "transaction_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not get transaction")
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) modelMetrics(w http.ResponseWriter, r *http.Request) {
	metrics, err := s.repository.ModelMetrics(r.Context(), r.URL.Query().Get("model_version"))
	if err != nil {
		s.logger.Error("model metrics failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not calculate model metrics")
		return
	}
	writeJSON(w, http.StatusOK, metrics)
}

func encodeCursor(when time.Time, id string) string {
	raw := when.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 || parts[1] == "" {
		return time.Time{}, "", fmt.Errorf("malformed cursor")
	}
	when, err := time.Parse(time.RFC3339Nano, parts[0])
	return when, parts[1], err
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Debug("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}
