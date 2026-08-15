package httpapi

import (
	"embed"
	"net/http"
)

//go:embed ui/dashboard.html
var dashboardHTML embed.FS

// dashboard serves the self-contained metrics UI. It reads from the embedded
// FS on every request rather than caching bytes at startup, so unit tests
// against a zero-value Server still work and the handler stays trivial.
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	data, err := dashboardHTML.ReadFile("ui/dashboard.html")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "dashboard unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}
