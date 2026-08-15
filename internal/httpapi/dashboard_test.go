package httpapi

import (
	"regexp"
	"strings"
	"testing"
)

// Transaction fields reach the dashboard verbatim from the consumed Kafka
// message — domain.Validate only checks most of them for non-emptiness — so a
// merchant category of `<img src=x onerror=...>` would execute in the
// operator's browser if any of them were concatenated into innerHTML.
//
// The invariant: innerHTML may only ever be assigned a single string literal.
// Any concatenation means a value is being interpolated as markup.
func TestDashboardNeverConcatenatesIntoInnerHTML(t *testing.T) {
	data, err := dashboardHTML.ReadFile("ui/dashboard.html")
	if err != nil {
		t.Fatal(err)
	}
	source := regexp.MustCompile(`\s+`).ReplaceAllString(string(data), " ")

	assignments := regexp.MustCompile(`innerHTML\s*=\s*([^;]*);`).FindAllStringSubmatch(source, -1)
	if len(assignments) == 0 {
		t.Skip("no innerHTML assignments left in the dashboard")
	}
	for _, match := range assignments {
		rhs := strings.TrimSpace(match[1])
		if strings.Contains(rhs, "+") {
			t.Errorf("innerHTML is assigned a concatenation, which lets Kafka data become markup: innerHTML = %s", rhs)
		}
	}
}

func TestDashboardBuildsCellsWithTextContent(t *testing.T) {
	data, err := dashboardHTML.ReadFile("ui/dashboard.html")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, want := range []string{"td.textContent = text", "span.textContent = action"} {
		if !strings.Contains(source, want) {
			t.Errorf("dashboard no longer contains %q — transaction cells must be built with textContent", want)
		}
	}
}
