package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	global := flag.NewFlagSet("fraudctl", flag.ContinueOnError)
	baseURL := global.String("url", "http://localhost:8080", "fraud service base URL")
	if err := global.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := global.Args()
	if len(args) == 0 {
		return fmt.Errorf("usage: fraudctl [--url URL] <list|show|stats>")
	}
	path := ""
	switch args[0] {
	case "list":
		listFlags := flag.NewFlagSet("list", flag.ContinueOnError)
		limit := listFlags.Int("limit", 20, "maximum records")
		action := listFlags.String("action", "", "no_action, review, or escalate")
		cursor := listFlags.String("cursor", "", "pagination cursor")
		if err := listFlags.Parse(args[1:]); err != nil {
			return err
		}
		values := url.Values{"limit": []string{fmt.Sprint(*limit)}}
		if *action != "" {
			values.Set("action", *action)
		}
		if *cursor != "" {
			values.Set("cursor", *cursor)
		}
		path = "/v1/transactions?" + values.Encode()
	case "show":
		if len(args) != 2 {
			return fmt.Errorf("usage: fraudctl show <transaction-id>")
		}
		path = "/v1/transactions/" + url.PathEscape(args[1])
	case "stats":
		statsFlags := flag.NewFlagSet("stats", flag.ContinueOnError)
		version := statsFlags.String("model-version", "", "optional model version")
		if err := statsFlags.Parse(args[1:]); err != nil {
			return err
		}
		path = "/v1/model-metrics"
		if *version != "" {
			path += "?model_version=" + url.QueryEscape(*version)
		}
	default:
		return fmt.Errorf("unknown command %q; use list, show, or stats", args[0])
	}

	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Get(strings.TrimRight(*baseURL, "/") + path)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode >= 400 {
		return fmt.Errorf("service returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	pretty, _ := json.MarshalIndent(value, "", "  ")
	fmt.Println(string(pretty))
	return nil
}
