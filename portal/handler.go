package portal

import (
	"embed"
	"encoding/json"
	"html/template"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

//go:embed static/*
var assets embed.FS

var staticFS = func() fs.FS {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
	return sub
}()

var indexTemplate = template.Must(template.ParseFS(assets, "static/index.html"))

type Options struct {
	GraphQLPath      string
	SnapshotPath     string
	SubscriptionPath string
	MCPPath          string
	GatewayVersion   string
	BuildID          string
}

type handler struct {
	opts  Options
	files http.Handler
}

func NewHandler(opts Options) http.Handler {
	if opts.GraphQLPath == "" {
		opts.GraphQLPath = "/graphql"
	}
	if opts.SnapshotPath == "" {
		opts.SnapshotPath = "/snapshot"
	}
	if opts.SubscriptionPath == "" {
		opts.SubscriptionPath = "/graphql/subscriptions"
	}
	if opts.MCPPath == "" {
		opts.MCPPath = "/mcp"
	}
	if opts.GatewayVersion == "" {
		opts.GatewayVersion = "dev"
	}
	if opts.BuildID == "" {
		opts.BuildID = "unknown"
	}
	return &handler{
		opts:  opts,
		files: http.FileServer(http.FS(staticFS)),
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "request missing", http.StatusBadRequest)
		return
	}
	if r.URL == nil {
		http.Error(w, "request url missing", http.StatusBadRequest)
		return
	}
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	if strings.HasPrefix(path, "/api/v1/") {
		h.handleAPI(w, r, strings.TrimPrefix(path, "/api/v1/"))
		return
	}
	if path == "/" || strings.EqualFold(path, "/index.html") {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = indexTemplate.Execute(w, map[string]any{
			"GraphQLPath": h.opts.GraphQLPath,
		})
		return
	}
	if strings.HasPrefix(path, "/assets/") {
		w.Header().Set("Cache-Control", "public, max-age=3600")
	}
	h.files.ServeHTTP(w, r)
}

func (h *handler) handleAPI(w http.ResponseWriter, r *http.Request, path string) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	switch strings.Trim(path, "/") {
	case "health":
		writeJSON(w, http.StatusOK, map[string]any{
			"status":          "ok",
			"gateway_version": h.opts.GatewayVersion,
			"build_id":        h.opts.BuildID,
			"time_utc":        time.Now().UTC().Format(time.RFC3339),
		})
	case "bootstrap":
		writeJSON(w, http.StatusOK, map[string]any{
			"capabilities": map[string]bool{
				"registry":   true,
				"semantic":   true,
				"projection": true,
				"stream":     false,
			},
			"endpoints": map[string]string{
				"graphql":       h.opts.GraphQLPath,
				"snapshot":      h.opts.SnapshotPath,
				"subscriptions": h.opts.SubscriptionPath,
				"mcp":           h.opts.MCPPath,
			},
			"limits": map[string]any{
				"max_events_per_second": 200,
				"snapshot_retention":    "disabled_in_m0",
			},
			"ui_version": "m0",
		})
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
