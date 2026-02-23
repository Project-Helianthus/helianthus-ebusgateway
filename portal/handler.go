package portal

import (
	"embed"
	"encoding/json"
	"expvar"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"path"
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

type assetHashEntry struct {
	SHA256 string `json:"sha256"`
}

type assetManifest struct {
	Assets map[string]assetHashEntry `json:"assets"`
}

var (
	portalRequestCount      = expvar.NewMap("portal_requests_total")
	portalRouteDurationMS   = expvar.NewMap("portal_route_duration_ms_total")
	portalAssetETagByTarget = loadAssetETags()
)

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
	route := classifyRoute(path)
	startedAt := time.Now()
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		duration := time.Since(startedAt).Milliseconds()
		portalRequestCount.Add(fmt.Sprintf("%s|%s|%d", r.Method, route, recorder.status), 1)
		portalRouteDurationMS.Add(route, duration)
		log.Printf("portal request method=%s path=%q route=%s status=%d duration_ms=%d", r.Method, path, route, recorder.status, duration)
	}()

	if strings.HasPrefix(path, "/api/v1/") {
		h.handleAPI(recorder, r, strings.TrimPrefix(path, "/api/v1/"))
		return
	}
	if path == "/" || strings.EqualFold(path, "/index.html") {
		recorder.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = indexTemplate.Execute(recorder, map[string]any{
			"GraphQLPath": h.opts.GraphQLPath,
		})
		return
	}
	if strings.HasPrefix(path, "/assets/") {
		recorder.Header().Set("Cache-Control", "public, max-age=3600")
		if applyAssetETag(recorder, r, path) {
			return
		}
	}
	h.files.ServeHTTP(recorder, r)
}

func (h *handler) handleAPI(w http.ResponseWriter, r *http.Request, path string) {
	w.Header().Set("Cache-Control", "no-store")
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

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Write(payload []byte) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return rec.ResponseWriter.Write(payload)
}

func classifyRoute(path string) string {
	switch {
	case strings.HasPrefix(path, "/api/v1/health"):
		return "api.health"
	case strings.HasPrefix(path, "/api/v1/bootstrap"):
		return "api.bootstrap"
	case strings.HasPrefix(path, "/assets/"):
		return "assets"
	case path == "/" || strings.EqualFold(path, "/index.html"):
		return "index"
	default:
		return "static"
	}
}

func loadAssetETags() map[string]string {
	raw, err := assets.ReadFile("static/assets/manifest.json")
	if err != nil {
		return nil
	}
	var manifest assetManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil
	}
	etags := make(map[string]string, len(manifest.Assets))
	for name, entry := range manifest.Assets {
		hash := strings.TrimSpace(entry.SHA256)
		if hash == "" {
			continue
		}
		etags[name] = fmt.Sprintf("\"sha256-%s\"", hash)
	}
	return etags
}

func applyAssetETag(w http.ResponseWriter, r *http.Request, assetPath string) bool {
	if len(portalAssetETagByTarget) == 0 || r == nil {
		return false
	}
	name := path.Base(assetPath)
	etag, ok := portalAssetETagByTarget[name]
	if !ok {
		return false
	}
	w.Header().Set("ETag", etag)
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == etag {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}
