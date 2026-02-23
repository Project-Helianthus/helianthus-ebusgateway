package portal

import (
	"cmp"
	"embed"
	"encoding/json"
	"expvar"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"path"
	"slices"
	"strconv"
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
	portalStreamEventsTotal = expvar.NewMap("portal_stream_events_total")
	portalStreamDropped     = expvar.NewMap("portal_stream_dropped_total")
)

type Options struct {
	GraphQLPath      string
	SnapshotPath     string
	SubscriptionPath string
	MCPPath          string
	GatewayVersion   string
	BuildID          string
	ListRegistry     func() []RegistryDevice
	ListSemantic     func() SemanticSnapshot
	ListProjections  func() []ProjectionDevice
	GetProjection    func(address byte, plane string) (ProjectionGraph, bool)
}

type handler struct {
	opts  Options
	files http.Handler
}

type RegistryDevice struct {
	Address      byte            `json:"address"`
	Addresses    []byte          `json:"addresses,omitempty"`
	Manufacturer string          `json:"manufacturer"`
	DeviceID     string          `json:"device_id"`
	SerialNumber string          `json:"serial_number,omitempty"`
	Software     string          `json:"software_version"`
	Hardware     string          `json:"hardware_version"`
	Planes       []RegistryPlane `json:"planes,omitempty"`
}

type RegistryPlane struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods,omitempty"`
}

type SemanticSnapshot struct {
	Zones       []SemanticZone        `json:"zones"`
	DHW         *SemanticDHW          `json:"dhw,omitempty"`
	Energy      *SemanticEnergyTotals `json:"energy_totals,omitempty"`
	CapturedUTC string                `json:"captured_utc"`
}

type SemanticZone struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	OperatingMode string   `json:"operating_mode,omitempty"`
	Preset        string   `json:"preset,omitempty"`
	CurrentTempC  *float64 `json:"current_temp_c,omitempty"`
	TargetTempC   *float64 `json:"target_temp_c,omitempty"`
	HeatingDemand *float64 `json:"heating_demand,omitempty"`
}

type SemanticDHW struct {
	OperatingMode string   `json:"operating_mode,omitempty"`
	Preset        string   `json:"preset,omitempty"`
	CurrentTempC  *float64 `json:"current_temp_c,omitempty"`
	TargetTempC   *float64 `json:"target_temp_c,omitempty"`
	HeatingDemand *float64 `json:"heating_demand,omitempty"`
}

type SemanticEnergyTotals struct {
	Gas      SemanticEnergyChannel `json:"gas"`
	Electric SemanticEnergyChannel `json:"electric"`
	Solar    SemanticEnergyChannel `json:"solar"`
}

type SemanticEnergyChannel struct {
	DHW     SemanticEnergySeries `json:"dhw"`
	Climate SemanticEnergySeries `json:"climate"`
}

type SemanticEnergySeries struct {
	Today  float64   `json:"today"`
	Yearly []float64 `json:"yearly,omitempty"`
}

type ProjectionDevice struct {
	Address      byte                `json:"address"`
	DeviceID     string              `json:"device_id,omitempty"`
	DisplayName  string              `json:"display_name,omitempty"`
	Manufacturer string              `json:"manufacturer,omitempty"`
	Projections  []ProjectionSummary `json:"projections"`
}

type ProjectionSummary struct {
	Plane     string `json:"plane"`
	NodeCount int    `json:"node_count"`
	EdgeCount int    `json:"edge_count"`
}

type ProjectionGraph struct {
	Address byte             `json:"address"`
	Plane   string           `json:"plane"`
	Nodes   []ProjectionNode `json:"nodes"`
	Edges   []ProjectionEdge `json:"edges"`
}

type ProjectionNode struct {
	ID            string `json:"id"`
	Path          string `json:"path"`
	CanonicalPath string `json:"canonical_path"`
}

type ProjectionEdge struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

type SearchResult struct {
	Layer    string `json:"layer"`
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Title    string `json:"title"`
	Subtitle string `json:"subtitle,omitempty"`
	Address  *byte  `json:"address,omitempty"`
}

type StreamEventEnvelope struct {
	At            string         `json:"at"`
	Type          string         `json:"type"`
	Layer         string         `json:"layer"`
	CorrelationID string         `json:"correlation_id"`
	Payload       map[string]any `json:"payload"`
	Provenance    StreamSource   `json:"provenance"`
}

type StreamSource struct {
	Source   string `json:"source"`
	Dropped  int    `json:"dropped"`
	Interval int    `json:"interval_ms"`
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
		streamEnabled := h.opts.ListRegistry != nil || h.opts.ListSemantic != nil || h.opts.ListProjections != nil
		writeJSON(w, http.StatusOK, map[string]any{
			"capabilities": map[string]bool{
				"registry":   h.opts.ListRegistry != nil,
				"semantic":   h.opts.ListSemantic != nil,
				"projection": h.opts.ListProjections != nil && h.opts.GetProjection != nil,
				"search":     h.opts.ListRegistry != nil || h.opts.ListSemantic != nil || h.opts.ListProjections != nil,
				"stream":     streamEnabled,
			},
			"endpoints": map[string]string{
				"graphql":       h.opts.GraphQLPath,
				"snapshot":      h.opts.SnapshotPath,
				"subscriptions": h.opts.SubscriptionPath,
				"mcp":           h.opts.MCPPath,
				"search":        "/portal/api/v1/search",
				"stream":        "/portal/api/v1/stream",
			},
			"limits": map[string]any{
				"max_events_per_second": 200,
				"snapshot_retention":    "disabled_in_m0",
			},
			"ui_version": "m0",
		})
	case "registry/devices":
		h.handleRegistryDevices(w, r)
	case "semantic/snapshot":
		h.handleSemanticSnapshot(w)
	case "projection/devices":
		h.handleProjectionDevices(w, r)
	case "projection/graph":
		h.handleProjectionGraph(w, r)
	case "search":
		h.handleSearch(w, r)
	case "stream":
		h.handleStream(w, r)
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

func (rec *statusRecorder) Flush() {
	if flusher, ok := rec.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func classifyRoute(path string) string {
	switch {
	case strings.HasPrefix(path, "/api/v1/health"):
		return "api.health"
	case strings.HasPrefix(path, "/api/v1/bootstrap"):
		return "api.bootstrap"
	case strings.HasPrefix(path, "/api/v1/registry/devices"):
		return "api.registry.devices"
	case strings.HasPrefix(path, "/api/v1/semantic/snapshot"):
		return "api.semantic.snapshot"
	case strings.HasPrefix(path, "/api/v1/projection/devices"):
		return "api.projection.devices"
	case strings.HasPrefix(path, "/api/v1/projection/graph"):
		return "api.projection.graph"
	case strings.HasPrefix(path, "/api/v1/search"):
		return "api.search"
	case strings.HasPrefix(path, "/api/v1/stream"):
		return "api.stream"
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

func (h *handler) handleRegistryDevices(w http.ResponseWriter, r *http.Request) {
	if h.opts.ListRegistry == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []RegistryDevice{},
		})
		return
	}

	devices := h.opts.ListRegistry()
	needle := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 200)
	if limit < 0 {
		limit = 0
	}

	filtered := make([]RegistryDevice, 0, len(devices))
	for _, device := range devices {
		if needle != "" && !matchesDeviceFilter(device, needle) {
			continue
		}
		filtered = append(filtered, device)
	}

	slices.SortFunc(filtered, func(a, b RegistryDevice) int {
		return cmp.Compare(int(a.Address), int(b.Address))
	})

	total := len(filtered)
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"count": total,
		"items": filtered,
	})
}

func parseQueryLimit(raw string, fallback int) int {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	switch {
	case value <= 0:
		return 0
	case value > 1000:
		return 1000
	default:
		return value
	}
}

func matchesDeviceFilter(device RegistryDevice, needle string) bool {
	if needle == "" {
		return true
	}
	candidate := strings.ToLower(strings.Join([]string{
		device.Manufacturer,
		device.DeviceID,
		device.SerialNumber,
		device.Software,
		device.Hardware,
	}, " "))
	if strings.Contains(candidate, needle) {
		return true
	}
	for _, plane := range device.Planes {
		if strings.Contains(strings.ToLower(plane.Name), needle) {
			return true
		}
		for _, method := range plane.Methods {
			if strings.Contains(strings.ToLower(method), needle) {
				return true
			}
		}
	}
	return false
}

func (h *handler) handleSemanticSnapshot(w http.ResponseWriter) {
	if h.opts.ListSemantic == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"zones":        []SemanticZone{},
			"captured_utc": time.Now().UTC().Format(time.RFC3339),
		})
		return
	}
	snapshot := h.opts.ListSemantic()
	if strings.TrimSpace(snapshot.CapturedUTC) == "" {
		snapshot.CapturedUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if snapshot.Zones == nil {
		snapshot.Zones = []SemanticZone{}
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *handler) handleProjectionDevices(w http.ResponseWriter, r *http.Request) {
	if h.opts.ListProjections == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"count": 0,
			"items": []ProjectionDevice{},
		})
		return
	}
	items := h.opts.ListProjections()
	needle := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 200)

	filtered := make([]ProjectionDevice, 0, len(items))
	for _, item := range items {
		if needle != "" && !matchesProjectionFilter(item, needle) {
			continue
		}
		filtered = append(filtered, item)
	}
	slices.SortFunc(filtered, func(a, b ProjectionDevice) int {
		return cmp.Compare(int(a.Address), int(b.Address))
	})

	total := len(filtered)
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count": total,
		"items": filtered,
	})
}

func (h *handler) handleProjectionGraph(w http.ResponseWriter, r *http.Request) {
	if h.opts.GetProjection == nil {
		http.NotFound(w, r)
		return
	}
	address, err := parseQueryAddress(r.URL.Query().Get("address"))
	if err != nil {
		http.Error(w, "invalid address", http.StatusBadRequest)
		return
	}
	plane := strings.TrimSpace(r.URL.Query().Get("plane"))
	if plane == "" {
		http.Error(w, "missing plane", http.StatusBadRequest)
		return
	}
	graph, ok := h.opts.GetProjection(address, plane)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

func parseQueryAddress(raw string) (byte, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(raw), 0, 8)
	if err != nil {
		return 0, err
	}
	return byte(parsed), nil
}

func matchesProjectionFilter(item ProjectionDevice, needle string) bool {
	text := strings.ToLower(strings.Join([]string{
		item.DeviceID,
		item.DisplayName,
		item.Manufacturer,
	}, " "))
	if strings.Contains(text, needle) {
		return true
	}
	for _, projection := range item.Projections {
		if strings.Contains(strings.ToLower(projection.Plane), needle) {
			return true
		}
	}
	return false
}

func (h *handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	needle := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	limit := parseQueryLimit(r.URL.Query().Get("limit"), 25)
	if needle == "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"query": needle,
			"count": 0,
			"items": []SearchResult{},
		})
		return
	}

	results := make([]SearchResult, 0, limit)
	appendResult := func(item SearchResult) {
		if limit > 0 && len(results) >= limit {
			return
		}
		results = append(results, item)
	}

	if h.opts.ListRegistry != nil {
		devices := h.opts.ListRegistry()
		slices.SortFunc(devices, func(a, b RegistryDevice) int {
			return cmp.Compare(int(a.Address), int(b.Address))
		})
		for _, device := range devices {
			label := strings.TrimSpace(strings.Join([]string{device.Manufacturer, device.DeviceID}, " "))
			if strings.Contains(strings.ToLower(strings.Join([]string{
				device.Manufacturer,
				device.DeviceID,
				device.SerialNumber,
				device.Software,
				device.Hardware,
			}, " ")), needle) {
				address := device.Address
				appendResult(SearchResult{
					Layer:    "registry",
					Kind:     "device",
					ID:       fmt.Sprintf("reg:%02x", device.Address),
					Title:    strings.TrimSpace(label),
					Subtitle: fmt.Sprintf("addr=0x%02x", device.Address),
					Address:  &address,
				})
			}
			for _, plane := range device.Planes {
				if strings.Contains(strings.ToLower(plane.Name), needle) {
					address := device.Address
					appendResult(SearchResult{
						Layer:    "registry",
						Kind:     "plane",
						ID:       fmt.Sprintf("reg:%02x:%s", device.Address, strings.ToLower(plane.Name)),
						Title:    fmt.Sprintf("%s plane", plane.Name),
						Subtitle: fmt.Sprintf("addr=0x%02x", device.Address),
						Address:  &address,
					})
				}
				for _, method := range plane.Methods {
					if strings.Contains(strings.ToLower(method), needle) {
						address := device.Address
						appendResult(SearchResult{
							Layer:    "registry",
							Kind:     "method",
							ID:       fmt.Sprintf("reg:%02x:%s:%s", device.Address, strings.ToLower(plane.Name), strings.ToLower(method)),
							Title:    method,
							Subtitle: fmt.Sprintf("%s plane addr=0x%02x", plane.Name, device.Address),
							Address:  &address,
						})
					}
				}
			}
		}
	}

	if h.opts.ListSemantic != nil {
		snapshot := h.opts.ListSemantic()
		for _, zone := range snapshot.Zones {
			if strings.Contains(strings.ToLower(strings.Join([]string{
				zone.ID,
				zone.Name,
				zone.OperatingMode,
				zone.Preset,
			}, " ")), needle) {
				appendResult(SearchResult{
					Layer:    "semantic",
					Kind:     "zone",
					ID:       zone.ID,
					Title:    zone.Name,
					Subtitle: strings.TrimSpace(strings.Join([]string{zone.OperatingMode, zone.Preset}, " / ")),
				})
			}
		}
		if snapshot.DHW != nil && strings.Contains(strings.ToLower(strings.Join([]string{
			"dhw",
			snapshot.DHW.OperatingMode,
			snapshot.DHW.Preset,
		}, " ")), needle) {
			appendResult(SearchResult{
				Layer:    "semantic",
				Kind:     "dhw",
				ID:       "dhw",
				Title:    "Domestic Hot Water",
				Subtitle: strings.TrimSpace(strings.Join([]string{snapshot.DHW.OperatingMode, snapshot.DHW.Preset}, " / ")),
			})
		}
	}

	if h.opts.ListProjections != nil {
		items := h.opts.ListProjections()
		slices.SortFunc(items, func(a, b ProjectionDevice) int {
			return cmp.Compare(int(a.Address), int(b.Address))
		})
		for _, item := range items {
			if strings.Contains(strings.ToLower(strings.Join([]string{
				item.DeviceID,
				item.DisplayName,
				item.Manufacturer,
			}, " ")), needle) {
				address := item.Address
				title := strings.TrimSpace(item.DisplayName)
				if title == "" {
					title = strings.TrimSpace(item.DeviceID)
				}
				if title == "" {
					title = fmt.Sprintf("Projection Device 0x%02x", item.Address)
				}
				appendResult(SearchResult{
					Layer:    "projection",
					Kind:     "device",
					ID:       fmt.Sprintf("proj:%02x", item.Address),
					Title:    title,
					Subtitle: fmt.Sprintf("addr=0x%02x", item.Address),
					Address:  &address,
				})
			}
			for _, projection := range item.Projections {
				if strings.Contains(strings.ToLower(projection.Plane), needle) {
					address := item.Address
					appendResult(SearchResult{
						Layer:    "projection",
						Kind:     "plane",
						ID:       fmt.Sprintf("proj:%02x:%s", item.Address, strings.ToLower(projection.Plane)),
						Title:    projection.Plane,
						Subtitle: fmt.Sprintf("addr=0x%02x nodes=%d edges=%d", item.Address, projection.NodeCount, projection.EdgeCount),
						Address:  &address,
					})
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"query": needle,
		"count": len(results),
		"items": results,
	})
}

func (h *handler) handleStream(w http.ResponseWriter, r *http.Request) {
	if h.opts.ListRegistry == nil && h.opts.ListSemantic == nil && h.opts.ListProjections == nil {
		http.Error(w, "stream unavailable", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	intervalMS := parseIntBounded(r.URL.Query().Get("interval_ms"), 1000, 200, 5000)
	maxEventsPerSecond := parseIntBounded(r.URL.Query().Get("max_events_per_second"), 3, 1, 30)
	maxEvents := parseIntBounded(r.URL.Query().Get("max_events"), 0, 0, 200)

	selectedLayers := parseLayerSelection(r.URL.Query().Get("layers"))

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")

	produceTicker := time.NewTicker(time.Duration(intervalMS) * time.Millisecond)
	defer produceTicker.Stop()
	flushTicker := time.NewTicker(time.Second / time.Duration(maxEventsPerSecond))
	defer flushTicker.Stop()
	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()

	var (
		pending    *StreamEventEnvelope
		dropped    int
		sentEvents int
	)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-produceTicker.C:
			for _, event := range h.snapshotStreamEvents(selectedLayers, intervalMS) {
				if pending != nil {
					dropped++
				}
				event.Provenance.Dropped = dropped
				pending = &event
			}
		case <-flushTicker.C:
			if pending == nil {
				continue
			}
			payload, err := json.Marshal(pending)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "event: update\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
			portalStreamEventsTotal.Add(pending.Layer, 1)
			if dropped > 0 {
				portalStreamDropped.Add(pending.Layer, int64(dropped))
			}
			dropped = 0
			pending = nil
			sentEvents++
			if maxEvents > 0 && sentEvents >= maxEvents {
				return
			}
		case <-heartbeatTicker.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func parseIntBounded(raw string, fallback, min, max int) int {
	value := fallback
	if strings.TrimSpace(raw) != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil {
			value = parsed
		}
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func parseLayerSelection(raw string) map[string]bool {
	layers := map[string]bool{
		"registry":   true,
		"semantic":   true,
		"projection": true,
	}
	if strings.TrimSpace(raw) == "" {
		return layers
	}
	selected := map[string]bool{
		"registry":   false,
		"semantic":   false,
		"projection": false,
	}
	for _, token := range strings.Split(strings.ToLower(raw), ",") {
		key := strings.TrimSpace(token)
		if _, ok := selected[key]; ok {
			selected[key] = true
		}
	}
	return selected
}

func (h *handler) snapshotStreamEvents(layers map[string]bool, intervalMS int) []StreamEventEnvelope {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	events := make([]StreamEventEnvelope, 0, 3)

	if layers["registry"] && h.opts.ListRegistry != nil {
		devices := h.opts.ListRegistry()
		events = append(events, StreamEventEnvelope{
			At:            now,
			Type:          "snapshot",
			Layer:         "registry",
			CorrelationID: fmt.Sprintf("reg-%d", time.Now().UnixNano()),
			Payload: map[string]any{
				"device_count": len(devices),
			},
			Provenance: StreamSource{
				Source:   "poll:registry",
				Interval: intervalMS,
			},
		})
	}

	if layers["semantic"] && h.opts.ListSemantic != nil {
		snapshot := h.opts.ListSemantic()
		events = append(events, StreamEventEnvelope{
			At:            now,
			Type:          "snapshot",
			Layer:         "semantic",
			CorrelationID: fmt.Sprintf("sem-%d", time.Now().UnixNano()),
			Payload: map[string]any{
				"zones_count":   len(snapshot.Zones),
				"has_dhw":       snapshot.DHW != nil,
				"captured_utc":  snapshot.CapturedUTC,
				"has_energy":    snapshot.Energy != nil,
				"energy_series": snapshot.Energy != nil,
			},
			Provenance: StreamSource{
				Source:   "poll:semantic",
				Interval: intervalMS,
			},
		})
	}

	if layers["projection"] && h.opts.ListProjections != nil {
		items := h.opts.ListProjections()
		projectionCount := 0
		for _, item := range items {
			projectionCount += len(item.Projections)
		}
		events = append(events, StreamEventEnvelope{
			At:            now,
			Type:          "snapshot",
			Layer:         "projection",
			CorrelationID: fmt.Sprintf("proj-%d", time.Now().UnixNano()),
			Payload: map[string]any{
				"device_count":     len(items),
				"projection_count": projectionCount,
			},
			Provenance: StreamSource{
				Source:   "poll:projection",
				Interval: intervalMS,
			},
		})
	}

	return events
}
