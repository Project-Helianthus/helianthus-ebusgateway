package portal

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
)

// ConfigCallbacks are host-owned persistence callbacks. They do not apply runtime changes.
type ConfigCallbacks struct {
	Read   func(context.Context) (portalconfig.Record, error)
	Update func(context.Context, portalconfig.Record) (portalconfig.Record, error)
}

type configSelection struct {
	Desired   bool   `json:"desired"`
	Effective bool   `json:"effective"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
}

func configView(r portalconfig.Record) map[string]any {
	selection := func(v bool) configSelection {
		return configSelection{Desired: v, Status: "withheld", Reason: "evidence_unavailable"}
	}
	drivers := make([]any, 0, len(r.Drivers))
	for _, d := range r.Drivers {
		drivers = append(drivers, struct {
			ID string `json:"driver_id"`
			configSelection
		}{d.ID, selection(d.Enabled)})
	}
	return map[string]any{"desired": r, "state": map[string]any{"drivers": drivers, "sharing": map[string]any{"graphql": selection(r.Sharing.GraphQL), "mcp": selection(r.Sharing.MCP)}, "adapter_proxy": selection(r.AdapterProxy), "output_bindings": map[string]any{"matter": selection(r.OutputBindings.Matter)}}}
}

func configError(w http.ResponseWriter, err error, historical ...portalconfig.Record) {
	safe := &portalconfig.Error{Code: "persistence_failed", Field: "record", NextStep: "check local storage"}
	var typed *portalconfig.Error
	if errors.As(err, &typed) {
		steps := map[string]string{"invalid_record": "repair structure", "unsupported_format": "use supported format", "unsupported_selection": "choose a provisioned option", "revision_conflict": "reload", "revision_exhausted": "use a future supported format migration"}
		if step, ok := steps[typed.Code]; ok {
			safe = &portalconfig.Error{Code: typed.Code, Field: "record", NextStep: step}
		}
	}
	status := http.StatusBadRequest
	switch safe.Code {
	case "persistence_failed":
		status = http.StatusServiceUnavailable
	case "revision_conflict", "revision_exhausted":
		status = http.StatusConflict
	}
	unavailable := map[string]any{"desired": nil, "effective": false, "status": "error", "reason": safe.Code}
	view := map[string]any{"desired": nil, "error": safe, "state": map[string]any{
		"drivers":         []any{},
		"sharing":         map[string]any{"graphql": unavailable, "mcp": unavailable},
		"adapter_proxy":   unavailable,
		"output_bindings": map[string]any{"matter": unavailable},
	}}
	if len(historical) == 1 && historical[0].Format == portalconfig.Format {
		view = configView(historical[0])
		view["historical"] = true
		view["error"] = safe
	}
	writeJSON(w, status, view)
}

func (h *handler) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	c := h.opts.Config
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		w.Header().Set("Allow", "GET, PUT")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if c == nil || c.Read == nil || (r.Method == http.MethodPut && c.Update == nil) {
		configError(w, errors.New("unavailable"))
		return
	}
	var record portalconfig.Record
	var err error
	if r.Method == http.MethodGet {
		record, err = c.Read(r.Context())
	} else {
		data, e := io.ReadAll(io.LimitReader(r.Body, portalconfig.MaxBytes+1))
		if e != nil {
			err = &portalconfig.Error{Code: "invalid_record"}
		} else {
			record, err = portalconfig.Decode(data)
		}
		if err != nil {
			// Read can retain accepted history even when storage is unavailable.
			record, _ = c.Read(r.Context())
		} else {
			record, err = c.Update(r.Context(), record)
		}
	}
	if err != nil {
		configError(w, err, record)
		return
	}
	writeJSON(w, http.StatusOK, configView(record))
}
