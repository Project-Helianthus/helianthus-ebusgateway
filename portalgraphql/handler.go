// Package portalgraphql implements the one closed Portal catalog endpoint.
package portalgraphql

import (
	"encoding/json"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"net/http"
	"time"
)

const Path = "/graphql/portal/v1"

type CallerResolver func(*http.Request) (any, error)
type Handler struct {
	Catalog *catalogv1.Composer
	Caller  CallerResolver
}
type request struct {
	OperationName string          `json:"operationName"`
	Variables     json.RawMessage `json:"variables"`
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.Catalog == nil {
		http.Error(w, "portal catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	var q request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if dec.Decode(&q) != nil {
		http.Error(w, "invalid portal graphql request", http.StatusBadRequest)
		return
	}
	var caller any
	if h.Caller != nil {
		var err error
		caller, err = h.Caller(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	switch q.OperationName {
	case "PortalCatalogV1":
		c, err := h.Catalog.Catalog(caller)
		if err != nil {
			writeError(w, err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"portalCatalogV1": c}})
	case "PortalActionInvokeV1":
		var v struct {
			Claim catalogv1.Claims `json:"claim"`
		}
		if json.Unmarshal(q.Variables, &v) != nil {
			http.Error(w, "invalid action variables", http.StatusBadRequest)
			return
		}
		if v.Claim.Deadline.IsZero() {
			v.Claim.Deadline = time.Time{}
		}
		result, err := h.Catalog.Invoke(caller, v.Claim)
		if err != nil {
			writeError(w, err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"portalActionInvokeV1": result}})
	default:
		http.Error(w, "unknown portal graphql operation", http.StatusBadRequest)
	}
}
func writeError(w http.ResponseWriter, err error) {
	_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"message": err.Error()}}})
}
