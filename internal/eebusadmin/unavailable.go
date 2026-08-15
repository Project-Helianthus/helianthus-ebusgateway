package eebusadmin

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
)

// NewUnavailableHandler keeps the operator namespace deterministic while the
// private runtime capability is unavailable.
func NewUnavailableHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		requestID, err := randomToken(rand.Reader)
		if err != nil {
			requestID = "request-unavailable"
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ownerEnvelope{
			Contract: ContractV1, RequestID: requestID, StateRevision: 0,
			Error: &errorData{Code: "admin_boundary_unavailable"},
		})
	})
}
