package eebusadmin

import "net/http"

// NewUnavailableHandler keeps the authenticated admin namespace closed and
// deterministic when its credentials or private runtime capability are not
// available. It intentionally performs no authentication parsing.
func NewUnavailableHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"contract":"helianthus.eebus.operator-admin.v1","error":{"code":"admin_boundary_unavailable"}}` + "\n"))
	})
}
