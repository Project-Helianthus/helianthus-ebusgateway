package graphql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
)

type ProjectionSnapshot struct {
	Address int              `json:"address"`
	Plane   string           `json:"plane"`
	Nodes   []ProjectionNode `json:"nodes"`
	Edges   []ProjectionEdge `json:"edges"`
}

func NewProjectionSnapshotHandler(builder *Builder) (http.Handler, error) {
	if builder == nil {
		return nil, fmt.Errorf("snapshot handler missing builder: %w", ebuserrors.ErrInvalidPayload)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			http.Error(w, "request missing", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL == nil {
			http.Error(w, "request url missing", http.StatusBadRequest)
			return
		}

		address, err := parseSnapshotAddress(r.URL.Query().Get("address"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		plane, err := parseSnapshotPlane(r.URL.Query().Get("plane"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		snapshot := builder.Schema()
		device, ok := findDevice(snapshot.Devices, address)
		if !ok {
			http.Error(w, "device not found", http.StatusNotFound)
			return
		}
		projection, ok := findProjection(device.Projections, plane)
		if !ok {
			http.Error(w, "projection not found", http.StatusNotFound)
			return
		}

		payload := ProjectionSnapshot{
			Address: int(device.Address),
			Plane:   projection.Plane,
			Nodes:   projection.Nodes,
			Edges:   projection.Edges,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			http.Error(w, "encode failed", http.StatusInternalServerError)
			return
		}
	}), nil
}

func parseSnapshotAddress(raw string) (byte, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("snapshot address missing: %w", ebuserrors.ErrInvalidPayload)
	}
	parsed, err := strconv.ParseInt(value, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("snapshot address invalid: %w", ebuserrors.ErrInvalidPayload)
	}
	if parsed < 0 || parsed > 0xFF {
		return 0, fmt.Errorf("snapshot address out of range: %w", ebuserrors.ErrInvalidPayload)
	}
	return byte(parsed), nil
}

func parseSnapshotPlane(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("snapshot plane missing: %w", ebuserrors.ErrInvalidPayload)
	}
	return value, nil
}

func findProjection(projections []Projection, plane string) (Projection, bool) {
	for _, projection := range projections {
		if projection.Plane == plane {
			return projection, true
		}
	}
	return Projection{}, false
}
