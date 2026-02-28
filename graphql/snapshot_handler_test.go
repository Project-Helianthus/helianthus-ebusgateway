package graphql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestProjectionSnapshotHandler_Success(t *testing.T) {
	info := registry.DeviceInfo{
		Address:         0x10,
		Manufacturer:    "vaillant",
		DeviceID:        "device-a",
		SoftwareVersion: "1.0",
		HardwareVersion: "7603",
	}
	projection, ok := mockProjection(info)
	if !ok {
		t.Fatalf("mockProjection failed")
	}
	reg := mockRegistry{
		entries: []registry.DeviceEntry{
			mockEntry{
				info:        info,
				projections: []registry.Projection{projection},
			},
		},
	}

	builder := NewBuilder(reg, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start error = %v", err)
	}

	handler, err := NewProjectionSnapshotHandler(builder)
	if err != nil {
		t.Fatalf("NewProjectionSnapshotHandler error = %v", err)
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/?address=0x10&plane=Observability")
	if err != nil {
		t.Fatalf("GET error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want %d", resp.StatusCode, http.StatusOK)
	}

	var payload ProjectionSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error = %v", err)
	}

	if payload.Address != 0x10 {
		t.Fatalf("address = %d; want %d", payload.Address, 0x10)
	}
	if payload.Plane != "Observability" {
		t.Fatalf("plane = %s; want Observability", payload.Plane)
	}
	if len(payload.Nodes) != len(projection.Nodes) {
		t.Fatalf("nodes = %d; want %d", len(payload.Nodes), len(projection.Nodes))
	}
	if len(payload.Edges) != len(projection.Edges) {
		t.Fatalf("edges = %d; want %d", len(payload.Edges), len(projection.Edges))
	}
}

func TestProjectionSnapshotHandler_Errors(t *testing.T) {
	info := registry.DeviceInfo{
		Address:         0x10,
		Manufacturer:    "vaillant",
		DeviceID:        "device-a",
		SoftwareVersion: "1.0",
		HardwareVersion: "7603",
	}
	projection, ok := mockProjection(info)
	if !ok {
		t.Fatalf("mockProjection failed")
	}
	reg := mockRegistry{
		entries: []registry.DeviceEntry{
			mockEntry{
				info:        info,
				projections: []registry.Projection{projection},
			},
		},
	}
	builder := NewBuilder(reg, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start error = %v", err)
	}
	handler, err := NewProjectionSnapshotHandler(builder)
	if err != nil {
		t.Fatalf("NewProjectionSnapshotHandler error = %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	cases := []struct {
		name       string
		path       string
		statusCode int
	}{
		{"missing_address", "/?plane=Observability", http.StatusBadRequest},
		{"missing_plane", "/?address=0x10", http.StatusBadRequest},
		{"invalid_address", "/?address=0xZZ&plane=Observability", http.StatusBadRequest},
		{"device_not_found", "/?address=0x11&plane=Observability", http.StatusNotFound},
		{"projection_not_found", "/?address=0x10&plane=Debug", http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(server.URL + tc.path)
			if err != nil {
				t.Fatalf("GET error = %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.statusCode {
				t.Fatalf("status = %d; want %d", resp.StatusCode, tc.statusCode)
			}
		})
	}
}
