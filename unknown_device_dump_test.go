package ebusgateway

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type testDumpEntry struct {
	info   registry.DeviceInfo
	planes []registry.Plane
}

func (e testDumpEntry) Address() byte        { return e.info.Address }
func (e testDumpEntry) Addresses() []byte    { return []byte{e.info.Address} }
func (e testDumpEntry) Manufacturer() string { return e.info.Manufacturer }
func (e testDumpEntry) DeviceID() string     { return e.info.DeviceID }
func (e testDumpEntry) SerialNumber() string { return e.info.SerialNumber }
func (e testDumpEntry) MacAddress() string   { return e.info.MacAddress }
func (e testDumpEntry) SoftwareVersion() string {
	return e.info.SoftwareVersion
}
func (e testDumpEntry) HardwareVersion() string {
	return e.info.HardwareVersion
}
func (e testDumpEntry) Planes() []registry.Plane           { return e.planes }
func (e testDumpEntry) Projections() []registry.Projection { return nil }

type stubBus struct {
	identifyPayload []byte
}

func (s stubBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	switch {
	case frame.Primary == 0x07 && frame.Secondary == 0x04:
		return &protocol.Frame{
			Source:    frame.Target,
			Target:    frame.Source,
			Primary:   frame.Primary,
			Secondary: frame.Secondary,
			Data:      s.identifyPayload,
		}, nil
	case frame.Primary == 0xB5 && frame.Secondary == 0x09:
		resp := []byte{0x0D}
		resp = append(resp, frame.Data[1:]...)
		resp = append(resp, 0x01)
		return &protocol.Frame{
			Source:    frame.Target,
			Target:    frame.Source,
			Primary:   frame.Primary,
			Secondary: frame.Secondary,
			Data:      resp,
		}, nil
	case frame.Primary == 0xB5 && frame.Secondary == 0x24:
		resp := []byte{frame.Data[0], 0x00, frame.Data[2], frame.Data[3], frame.Data[4], frame.Data[5]}
		return &protocol.Frame{
			Source:    frame.Target,
			Target:    frame.Source,
			Primary:   frame.Primary,
			Secondary: frame.Secondary,
			Data:      resp,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected frame %02x/%02x", frame.Primary, frame.Secondary)
	}
}

type countDumpBus struct {
	sendCount int
}

func (bus *countDumpBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.sendCount++
	return &protocol.Frame{
		Source:    frame.Target,
		Target:    frame.Source,
		Primary:   frame.Primary,
		Secondary: frame.Secondary,
		Data:      []byte{frame.Data[0], 0x00, frame.Data[2], frame.Data[3], frame.Data[4], frame.Data[5]},
	}, nil
}

func TestUnknownDeviceDump_BundleAndRedaction(t *testing.T) {
	payload := []byte{0xB5, 'A', 'B', 'C', 'D', ' ', 0x01, 0x02, 0x76, 0x03}
	bus := stubBus{identifyPayload: payload}

	unknown := testDumpEntry{
		info: registry.DeviceInfo{
			Address:         0x30,
			Manufacturer:    "Vaillant",
			DeviceID:        "ABCD",
			SerialNumber:    "SERIAL",
			MacAddress:      "00:11:22:33:44:55",
			SoftwareVersion: "0102",
			HardwareVersion: "7603",
		},
	}
	known := testDumpEntry{
		info: registry.DeviceInfo{Address: 0x10, Manufacturer: "Vaillant"},
		planes: []registry.Plane{
			dumpPlane{name: "system"},
		},
	}

	tempDir := t.TempDir()
	now := time.Date(2026, 2, 9, 12, 0, 0, 0, time.UTC)
	results, err := DumpUnknownDevices(context.Background(), bus, []registry.DeviceEntry{unknown, known}, UnknownDeviceDumpOptions{
		OutputDir:      tempDir,
		IncludePII:     false,
		IncludeTraffic: true,
		SourceAddress:  0x10,
		Now:            func() time.Time { return now },
		B509Addresses:  []uint16{0x2800},
		B524Requests: []ExtRegisterRequest{
			{Opcode: 0x02, Group: 0x00, Instance: 0x00, Addr: 0x0000},
		},
	})
	if err != nil {
		t.Fatalf("DumpUnknownDevices error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d; want 1", len(results))
	}

	result := results[0]
	if result.BundlePath == "" || result.ManifestPath == "" {
		t.Fatalf("result missing paths: %+v", result)
	}
	if _, err := os.Stat(result.BundlePath); err != nil {
		t.Fatalf("bundle missing: %v", err)
	}
	if _, err := os.Stat(result.ManifestPath); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}

	manifestData, err := os.ReadFile(result.ManifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest dumpManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if manifest.Device.DeviceID != "" {
		t.Fatalf("DeviceID not redacted")
	}
	if manifest.Device.DeviceIDHash == "" {
		t.Fatalf("DeviceIDHash missing")
	}
	if manifest.Privacy.IncludePII {
		t.Fatalf("IncludePII true; want false")
	}
	if manifest.Captures.B509Reads != 1 || manifest.Captures.B524Reads != 1 {
		t.Fatalf("capture counts = %+v", manifest.Captures)
	}
	if manifest.Captures.TrafficFrames == 0 {
		t.Fatalf("traffic frames missing")
	}

	zipReader, err := zip.OpenReader(result.BundlePath)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer func() { _ = zipReader.Close() }()
	wantFiles := map[string]bool{
		"manifest.json":       false,
		"identify.json":       false,
		"b509_registers.json": false,
		"b524_registers.json": false,
		"traffic.json":        false,
	}
	for _, file := range zipReader.File {
		if _, ok := wantFiles[file.Name]; ok {
			wantFiles[file.Name] = true
		}
	}
	for name, seen := range wantFiles {
		if !seen {
			t.Fatalf("zip missing %s", name)
		}
	}
}

func TestUnknownDeviceDump_Upload(t *testing.T) {
	payload := []byte{0xB5, 'A', 'B', 'C', 'D', ' ', 0x01, 0x02, 0x76, 0x03}
	bus := stubBus{identifyPayload: payload}

	unknown := testDumpEntry{
		info: registry.DeviceInfo{
			Address:      0x30,
			Manufacturer: "Vaillant",
		},
	}

	var gotParts = make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Fatalf("parse media type: %v", err)
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("read part: %v", err)
			}
			data, _ := io.ReadAll(part)
			if len(data) == 0 {
				t.Fatalf("part %s empty", part.FormName())
			}
			gotParts[part.FormName()] = true
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	tempDir := t.TempDir()
	results, err := DumpUnknownDevices(context.Background(), bus, []registry.DeviceEntry{unknown}, UnknownDeviceDumpOptions{
		OutputDir:     tempDir,
		UploadURL:     server.URL,
		SourceAddress: 0x10,
		B509Addresses: []uint16{0x2800},
		B524Requests: []ExtRegisterRequest{
			{Opcode: 0x02, Group: 0x00, Instance: 0x00, Addr: 0x0000},
		},
	})
	if err != nil {
		t.Fatalf("DumpUnknownDevices error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d; want 1", len(results))
	}
	if !results[0].Uploaded || results[0].UploadHTTPCode != http.StatusCreated {
		t.Fatalf("upload result = %+v", results[0])
	}
	for _, name := range []string{"manifest", "metadata", "bundle"} {
		if !gotParts[name] {
			t.Fatalf("missing multipart part %s", name)
		}
	}
}

func TestDefaultB524Requests_ExcludeConstraintOpcode(t *testing.T) {
	reqs := defaultB524Requests()
	if len(reqs) == 0 {
		t.Fatalf("defaultB524Requests returned no requests")
	}
	for _, req := range reqs {
		if req.Opcode == 0x01 {
			t.Fatalf("defaultB524Requests contains opcode 0x01: %+v", req)
		}
	}
}

func TestRunB524Dump_SkipsConstraintOpcode(t *testing.T) {
	bus := &countDumpBus{}
	now := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	opts := UnknownDeviceDumpOptions{
		Now: func() time.Time { return now },
		B524Requests: []ExtRegisterRequest{
			{Opcode: 0x01, Group: 0x03, Instance: 0x01, Addr: 0x0400},
			{Opcode: 0x02, Group: 0x03, Instance: 0x01, Addr: 0x0400},
		},
	}

	dump := runB524Dump(context.Background(), bus, 0x15, 0x10, opts, func(trafficFrame) {})

	if bus.sendCount != 1 {
		t.Fatalf("bus send count = %d; want 1", bus.sendCount)
	}
	if len(dump.Requests) != 2 {
		t.Fatalf("dump requests len = %d; want 2", len(dump.Requests))
	}
	if dump.Requests[0].Error == "" {
		t.Fatalf("first request error empty; want static-only opcode 0x01 error")
	}
	if dump.Requests[0].Request.Data != "010003010004" {
		t.Fatalf("first request data = %q; want 010003010004", dump.Requests[0].Request.Data)
	}
	if dump.Requests[1].Error != "" {
		t.Fatalf("second request error = %q; want empty", dump.Requests[1].Error)
	}
}

type dumpPlane struct {
	name string
}

func (p dumpPlane) Name() string               { return p.name }
func (p dumpPlane) Methods() []registry.Method { return nil }
