package ebusgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisterDumpUploadHandler_WritesFile(t *testing.T) {
	dir := t.TempDir()
	handler := NewRegisterDumpUploadHandler(dir)
	payload := registerDumpJSON{
		Metadata: registerDumpJSONMetadata{
			Timestamp:  "2026-02-11T05:30:00Z",
			Target:     "0x08",
			TSPSource:  "test.tsp",
			EntryCount: 1,
		},
		Entries: []registerDumpJSONEntry{
			{
				Method:   "get_register",
				Group:    "0x00",
				Instance: "0x00",
				Address:  "0x0001",
				Raw:      "0102",
				Decoded:  "ok",
			},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/dump/upload", bytes.NewReader(data))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusCreated)
	}

	var resp registerDumpUploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Path == "" {
		t.Fatalf("response path empty")
	}
	if _, err := os.Stat(resp.Path); err != nil {
		t.Fatalf("stat uploaded file: %v", err)
	}
}

func TestUploadRegisterDumpJSON_PostsPayload(t *testing.T) {
	dir := t.TempDir()
	handler := NewRegisterDumpUploadHandler(dir)
	server := httptest.NewServer(handler)
	defer server.Close()

	path := filepath.Join(dir, "dump.json")
	payload := registerDumpJSON{
		Metadata: registerDumpJSONMetadata{
			Timestamp:  "2026-02-11T05:30:00Z",
			Target:     "0x10",
			TSPSource:  "test.tsp",
			EntryCount: 0,
		},
		Entries: []registerDumpJSONEntry{},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write temp json: %v", err)
	}

	if err := uploadRegisterDumpJSON(context.Background(), server.URL, path); err != nil {
		t.Fatalf("uploadRegisterDumpJSON error: %v", err)
	}
}

func TestUploadRegisterDumpJSON_TimesOut(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.json")
	if err := os.WriteFile(path, []byte(`{"metadata":{},"entries":[]}`), 0o644); err != nil {
		t.Fatalf("write temp json: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	originalTimeout := registerDumpUploadTimeout
	registerDumpUploadTimeout = 50 * time.Millisecond
	defer func() { registerDumpUploadTimeout = originalTimeout }()

	if err := uploadRegisterDumpJSON(context.Background(), server.URL, path); err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}
