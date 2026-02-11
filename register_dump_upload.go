package ebusgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const registerDumpUploadMaxBytes = 10 << 20

var registerDumpUploadTimeout = 20 * time.Second

type registerDumpUploadResponse struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func NewRegisterDumpUploadHandler(outputDir string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body := http.MaxBytesReader(w, r.Body, registerDumpUploadMaxBytes)
		defer body.Close()

		var payload registerDumpJSON
		if err := json.NewDecoder(body).Decode(&payload); err != nil {
			http.Error(w, "invalid json payload", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(payload.Metadata.Timestamp) == "" || strings.TrimSpace(payload.Metadata.Target) == "" {
			http.Error(w, "missing metadata", http.StatusBadRequest)
			return
		}
		if payload.Metadata.EntryCount == 0 {
			payload.Metadata.EntryCount = len(payload.Entries)
		}
		if payload.Metadata.EntryCount != len(payload.Entries) {
			http.Error(w, "entry count mismatch", http.StatusBadRequest)
			return
		}

		dir := filepath.Join(outputDir, "register_dumps")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			http.Error(w, "cannot create output directory", http.StatusInternalServerError)
			return
		}

		filename := registerDumpFilename(payload.Metadata)
		path := filepath.Join(dir, filename)
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			http.Error(w, "cannot encode payload", http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			http.Error(w, "cannot write output file", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(registerDumpUploadResponse{
			Path: path,
			Size: int64(len(data)),
		})
	})
}

func uploadRegisterDumpJSON(ctx context.Context, uploadURL string, jsonPath string) error {
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: registerDumpUploadTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("register dump upload status %s", resp.Status)
	}
	return nil
}

func registerDumpFilename(meta registerDumpJSONMetadata) string {
	timestamp := strings.TrimSpace(meta.Timestamp)
	if timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
			timestamp = parsed.UTC().Format("20060102T150405.000000000Z")
		}
	}
	timestamp = strings.ReplaceAll(timestamp, ":", "")
	timestamp = strings.ReplaceAll(timestamp, "/", "")
	target := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(meta.Target)), "0x")
	if target == "" {
		target = "unknown"
	}
	if timestamp == "" {
		timestamp = "unknown"
	}
	return fmt.Sprintf("register_dump_%s_%s.json", target, timestamp)
}
