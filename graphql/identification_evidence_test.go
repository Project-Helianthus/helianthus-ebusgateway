package graphql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type testIdentificationEvidenceProvider struct {
	snapshot IdentificationEvidenceSnapshot
}

func (p testIdentificationEvidenceProvider) IdentificationEvidence() IdentificationEvidenceSnapshot {
	return p.snapshot
}

func TestBusIdentificationEvidenceQueryReturnsRawBytesOnly(t *testing.T) {
	at := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	builder := NewBuilder(mockRegistry{}, nil)
	builder.SetIdentificationEvidenceProvider(testIdentificationEvidenceProvider{snapshot: IdentificationEvidenceSnapshot{
		SessionID: "observer-x",
		Exchanges: []IdentificationEvidenceExchange{
			{Address: 0x08, Sequence: 1, SessionID: "observer-x", CapturedAt: at, Request: []byte{0x71, 0x08, 0x07, 0x04, 0x00}, Response: []byte{0x01, 0xB5}},
			{Address: 0x15, Sequence: 2, SessionID: "observer-x", CapturedAt: at.Add(time.Second), Request: []byte{0x71, 0x15, 0x07, 0x04, 0x00}, Response: []byte{0x01, 0xB6}},
		},
	}})
	handler, err := NewHandler(builder)
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}
	query := `{"query":"{ bus_identification_evidence(limit: 1) { evidence_class decoded qualification session_id count items { address sequence session_id captured_at request_hex response_hex } } }"}`
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(query))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Data struct {
			Evidence struct {
				EvidenceClass string `json:"evidence_class"`
				Decoded       bool   `json:"decoded"`
				Qualification string `json:"qualification"`
				SessionID     string `json:"session_id"`
				Count         int    `json:"count"`
				Items         []struct {
					Address     int    `json:"address"`
					Sequence    int    `json:"sequence"`
					SessionID   string `json:"session_id"`
					CapturedAt  string `json:"captured_at"`
					RequestHex  string `json:"request_hex"`
					ResponseHex string `json:"response_hex"`
				} `json:"items"`
			} `json:"bus_identification_evidence"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Errors) > 0 {
		t.Fatalf("response err=%v errors=%v body=%s", err, out.Errors, rec.Body.String())
	}
	e := out.Data.Evidence
	if e.EvidenceClass != "raw" || e.Decoded || e.Qualification != "none" || e.SessionID != "observer-x" || e.Count != 2 {
		t.Fatalf("envelope = %+v", e)
	}
	if len(e.Items) != 1 || e.Items[0].Address != 0x15 || e.Items[0].RequestHex != "7115070400" || e.Items[0].ResponseHex != "01b6" || e.Items[0].CapturedAt != "2026-10-14T08:00:01Z" {
		t.Fatalf("items = %+v", e.Items)
	}
}

func TestBusIdentificationEvidenceQueryWithoutProviderIsEmpty(t *testing.T) {
	handler, err := NewHandler(NewBuilder(mockRegistry{}, nil))
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ bus_identification_evidence { count items { address } } }"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var out struct {
		Data struct {
			Evidence struct {
				Count int   `json:"count"`
				Items []any `json:"items"`
			} `json:"bus_identification_evidence"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Errors) > 0 || out.Data.Evidence.Count != 0 || len(out.Data.Evidence.Items) != 0 {
		t.Fatalf("err=%v body=%s", err, rec.Body.String())
	}
}
