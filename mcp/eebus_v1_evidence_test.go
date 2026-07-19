package mcp

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMSP065CaptureEEBusV1ServicesEvidenceUsesFrozenHashView(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-evidence")}
	payload, observedAt, err := CaptureEEBusV1ServicesEvidence(provider, bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatalf("CaptureEEBusV1ServicesEvidence() error = %v", err)
	}
	if observedAt.IsZero() {
		t.Fatal("CaptureEEBusV1ServicesEvidence() returned a zero observation time")
	}
	var envelope eebusV1EnvelopeV1
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.Meta.Tool != eebusV1ServicesListTool || envelope.Meta.Scope != "services" || envelope.Meta.Mode != "evidence" {
		t.Fatalf("envelope binding = %#v", envelope.Meta)
	}
	view := eebusV1HashView{
		Contract: envelope.Meta.Contract, Tool: envelope.Meta.Tool, Scope: envelope.Meta.Scope,
		MaskTier: envelope.Meta.MaskTier, AuthScope: envelope.Meta.AuthScope, Mode: envelope.Meta.Mode,
		DataTimestamp: envelope.Meta.DataTimestamp, RuntimeState: envelope.Meta.Runtime.State,
		Degradation: envelope.Meta.Runtime.Degradation, Data: envelope.Data, Error: envelope.Error,
	}
	encodedView, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	_, expectedHash, err := eebusV1CanonicalHashJSON(encodedView)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Meta.DataHash != expectedHash {
		t.Fatalf("data_hash = %q, want %q", envelope.Meta.DataHash, expectedHash)
	}
}

func TestMSP065CaptureEEBusV1ServicesEvidenceRejectsInvalidBoundary(t *testing.T) {
	if _, _, err := CaptureEEBusV1ServicesEvidence(nil, make([]byte, 32)); err == nil {
		t.Fatal("nil provider accepted")
	}
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-evidence")}
	if _, _, err := CaptureEEBusV1ServicesEvidence(provider, make([]byte, 31)); err == nil {
		t.Fatal("short pseudonym key accepted")
	}
}
