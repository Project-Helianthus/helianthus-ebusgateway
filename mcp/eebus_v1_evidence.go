package mcp

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"
)

// CaptureEEBusV1ServicesEvidence produces the frozen, read-only services-list
// envelope without registering a tool or exposing a mutation capability.
func CaptureEEBusV1ServicesEvidence(provider EEBusV1Provider, pseudonymKey []byte) (json.RawMessage, time.Time, error) {
	if eebusV1NilProvider(provider) || len(pseudonymKey) != sha256.Size {
		return nil, time.Time{}, errors.New("invalid eeBUS evidence source")
	}
	runtime := &eebusV1Runtime{
		provider:     provider,
		pseudonymKey: append([]byte(nil), pseudonymKey...),
	}
	projection, code := runtime.liveProjection(eebusV1PublicBoundary)
	if code != "" {
		return nil, time.Time{}, errors.New("eeBUS evidence source unavailable")
	}
	data, code := eebusV1DataForTool(eebusV1ServicesListTool, projection, "")
	if code != "" {
		return nil, time.Time{}, errors.New("eeBUS evidence contract violation")
	}
	spec := eebusV1ToolSpec{name: eebusV1ServicesListTool, scope: "services"}
	envelope := runtime.envelopeAt(spec, "evidence", projection.DataTimestamp, projection.Runtime, eebusV1PublicBoundary, data, nil)
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, time.Time{}, errors.New("marshal eeBUS evidence envelope")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, projection.DataTimestamp)
	if err != nil {
		return nil, time.Time{}, errors.New("invalid eeBUS evidence timestamp")
	}
	return encoded, observedAt, nil
}
