package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
)

const (
	TeslaWCVitalsV1GetTool                = "modbus.v1.tesla.wcapi.get_vitals.replay.get"
	TeslaWCVitalsV1Operation              = "tesla.hsc.fc100.wc_vitals.v1"
	TeslaWCVitalsV1ProfileVersion         = "tesla_hsc_modbus_v1"
	TeslaWCVitalsV1OperationVersion       = "wc3_24_44_3"
	TeslaWCVitalsV1QualificationQualified = "qualified_read_only"
	TeslaWCVitalsV1ReplayIntermediate     = "intermediate"
	TeslaWCVitalsV1ReplayTerminal         = "terminal"
	maxTeslaWCVitalsV1SnapshotBytes       = 251
)

// TeslaWCVitalsV1Result is an already-selected, bounded replay result. It
// deliberately carries no raw PDU or decoded vitals values.
type TeslaWCVitalsV1Result struct {
	Operation        string `json:"operation"`
	ProfileVersion   string `json:"profile_version"`
	OperationVersion string `json:"operation_version"`
	Qualification    string `json:"qualification"`
	ReplayKind       string `json:"replay_kind"`
	SnapshotLength   int    `json:"snapshot_length"`
	SnapshotDigest   string `json:"snapshot_digest,omitempty"`
	OutboundAllowed  bool   `json:"outbound_allowed"`
}

// TeslaWCVitalsV1Provider supplies an already-qualified replay. It has no
// transport, request-construction, or serial-configuration method.
type TeslaWCVitalsV1Provider interface {
	TeslaWCVitalsV1(context.Context) (TeslaWCVitalsV1Result, error)
}

var teslaWCVitalsV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]TeslaWCVitalsV1Provider
}{byServer: make(map[*Server]TeslaWCVitalsV1Provider)}

func registerTeslaWCVitalsV1Tool(server *Server, provider ModbusV1Provider) {
	tesla, ok := provider.(TeslaWCVitalsV1Provider)
	if !ok || tesla == nil {
		return
	}
	teslaWCVitalsV1Providers.Lock()
	teslaWCVitalsV1Providers.byServer[server] = tesla
	teslaWCVitalsV1Providers.Unlock()
	server.tools = append(server.tools, Tool{
		Name:        TeslaWCVitalsV1GetTool,
		Description: "Get the read-only redacted replay for the qualified Tesla WC vitals operation.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false},
	})
}

func (server *Server) handleTeslaWCVitalsV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != TeslaWCVitalsV1GetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid tesla WC vitals arguments"), false, "RETAINED_QUALIFIED_OPERATION_REPLAY", "")), true), true
	}
	teslaWCVitalsV1Providers.RLock()
	provider := teslaWCVitalsV1Providers.byServer[server]
	teslaWCVitalsV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("tesla WC vitals provider unavailable"), false, "RETAINED_QUALIFIED_OPERATION_REPLAY", "")), true), true
	}
	result, err := provider.TeslaWCVitalsV1(ctx)
	if err != nil || !validTeslaWCVitalsV1Result(result) {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("tesla WC vitals replay unavailable"), true, "RETAINED_QUALIFIED_OPERATION_REPLAY", "")), true), true
	}
	result.OutboundAllowed = false
	return callToolResultText(mustJSON(newModbusV1Envelope(result, nil, true, "RETAINED_QUALIFIED_OPERATION_REPLAY", "")), false), true
}

func validTeslaWCVitalsV1Result(result TeslaWCVitalsV1Result) bool {
	if result.Operation != TeslaWCVitalsV1Operation ||
		result.ProfileVersion != TeslaWCVitalsV1ProfileVersion ||
		result.OperationVersion != TeslaWCVitalsV1OperationVersion ||
		result.Qualification != TeslaWCVitalsV1QualificationQualified ||
		result.SnapshotLength < 0 || result.SnapshotLength > maxTeslaWCVitalsV1SnapshotBytes {
		return false
	}
	switch result.ReplayKind {
	case TeslaWCVitalsV1ReplayIntermediate:
		return result.SnapshotLength == 0 && result.SnapshotDigest == ""
	case TeslaWCVitalsV1ReplayTerminal:
		digest, err := hex.DecodeString(result.SnapshotDigest)
		return err == nil && len(digest) == sha256.Size
	default:
		return false
	}
}
