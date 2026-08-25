package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
)

const (
	TeslaFC100SummaryV1GetTool                        = "modbus.v1.tesla.fc100.summary.get"
	TeslaFC100SummaryV1QualificationFramingOnly       = "framing_only"
	TeslaFC100SummaryV1QualificationQualifiedReadOnly = "qualified_read_only"
	maxTeslaFC100SummaryV1Entries                     = 64
	maxTeslaFC100SummaryV1Message                     = 251
)

// TeslaFC100SummaryV1WireEntry is one redacted numeric protobuf wire key.
// It contains no encoded value, field name, or operation identity.
type TeslaFC100SummaryV1WireEntry struct {
	FieldNumber uint64 `json:"field_number"`
	WireType    uint8  `json:"wire_type"`
}

// TeslaFC100SummaryV1Result is an already-validated FC100 structural replay
// summary. It contains no raw bytes, values, request, or send authority.
type TeslaFC100SummaryV1Result struct {
	Qualification   string                         `json:"qualification"`
	EnvelopeLength  int                            `json:"envelope_length"`
	MessageLength   int                            `json:"message_length"`
	EntryCount      int                            `json:"entry_count"`
	Entries         []TeslaFC100SummaryV1WireEntry `json:"entries"`
	PayloadDigest   string                         `json:"payload_digest"`
	OutboundAllowed bool                           `json:"outbound_allowed"`
}

// TeslaFC100SummaryV1Provider supplies only an already-validated FC100
// structural summary. It has no request-construction or transport-I/O method.
type TeslaFC100SummaryV1Provider interface {
	TeslaFC100SummaryV1(context.Context) (TeslaFC100SummaryV1Result, error)
}

var teslaFC100SummaryV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]TeslaFC100SummaryV1Provider
}{byServer: make(map[*Server]TeslaFC100SummaryV1Provider)}

func registerTeslaFC100SummaryV1Tool(server *Server, provider ModbusV1Provider) {
	tesla, ok := provider.(TeslaFC100SummaryV1Provider)
	if !ok || tesla == nil {
		return
	}
	teslaFC100SummaryV1Providers.Lock()
	teslaFC100SummaryV1Providers.byServer[server] = tesla
	teslaFC100SummaryV1Providers.Unlock()
	server.tools = append(server.tools, Tool{
		Name:        TeslaFC100SummaryV1GetTool,
		Description: "Get one read-only redacted Tesla FC100 structural wire summary.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false},
	})
}

func (server *Server) handleTeslaFC100SummaryV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != TeslaFC100SummaryV1GetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Tesla FC100 summary arguments"), false, "RETAINED_STRUCTURAL_PROVENANCE", "")), true), true
	}
	teslaFC100SummaryV1Providers.RLock()
	provider := teslaFC100SummaryV1Providers.byServer[server]
	teslaFC100SummaryV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("tesla FC100 summary provider unavailable"), false, "RETAINED_STRUCTURAL_PROVENANCE", "")), true), true
	}
	result, err := provider.TeslaFC100SummaryV1(ctx)
	if err != nil || !validTeslaFC100SummaryV1Result(result) {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("tesla FC100 summary unavailable"), true, "RETAINED_STRUCTURAL_PROVENANCE", "")), true), true
	}
	result = failClosedTeslaFC100SummaryV1Result(result)
	return callToolResultText(mustJSON(newModbusV1Envelope(result, nil, true, "RETAINED_STRUCTURAL_PROVENANCE", "")), false), true
}

func validTeslaFC100SummaryV1Result(result TeslaFC100SummaryV1Result) bool {
	if (result.Qualification != TeslaFC100SummaryV1QualificationFramingOnly &&
		result.Qualification != TeslaFC100SummaryV1QualificationQualifiedReadOnly) ||
		result.MessageLength < 1 || result.MessageLength > maxTeslaFC100SummaryV1Message ||
		result.EnvelopeLength != result.MessageLength+1 ||
		result.EntryCount < 1 || result.EntryCount > maxTeslaFC100SummaryV1Entries ||
		len(result.Entries) != result.EntryCount {
		return false
	}
	digest, err := hex.DecodeString(result.PayloadDigest)
	if err != nil || len(digest) != sha256.Size {
		return false
	}
	groups := make([]uint64, 0, result.EntryCount)
	for _, entry := range result.Entries {
		if entry.FieldNumber == 0 || entry.FieldNumber > (1<<29)-1 || entry.WireType > 5 {
			return false
		}
		switch entry.WireType {
		case 3:
			groups = append(groups, entry.FieldNumber)
		case 4:
			if len(groups) == 0 || groups[len(groups)-1] != entry.FieldNumber {
				return false
			}
			groups = groups[:len(groups)-1]
		}
	}
	return len(groups) == 0
}

func failClosedTeslaFC100SummaryV1Result(result TeslaFC100SummaryV1Result) TeslaFC100SummaryV1Result {
	result.Entries = append([]TeslaFC100SummaryV1WireEntry(nil), result.Entries...)
	result.OutboundAllowed = false
	return result
}
