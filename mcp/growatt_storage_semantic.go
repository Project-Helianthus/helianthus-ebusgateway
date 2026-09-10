package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
)

const SemanticV1GrowattStorageCurrentGetTool = "semantic.v1.storage.growatt.current.get"

var ErrGrowattStorageSemanticProviderUnavailable = errors.New("growatt BMS storage semantic provider unavailable")

type GrowattStorageSemanticProvider interface {
	GrowattStorageSemanticCurrent(context.Context) (any, error)
}

var growattStorageSemanticProviders = struct {
	sync.RWMutex
	byServer map[*Server]GrowattStorageSemanticProvider
}{byServer: make(map[*Server]GrowattStorageSemanticProvider)}

func registerGrowattStorageSemanticTool(server *Server, provider ModbusV1Provider) {
	p, ok := provider.(GrowattStorageSemanticProvider)
	if !ok || p == nil {
		return
	}
	growattStorageSemanticProviders.Lock()
	growattStorageSemanticProviders.byServer[server] = p
	growattStorageSemanticProviders.Unlock()
	server.tools = append(server.tools, Tool{Name: SemanticV1GrowattStorageCurrentGetTool, Description: "Get the one qualified SemReg Growatt BMS storage projection.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleGrowattStorageSemanticCall(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != SemanticV1GrowattStorageCurrentGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Growatt storage semantic arguments"), false, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	growattStorageSemanticProviders.RLock()
	provider := growattStorageSemanticProviders.byServer[server]
	growattStorageSemanticProviders.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, ErrGrowattStorageSemanticProviderUnavailable, false, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	data, err := provider.GrowattStorageSemanticCurrent(ctx)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	timestamp, err := growattStorageEvaluatedTimestamp(data)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(data, nil, true, "EVALUATED_SEMREG_PUBLICATION", timestamp)), false), true
}

// growattStorageEvaluatedTimestamp derives the public envelope timestamp from
// the authoritative SemReg evaluation, never from handler wall time.
func growattStorageEvaluatedTimestamp(data any) (string, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return "", errors.New("growatt BMS storage semantic data is invalid")
	}
	var projection struct {
		Evaluation struct {
			Context struct {
				EvaluatedAt struct {
					UnixNanoseconds string `json:"unix_nanoseconds"`
				} `json:"evaluated_at"`
			} `json:"context"`
		} `json:"evaluation"`
	}
	if err := json.Unmarshal(encoded, &projection); err != nil {
		return "", errors.New("growatt BMS storage semantic data is invalid")
	}
	nanoseconds, err := strconv.ParseInt(projection.Evaluation.Context.EvaluatedAt.UnixNanoseconds, 10, 64)
	if err != nil {
		return "", errors.New("growatt BMS storage evaluation time is unavailable")
	}
	return time.Unix(0, nanoseconds).UTC().Format(time.RFC3339Nano), nil
}
