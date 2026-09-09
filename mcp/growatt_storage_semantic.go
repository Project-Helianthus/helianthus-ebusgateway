package mcp

import (
	"context"
	"errors"
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
	return callToolResultText(mustJSON(newModbusV1Envelope(data, nil, true, "EVALUATED_SEMREG_PUBLICATION", time.Now().UTC().Format(time.RFC3339Nano))), false), true
}
