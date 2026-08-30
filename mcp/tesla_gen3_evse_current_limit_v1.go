package mcp

import (
	"context"
	"errors"
	"sync"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const TeslaGen3EVSECurrentLimitV1GetTool = "modbus.v1.tesla.gen3.evse.current_limit.get"

var (
	ErrTeslaGen3EVSECurrentLimitV1ProviderUnavailable = errors.New("tesla Gen3 EVSE current-limit provider unavailable")
	ErrTeslaGen3EVSECurrentLimitV1Invalid             = errors.New("tesla Gen3 EVSE current-limit record is invalid")
)

// TeslaGen3EVSECurrentLimitV1Source supplies already-injected registry records.
// It has no transport, acquisition, or request-construction capability.
type TeslaGen3EVSECurrentLimitV1Source struct {
	Persistent  *modbusreg.TeslaGen3PersistentCurrentLimit
	Provisional *modbusreg.TeslaGen3ProvisionalCurrentLimit
}

// TeslaGen3EVSECurrentLimitV1Provider supplies the version-qualified records
// for a read-only MCP projection.
type TeslaGen3EVSECurrentLimitV1Provider interface {
	TeslaGen3EVSECurrentLimitV1(context.Context) (TeslaGen3EVSECurrentLimitV1Source, error)
}

type TeslaGen3EVSECurrentLimitV1Persistent struct {
	MaxOutputCurrentAmps uint32 `json:"max_output_current_amps"`
	RequestPayload       []byte `json:"request_payload"`
	TerminalPayload      []byte `json:"terminal_payload"`
}

type TeslaGen3EVSECurrentLimitV1Provisional struct {
	LimitCurrentMaxAmps     uint32 `json:"limit_current_max_amps"`
	LimitTimeoutSeconds     uint32 `json:"limit_timeout_s"`
	InhibitCharging         bool   `json:"inhibit_charging"`
	SetRequestPayload       []byte `json:"set_request_payload"`
	AckPayload              []byte `json:"ack_payload"`
	ReadbackRequestPayload  []byte `json:"readback_request_payload"`
	ReadbackTerminalPayload []byte `json:"readback_terminal_payload"`
}

type TeslaGen3EVSECurrentLimitV1Result struct {
	OperationVersion string                                  `json:"operation_version"`
	Persistent       *TeslaGen3EVSECurrentLimitV1Persistent  `json:"persistent,omitempty"`
	Provisional      *TeslaGen3EVSECurrentLimitV1Provisional `json:"provisional,omitempty"`
	OutboundAllowed  bool                                    `json:"outbound_allowed"`
}

var teslaGen3EVSECurrentLimitV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]TeslaGen3EVSECurrentLimitV1Provider
}{byServer: make(map[*Server]TeslaGen3EVSECurrentLimitV1Provider)}

func registerTeslaGen3EVSECurrentLimitV1Tool(server *Server, provider ModbusV1Provider) {
	currentLimit, ok := provider.(TeslaGen3EVSECurrentLimitV1Provider)
	if !ok || currentLimit == nil {
		return
	}
	teslaGen3EVSECurrentLimitV1Providers.Lock()
	teslaGen3EVSECurrentLimitV1Providers.byServer[server] = currentLimit
	teslaGen3EVSECurrentLimitV1Providers.Unlock()
	server.tools = append(server.tools, Tool{
		Name:        TeslaGen3EVSECurrentLimitV1GetTool,
		Description: "Get injected read-only Tesla Gen3 EVSE current-limit records.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false},
	})
}

func (server *Server) handleTeslaGen3EVSECurrentLimitV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != TeslaGen3EVSECurrentLimitV1GetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Tesla Gen3 EVSE current-limit arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	teslaGen3EVSECurrentLimitV1Providers.RLock()
	provider := teslaGen3EVSECurrentLimitV1Providers.byServer[server]
	teslaGen3EVSECurrentLimitV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, ErrTeslaGen3EVSECurrentLimitV1ProviderUnavailable, false, "RETAINED_PROFILE", "")), true), true
	}
	source, err := provider.TeslaGen3EVSECurrentLimitV1(ctx)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "RETAINED_PROFILE", "")), true), true
	}
	result, err := teslaGen3EVSECurrentLimitV1Result(source)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "RETAINED_PROFILE", "")), true), true
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(result, nil, true, "RETAINED_PROFILE", "")), false), true
}

func teslaGen3EVSECurrentLimitV1Result(source TeslaGen3EVSECurrentLimitV1Source) (TeslaGen3EVSECurrentLimitV1Result, error) {
	if source.Persistent == nil && source.Provisional == nil {
		return TeslaGen3EVSECurrentLimitV1Result{}, ErrTeslaGen3EVSECurrentLimitV1Invalid
	}
	result := TeslaGen3EVSECurrentLimitV1Result{
		OperationVersion: modbusreg.TeslaGen3CurrentLimitOperationVersion24443,
		OutboundAllowed:  false,
	}
	if source.Persistent != nil {
		if source.Persistent.OperationVersion() != result.OperationVersion {
			return TeslaGen3EVSECurrentLimitV1Result{}, ErrTeslaGen3EVSECurrentLimitV1Invalid
		}
		request, terminal := source.Persistent.RequestPayload(), source.Persistent.TerminalPayload()
		if len(request) == 0 || len(terminal) == 0 {
			return TeslaGen3EVSECurrentLimitV1Result{}, ErrTeslaGen3EVSECurrentLimitV1Invalid
		}
		result.Persistent = &TeslaGen3EVSECurrentLimitV1Persistent{
			MaxOutputCurrentAmps: source.Persistent.MaxOutputCurrentAmps(),
			RequestPayload:       request,
			TerminalPayload:      terminal,
		}
	}
	if source.Provisional != nil {
		if source.Provisional.OperationVersion() != result.OperationVersion {
			return TeslaGen3EVSECurrentLimitV1Result{}, ErrTeslaGen3EVSECurrentLimitV1Invalid
		}
		setRequest := source.Provisional.SetRequestPayload()
		ack := source.Provisional.AckPayload()
		readbackRequest := source.Provisional.ReadbackRequestPayload()
		readbackTerminal := source.Provisional.ReadbackTerminalPayload()
		if len(setRequest) == 0 || len(ack) == 0 || len(readbackRequest) == 0 || len(readbackTerminal) == 0 {
			return TeslaGen3EVSECurrentLimitV1Result{}, ErrTeslaGen3EVSECurrentLimitV1Invalid
		}
		result.Provisional = &TeslaGen3EVSECurrentLimitV1Provisional{
			LimitCurrentMaxAmps:     source.Provisional.LimitCurrentMaxAmps(),
			LimitTimeoutSeconds:     source.Provisional.LimitTimeoutSeconds(),
			InhibitCharging:         source.Provisional.InhibitCharging(),
			SetRequestPayload:       setRequest,
			AckPayload:              ack,
			ReadbackRequestPayload:  readbackRequest,
			ReadbackTerminalPayload: readbackTerminal,
		}
	}
	return result, nil
}
