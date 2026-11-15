package mcp

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const (
	HuaweiSmartLoggerV1StatusGetTool = "modbus_v1_huawei_smartlogger_status_get"
	HuaweiSmartLoggerV1Profile       = "huawei.smartlogger.readonly.v1"
)

// HuaweiSmartLoggerV1Result is the provider-supplied native qualification
// status. Inventory acquisition remains outside this MCP seam.
type HuaweiSmartLoggerV1Result struct {
	Profile         string `json:"profile"`
	Family          string `json:"family"`
	Qualified       bool   `json:"qualified"`
	RawRedacted     bool   `json:"raw_redacted"`
	OutboundAllowed bool   `json:"outbound_allowed"`
}

type HuaweiSmartLoggerV1Provider interface {
	HuaweiSmartLoggerV1(context.Context) (HuaweiSmartLoggerV1Result, error)
}

var huaweiSmartLoggerV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]HuaweiSmartLoggerV1Provider
}{byServer: make(map[*Server]HuaweiSmartLoggerV1Provider)}

func registerHuaweiSmartLoggerV1Tool(server *Server, provider ModbusV1Provider) {
	if registerHuaweiInstanceTool(server, provider, HuaweiSmartLoggerV1StatusGetTool, "huawei.smartlogger") {
		return
	}
	value, ok := provider.(HuaweiSmartLoggerV1Provider)
	if !ok || value == nil {
		return
	}
	huaweiSmartLoggerV1Providers.Lock()
	huaweiSmartLoggerV1Providers.byServer[server] = value
	huaweiSmartLoggerV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: HuaweiSmartLoggerV1StatusGetTool, Description: "Get the native Huawei SmartLogger status.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleHuaweiSmartLoggerV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != HuaweiSmartLoggerV1StatusGetTool {
		return nil, false
	}
	if result, handled := server.handleHuaweiInstanceCall(ctx, name, "huawei.smartlogger", args); handled {
		return result, true
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Huawei SmartLogger status arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	huaweiSmartLoggerV1Providers.RLock()
	provider := huaweiSmartLoggerV1Providers.byServer[server]
	huaweiSmartLoggerV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("huawei SmartLogger provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.HuaweiSmartLoggerV1(ctx)
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}

// HuaweiInstanceStatusProvider reads a configured managed instance only. The
// family and label are configuration, never inferred native device identity.
type HuaweiInstanceStatusProvider interface {
	HuaweiInstanceStatus(context.Context, string, string) (HuaweiInstanceStatus, error)
}

type HuaweiInstanceChildStatus struct {
	Unit     uint8  `json:"unit"`
	Family   string `json:"family"`
	Profile  string `json:"profile"`
	Evidence string `json:"evidence"`
	Reason   string `json:"reason"`
	AssetID  string `json:"asset_id,omitempty"`
	// Refusal names why the gateway publication refused a child the provider
	// admitted (for example binding_capacity). It is absent for published
	// children and for children the provider withheld.
	Refusal string `json:"refusal,omitempty"`
	// IdentityLinkState is the state of the child's identity link in the
	// publishing generation: qualified, candidate or withdrawn. It is absent
	// while that generation holds no link for the child.
	IdentityLinkState string `json:"identity_link_state,omitempty"`
	// BindingIncarnations and SourceDescriptors count the native bindings and
	// the source descriptors of the child's asset, every contributor's
	// included, against HuaweiAssetBindingBound and
	// HuaweiAssetSourceDescriptorBound. Both are absent while the asset does
	// not exist.
	BindingIncarnations int `json:"binding_incarnations,omitempty"`
	SourceDescriptors   int `json:"source_descriptors,omitempty"`
}

// The per-asset bounds of the pinned semantic kernel the status counts are
// measured against. A new binding incarnation beyond the binding bound is
// refused with binding_capacity; a START whose source would exceed the
// source-descriptor bound fails.
const (
	HuaweiAssetBindingBound          = 128
	HuaweiAssetSourceDescriptorBound = 32
)

// Identity-link states the instance status reports for a child.
const (
	HuaweiIdentityLinkQualified = "qualified"
	HuaweiIdentityLinkCandidate = "candidate"
	HuaweiIdentityLinkWithdrawn = "withdrawn"
)

// huaweiInstanceStatusSchemaDescription states the per-child identity members
// of the status and their bounds in the tool schema.
var huaweiInstanceStatusSchemaDescription = fmt.Sprintf("Each child reports identity_link_state (%s, %s or %s) and, for its asset, binding_incarnations against the bound of %d and source_descriptors against the bound of %d.", HuaweiIdentityLinkQualified, HuaweiIdentityLinkCandidate, HuaweiIdentityLinkWithdrawn, HuaweiAssetBindingBound, HuaweiAssetSourceDescriptorBound)

type HuaweiInstanceStatus struct {
	Instance          string                      `json:"instance"`
	Family            string                      `json:"family"`
	Disposition       string                      `json:"disposition"`
	Qualified         bool                        `json:"qualified"`
	LiveQualified     bool                        `json:"live_qualified"`
	OutboundAllowed   bool                        `json:"outbound_allowed"`
	Children          []HuaweiInstanceChildStatus `json:"children"`
	PublishedAssetIDs []string                    `json:"published_asset_ids"`
	Loss              []string                    `json:"loss"`
	// RetainedWithdrawals lists retained observations of an older generation
	// that this generation withdrew explicitly to stay inside the per-asset
	// retained reservation. It is absent until such a withdrawal happens.
	RetainedWithdrawals []HuaweiRetainedWithdrawalStatus `json:"retained_withdrawals,omitempty"`
}

// HuaweiRetainedWithdrawalStatus records one explicit withdrawal of an older
// generation's retained observations from one published asset.
type HuaweiRetainedWithdrawalStatus struct {
	AssetID       string `json:"asset_id"`
	SourceEpochID string `json:"source_epoch_id"`
	Generation    string `json:"generation"`
	Count         int    `json:"count"`
	Reason        string `json:"reason"`
}

var huaweiInstanceProviders = struct {
	sync.RWMutex
	byServer map[*Server]HuaweiInstanceStatusProvider
}{byServer: map[*Server]HuaweiInstanceStatusProvider{}}

func registerHuaweiInstanceTool(server *Server, provider ModbusV1Provider, name, family string) bool {
	value, ok := provider.(HuaweiInstanceStatusProvider)
	if !ok || value == nil {
		return false
	}
	if availability, ok := provider.(interface{ ModbusV1OptionalToolAvailable(string) bool }); ok && !availability.ModbusV1OptionalToolAvailable(name) {
		return true
	}
	huaweiInstanceProviders.Lock()
	huaweiInstanceProviders.byServer[server] = value
	huaweiInstanceProviders.Unlock()
	server.tools = append(server.tools, Tool{Name: name, Description: "Get the managed Huawei " + family + " instance status.", InputSchema: map[string]any{"type": "object", "description": huaweiInstanceStatusSchemaDescription, "properties": map[string]any{"instance": map[string]any{"type": "string", "minLength": 1}}, "required": []string{"instance"}, "additionalProperties": false}})
	return true
}
func (server *Server) handleHuaweiInstanceCall(ctx context.Context, name, family string, args map[string]any) (map[string]any, bool) {
	huaweiInstanceProviders.RLock()
	provider := huaweiInstanceProviders.byServer[server]
	huaweiInstanceProviders.RUnlock()
	if provider == nil {
		return nil, false
	}
	instance, ok := args["instance"].(string)
	if !ok || instance == "" || len(args) != 1 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Huawei instance arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.HuaweiInstanceStatus(ctx, family, instance)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "RETAINED_PROFILE", "")), true), true
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}
