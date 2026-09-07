package mcp

import (
	"context"
	"errors"
	"sync"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const (
	GrowattProtocolIIV1IdentityGetTool = "modbus.v1.growatt.protocol_ii.identity.get"
	GrowattProtocolIIV1Profile         = "growatt.protocol_ii.tl3_x.identity.readonly.v1"
)

type GrowattProtocolIIV1NativeIdentity struct {
	Family          string                                   `json:"family"`
	UnitID          byte                                     `json:"unit_id"`
	Firmware        string                                   `json:"firmware"`
	Serial          string                                   `json:"serial"`
	DeviceType      uint16                                   `json:"device_type"`
	ModelBuild      [2]uint16                                `json:"model_build"`
	ProtocolVersion uint16                                   `json:"protocol_version"`
	Slices          []GrowattProtocolIIV1NativeIdentitySlice `json:"slices"`
}

type GrowattProtocolIIV1NativeIdentitySlice struct {
	Offset uint16   `json:"offset"`
	Words  []uint16 `json:"words"`
}

// GrowattProtocolIIV1Result projects the validated native identity supplied
// by the caller-selected Protocol II runtime.
type GrowattProtocolIIV1Result struct {
	Profile           string                            `json:"profile"`
	Disposition       string                            `json:"disposition"`
	Family            string                            `json:"family"`
	IdentityQualified bool                              `json:"identity_qualified"`
	NativeIdentity    GrowattProtocolIIV1NativeIdentity `json:"native_identity"`
}

type GrowattProtocolIIV1Provider interface {
	GrowattProtocolIIV1(context.Context) (GrowattProtocolIIV1Result, error)
}

var growattProtocolIIV1Providers = struct {
	sync.RWMutex
	byServer map[*Server]GrowattProtocolIIV1Provider
}{byServer: make(map[*Server]GrowattProtocolIIV1Provider)}

func registerGrowattProtocolIIV1Tool(server *Server, provider ModbusV1Provider) {
	growatt, ok := provider.(GrowattProtocolIIV1Provider)
	if !ok || growatt == nil {
		return
	}
	growattProtocolIIV1Providers.Lock()
	growattProtocolIIV1Providers.byServer[server] = growatt
	growattProtocolIIV1Providers.Unlock()
	server.tools = append(server.tools, Tool{Name: GrowattProtocolIIV1IdentityGetTool, Description: "Get the native Growatt Protocol II identity qualification.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}

func (server *Server) handleGrowattProtocolIIV1Call(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != GrowattProtocolIIV1IdentityGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Growatt Protocol II identity arguments"), false, "RETAINED_PROFILE", "")), true), true
	}
	growattProtocolIIV1Providers.RLock()
	provider := growattProtocolIIV1Providers.byServer[server]
	growattProtocolIIV1Providers.RUnlock()
	if provider == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("growatt Protocol II provider unavailable"), false, "RETAINED_PROFILE", "")), true), true
	}
	result, err := provider.GrowattProtocolIIV1(ctx)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "RETAINED_PROFILE", "")), true), true
	}
	result, err = validateGrowattProtocolIIV1Result(result)
	if err != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, err, true, "RETAINED_PROFILE", "")), true), true
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(result, err, true, "RETAINED_PROFILE", "")), err != nil), true
}

// validateGrowattProtocolIIV1Result is the final admission boundary for a
// directly registered provider. It replays the exact offline tuple through
// modbusreg instead of trusting an implementation of the optional interface.
func validateGrowattProtocolIIV1Result(result GrowattProtocolIIV1Result) (GrowattProtocolIIV1Result, error) {
	native := result.NativeIdentity
	if result.Profile != GrowattProtocolIIV1Profile || result.Disposition != "OFFLINE_IDENTITY_ADMITTED" ||
		!result.IdentityQualified || result.Family != native.Family {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1NotAdmitted
	}

	slices := make([]modbusreg.GrowattProtocolIIIdentitySlice, len(native.Slices))
	for index, slice := range native.Slices {
		slices[index] = modbusreg.GrowattProtocolIIIdentitySlice{
			Offset: slice.Offset,
			Words:  append([]uint16(nil), slice.Words...),
		}
	}
	observation, err := modbusreg.DecodeGrowattProtocolIIIdentity(modbusreg.GrowattProtocolIIIdentityInput{
		UnitID:   native.UnitID,
		Function: modbusreg.FunctionReadHoldingRegisters,
		Profile: modbusreg.GrowattProtocolIIIdentityProfile{
			Schema:          "Protocol II v1.24 TL3-X",
			Family:          native.Family,
			DeviceType:      native.DeviceType,
			ModelBuild:      native.ModelBuild,
			ProtocolVersion: native.ProtocolVersion,
		},
		Slices: slices,
	})
	if err != nil || observation.FirmwareText() != native.Firmware || observation.SerialText() != native.Serial {
		return GrowattProtocolIIV1Result{}, ErrGrowattProtocolIIV1NotAdmitted
	}
	return growattProtocolIIV1ResultFromObservation(observation), nil
}

func growattProtocolIIV1ResultFromObservation(observation modbusreg.GrowattProtocolIIIdentityObservation) GrowattProtocolIIV1Result {
	profile := observation.Profile()
	slices := observation.Slices()
	nativeSlices := make([]GrowattProtocolIIV1NativeIdentitySlice, len(slices))
	for index, slice := range slices {
		nativeSlices[index] = GrowattProtocolIIV1NativeIdentitySlice{
			Offset: slice.Offset,
			Words:  append([]uint16(nil), slice.Words...),
		}
	}
	return GrowattProtocolIIV1Result{
		Profile:           GrowattProtocolIIV1Profile,
		Disposition:       "OFFLINE_IDENTITY_ADMITTED",
		Family:            profile.Family,
		IdentityQualified: true,
		NativeIdentity: GrowattProtocolIIV1NativeIdentity{
			Family:          profile.Family,
			UnitID:          observation.UnitID(),
			Firmware:        observation.FirmwareText(),
			Serial:          observation.SerialText(),
			DeviceType:      observation.DeviceType(),
			ModelBuild:      observation.ModelBuild(),
			ProtocolVersion: observation.ProtocolVersion(),
			Slices:          nativeSlices,
		},
	}
}
