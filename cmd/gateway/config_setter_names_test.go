package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

const (
	cfgKeysSystemAllowed = "adaptive_heating_curve_enabled, dhw_flow_temperature_setpoint_maximum, dhw_outdoor_bivalence_temperature_limit, heating_flow_temperature_setpoint_maximum_at_heat_pump_fault, heating_outdoor_bivalence_temperature_limit, installer_access_code, installer_name, installer_phone_number, maintenance_date, room_humidity_limit_maximum"
	cfgKeysBoilerAllowed = "central_heating_pump_mode, dhw_partial_load_power, heating_partial_load_power, installer_phone_number, maximum_dhw_flow_temperature_setpoint, maximum_heating_flow_temperature_setpoint"
	cfgKeysBoilerAddress = byte(0x08)
)

// cfgKeysWrite is one acknowledged write seen by cfgKeysFakeBus.
type cfgKeysWrite struct {
	service  string // "B524" or "B509"
	group    byte
	instance byte
	addr     uint16
	payload  []byte
}

// cfgKeysFakeBus answers B524 OP02 and B509 reads and writes from one register
// store per service. Reads of an unseeded register fail, so the B509 fallback
// selector path can be exercised.
type cfgKeysFakeBus struct {
	mu       sync.Mutex
	requests int
	b524     map[uint16][]byte
	b509     map[uint16][]byte
	writes   []cfgKeysWrite
	ops      []string // "W<service>:<addr>" or "R<service>:<addr>" in order
}

func newCfgKeysFakeBus() *cfgKeysFakeBus {
	return &cfgKeysFakeBus{b524: map[uint16][]byte{}, b509: map[uint16][]byte{}}
}

func (bus *cfgKeysFakeBus) send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	bus.requests++
	data := frame.Data
	if frame.Secondary == vaillantB509Secondary {
		if len(data) < 3 {
			return nil, errors.New("short B509 frame")
		}
		addr := uint16(data[1])<<8 | uint16(data[2])
		switch data[0] {
		case vaillantB509OpcodeRead:
			bus.ops = append(bus.ops, fmt.Sprintf("RB509:%04X", addr))
			value, ok := bus.b509[addr]
			if !ok {
				return nil, errors.New("unseeded B509 register")
			}
			return &protocol.Frame{Data: append([]byte{vaillantB509OpcodeRead, data[1], data[2]}, value...)}, nil
		case vaillantB509OpcodeWrite:
			bus.ops = append(bus.ops, fmt.Sprintf("WB509:%04X", addr))
			payload := append([]byte(nil), data[3:]...)
			bus.b509[addr] = payload
			bus.writes = append(bus.writes, cfgKeysWrite{service: "B509", addr: addr, payload: payload})
			return &protocol.Frame{Data: []byte{vaillantB509OpcodeWrite, data[1], data[2]}}, nil
		}
		return nil, errors.New("unexpected B509 operation")
	}
	if len(data) < 6 || data[0] != systemconfig.OpcodeLocal {
		return nil, errors.New("unexpected B524 frame")
	}
	addr := uint16(data[4]) | uint16(data[5])<<8
	switch data[1] {
	case vaillantB524OpWrite:
		bus.ops = append(bus.ops, fmt.Sprintf("WB524:%04X", addr))
		payload := append([]byte(nil), data[6:]...)
		bus.b524[addr] = payload
		bus.writes = append(bus.writes, cfgKeysWrite{service: "B524", group: data[2], instance: data[3], addr: addr, payload: payload})
		return &protocol.Frame{Data: []byte{0x00}}, nil
	case vaillantB524OpRead:
		bus.ops = append(bus.ops, fmt.Sprintf("RB524:%04X", addr))
		value, ok := bus.b524[addr]
		if !ok {
			return nil, errors.New("unwritten B524 register")
		}
		return testB524ResponseForSelectorPayload(data, value...), nil
	}
	return nil, errors.New("unexpected B524 operation")
}

func (bus *cfgKeysFakeBus) snapshot() (int, []cfgKeysWrite, []string) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return bus.requests, append([]cfgKeysWrite(nil), bus.writes...), append([]string(nil), bus.ops...)
}

func newCfgKeysPoller(t *testing.T, bus *cfgKeysFakeBus) (*vaillantSemanticPoller, *graphql.LiveSemanticProvider) {
	t.Helper()
	reg := registry.NewDeviceRegistry(nil)
	// Synthetic controller identity without API qualifiers: the DHW bivalence
	// key resolves the conservative unmatched policy bound [-20, 50].
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0", SoftwareVersion: "0217", HardwareVersion: "8803"})
	provider := graphql.NewLiveSemanticProvider()
	poller := &vaillantSemanticPoller{
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		reg:            reg,
		provider:       provider,
		source:         0x71,
		controller:     0x15,
		boilerAddress:  cfgKeysBoilerAddress,
		requestTimeout: 50 * time.Millisecond,
		system:         &vaillantSystemSnapshot{Controller: 0x15},
		sendFrameFn:    bus.send,
	}
	return poller, provider
}

// cfgKeysSet sends one setter write either as a GraphQL mutation document with
// variables (set_system_config / set_boiler_config) or through the MCP config
// writer adapter, and returns the result.
func cfgKeysSet(t *testing.T, path, mutation string, poller *vaillantSemanticPoller, field, value string) (bool, string) {
	t.Helper()
	if path == "MCP" {
		adapter := &mcpConfigWriterAdapter{poller: poller}
		if mutation == "set_system_config" {
			result := adapter.SetSystemConfig(context.Background(), field, value)
			return result.Success, result.Error
		}
		result := adapter.SetBoilerConfig(context.Background(), field, value)
		return result.Success, result.Error
	}
	builder := graphql.NewBuilder(poller.reg, nil)
	builder.SetSystemConfigWriter(poller)
	builder.SetBoilerConfigWriter(poller)
	builder.SetAdmittedMutationSource(0x71)
	handler, err := graphql.NewInvokeHandler(builder, poller.reg, &dhwBoundInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"query":     fmt.Sprintf(`mutation Write($field: String!, $value: String!) { %s(field: $field, value: $value) { success error } }`, mutation),
		"variables": map[string]string{"field": field, "value": value},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var response struct {
		Data   map[string]graphql.ConfigMutationResult `json:"data"`
		Errors []any                                   `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || rec.Code != http.StatusOK || len(response.Errors) != 0 {
		t.Fatalf("GraphQL response: err=%v status=%d body=%s", err, rec.Code, rec.Body.String())
	}
	result, ok := response.Data[mutation]
	if !ok {
		t.Fatalf("GraphQL response lacks %s: %s", mutation, rec.Body.String())
	}
	return result.Success, result.Error
}

type cfgKeysCase struct {
	key   string
	value string
	// seed pre-loads B509 registers so the selector resolution reads a value.
	seed map[uint16][]byte
	// writes are the expected acknowledged writes, in order.
	writes []cfgKeysWrite
	// readback is true when the setter confirms its write with a live read of
	// the written register.
	readback bool
	// published reads the value the setter published; nil when the setter
	// publishes no value for this key.
	published func(*graphql.LiveSemanticProvider) any
	want      any
}

func cfgKeysB524(addr uint16, payload ...byte) cfgKeysWrite {
	return cfgKeysWrite{service: "B524", group: 0x00, instance: 0x00, addr: addr, payload: payload}
}

func cfgKeysB509(addr uint16, payload ...byte) cfgKeysWrite {
	return cfgKeysWrite{service: "B509", addr: addr, payload: payload}
}

func cfgKeysPadded(text string) []byte {
	out := make([]byte, 7)
	copy(out, text)
	return out
}

func cfgKeysSystemString(read func(graphql.SystemConfig) *string) func(*graphql.LiveSemanticProvider) any {
	return func(provider *graphql.LiveSemanticProvider) any {
		status := provider.System()
		if status == nil || read(status.Config) == nil {
			return nil
		}
		return *read(status.Config)
	}
}

func cfgKeysBoilerFloat(read func(graphql.BoilerConfig) *float64) func(*graphql.LiveSemanticProvider) any {
	return func(provider *graphql.LiveSemanticProvider) any {
		status := provider.BoilerStatus()
		if status == nil || read(status.Config) == nil {
			return nil
		}
		return *read(status.Config)
	}
}

// cfgKeysSystemCases covers every set_system_config key. Per key the case
// catches: the key not accepted (unknown field), a moved register (address),
// a changed value type or encoding (payload), a pair key leaving the pair
// path (one 13-byte write instead of two 7-byte writes), a lost read-back and,
// where the setter publishes a value, a snapshot switch that no longer
// matches the key (published value nil).
func cfgKeysSystemCases() []cfgKeysCase {
	return []cfgKeysCase{
		{key: "dhw_outdoor_bivalence_temperature_limit", value: "7.5", writes: []cfgKeysWrite{cfgKeysB524(0x0001, 0x00, 0x00, 0xF0, 0x40)}, readback: true},
		{key: "room_humidity_limit_maximum", value: "55", writes: []cfgKeysWrite{cfgKeysB524(0x000E, 0x37, 0x00)}, readback: true},
		{key: "adaptive_heating_curve_enabled", value: "true", writes: []cfgKeysWrite{cfgKeysB524(0x0014, 0x01)}, readback: true},
		{key: "heating_outdoor_bivalence_temperature_limit", value: "-3.5", writes: []cfgKeysWrite{cfgKeysB524(0x0023, 0x00, 0x00, 0x60, 0xC0)}, readback: true},
		{key: "heating_flow_temperature_setpoint_maximum_at_heat_pump_fault", value: "45", writes: []cfgKeysWrite{cfgKeysB524(0x0026, 0x00, 0x00, 0x34, 0x42)}, readback: true},
		{key: "dhw_flow_temperature_setpoint_maximum", value: "62", writes: []cfgKeysWrite{cfgKeysB524(0x0046, 0x00, 0x00, 0x78, 0x42)}, readback: true},
		{
			key: "maintenance_date", value: "2027-03-14",
			writes: []cfgKeysWrite{cfgKeysB524(0x002C, 0x0E, 0x03, 0x1B)}, readback: true,
			published: cfgKeysSystemString(func(c graphql.SystemConfig) *string { return c.MaintenanceDate }), want: "2027-03-14",
		},
		{
			key: "installer_name", value: "Sunflower Co",
			writes:    []cfgKeysWrite{cfgKeysB524(0x006C, cfgKeysPadded("Sunflo")...), cfgKeysB524(0x006D, cfgKeysPadded("wer Co")...)},
			published: cfgKeysSystemString(func(c graphql.SystemConfig) *string { return c.InstallerName }), want: "Sunflower Co",
		},
		{
			key: "installer_phone_number", value: "+99 123 456",
			writes:    []cfgKeysWrite{cfgKeysB524(0x006F, cfgKeysPadded("+99 12")...), cfgKeysB524(0x0070, cfgKeysPadded("3 456")...)},
			published: cfgKeysSystemString(func(c graphql.SystemConfig) *string { return c.InstallerPhone }), want: "+99 123 456",
		},
		{
			key: "installer_access_code", value: "417",
			writes: []cfgKeysWrite{cfgKeysB524(0x0076, 0xA1, 0x01)}, readback: true,
			published: func(provider *graphql.LiveSemanticProvider) any {
				status := provider.System()
				if status == nil || status.Config.InstallerMenuCode == nil {
					return nil
				}
				return *status.Config.InstallerMenuCode
			},
			want: 417,
		},
	}
}

// cfgKeysBoilerCases covers every set_boiler_config key, including the
// fallback selector of the heating flow maximum and the 0x4904 key under its
// documentation name, written as one unsigned byte as before.
func cfgKeysBoilerCases() []cfgKeysCase {
	return []cfgKeysCase{
		{
			key: "maximum_heating_flow_temperature_setpoint", value: "55",
			seed:   map[uint16][]byte{0x0E04: {0x20, 0x03}},
			writes: []cfgKeysWrite{cfgKeysB509(0x0E04, 0x70, 0x03)}, readback: true,
			published: cfgKeysBoilerFloat(func(c graphql.BoilerConfig) *float64 { return c.FlowsetHcMaxC }), want: 55.0,
		},
		{
			key: "maximum_heating_flow_temperature_setpoint", value: "56",
			seed:   map[uint16][]byte{0xA500: {0x20, 0x03}},
			writes: []cfgKeysWrite{cfgKeysB509(0xA500, 0x80, 0x03)}, readback: true,
			published: cfgKeysBoilerFloat(func(c graphql.BoilerConfig) *float64 { return c.FlowsetHcMaxC }), want: 56.0,
		},
		{
			key: "maximum_dhw_flow_temperature_setpoint", value: "59",
			seed:   map[uint16][]byte{0x0F04: {0x00, 0x03}},
			writes: []cfgKeysWrite{cfgKeysB509(0x0F04, 0xB0, 0x03)}, readback: true,
			published: cfgKeysBoilerFloat(func(c graphql.BoilerConfig) *float64 { return c.FlowsetHwcMaxC }), want: 59.0,
		},
		{
			key: "heating_partial_load_power", value: "18",
			seed:   map[uint16][]byte{0x0704: {0x0A}},
			writes: []cfgKeysWrite{cfgKeysB509(0x0704, 0x12)}, readback: true,
			published: cfgKeysBoilerFloat(func(c graphql.BoilerConfig) *float64 { return c.PartloadHcKW }), want: 18.0,
		},
		{
			key: "dhw_partial_load_power", value: "24",
			seed:   map[uint16][]byte{0x0804: {0x0A}},
			writes: []cfgKeysWrite{cfgKeysB509(0x0804, 0x18)}, readback: true,
			published: cfgKeysBoilerFloat(func(c graphql.BoilerConfig) *float64 { return c.PartloadHwcKW }), want: 24.0,
		},
		{
			key: "installer_phone_number", value: "0123 456 789",
			writes: []cfgKeysWrite{cfgKeysB509(0x8104, 0x01, 0x23, 0x45, 0x67, 0x89, 0xFF, 0xFF, 0xFF)},
			published: func(provider *graphql.LiveSemanticProvider) any {
				status := provider.BoilerStatus()
				if status == nil || status.Config.PhoneNumber == nil {
					return nil
				}
				return *status.Config.PhoneNumber
			},
			want: "0123456789",
		},
		{
			key: "central_heating_pump_mode", value: "17",
			writes: []cfgKeysWrite{cfgKeysB509(0x4904, 0x11)},
			published: func(provider *graphql.LiveSemanticProvider) any {
				status := provider.BoilerStatus()
				if status == nil || status.Config.InstallerMenuCode == nil {
					return nil
				}
				return *status.Config.InstallerMenuCode
			},
			want: 17,
		},
	}
}

func runCfgKeysCase(t *testing.T, path, mutation string, tc cfgKeysCase) {
	bus := newCfgKeysFakeBus()
	for addr, value := range tc.seed {
		bus.b509[addr] = append([]byte(nil), value...)
	}
	poller, provider := newCfgKeysPoller(t, bus)
	success, message := cfgKeysSet(t, path, mutation, poller, tc.key, tc.value)
	if !success {
		t.Fatalf("%s %s(%s=%q) failed: %q", path, mutation, tc.key, tc.value, message)
	}
	_, writes, ops := bus.snapshot()
	if !reflect.DeepEqual(writes, tc.writes) {
		t.Fatalf("writes = %+v; want %+v", writes, tc.writes)
	}
	if tc.readback {
		last := tc.writes[len(tc.writes)-1]
		write := fmt.Sprintf("W%s:%04X", last.service, last.addr)
		read := fmt.Sprintf("R%s:%04X", last.service, last.addr)
		at := -1
		for i, op := range ops {
			if op == write {
				at = i
			}
		}
		confirmed := false
		for _, op := range ops[at+1:] {
			confirmed = confirmed || op == read
		}
		if at < 0 || !confirmed {
			t.Fatalf("operations = %v; want a read-back of %s after the write", ops, read)
		}
	}
	if tc.published != nil {
		if got := tc.published(provider); got != tc.want {
			t.Fatalf("published value = %#v; want %#v", got, tc.want)
		}
	}
}

func TestConfigSetterNamesSystemKeysWriteThroughGraphQLAndMCP(t *testing.T) {
	cases := cfgKeysSystemCases()
	seen := map[string]bool{}
	for _, tc := range cases {
		seen[tc.key] = true
	}
	if len(seen) != len(systemconfig.SystemConfigFieldSpecs()) {
		t.Fatalf("cases cover %d system keys; specs have %d", len(seen), len(systemconfig.SystemConfigFieldSpecs()))
	}
	for _, path := range []string{"GraphQL", "MCP"} {
		for _, tc := range cases {
			t.Run(path+"/"+tc.key, func(t *testing.T) { runCfgKeysCase(t, path, "set_system_config", tc) })
		}
	}
}

func TestConfigSetterNamesBoilerKeysWriteThroughGraphQLAndMCP(t *testing.T) {
	cases := cfgKeysBoilerCases()
	seen := map[string]bool{}
	for _, tc := range cases {
		seen[tc.key] = true
	}
	if len(seen) != len(boilerConfigFieldSpecs)+2 {
		t.Fatalf("cases cover %d boiler keys; setter accepts %d", len(seen), len(boilerConfigFieldSpecs)+2)
	}
	for _, path := range []string{"GraphQL", "MCP"} {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("%s/%s/%d", path, tc.key, i), func(t *testing.T) { runCfgKeysCase(t, path, "set_boiler_config", tc) })
		}
	}
}

// TestConfigSetterNamesSystemPhoneValidationFollowsKey catches the phone
// character check of the pair writer staying on the former key: a letter in
// installer_phone_number is rejected before any bus request.
func TestConfigSetterNamesSystemPhoneValidationFollowsKey(t *testing.T) {
	bus := newCfgKeysFakeBus()
	poller, _ := newCfgKeysPoller(t, bus)
	result := poller.SetSystemConfig(context.Background(), "installer_phone_number", "12a")
	if result.Success || !strings.Contains(result.Error, "allowed: digits") {
		t.Fatalf("SetSystemConfig(installer_phone_number, 12a) = %+v; want the phone character rejection", result)
	}
	if requests, _, _ := bus.snapshot(); requests != 0 {
		t.Fatalf("bus requests = %d; want none", requests)
	}
}

// TestConfigSetterNamesFormerKeysRejected sends every former camelCase key,
// every name the setters no longer accept (among them the former boiler key
// installerMenuCode) and spellings of the boiler pump mode key that differ from
// the documentation name to the system and boiler setters on both surfaces.
// Each must fail before any bus request with the exact list of accepted keys.
func TestConfigSetterNamesFormerKeysRejected(t *testing.T) {
	cases := []struct {
		mutation, want string
		keys           []string
	}{
		{
			mutation: "set_system_config",
			want:     "unknown system field %q (allowed: " + cfgKeysSystemAllowed + "): " + ebuserrors.ErrInvalidPayload.Error(),
			keys: []string{
				"dhwBivalencePointC", "maxRoomHumidityPct", "adaptiveHeatingCurve", "hcBivalencePointC", "hcEmergencyTempC",
				"hwcMaxFlowTempC", "maintenanceDate", "installerName", "installerPhone", "installerMenuCode",
				"hc_bivalence_point_c", "dhw_bivalence_point_c", "hc_emergency_temp_c", "hwc_max_flow_temp_c",
				"max_room_humidity_pct", "installer_phone", "installer_menu_code",
			},
		},
		{
			mutation: "set_boiler_config",
			want:     "unknown boiler field %q (allowed: " + cfgKeysBoilerAllowed + ")",
			keys: []string{
				"flowsetHcMaxC", "flowsetHwcMaxC", "partloadHcKW", "partloadHwcKW", "phoneNumber", "phone_number", "installer_menu_code",
				"installerMenuCode", "CentralHeatingPumpMode", "central_heating_pump_mode ",
			},
		},
	}
	for _, path := range []string{"GraphQL", "MCP"} {
		for _, tc := range cases {
			for _, key := range tc.keys {
				t.Run(path+"/"+tc.mutation+"/"+key, func(t *testing.T) {
					bus := newCfgKeysFakeBus()
					poller, _ := newCfgKeysPoller(t, bus)
					success, message := cfgKeysSet(t, path, tc.mutation, poller, key, "1")
					if want := fmt.Sprintf(tc.want, key); success || message != want {
						t.Fatalf("success=%v error=%q; want %q", success, message, want)
					}
					if requests, _, _ := bus.snapshot(); requests != 0 {
						t.Fatalf("bus requests = %d; want none", requests)
					}
				})
			}
		}
	}
}

// cfgKeysFixture mirrors docs/testing/config-setter-keys.json.
type cfgKeysFixture struct {
	Setters map[string]struct {
		Service string          `json:"service"`
		Keys    []cfgKeysRowDoc `json:"keys"`
	} `json:"setters"`
}

type cfgKeysRowDoc struct {
	Key      string `json:"key"`
	Selector struct {
		Opcode    string   `json:"opcode,omitempty"`
		Group     string   `json:"group,omitempty"`
		Instance  string   `json:"instance,omitempty"`
		Registers []string `json:"registers"`
	} `json:"selector"`
	ValueType    string            `json:"value_type"`
	Enum         map[string]uint16 `json:"enum,omitempty"`
	Provisional  bool              `json:"provisional"`
	OpenQuestion string            `json:"open_question,omitempty"`
}

func cfgKeysSystemValueType(valueType systemconfig.ValueType) string {
	switch valueType {
	case systemconfig.ValueFloat32:
		return "float32"
	case systemconfig.ValueUint16:
		return "uint16"
	case systemconfig.ValueBoolU8:
		return "bool_u8"
	case systemconfig.ValueEnumU16:
		return "enum_u16"
	case systemconfig.ValueCString:
		return "cstring"
	case systemconfig.ValueDateHDA3:
		return "date_hda3"
	default:
		return fmt.Sprintf("unknown(%d)", valueType)
	}
}

// cfgKeysSystemRows renders the system setter specs as fixture rows; a key of
// the text-pair writer lists its prefix and suffix registers.
func cfgKeysSystemRows(t *testing.T) []cfgKeysRowDoc {
	specs := systemconfig.SystemConfigFieldSpecs()
	for key, pair := range systemCStringPairSpecs {
		spec, ok := specs[key]
		if !ok || spec.RegisterAddr != pair.addr1 {
			t.Fatalf("pair key %s is not the system spec of its prefix register (spec=%+v)", key, spec)
		}
	}
	keys := make([]string, 0, len(specs))
	for key := range specs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]cfgKeysRowDoc, 0, len(keys))
	for _, key := range keys {
		spec := specs[key]
		row := cfgKeysRowDoc{Key: key, ValueType: cfgKeysSystemValueType(spec.ValueType), Enum: spec.Enum}
		row.Selector.Opcode = fmt.Sprintf("0x%02X", systemconfig.OpcodeLocal)
		row.Selector.Group = fmt.Sprintf("0x%02X", spec.RegisterGroup)
		row.Selector.Instance = fmt.Sprintf("0x%02X", systemconfig.Instance)
		row.Selector.Registers = []string{fmt.Sprintf("0x%04X", spec.RegisterAddr)}
		if pair, ok := systemCStringPairSpecs[key]; ok {
			row.ValueType = "cstring_pair"
			row.Selector.Registers = []string{fmt.Sprintf("0x%04X", pair.addr1), fmt.Sprintf("0x%04X", pair.addr2)}
		}
		rows = append(rows, row)
	}
	return rows
}

// cfgKeysBoilerRows renders the boiler setter: the float specs and the two
// writers with their own codec.
func cfgKeysBoilerRows() []cfgKeysRowDoc {
	rows := make([]cfgKeysRowDoc, 0, len(boilerConfigFieldSpecs)+2)
	add := func(key, valueType string, addrs ...uint16) {
		row := cfgKeysRowDoc{Key: key, ValueType: valueType}
		for _, addr := range addrs {
			row.Selector.Registers = append(row.Selector.Registers, fmt.Sprintf("0x%04X", addr))
		}
		rows = append(rows, row)
	}
	for key, spec := range boilerConfigFieldSpecs {
		valueType := fmt.Sprintf("unknown(%d)", spec.codec)
		switch spec.codec {
		case boilerConfigCodecTempDATA2c:
			valueType = "data2c"
		case boilerConfigCodecUCH:
			valueType = "uch"
		}
		add(key, valueType, spec.addrs...)
	}
	add("installer_phone_number", "bcd", boiler_b509_phone_number)
	add("central_heating_pump_mode", "uch", boiler_b509_installer_menu_code)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows
}

func cfgKeysDiff(name string, want, got []cfgKeysRowDoc) error {
	if len(want) != len(got) {
		return fmt.Errorf("%s has %d keys; setter has %d", name, len(got), len(want))
	}
	for i := range want {
		row := got[i]
		// The documentation status is checked against the documentation in the
		// graphql package; the setter fixes everything else of the row.
		row.Provisional = false
		row.OpenQuestion = ""
		if !reflect.DeepEqual(want[i], row) {
			return fmt.Errorf("%s key %d = %+v; setter gives %+v", name, i, row, want[i])
		}
	}
	return nil
}

// cfgKeysAllowedFromSetter reads the accepted key list from the setter's own
// unknown-field error, so the fixture is bound to the keys the code accepts.
func cfgKeysAllowedFromSetter(t *testing.T, mutation string) []string {
	poller, _ := newCfgKeysPoller(t, newCfgKeysFakeBus())
	var message string
	if mutation == "set_system_config" {
		message = poller.SetSystemConfig(context.Background(), "not_a_key", "1").Error
	} else {
		message = poller.SetBoilerConfig(context.Background(), "not_a_key", "1").Error
	}
	start := strings.Index(message, "(allowed: ")
	end := strings.Index(message, ")")
	if start < 0 || end < start {
		t.Fatalf("%s unknown-field error %q lists no keys", mutation, message)
	}
	return strings.Split(message[start+len("(allowed: "):end], ", ")
}

func TestConfigSetterNamesFixtureMatchesSystemAndBoilerSetters(t *testing.T) {
	raw, err := os.ReadFile("../../docs/testing/config-setter-keys.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture cfgKeysFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]struct {
		service string
		rows    []cfgKeysRowDoc
	}{
		"set_system_config": {"B524", cfgKeysSystemRows(t)},
		"set_boiler_config": {"B509", cfgKeysBoilerRows()},
	} {
		got, ok := fixture.Setters[name]
		if !ok || got.Service != want.service {
			t.Fatalf("fixture %s = %+v; want service %s", name, got, want.service)
		}
		if err := cfgKeysDiff(name, want.rows, got.Keys); err != nil {
			t.Fatal(err)
		}
		accepted := cfgKeysAllowedFromSetter(t, name)
		keys := make([]string, 0, len(got.Keys))
		for _, row := range got.Keys {
			keys = append(keys, row.Key)
		}
		if !reflect.DeepEqual(accepted, keys) {
			t.Fatalf("%s setter accepts %v; fixture lists %v", name, accepted, keys)
		}
	}
}

// TestConfigSetterNamesFixtureComparisonCoversEveryField changes each setter
// field of each system and boiler row in turn (key, opcode, group, instance,
// registers, value type, enum) and requires the comparison to fail.
func TestConfigSetterNamesFixtureComparisonCoversEveryField(t *testing.T) {
	mutations := map[string]func(*cfgKeysRowDoc){
		"key":       func(row *cfgKeysRowDoc) { row.Key += "_x" },
		"opcode":    func(row *cfgKeysRowDoc) { row.Selector.Opcode = "0x7E" },
		"group":     func(row *cfgKeysRowDoc) { row.Selector.Group = "0x7F" },
		"instance":  func(row *cfgKeysRowDoc) { row.Selector.Instance = "index" },
		"register":  func(row *cfgKeysRowDoc) { row.Selector.Registers = []string{"0xFFFF"} },
		"registers": func(row *cfgKeysRowDoc) { row.Selector.Registers = append(row.Selector.Registers, "0xFFFF") },
		"value":     func(row *cfgKeysRowDoc) { row.ValueType = "uint8" },
		"enum":      func(row *cfgKeysRowDoc) { row.Enum = map[string]uint16{"other": 9} },
	}
	for name, want := range map[string][]cfgKeysRowDoc{"set_system_config": cfgKeysSystemRows(t), "set_boiler_config": cfgKeysBoilerRows()} {
		clone := func() []cfgKeysRowDoc {
			rows := make([]cfgKeysRowDoc, len(want))
			for i, row := range want {
				row.Selector.Registers = append([]string(nil), row.Selector.Registers...)
				rows[i] = row
			}
			return rows
		}
		if err := cfgKeysDiff(name, want, clone()); err != nil {
			t.Fatalf("unchanged rows rejected: %v", err)
		}
		for i := range want {
			for field, mutate := range mutations {
				got := clone()
				mutate(&got[i])
				if err := cfgKeysDiff(name, want, got); err == nil {
					t.Fatalf("%s row %s: changed %s not detected", name, want[i].Key, field)
				}
			}
		}
		if err := cfgKeysDiff(name, want, clone()[1:]); err == nil {
			t.Fatalf("%s: missing key not detected", name)
		}
	}
}

// TestConfigSetterNamesBoilerPumpModeKeepsItsBound catches a change of the
// value type or bound of the renamed 0x4904 key: a value outside one unsigned
// byte or a non-integer is rejected before any bus request on both surfaces.
func TestConfigSetterNamesBoilerPumpModeKeepsItsBound(t *testing.T) {
	for _, path := range []string{"GraphQL", "MCP"} {
		for value, want := range map[string]string{
			"256": "value 256 out of range [0, 255]",
			"-1":  "value -1 out of range [0, 255]",
			"1.5": `invalid integer "1.5"`,
		} {
			t.Run(path+"/"+value, func(t *testing.T) {
				bus := newCfgKeysFakeBus()
				poller, _ := newCfgKeysPoller(t, bus)
				success, message := cfgKeysSet(t, path, "set_boiler_config", poller, "central_heating_pump_mode", value)
				if success || !strings.HasPrefix(message, want) {
					t.Fatalf("success=%v error=%q; want prefix %q", success, message, want)
				}
				if requests, _, _ := bus.snapshot(); requests != 0 {
					t.Fatalf("bus requests = %d; want none", requests)
				}
			})
		}
	}
}

// TestConfigSetterNamesFieldDescriptionListsAcceptedKeys binds the key list of
// the GraphQL field argument description of the system and boiler setters to
// the keys those setters accept, so the description and the accepted keys
// cannot drift.
func TestConfigSetterNamesFieldDescriptionListsAcceptedKeys(t *testing.T) {
	poller, _ := newCfgKeysPoller(t, newCfgKeysFakeBus())
	schema, err := graphql.NewSchema(graphql.NewBuilder(poller.reg, nil), poller.reg, &dhwBoundInvoker{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"set_system_config", "set_boiler_config"} {
		field, ok := schema.MutationType().Fields()[name]
		if !ok {
			t.Fatalf("mutation lacks %s", name)
		}
		description := ""
		for _, arg := range field.Args {
			if arg.Name() == "field" {
				description = arg.Description()
			}
		}
		const lead, tail = "Key of the value to write; one of: ", ". Any other key"
		end := strings.Index(description, tail)
		if !strings.HasPrefix(description, lead) || end < len(lead) {
			t.Fatalf("%s field description %q lists no keys", name, description)
		}
		listed := strings.Split(description[len(lead):end], ", ")
		if accepted := cfgKeysAllowedFromSetter(t, name); !reflect.DeepEqual(listed, accepted) {
			t.Fatalf("%s field description lists %v; setter accepts %v", name, listed, accepted)
		}
	}
}
