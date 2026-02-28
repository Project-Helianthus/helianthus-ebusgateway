package ebusgateway

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
)

type testMethod struct {
	name     string
	readOnly bool
	template registry.FrameTemplate
	response schema.SchemaSelector
}

func (m testMethod) Name() string                     { return m.name }
func (m testMethod) ReadOnly() bool                   { return m.readOnly }
func (m testMethod) Template() registry.FrameTemplate { return m.template }
func (m testMethod) ResponseSchema() schema.SchemaSelector {
	return m.response
}

type testTemplate struct {
	primary   byte
	secondary byte
	schema    schema.Schema
}

func (t testTemplate) Primary() byte   { return t.primary }
func (t testTemplate) Secondary() byte { return t.secondary }
func (t testTemplate) ParamSchema() schema.Schema {
	return t.schema
}

type testEntry struct {
	info   registry.DeviceInfo
	planes []registry.Plane
}

func (e testEntry) Address() byte            { return e.info.Address }
func (e testEntry) Addresses() []byte        { return []byte{e.info.Address} }
func (e testEntry) Manufacturer() string     { return e.info.Manufacturer }
func (e testEntry) DeviceID() string         { return e.info.DeviceID }
func (e testEntry) SerialNumber() string     { return e.info.SerialNumber }
func (e testEntry) MacAddress() string       { return e.info.MacAddress }
func (e testEntry) SoftwareVersion() string  { return e.info.SoftwareVersion }
func (e testEntry) HardwareVersion() string  { return e.info.HardwareVersion }
func (e testEntry) Planes() []registry.Plane { return e.planes }
func (e testEntry) Projections() []registry.Projection {
	return nil
}

type testPlane struct {
	name    string
	methods []registry.Method
}

func (p testPlane) Name() string               { return p.name }
func (p testPlane) Methods() []registry.Method { return p.methods }

func TestFirstReadOnlyMethod(t *testing.T) {
	methods := []registry.Method{
		testMethod{name: "set", readOnly: false},
		testMethod{name: "get", readOnly: true},
	}
	got, ok := firstReadOnlyMethod(methods)
	if !ok || got.Name() != "get" {
		t.Fatalf("firstReadOnlyMethod() = %v, ok=%v; want get,true", got, ok)
	}
}

func TestSmokePlaneBuildRequestAndDecode(t *testing.T) {
	paramSchema := schema.Schema{
		Fields: []schema.SchemaField{
			{Name: "value", Type: types.DATA1b{}},
		},
	}
	responseSchema := schema.Schema{
		Fields: []schema.SchemaField{
			{Name: "resp", Type: types.DATA1b{}},
		},
	}
	method := testMethod{
		name:     "get",
		readOnly: true,
		template: testTemplate{primary: 0xB5, secondary: 0x04, schema: paramSchema},
		response: schema.SchemaSelector{Default: responseSchema},
	}
	plane := testPlane{name: "test", methods: []registry.Method{method}}
	entry := testEntry{info: registry.DeviceInfo{Address: 0x08, HardwareVersion: "7603"}, planes: []registry.Plane{plane}}

	smokePlane := newSmokePlane(entry, plane, 0x10)
	request, err := smokePlane.BuildRequest(method, map[string]any{"value": int8(5)})
	if err != nil {
		t.Fatalf("BuildRequest error = %v", err)
	}
	if request.Source != 0x10 || request.Target != 0x08 || request.Primary != 0xB5 || request.Secondary != 0x04 {
		t.Fatalf("unexpected request frame: %+v", request)
	}
	if len(request.Data) != 1 || request.Data[0] != 0x05 {
		t.Fatalf("unexpected request data: %x", request.Data)
	}

	resp := protocol.Frame{Data: []byte{0x07}}
	decoded, err := smokePlane.DecodeResponse(method, resp, map[string]any{"value": int8(5)})
	if err != nil {
		t.Fatalf("DecodeResponse error = %v", err)
	}
	values, ok := decoded.(map[string]types.Value)
	if !ok {
		t.Fatalf("decoded type = %T; want map[string]types.Value", decoded)
	}
	value, ok := values["resp"]
	if !ok || !value.Valid || value.Value != int8(7) {
		t.Fatalf("decoded resp = %+v; want 7 valid", value)
	}
}

func TestDecodeDeviceInfoPayload(t *testing.T) {
	payload := []byte{0xB5, ' ', 'A', 'B', ' ', 0x00, 0x01, 0x02, 0x76, 0x03}
	info, err := decodeDeviceInfoPayload(payload)
	if err != nil {
		t.Fatalf("decodeDeviceInfoPayload error = %v", err)
	}
	if info["manufacturer"] != "Vaillant" || info["device_id"] != "AB" || info["sw_version"] != "0102" || info["hw_version"] != "7603" {
		t.Fatalf("unexpected decoded info: %+v", info)
	}
	if _, err := decodeDeviceInfoPayload([]byte{0x01}); err == nil {
		t.Fatalf("expected error on short payload")
	}
}

func TestScanTargetsFromExpectedDevices(t *testing.T) {
	expected := []expectedDevice{
		{Address: hexByte(0x08), Description: "Boiler"},
		{Address: hexByte(0x10), Description: "Controller"},
		{Address: hexByte(0x08), Description: "Duplicate"},
	}

	got := scanTargetsFromExpectedDevices(expected)
	want := []byte{0x08, 0x10}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scan targets = %v; want %v", got, want)
	}
}

func TestDefaultSmokeProviders(t *testing.T) {
	providers := defaultSmokeProviders()
	if len(providers) != 3 {
		t.Fatalf("defaultSmokeProviders() returned %d providers; want 3", len(providers))
	}
	if providers[0].Name() != vaillantproviders.System().Name() {
		t.Fatalf("defaultSmokeProviders()[0] name = %q; want %q", providers[0].Name(), vaillantproviders.System().Name())
	}
	if providers[1].Name() != vaillantproviders.Heating().Name() {
		t.Fatalf("defaultSmokeProviders()[1] name = %q; want %q", providers[1].Name(), vaillantproviders.Heating().Name())
	}
	if providers[2].Name() != vaillantproviders.DHW().Name() {
		t.Fatalf("defaultSmokeProviders()[2] name = %q; want %q", providers[2].Name(), vaillantproviders.DHW().Name())
	}
}

type smokeMockBus struct {
	lastRequest protocol.Frame
	response    *protocol.Frame
	err         error
}

func (bus *smokeMockBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.lastRequest = frame
	return bus.response, bus.err
}

func TestInvokeIdentify_SendsRequestAndDecodesResponse(t *testing.T) {
	t.Parallel()

	payload := []byte{0xB5, 'B', 'A', 'I', '0', '0', 0x01, 0x02, 0x76, 0x03}
	bus := &smokeMockBus{
		response: &protocol.Frame{
			Data: payload,
		},
	}
	eventRouter := router.NewBusEventRouter(bus)

	entry := testEntry{
		info: registry.DeviceInfo{
			Address: 0x08,
		},
	}
	err := invokeIdentify(context.Background(), eventRouter, entry, 0x10, time.Second, nil)
	if err != nil {
		t.Fatalf("invokeIdentify error = %v", err)
	}

	if bus.lastRequest.Source != 0x10 || bus.lastRequest.Target != 0x08 {
		t.Fatalf("unexpected request addresses: %+v", bus.lastRequest)
	}
	if bus.lastRequest.Primary != 0x07 || bus.lastRequest.Secondary != 0x04 {
		t.Fatalf("unexpected request type: %+v", bus.lastRequest)
	}
	if len(bus.lastRequest.Data) != 0 {
		t.Fatalf("unexpected request data: %x", bus.lastRequest.Data)
	}
}

func TestSmokeInvoke_UsesDirectRouterPlaneAndDefaultsParams(t *testing.T) {
	t.Parallel()

	planes := vaillantproviders.System().CreatePlanes(registry.DeviceInfo{
		Manufacturer: "Vaillant",
		Address:      0x08,
	})
	if len(planes) != 1 {
		t.Fatalf("expected 1 plane, got %d", len(planes))
	}
	plane := planes[0]

	entry := testEntry{
		info: registry.DeviceInfo{
			Manufacturer: "Vaillant",
			DeviceID:     "BAI00",
			Address:      0x08,
		},
		planes: []registry.Plane{plane},
	}

	invokePlane, direct := smokeInvocationPlane(entry, plane, 0x10)
	if !direct {
		t.Fatalf("expected plane to be invoked directly")
	}
	if typed, ok := plane.(router.Plane); !ok || invokePlane != typed {
		t.Fatalf("expected returned plane to match the original router.Plane")
	}

	params, ok := smokeParams(entry, invokePlane.Name(), "get_operational_data", 0x10)
	if !ok {
		t.Fatalf("expected smoke params for get_operational_data")
	}

	payload := []byte{
		0x01,       // dcfstate
		0x12,       // hour (BCD 12)
		0x34,       // minute (BCD 34)
		0x03,       // day (BCD 03)
		0x02,       // month (BCD 02)
		0x26,       // year (BCD 26)
		0x80, 0x14, // temp (DATA2b 20.5)
	}

	bus := &smokeMockBus{
		response: &protocol.Frame{
			Source:    0x08,
			Target:    0x10,
			Primary:   0xB5,
			Secondary: 0x04,
			Data:      payload,
		},
	}
	eventRouter := router.NewBusEventRouter(bus)

	result, err := eventRouter.Invoke(context.Background(), invokePlane, "get_operational_data", params)
	if err != nil {
		t.Fatalf("Invoke error = %v", err)
	}

	if bus.lastRequest.Source != 0x10 || bus.lastRequest.Target != 0x08 {
		t.Fatalf("unexpected request addresses: %+v", bus.lastRequest)
	}
	if bus.lastRequest.Primary != 0xB5 || bus.lastRequest.Secondary != 0x04 {
		t.Fatalf("unexpected request type: %+v", bus.lastRequest)
	}
	if !bytes.Equal(bus.lastRequest.Data, []byte{0x00}) {
		t.Fatalf("unexpected request data %v", bus.lastRequest.Data)
	}

	values, ok := result.(map[string]types.Value)
	if !ok {
		t.Fatalf("expected map[string]types.Value, got %T", result)
	}
	if op := values["op"]; !op.Valid || op.Value != byte(0x00) {
		t.Fatalf("op = %+v; want 0x00 valid", op)
	}
	if got := values["payload"]; !got.Valid || !bytes.Equal(got.Value.([]byte), payload) {
		t.Fatalf("payload = %+v; want %v", got, payload)
	}
	if got := values["dcfstate"]; !got.Valid || got.Value != uint8(0x01) {
		t.Fatalf("dcfstate = %+v; want 1 valid", got)
	}
}

func TestSmokeParams_SelectsBASV2ExtRegister(t *testing.T) {
	t.Parallel()

	entry := testEntry{
		info: registry.DeviceInfo{
			Manufacturer: "Vaillant",
			DeviceID:     "BASV2",
			Address:      0x15,
		},
	}

	params, ok := smokeParams(entry, "system", "get_ext_register", 0x10)
	if !ok {
		t.Fatalf("expected smoke params for BASV2 get_ext_register")
	}

	if len(params) != 5 {
		t.Fatalf("params keys=%d; want 5", len(params))
	}
	if got, ok := params["source"].(byte); !ok || got != 0x10 {
		t.Fatalf("source=%v (%T); want 0x10 byte", params["source"], params["source"])
	}
	if got, ok := params["opcode"].(byte); !ok || got != 0x02 {
		t.Fatalf("opcode=%v (%T); want 0x02 byte", params["opcode"], params["opcode"])
	}
	if got, ok := params["group"].(byte); !ok || got != 0x00 {
		t.Fatalf("group=%v (%T); want 0x00 byte", params["group"], params["group"])
	}
	if got, ok := params["instance"].(byte); !ok || got != 0x00 {
		t.Fatalf("instance=%v (%T); want 0x00 byte", params["instance"], params["instance"])
	}
	if got, ok := params["addr"].(uint16); !ok || got != 0x5C00 {
		t.Fatalf("addr=%v (%T); want 0x5C00 uint16", params["addr"], params["addr"])
	}
}
