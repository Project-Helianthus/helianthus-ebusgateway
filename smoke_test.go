package ebusgateway

import (
	"reflect"
	"testing"

	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/schema"
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
func (e testEntry) Manufacturer() string     { return e.info.Manufacturer }
func (e testEntry) DeviceID() string         { return e.info.DeviceID }
func (e testEntry) SoftwareVersion() string  { return e.info.SoftwareVersion }
func (e testEntry) HardwareVersion() string  { return e.info.HardwareVersion }
func (e testEntry) Planes() []registry.Plane { return e.planes }

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
	decoded, err := smokePlane.DecodeResponse(method, resp)
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
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	info, err := decodeDeviceInfoPayload(payload)
	if err != nil {
		t.Fatalf("decodeDeviceInfoPayload error = %v", err)
	}
	if info["manufacturer"] != "0102" || info["device_id"] != "0304" || info["sw_version"] != "0506" || info["hw_version"] != "0708" {
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
