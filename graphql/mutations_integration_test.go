package graphql

import (
	"context"
	"sync"
	"testing"

	"github.com/d3vi1/helianthus-ebusgateway"
	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
	"github.com/d3vi1/helianthus-ebusreg/schema"
	graphqlclient "github.com/machinebox/graphql"
	"net/http/httptest"
)

type invokeTemplate struct {
	primary   byte
	secondary byte
	params    schema.Schema
}

func (template invokeTemplate) Primary() byte {
	return template.primary
}

func (template invokeTemplate) Secondary() byte {
	return template.secondary
}

func (template invokeTemplate) ParamSchema() schema.Schema {
	return template.params
}

type invokePlane struct {
	name            string
	source          byte
	target          byte
	hardwareVersion string
	methods         []registry.Method
}

func (plane *invokePlane) Name() string {
	return plane.name
}

func (plane *invokePlane) Methods() []registry.Method {
	return plane.methods
}

func (plane *invokePlane) Subscriptions() []router.Subscription {
	return nil
}

func (plane *invokePlane) OnBroadcast(protocol.Frame) error {
	return nil
}

func (plane *invokePlane) BuildRequest(method registry.Method, params map[string]any) (protocol.Frame, error) {
	template := method.Template()
	if template == nil {
		return protocol.Frame{}, ebuserrors.ErrInvalidPayload
	}
	data := []byte{}
	if schemaProvider, ok := template.(interface{ ParamSchema() schema.Schema }); ok {
		encoded, err := schemaProvider.ParamSchema().Encode(params)
		if err != nil {
			return protocol.Frame{}, err
		}
		data = encoded
	}

	return protocol.Frame{
		Source:    plane.source,
		Target:    plane.target,
		Primary:   template.Primary(),
		Secondary: template.Secondary(),
		Data:      data,
	}, nil
}

func (plane *invokePlane) DecodeResponse(method registry.Method, response protocol.Frame) (any, error) {
	selector := method.ResponseSchema()
	schemaValue := selector.Select(plane.target, plane.hardwareVersion)
	return schemaValue.Decode(response.Data)
}

type invokeProvider struct {
	plane *invokePlane
}

func (invokeProvider) Name() string {
	return "invoke"
}

func (invokeProvider) Match(registry.DeviceInfo) bool {
	return true
}

func (provider invokeProvider) CreatePlanes(registry.DeviceInfo) []registry.Plane {
	return []registry.Plane{provider.plane}
}

type readEvent struct {
	value byte
	err   error
}

type scriptedTransport struct {
	mu     sync.Mutex
	reads  []readEvent
	writes [][]byte
}

func (s *scriptedTransport) ReadByte() (byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reads) == 0 {
		return 0, ebuserrors.ErrTimeout
	}
	ev := s.reads[0]
	s.reads = s.reads[1:]
	return ev.value, ev.err
}

func (s *scriptedTransport) Write(payload []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyPayload := append([]byte(nil), payload...)
	s.writes = append(s.writes, copyPayload)
	return len(payload), nil
}

func (s *scriptedTransport) Close() error {
	return nil
}

func (s *scriptedTransport) lastWrite() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.writes) == 0 {
		return nil
	}
	return s.writes[len(s.writes)-1]
}

func TestInvokeMutation_Integration(t *testing.T) {
	requestSchema := schema.Schema{
		Fields: []schema.SchemaField{
			{Name: "level", Type: types.DATA1b{}},
		},
	}
	responseSchema := schema.Schema{
		Fields: []schema.SchemaField{
			{Name: "status", Type: types.DATA1b{}},
		},
	}

	method := mockMethod{
		name:     "set_level",
		readOnly: false,
		template: invokeTemplate{primary: 0xB5, secondary: 0x05, params: requestSchema},
		response: schema.SchemaSelector{Default: responseSchema},
	}

	plane := &invokePlane{
		name:            "heating",
		source:          0x10,
		target:          0x08,
		hardwareVersion: "7603",
		methods:         []registry.Method{method},
	}

	payload, err := requestSchema.Encode(map[string]any{"level": 5})
	if err != nil {
		t.Fatalf("requestSchema.Encode error = %v", err)
	}

	responsePayload, err := responseSchema.Encode(map[string]any{"status": 42})
	if err != nil {
		t.Fatalf("responseSchema.Encode error = %v", err)
	}

	length := byte(len(responsePayload))
	crc := protocol.CRC(append([]byte{length}, responsePayload...))
	transport := &scriptedTransport{
		reads: []readEvent{
			{value: protocol.SymbolAck},
			{value: length},
		},
	}
	for _, b := range responsePayload {
		transport.reads = append(transport.reads, readEvent{value: b})
	}
	transport.reads = append(transport.reads, readEvent{value: crc})

	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport,
		Providers: []registry.PlaneProvider{invokeProvider{plane: plane}},
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	defer func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	}()

	gateway.Registry.Register(registry.DeviceInfo{
		Address:         plane.target,
		Manufacturer:    "vaillant",
		DeviceID:        "device-a",
		SoftwareVersion: "1.0",
		HardwareVersion: plane.hardwareVersion,
	})

	gateway.RefreshRouterPlanes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gateway.Start(ctx)

	builder := NewBuilder(gateway.Registry, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start error = %v", err)
	}

	handler, err := NewInvokeHandler(builder, gateway.Registry, gateway.Router)
	if err != nil {
		t.Fatalf("NewInvokeHandler error = %v", err)
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	client := graphqlclient.NewClient(server.URL)
	request := graphqlclient.NewRequest(`
		mutation($address: Int!, $plane: String!, $method: String!, $params: JSON) {
			invoke(address: $address, plane: $plane, method: $method, params: $params) {
				ok
				error {
					category
					code
					message
				}
				result
			}
		}
	`)
	request.Var("address", int(plane.target))
	request.Var("plane", "heating")
	request.Var("method", "set_level")
	request.Var("params", map[string]any{"level": 5})

	var response struct {
		Invoke struct {
			Ok     bool                   `json:"ok"`
			Error  *InvokeError           `json:"error"`
			Result map[string]interface{} `json:"result"`
		} `json:"invoke"`
	}

	if err := client.Run(context.Background(), request, &response); err != nil {
		t.Fatalf("invoke mutation error = %v", err)
	}
	if !response.Invoke.Ok || response.Invoke.Error != nil {
		t.Fatalf("invoke result = %+v; want ok", response.Invoke)
	}
	if got := response.Invoke.Result["status"]; got != float64(42) {
		t.Fatalf("invoke result status = %#v; want 42", got)
	}

	expectedWrite := append([]byte{
		plane.source,
		plane.target,
		method.Template().Primary(),
		method.Template().Secondary(),
		byte(len(payload)),
	}, payload...)
	expectedWrite = append(expectedWrite, protocol.CRC(expectedWrite))

	if got := transport.lastWrite(); len(got) == 0 {
		t.Fatalf("transport write missing")
	} else if !equalBytes(got, expectedWrite) {
		t.Fatalf("transport write = %v; want %v", got, expectedWrite)
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
