package graphql

import (
	"errors"
	"testing"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/schema"
)

type schemaTemplate struct {
	primary   byte
	secondary byte
	params    schema.Schema
}

func (template schemaTemplate) Primary() byte {
	return template.primary
}

func (template schemaTemplate) Secondary() byte {
	return template.secondary
}

func (template schemaTemplate) ParamSchema() schema.Schema {
	return template.params
}

type buildTemplate struct {
	primary   byte
	secondary byte
}

func (template buildTemplate) Primary() byte {
	return template.primary
}

func (template buildTemplate) Secondary() byte {
	return template.secondary
}

func (template buildTemplate) Build(params map[string]any) ([]byte, error) {
	if params == nil {
		return nil, ebuserrors.ErrInvalidPayload
	}
	value, ok := params["period"]
	if !ok {
		return nil, ebuserrors.ErrInvalidPayload
	}
	switch typed := value.(type) {
	case int:
		return []byte{byte(typed)}, nil
	case int64:
		return []byte{byte(typed)}, nil
	case uint8:
		return []byte{typed}, nil
	default:
		return nil, ebuserrors.ErrInvalidPayload
	}
}

func TestValidateInvokeParams_SchemaCoercion(t *testing.T) {
	method := mockMethod{
		name:     "set_level",
		readOnly: false,
		template: schemaTemplate{
			primary:   0xB5,
			secondary: 0x05,
			params: schema.Schema{
				Fields: []schema.SchemaField{
					{Name: "level", Type: types.DATA1b{}},
				},
			},
		},
	}

	got, err := validateInvokeParams(method, map[string]any{"level": float64(3)})
	if err != nil {
		t.Fatalf("validateInvokeParams error = %v", err)
	}
	if value, ok := got["level"].(int64); !ok || value != 3 {
		t.Fatalf("normalized level = %#v; want int64(3)", got["level"])
	}

	_, err = validateInvokeParams(method, map[string]any{})
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Fatalf("missing field error = %v; want ErrInvalidPayload", err)
	}

	_, err = validateInvokeParams(method, map[string]any{"level": 1, "extra": 2})
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Fatalf("extra field error = %v; want ErrInvalidPayload", err)
	}
}

func TestValidateInvokeParams_NoParamsAllowed(t *testing.T) {
	method := mockMethod{
		name:     "get_status",
		readOnly: true,
		template: mockTemplate{primary: 0xB5, secondary: 0x04},
	}

	_, err := validateInvokeParams(method, map[string]any{"unexpected": 1})
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Fatalf("unexpected params error = %v; want ErrInvalidPayload", err)
	}
}

func TestValidateInvokeParams_BuilderCoercion(t *testing.T) {
	method := mockMethod{
		name:     "get_energy",
		readOnly: true,
		template: buildTemplate{primary: 0xB5, secondary: 0x16},
	}

	got, err := validateInvokeParams(method, map[string]any{"period": float64(2)})
	if err != nil {
		t.Fatalf("validateInvokeParams error = %v", err)
	}
	if value, ok := got["period"].(int64); !ok || value != 2 {
		t.Fatalf("normalized period = %#v; want int64(2)", got["period"])
	}
}

func TestMapInvokeError_Categories(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     string
		category string
	}{
		{name: "invalid", err: ebuserrors.ErrInvalidPayload, code: "INVALID_PAYLOAD", category: "INVALID"},
		{name: "timeout", err: ebuserrors.ErrTimeout, code: "TIMEOUT", category: "TRANSIENT"},
		{name: "nack", err: ebuserrors.ErrNACK, code: "NACK", category: "DEFINITIVE"},
		{name: "closed", err: ebuserrors.ErrTransportClosed, code: "TRANSPORT_CLOSED", category: "FATAL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mapInvokeError(tc.err)
			if got == nil {
				t.Fatalf("mapInvokeError returned nil")
			}
			if got.Code != tc.code || got.Category != tc.category {
				t.Fatalf("mapInvokeError = %+v; want code=%s category=%s", got, tc.code, tc.category)
			}
		})
	}
}

var _ registry.FrameTemplate = schemaTemplate{}
var _ registry.FrameTemplate = buildTemplate{}
