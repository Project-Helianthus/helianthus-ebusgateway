package graphql

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
	graphqlgo "github.com/graphql-go/graphql"
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

type mockBoilerWriter struct {
	result BoilerConfigMutationResult
	calls  []struct {
		field string
		value string
	}
}

func (writer *mockBoilerWriter) SetBoilerConfig(_ context.Context, fieldName string, rawValue string) BoilerConfigMutationResult {
	writer.calls = append(writer.calls, struct {
		field string
		value string
	}{field: fieldName, value: rawValue})
	return writer.result
}

type mockScheduleWriter struct {
	zoneResult *mcp.TimeProgramWriteResult
	dhwResult  *mcp.TimeProgramWriteResult
	zoneErr    error
	dhwErr     error
	zoneCalls  []struct {
		zone    int
		weekday int
		slots   []mcp.TimeProgramSlot
	}
	dhwCalls []struct {
		weekday int
		slots   []mcp.TimeProgramSlot
	}
}

func (writer *mockScheduleWriter) SetZoneTimeProgram(_ context.Context, zone int, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	cloned := append([]mcp.TimeProgramSlot(nil), slots...)
	writer.zoneCalls = append(writer.zoneCalls, struct {
		zone    int
		weekday int
		slots   []mcp.TimeProgramSlot
	}{
		zone:    zone,
		weekday: weekday,
		slots:   cloned,
	})
	return writer.zoneResult, writer.zoneErr
}

func (writer *mockScheduleWriter) SetDhwTimeProgram(_ context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	cloned := append([]mcp.TimeProgramSlot(nil), slots...)
	writer.dhwCalls = append(writer.dhwCalls, struct {
		weekday int
		slots   []mcp.TimeProgramSlot
	}{
		weekday: weekday,
		slots:   cloned,
	})
	return writer.dhwResult, writer.dhwErr
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

func TestNormalizeInvokeResult_ConstraintFields(t *testing.T) {
	result := map[string]types.Value{
		"constraints": {
			Value: map[string]any{
				"type": "tempv",
				"min":  float64(35),
				"max":  float64(70),
				"step": float64(1),
			},
			Valid: true,
		},
		"constraint_type": {Value: "tempv", Valid: true},
		"constraint_min":  {Value: float64(35), Valid: true},
		"constraint_max":  {Value: float64(70), Valid: true},
		"constraint_step": {Value: float64(1), Valid: true},
		"value":           {Valid: false},
	}

	normalized, ok := normalizeInvokeResult(result).(map[string]any)
	if !ok {
		t.Fatalf("normalizeInvokeResult type = %T; want map[string]any", normalizeInvokeResult(result))
	}
	constraints, ok := normalized["constraints"].(map[string]any)
	if !ok {
		t.Fatalf("constraints type = %T; want map[string]any", normalized["constraints"])
	}
	if constraints["type"] != "tempv" || constraints["min"] != float64(35) || constraints["max"] != float64(70) || constraints["step"] != float64(1) {
		t.Fatalf("constraints mismatch: %#v", constraints)
	}
	if normalized["constraint_type"] != "tempv" || normalized["constraint_min"] != float64(35) ||
		normalized["constraint_max"] != float64(70) || normalized["constraint_step"] != float64(1) {
		t.Fatalf("flattened constraints mismatch: %#v", normalized)
	}
	if normalized["value"] != nil {
		t.Fatalf("value = %#v; want nil for invalid source value", normalized["value"])
	}
}

func TestSetBoilerConfigMutation_UnsupportedInReducedProfile(t *testing.T) {
	queryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Query",
		Fields: graphqlgo.Fields{
			"noop": &graphqlgo.Field{Type: graphqlgo.String},
		},
	})
	mutationType := buildMutationType(nil, nil, nil, nil, nil)
	schema, err := graphqlgo.NewSchema(graphqlgo.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
	if err != nil {
		t.Fatalf("NewSchema error = %v", err)
	}

	result := graphqlgo.Do(graphqlgo.Params{
		Schema: schema,
		RequestString: `mutation {
			setBoilerConfig(field: "flowsetHcMaxC", value: "55") {
				success
				error
			}
		}`,
	})
	if len(result.Errors) > 0 {
		t.Fatalf("mutation errors = %+v", result.Errors)
	}

	data, ok := result.Data.(map[string]any)
	if !ok {
		t.Fatalf("result data type = %T; want map", result.Data)
	}
	payload, ok := data["setBoilerConfig"].(map[string]any)
	if !ok {
		t.Fatalf("setBoilerConfig payload type = %T; want map", data["setBoilerConfig"])
	}
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("setBoilerConfig success = %v; want false", got)
	}
	errMessage, _ := payload["error"].(string)
	if errMessage == "" {
		t.Fatalf("setBoilerConfig error message empty")
	}
}

func TestSetCircuitConfigMutation_Success(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x00, 0x00, 0xC0, 0x3F}, Valid: true}, // 1.5
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		setCircuitConfig(index: 1, field: "heatingCurve", value: "1.5") {
			success
			error
		}
	}`)

	payload, ok := data["setCircuitConfig"].(map[string]any)
	if !ok {
		t.Fatalf("setCircuitConfig payload type = %T; want map", data["setCircuitConfig"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("setCircuitConfig success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("setCircuitConfig error = %#v; want nil", got)
	}

	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
	first := invoker.calls[0]
	if first.Plane != mutationSystemPlane || first.Method != mutationSetExtRegisterMethod {
		t.Fatalf("first call = %+v; want system/%s", first, mutationSetExtRegisterMethod)
	}
	if got := first.Params["group"]; got != byte(0x02) {
		t.Fatalf("first call group = %#v; want 0x02", got)
	}
	if got := first.Params["instance"]; got != byte(0x01) {
		t.Fatalf("first call instance = %#v; want 0x01", got)
	}
	if got := first.Params["addr"]; got != uint16(0x000F) {
		t.Fatalf("first call addr = %#v; want 0x000F", got)
	}
	if got := first.Params["opcode"]; got != byte(0x02) {
		t.Fatalf("first call opcode = %#v; want 0x02", got)
	}
	if got := first.Params["source"]; got != byte(0x31) {
		t.Fatalf("first call source = %#v; want 0x31", got)
	}
	dataBytes, ok := first.Params["data"].([]byte)
	if !ok || !bytes.Equal(dataBytes, []byte{0x00, 0x00, 0xC0, 0x3F}) {
		t.Fatalf("first call data = %v; want [00 00 C0 3F]", first.Params["data"])
	}

	second := invoker.calls[1]
	if second.Method != mutationGetExtRegisterMethod {
		t.Fatalf("second call method = %q; want %q", second.Method, mutationGetExtRegisterMethod)
	}
	if _, ok := second.Params["data"]; ok {
		t.Fatalf("second call unexpectedly has data param: %#v", second.Params["data"])
	}
}

func TestSetBoilerConfigMutation_DelegatesToWriter(t *testing.T) {
	writer := &mockBoilerWriter{
		result: BoilerConfigMutationResult{Success: true},
	}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, writer, nil), `mutation {
		setBoilerConfig(field: "flowsetHcMaxC", value: "55") {
			success
			error
		}
	}`)

	payload, ok := data["setBoilerConfig"].(map[string]any)
	if !ok {
		t.Fatalf("setBoilerConfig payload type = %T; want map", data["setBoilerConfig"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("setBoilerConfig success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("setBoilerConfig error = %#v; want nil", got)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("writer calls = %d; want 1", len(writer.calls))
	}
	if writer.calls[0].field != "flowsetHcMaxC" || writer.calls[0].value != "55" {
		t.Fatalf("writer call = %#v; want flowsetHcMaxC/55", writer.calls[0])
	}
}

func TestSetSystemConfigMutation_Success(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x01}, Valid: true},
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		setSystemConfig(field: "adaptiveHeatingCurve", value: "true") {
			success
			error
		}
	}`)

	payload, ok := data["setSystemConfig"].(map[string]any)
	if !ok {
		t.Fatalf("setSystemConfig payload type = %T; want map", data["setSystemConfig"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("setSystemConfig success = %v; want true", got)
	}

	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
	first := invoker.calls[0]
	if first.Method != mutationSetExtRegisterMethod {
		t.Fatalf("first call method = %q; want %q", first.Method, mutationSetExtRegisterMethod)
	}
	if got := first.Params["group"]; got != byte(0x00) {
		t.Fatalf("first call group = %#v; want 0x00", got)
	}
	if got := first.Params["instance"]; got != byte(0x00) {
		t.Fatalf("first call instance = %#v; want 0x00", got)
	}
	if got := first.Params["addr"]; got != uint16(0x0014) {
		t.Fatalf("first call addr = %#v; want 0x0014", got)
	}
	dataBytes, ok := first.Params["data"].([]byte)
	if !ok || !bytes.Equal(dataBytes, []byte{0x01}) {
		t.Fatalf("first call data = %v; want [01]", first.Params["data"])
	}
}

func TestSetSystemConfigMutation_IntegerLikeFloatString(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x37, 0x00}, Valid: true}, // 55
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		setSystemConfig(field: "maxRoomHumidityPct", value: "55.0") {
			success
			error
		}
	}`)

	payload, ok := data["setSystemConfig"].(map[string]any)
	if !ok {
		t.Fatalf("setSystemConfig payload type = %T; want map", data["setSystemConfig"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("setSystemConfig success = %v; want true", got)
	}

	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
	first := invoker.calls[0]
	if got := first.Params["addr"]; got != uint16(0x000E) {
		t.Fatalf("first call addr = %#v; want 0x000E", got)
	}
	dataBytes, ok := first.Params["data"].([]byte)
	if !ok || !bytes.Equal(dataBytes, []byte{0x37, 0x00}) {
		t.Fatalf("first call data = %v; want [37 00]", first.Params["data"])
	}
}

func TestSetCircuitConfigMutation_ValidationFailures(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantErrPart string
	}{
		{
			name: "unknown field",
			query: `mutation {
				setCircuitConfig(index: 1, field: "bogus", value: "1") { success error }
			}`,
			wantErrPart: "unknown circuit field",
		},
		{
			name: "float out of range",
			query: `mutation {
				setCircuitConfig(index: 1, field: "heatingCurve", value: "8.1") { success error }
			}`,
			wantErrPart: "out of range",
		},
		{
			name: "invalid enum",
			query: `mutation {
				setCircuitConfig(index: 1, field: "roomTempControl", value: "manual") { success error }
			}`,
			wantErrPart: "invalid enum value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := mutationTestRegistry{
				entries: map[byte]registry.DeviceEntry{
					0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
				},
				order: []byte{0x15},
			}
			invoker := &mutationTestInvoker{}
			data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), test.query)

			payload, ok := data["setCircuitConfig"].(map[string]any)
			if !ok {
				t.Fatalf("setCircuitConfig payload type = %T; want map", data["setCircuitConfig"])
			}
			if got, _ := payload["success"].(bool); got {
				t.Fatalf("setCircuitConfig success = %v; want false", got)
			}
			errMessage, _ := payload["error"].(string)
			if !strings.Contains(errMessage, test.wantErrPart) {
				t.Fatalf("setCircuitConfig error = %q; want contains %q", errMessage, test.wantErrPart)
			}
			if len(invoker.calls) != 0 {
				t.Fatalf("invoker calls = %d; want 0 for validation failure", len(invoker.calls))
			}
		})
	}
}

func TestSetSystemConfigMutation_FailureScenarios(t *testing.T) {
	t.Run("invalid bool", func(t *testing.T) {
		registry := mutationTestRegistry{
			entries: map[byte]registry.DeviceEntry{
				0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
			},
			order: []byte{0x15},
		}
		invoker := &mutationTestInvoker{}
		data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
			setSystemConfig(field: "adaptiveHeatingCurve", value: "maybe") { success error }
		}`)
		payload, _ := data["setSystemConfig"].(map[string]any)
		if got, _ := payload["success"].(bool); got {
			t.Fatalf("setSystemConfig success = %v; want false", got)
		}
		if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, "invalid boolean value") {
			t.Fatalf("setSystemConfig error = %q; want invalid boolean", errMessage)
		}
		if len(invoker.calls) != 0 {
			t.Fatalf("invoker calls = %d; want 0", len(invoker.calls))
		}
	})

	t.Run("missing controller", func(t *testing.T) {
		registry := mutationTestRegistry{}
		invoker := &mutationTestInvoker{}
		data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
			setSystemConfig(field: "adaptiveHeatingCurve", value: "true") { success error }
		}`)
		payload, _ := data["setSystemConfig"].(map[string]any)
		if got, _ := payload["success"].(bool); got {
			t.Fatalf("setSystemConfig success = %v; want false", got)
		}
		if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, "controller BASV2 not found") {
			t.Fatalf("setSystemConfig error = %q; want missing controller", errMessage)
		}
	})

	t.Run("missing get method", func(t *testing.T) {
		registry := mutationTestRegistry{
			entries: map[byte]registry.DeviceEntry{
				0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod),
			},
			order: []byte{0x15},
		}
		invoker := &mutationTestInvoker{}
		data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
			setSystemConfig(field: "adaptiveHeatingCurve", value: "true") { success error }
		}`)
		payload, _ := data["setSystemConfig"].(map[string]any)
		if got, _ := payload["success"].(bool); got {
			t.Fatalf("setSystemConfig success = %v; want false", got)
		}
		if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, mutationGetExtRegisterMethod) {
			t.Fatalf("setSystemConfig error = %q; want mention missing get method", errMessage)
		}
		if len(invoker.calls) != 0 {
			t.Fatalf("invoker calls = %d; want 0", len(invoker.calls))
		}
	})
}

func TestSetCircuitConfigMutation_ReadbackConfirmFailure(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x00, 0x00, 0x40, 0x40}, Valid: true}, // 3.0 (mismatch)
			},
		},
	}
	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		setCircuitConfig(index: 2, field: "heatingCurve", value: "1.5") { success error }
	}`)
	payload, _ := data["setCircuitConfig"].(map[string]any)
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("setCircuitConfig success = %v; want false", got)
	}
	if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, "read-back mismatch") {
		t.Fatalf("setCircuitConfig error = %q; want read-back mismatch", errMessage)
	}
	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
}

func TestSetZoneTimeProgramMutation_Success(t *testing.T) {
	writer := &mockScheduleWriter{
		zoneResult: &mcp.TimeProgramWriteResult{
			Success: true,
			SlotResults: []mcp.TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0},
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, nil, writer), `mutation {
		setZoneTimeProgram(
			zone: 2
			weekday: 1
			slots: "[{\"start_hour\":6,\"start_minute\":0,\"end_hour\":22,\"end_minute\":0,\"temperature_c\":21}]"
		) {
			success
			error
			slotResults {
				slotIndex
				accepted
				errorCode
				errorDescription
			}
		}
	}`)

	payload, ok := data["setZoneTimeProgram"].(map[string]any)
	if !ok {
		t.Fatalf("setZoneTimeProgram payload type = %T; want map", data["setZoneTimeProgram"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("setZoneTimeProgram success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("setZoneTimeProgram error = %#v; want nil", got)
	}
	slotResults, ok := payload["slotResults"].([]any)
	if !ok || len(slotResults) != 1 {
		t.Fatalf("setZoneTimeProgram slotResults = %#v; want one result", payload["slotResults"])
	}
	firstSlot, ok := slotResults[0].(map[string]any)
	if !ok {
		t.Fatalf("setZoneTimeProgram first slot type = %T; want map", slotResults[0])
	}
	if got, _ := firstSlot["slotIndex"].(int); got != 0 {
		t.Fatalf("slotIndex = %v; want 0", got)
	}
	if got, _ := firstSlot["accepted"].(bool); !got {
		t.Fatalf("accepted = %v; want true", got)
	}
	if got, _ := firstSlot["errorCode"].(int); got != 0 {
		t.Fatalf("errorCode = %v; want 0", got)
	}
	if got := firstSlot["errorDescription"]; got != nil {
		t.Fatalf("errorDescription = %#v; want nil", got)
	}

	if len(writer.zoneCalls) != 1 {
		t.Fatalf("zone writer calls = %d; want 1", len(writer.zoneCalls))
	}
	call := writer.zoneCalls[0]
	if call.zone != 2 || call.weekday != 1 {
		t.Fatalf("zone writer call = %#v; want zone=2 weekday=1", call)
	}
	if len(call.slots) != 1 {
		t.Fatalf("zone writer slots = %#v; want one slot", call.slots)
	}
	slot := call.slots[0]
	if slot.StartHour != 6 || slot.StartMinute != 0 || slot.EndHour != 22 || slot.EndMinute != 0 {
		t.Fatalf("zone slot = %#v; want 06:00-22:00", slot)
	}
	if slot.TemperatureC == nil || *slot.TemperatureC != 21 {
		t.Fatalf("zone slot temperature = %#v; want 21", slot.TemperatureC)
	}
}

func TestSetDhwTimeProgramMutation_Success(t *testing.T) {
	writer := &mockScheduleWriter{
		dhwResult: &mcp.TimeProgramWriteResult{
			Success: true,
			SlotResults: []mcp.TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0},
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, nil, writer), `mutation {
		setDhwTimeProgram(
			weekday: 5
			slots: "[{\"start_hour\":5,\"start_minute\":30,\"end_hour\":7,\"end_minute\":0}]"
		) {
			success
			error
			slotResults {
				slotIndex
				accepted
				errorCode
				errorDescription
			}
		}
	}`)

	payload, ok := data["setDhwTimeProgram"].(map[string]any)
	if !ok {
		t.Fatalf("setDhwTimeProgram payload type = %T; want map", data["setDhwTimeProgram"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("setDhwTimeProgram success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("setDhwTimeProgram error = %#v; want nil", got)
	}
	if len(writer.dhwCalls) != 1 {
		t.Fatalf("dhw writer calls = %d; want 1", len(writer.dhwCalls))
	}
	call := writer.dhwCalls[0]
	if call.weekday != 5 {
		t.Fatalf("dhw writer weekday = %d; want 5", call.weekday)
	}
	if len(call.slots) != 1 {
		t.Fatalf("dhw writer slots = %#v; want one slot", call.slots)
	}
	slot := call.slots[0]
	if slot.StartHour != 5 || slot.StartMinute != 30 || slot.EndHour != 7 || slot.EndMinute != 0 {
		t.Fatalf("dhw slot = %#v; want 05:30-07:00", slot)
	}
	if slot.TemperatureC != nil {
		t.Fatalf("dhw slot temperature = %#v; want nil", slot.TemperatureC)
	}
}

func TestSetZoneTimeProgramMutation_InvalidJSON(t *testing.T) {
	writer := &mockScheduleWriter{}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, nil, writer), `mutation {
		setZoneTimeProgram(zone: 1, weekday: 2, slots: "not-json") {
			success
			error
			slotResults {
				slotIndex
			}
		}
	}`)

	payload, ok := data["setZoneTimeProgram"].(map[string]any)
	if !ok {
		t.Fatalf("setZoneTimeProgram payload type = %T; want map", data["setZoneTimeProgram"])
	}
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("setZoneTimeProgram success = %v; want false", got)
	}
	errMessage, _ := payload["error"].(string)
	if !strings.Contains(errMessage, "invalid slots JSON") {
		t.Fatalf("setZoneTimeProgram error = %q; want invalid slots JSON", errMessage)
	}
	slotResults, ok := payload["slotResults"].([]any)
	if !ok || len(slotResults) != 0 {
		t.Fatalf("setZoneTimeProgram slotResults = %#v; want empty list", payload["slotResults"])
	}
	if len(writer.zoneCalls) != 0 {
		t.Fatalf("zone writer calls = %d; want 0", len(writer.zoneCalls))
	}
}

func buildMutationSchema(t *testing.T, registry InvokeRegistry, invoker Invoker, boilerWriter BoilerConfigWriter, scheduleWriter ScheduleWriter) graphqlgo.Schema {
	t.Helper()
	queryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Query",
		Fields: graphqlgo.Fields{
			"noop": &graphqlgo.Field{Type: graphqlgo.String},
		},
	})
	mutationType := buildMutationType(registry, invoker, boilerWriter, nil, scheduleWriter)
	schema, err := graphqlgo.NewSchema(graphqlgo.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
	if err != nil {
		t.Fatalf("NewSchema error = %v", err)
	}
	return schema
}

func executeMutation(t *testing.T, schema graphqlgo.Schema, request string) map[string]any {
	t.Helper()
	result := graphqlgo.Do(graphqlgo.Params{
		Schema:        schema,
		RequestString: request,
	})
	if len(result.Errors) > 0 {
		t.Fatalf("mutation errors = %+v", result.Errors)
	}
	data, ok := result.Data.(map[string]any)
	if !ok {
		t.Fatalf("result data type = %T; want map", result.Data)
	}
	return data
}

func testControllerEntryWithMethods(methodNames ...string) registry.DeviceEntry {
	methods := make([]registry.Method, 0, len(methodNames))
	for _, name := range methodNames {
		methods = append(methods, mockMethod{
			name:     name,
			readOnly: name == mutationGetExtRegisterMethod,
			template: mockTemplate{primary: 0xB5, secondary: 0x24},
		})
	}

	plane := &invokePlane{
		name:            mutationSystemPlane,
		source:          0x10,
		target:          0x15,
		hardwareVersion: "7603",
		methods:         methods,
	}

	return mockEntry{
		info: registry.DeviceInfo{
			Address:      0x15,
			Manufacturer: "Vaillant",
			DeviceID:     "BASV2",
		},
		planes: []registry.Plane{plane},
	}
}

type mutationTestRegistry struct {
	entries map[byte]registry.DeviceEntry
	order   []byte
}

func (reg mutationTestRegistry) Lookup(address byte) (registry.DeviceEntry, bool) {
	if reg.entries == nil {
		return nil, false
	}
	entry, ok := reg.entries[address]
	return entry, ok
}

func (reg mutationTestRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	if fn == nil || reg.entries == nil {
		return
	}
	for _, addr := range reg.order {
		entry, ok := reg.entries[addr]
		if !ok || entry == nil {
			continue
		}
		if !fn(entry) {
			return
		}
	}
}

type mutationInvokeCall struct {
	Plane  string
	Method string
	Params map[string]any
}

type mutationTestInvoker struct {
	calls     []mutationInvokeCall
	responses []any
	errs      []error
}

func (invoker *mutationTestInvoker) Invoke(_ context.Context, plane router.Plane, methodName string, params map[string]any) (any, error) {
	if invoker == nil {
		return nil, nil
	}
	copied := cloneMutationParams(params)
	planeName := ""
	if plane != nil {
		planeName = plane.Name()
	}
	invoker.calls = append(invoker.calls, mutationInvokeCall{
		Plane:  planeName,
		Method: methodName,
		Params: copied,
	})

	index := len(invoker.calls) - 1
	if index < len(invoker.errs) && invoker.errs[index] != nil {
		return nil, invoker.errs[index]
	}
	if index < len(invoker.responses) {
		return invoker.responses[index], nil
	}
	return map[string]types.Value{}, nil
}

func cloneMutationParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		if data, ok := value.([]byte); ok {
			cloned := make([]byte, len(data))
			copy(cloned, data)
			out[key] = cloned
			continue
		}
		out[key] = value
	}
	return out
}

var _ registry.FrameTemplate = schemaTemplate{}
var _ registry.FrameTemplate = buildTemplate{}
