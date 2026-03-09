package graphql

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/handler"
)

type InvokeRegistry interface {
	Lookup(address byte) (registry.DeviceEntry, bool)
}

type Invoker interface {
	Invoke(ctx context.Context, plane router.Plane, methodName string, params map[string]any) (any, error)
}

type InvokeResult struct {
	Ok     bool
	Error  *InvokeError
	Result any
}

type InvokeError struct {
	Message  string
	Code     string
	Category string
}

type BoilerConfigMutationResult struct {
	Success bool
	Error   string
}

type BoilerConfigWriter interface {
	SetBoilerConfig(ctx context.Context, fieldName string, rawValue string) BoilerConfigMutationResult
}

type ConfigMutationResult struct {
	Success bool
	Error   string
}

type configValueType int

const (
	configValueFloat32 configValueType = iota
	configValueUint16
	configValueBoolU8
	configValueEnumU16
)

type configFieldSpec struct {
	group     byte
	addr      uint16
	valueType configValueType
	min       float64
	max       float64
	enum      map[string]uint16
}

const (
	mutationControllerFallbackAddr = byte(0x15)
	mutationSourceAddr             = byte(0x31)
	mutationB524OpcodeLocal        = byte(0x02)
	mutationSystemPlane            = "system"
	mutationSetExtRegisterMethod   = "set_ext_register"
	mutationGetExtRegisterMethod   = "get_ext_register"
)

var circuitConfigFieldSpecs = map[string]configFieldSpec{
	"heatingCurve":    {group: 0x02, addr: 0x000F, valueType: configValueFloat32, min: 0.1, max: 4.0},
	"flowTempMaxC":    {group: 0x02, addr: 0x0010, valueType: configValueFloat32, min: 15.0, max: 80.0},
	"flowTempMinC":    {group: 0x02, addr: 0x0012, valueType: configValueFloat32, min: 5.0, max: 30.0},
	"summerLimitC":    {group: 0x02, addr: 0x0014, valueType: configValueFloat32, min: 15.0, max: 30.0},
	"frostProtC":      {group: 0x02, addr: 0x001D, valueType: configValueFloat32, min: -20.0, max: 10.0},
	"roomTempControl": {group: 0x02, addr: 0x0015, valueType: configValueEnumU16, enum: map[string]uint16{"off": 0, "modulating": 1, "thermostat": 2}},
	"coolingEnabled":  {group: 0x02, addr: 0x0006, valueType: configValueBoolU8},
}

var zoneConfigFieldSpecs = map[string]configFieldSpec{
	"operatingMode":        {group: 0x03, addr: 0x0006, valueType: configValueEnumU16, enum: map[string]uint16{"off": 0, "auto": 1, "manual": 2}},
	"quickVetoTemperature": {group: 0x03, addr: 0x0008, valueType: configValueFloat32, min: 5.0, max: 30.0},
	"quickVetoDuration":    {group: 0x03, addr: 0x0026, valueType: configValueFloat32, min: 0.5, max: 12.0},
	"desiredSetpoint":      {group: 0x03, addr: 0x0022, valueType: configValueFloat32, min: 5.0, max: 30.0},
}

var systemConfigFieldSpecs = map[string]configFieldSpec{
	"dhwBivalencePointC":   {group: 0x00, addr: 0x0001, valueType: configValueFloat32, min: -20.0, max: 50.0},
	"maxRoomHumidityPct":   {group: 0x00, addr: 0x000E, valueType: configValueUint16, min: 30, max: 80},
	"adaptiveHeatingCurve": {group: 0x00, addr: 0x0014, valueType: configValueBoolU8},
	"hcBivalencePointC":    {group: 0x00, addr: 0x0023, valueType: configValueFloat32, min: -20.0, max: 30.0},
	"hcEmergencyTempC":     {group: 0x00, addr: 0x0026, valueType: configValueFloat32, min: 20.0, max: 80.0},
	"hwcMaxFlowTempC":      {group: 0x00, addr: 0x0046, valueType: configValueFloat32, min: 15.0, max: 80.0},
}

type paramSchemaProvider interface {
	ParamSchema() schema.Schema
}

type paramBuilder interface {
	Build(params map[string]any) ([]byte, error)
}

func NewSchema(builder *Builder, registry InvokeRegistry, invoker Invoker, hub *BroadcastHub) (graphqlgo.Schema, error) {
	if builder == nil {
		return graphqlgo.Schema{}, fmt.Errorf("graphql schema missing builder: %w", ebuserrors.ErrInvalidPayload)
	}

	types := buildSchemaTypes()
	addEnergyTotalsToDevice(types.deviceType, types.energyTotals, builder)
	queryType := buildQueryType(builder, types)

	var mutationType *graphqlgo.Object
	if registry != nil && invoker != nil {
		mutationType = buildMutationType(registry, invoker, builder.boilerConfigWriter())
	}

	var subscriptionType *graphqlgo.Object
	if hub != nil {
		subscriptionType = buildSubscriptionType(hub, types)
	}

	return graphqlgo.NewSchema(graphqlgo.SchemaConfig{
		Query:        queryType,
		Mutation:     mutationType,
		Subscription: subscriptionType,
	})
}

func NewInvokeHandler(builder *Builder, registry InvokeRegistry, invoker Invoker) (http.Handler, error) {
	schema, err := NewSchema(builder, registry, invoker, nil)
	if err != nil {
		return nil, err
	}

	return handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false,
	}), nil
}

func buildMutationType(registry InvokeRegistry, invoker Invoker, boilerWriter BoilerConfigWriter) *graphqlgo.Object {
	jsonScalar := jsonScalarType()
	errorType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "InvokeError",
		Fields: graphqlgo.Fields{
			"message": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					err, ok := invokeErrorFromSource(params)
					if !ok {
						return nil, nil
					}
					return err.Message, nil
				},
			},
			"code": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					err, ok := invokeErrorFromSource(params)
					if !ok {
						return nil, nil
					}
					return err.Code, nil
				},
			},
			"category": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					err, ok := invokeErrorFromSource(params)
					if !ok {
						return nil, nil
					}
					return err.Category, nil
				},
			},
		},
	})

	resultType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "InvokeResult",
		Fields: graphqlgo.Fields{
			"ok": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := invokeResultFromSource(params)
					if !ok {
						return nil, nil
					}
					return result.Ok, nil
				},
			},
			"error": &graphqlgo.Field{
				Type: errorType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := invokeResultFromSource(params)
					if !ok {
						return nil, nil
					}
					return result.Error, nil
				},
			},
			"result": &graphqlgo.Field{
				Type: jsonScalar,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := invokeResultFromSource(params)
					if !ok {
						return nil, nil
					}
					return result.Result, nil
				},
			},
		},
	})

	boilerConfigResultType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BoilerConfigMutationResult",
		Fields: graphqlgo.Fields{
			"success": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := boilerConfigResultFromSource(params)
					if !ok {
						return false, nil
					}
					return result.Success, nil
				},
			},
			"error": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := boilerConfigResultFromSource(params)
					if !ok || result.Error == "" {
						return nil, nil
					}
					return result.Error, nil
				},
			},
		},
	})

	configResultType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ConfigMutationResult",
		Fields: graphqlgo.Fields{
			"success": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := configResultFromSource(params)
					if !ok {
						return false, nil
					}
					return result.Success, nil
				},
			},
			"error": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := configResultFromSource(params)
					if !ok || result.Error == "" {
						return nil, nil
					}
					return result.Error, nil
				},
			},
		},
	})

	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Mutation",
		Fields: graphqlgo.Fields{
			"invoke": &graphqlgo.Field{
				Type: resultType,
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"plane":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"method":  &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"params":  &graphqlgo.ArgumentConfig{Type: jsonScalar},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, err := invokeResolve(params, registry, invoker)
					if err != nil {
						return InvokeResult{
							Ok:    false,
							Error: mapInvokeError(err),
						}, nil
					}
					return result, nil
				},
			},
			"setBoilerConfig": &graphqlgo.Field{
				Type: boilerConfigResultType,
				Args: graphqlgo.FieldConfigArgument{
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if boilerWriter != nil {
						fieldName, _ := params.Args["field"].(string)
						fieldValue, _ := params.Args["value"].(string)
						return boilerWriter.SetBoilerConfig(params.Context, fieldName, fieldValue), nil
					}
					return boilerConfigUnsupportedResult(), nil
				},
			},
			"setCircuitConfig": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"index": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setCircuitConfigResolve(params, registry, invoker), nil
				},
			},
			"setSystemConfig": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setSystemConfigResolve(params, registry, invoker), nil
				},
			},
			"setZoneConfig": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"index": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setZoneConfigResolve(params, registry, invoker), nil
				},
			},
		},
	})
}

func boilerConfigUnsupportedResult() BoilerConfigMutationResult {
	return BoilerConfigMutationResult{
		Success: false,
		Error:   "unsupported source: B509 writes are disabled in reduced profile",
	}
}

func setCircuitConfigResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker) ConfigMutationResult {
	instance, err := parseConfigInstance(params.Args["index"])
	if err != nil {
		return configMutationError(err)
	}
	fieldName, _ := params.Args["field"].(string)
	fieldValue, _ := params.Args["value"].(string)
	spec, err := resolveConfigFieldSpec("circuit", fieldName, circuitConfigFieldSpecs)
	if err != nil {
		return configMutationError(err)
	}
	return applyConfigMutation(params.Context, registry, invoker, spec, instance, fieldValue)
}

func setSystemConfigResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker) ConfigMutationResult {
	fieldName, _ := params.Args["field"].(string)
	fieldValue, _ := params.Args["value"].(string)
	spec, err := resolveConfigFieldSpec("system", fieldName, systemConfigFieldSpecs)
	if err != nil {
		return configMutationError(err)
	}
	return applyConfigMutation(params.Context, registry, invoker, spec, 0x00, fieldValue)
}

func setZoneConfigResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker) ConfigMutationResult {
	instance, err := parseConfigInstance(params.Args["index"])
	if err != nil {
		return configMutationError(err)
	}
	fieldName, _ := params.Args["field"].(string)
	fieldValue, _ := params.Args["value"].(string)
	spec, err := resolveConfigFieldSpec("zone", fieldName, zoneConfigFieldSpecs)
	if err != nil {
		return configMutationError(err)
	}
	return applyConfigMutation(params.Context, registry, invoker, spec, instance, fieldValue)
}

func applyConfigMutation(ctx context.Context, registry InvokeRegistry, invoker Invoker, spec configFieldSpec, instance byte, rawValue string) ConfigMutationResult {
	if registry == nil || invoker == nil {
		return configMutationError(fmt.Errorf("config mutation missing dependencies: %w", ebuserrors.ErrInvalidPayload))
	}

	data, err := encodeConfigValue(spec, rawValue)
	if err != nil {
		return configMutationError(err)
	}

	plane, err := resolveControllerSystemPlane(registry)
	if err != nil {
		return configMutationError(err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	writeParams := map[string]any{
		"source":   mutationSourceAddr,
		"opcode":   mutationB524OpcodeLocal,
		"group":    spec.group,
		"instance": instance,
		"addr":     spec.addr,
		"data":     data,
	}
	if _, err := invoker.Invoke(ctx, plane, mutationSetExtRegisterMethod, writeParams); err != nil {
		return configMutationError(fmt.Errorf("set_ext_register failed: %w", err))
	}

	readParams := map[string]any{
		"source":   mutationSourceAddr,
		"opcode":   mutationB524OpcodeLocal,
		"group":    spec.group,
		"instance": instance,
		"addr":     spec.addr,
	}
	readResult, err := invoker.Invoke(ctx, plane, mutationGetExtRegisterMethod, readParams)
	if err != nil {
		return configMutationError(fmt.Errorf("get_ext_register failed: %w", err))
	}

	readValue, err := extractExtRegisterValue(readResult)
	if err != nil {
		return configMutationError(fmt.Errorf("write confirm failed: %w", err))
	}
	if err := confirmDecodableReadback(spec, readValue); err != nil {
		return configMutationError(fmt.Errorf("write confirm failed: %w", err))
	}
	if !configReadbackMatchesWrite(spec, data, readValue) {
		return configMutationError(fmt.Errorf("write confirm failed: read-back mismatch: %w", ebuserrors.ErrInvalidPayload))
	}

	return ConfigMutationResult{Success: true}
}

func configMutationError(err error) ConfigMutationResult {
	if err == nil {
		return ConfigMutationResult{Success: false, Error: "config mutation failed"}
	}
	return ConfigMutationResult{Success: false, Error: err.Error()}
}

func parseConfigInstance(raw any) (byte, error) {
	index, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("invalid circuit index: %w", ebuserrors.ErrInvalidPayload)
	}
	if index < 0 || index > 0xFF {
		return 0, fmt.Errorf("circuit index out of range: %w", ebuserrors.ErrInvalidPayload)
	}
	return byte(index), nil
}

func resolveConfigFieldSpec(scope, fieldName string, specs map[string]configFieldSpec) (configFieldSpec, error) {
	if spec, ok := specs[fieldName]; ok {
		return spec, nil
	}
	allowed := make([]string, 0, len(specs))
	for key := range specs {
		allowed = append(allowed, key)
	}
	sort.Strings(allowed)
	return configFieldSpec{}, fmt.Errorf("unknown %s field %q (allowed: %s): %w", scope, fieldName, strings.Join(allowed, ", "), ebuserrors.ErrInvalidPayload)
}

func encodeConfigValue(spec configFieldSpec, raw string) ([]byte, error) {
	switch spec.valueType {
	case configValueFloat32:
		value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float value %q: %w", raw, ebuserrors.ErrInvalidPayload)
		}
		if value < spec.min || value > spec.max {
			return nil, fmt.Errorf("value %.4g out of range [%.4g, %.4g]: %w", value, spec.min, spec.max, ebuserrors.ErrInvalidPayload)
		}
		payload := make([]byte, 4)
		binary.LittleEndian.PutUint32(payload, math.Float32bits(float32(value)))
		return payload, nil
	case configValueUint16:
		value, err := parseIntegerString(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid integer value %q: %w", raw, ebuserrors.ErrInvalidPayload)
		}
		if float64(value) < spec.min || float64(value) > spec.max {
			return nil, fmt.Errorf("value %d out of range [%.0f, %.0f]: %w", value, spec.min, spec.max, ebuserrors.ErrInvalidPayload)
		}
		payload := make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, uint16(value))
		return payload, nil
	case configValueBoolU8:
		parsed, ok := parseBoolString(raw)
		if !ok {
			return nil, fmt.Errorf("invalid boolean value %q (expected true/false): %w", raw, ebuserrors.ErrInvalidPayload)
		}
		if parsed {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case configValueEnumU16:
		key := strings.ToLower(strings.TrimSpace(raw))
		if spec.enum == nil {
			return nil, fmt.Errorf("missing enum mapping: %w", ebuserrors.ErrInvalidPayload)
		}
		mapped, ok := spec.enum[key]
		if !ok {
			return nil, fmt.Errorf("invalid enum value %q (allowed: %s): %w", raw, strings.Join(sortedEnumKeys(spec.enum), ", "), ebuserrors.ErrInvalidPayload)
		}
		payload := make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, mapped)
		return payload, nil
	default:
		return nil, fmt.Errorf("unsupported value encoding: %w", ebuserrors.ErrInvalidPayload)
	}
}

func parseBoolString(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1", "on", "yes":
		return true, true
	case "false", "0", "off", "no":
		return false, true
	default:
		return false, false
	}
}

func parseIntegerString(raw string) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("empty integer")
	}
	if strings.ContainsAny(raw, ".eE") {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, err
		}
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
			return 0, fmt.Errorf("not an integer")
		}
		return int(value), nil
	}
	return strconv.Atoi(raw)
}

func sortedEnumKeys(values map[string]uint16) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func resolveControllerSystemPlane(reg InvokeRegistry) (router.Plane, error) {
	entry, err := findControllerEntry(reg)
	if err != nil {
		return nil, err
	}

	plane, ok := findRegistryPlane(entry.Planes(), mutationSystemPlane)
	if !ok {
		return nil, fmt.Errorf("controller missing system plane: %w", ebuserrors.ErrInvalidPayload)
	}

	if _, ok := findRegistryMethod(plane.Methods(), mutationSetExtRegisterMethod); !ok {
		return nil, fmt.Errorf("controller missing method %q: %w", mutationSetExtRegisterMethod, ebuserrors.ErrInvalidPayload)
	}
	if _, ok := findRegistryMethod(plane.Methods(), mutationGetExtRegisterMethod); !ok {
		return nil, fmt.Errorf("controller missing method %q: %w", mutationGetExtRegisterMethod, ebuserrors.ErrInvalidPayload)
	}

	routerPlane, ok := plane.(router.Plane)
	if !ok {
		return nil, fmt.Errorf("controller system plane not routable: %w", ebuserrors.ErrInvalidPayload)
	}
	return routerPlane, nil
}

func findControllerEntry(reg InvokeRegistry) (registry.DeviceEntry, error) {
	if reg == nil {
		return nil, fmt.Errorf("registry missing: %w", ebuserrors.ErrInvalidPayload)
	}

	type iterativeRegistry interface {
		Iterate(func(registry.DeviceEntry) bool)
	}
	if iter, ok := reg.(iterativeRegistry); ok {
		var controller registry.DeviceEntry
		iter.Iterate(func(entry registry.DeviceEntry) bool {
			if entry == nil {
				return true
			}
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(entry.DeviceID())), "BASV") {
				controller = entry
				return false
			}
			return true
		})
		if controller != nil {
			return controller, nil
		}
	}

	if fallback, ok := reg.Lookup(mutationControllerFallbackAddr); ok && fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("controller BASV2 not found: %w", ebuserrors.ErrInvalidPayload)
}

func extractExtRegisterValue(result any) ([]byte, error) {
	switch typed := result.(type) {
	case map[string]types.Value:
		value, ok := typed["value"]
		if !ok || !value.Valid {
			return nil, fmt.Errorf("read result missing value: %w", ebuserrors.ErrInvalidPayload)
		}
		bytes, ok := toByteSlice(value.Value)
		if !ok || len(bytes) == 0 {
			return nil, fmt.Errorf("read value missing payload bytes: %w", ebuserrors.ErrInvalidPayload)
		}
		return bytes, nil
	case map[string]any:
		value, ok := typed["value"]
		if !ok || value == nil {
			return nil, fmt.Errorf("read result missing value: %w", ebuserrors.ErrInvalidPayload)
		}
		bytes, ok := toByteSlice(value)
		if !ok || len(bytes) == 0 {
			return nil, fmt.Errorf("read value missing payload bytes: %w", ebuserrors.ErrInvalidPayload)
		}
		return bytes, nil
	default:
		return nil, fmt.Errorf("unexpected read result type %T: %w", result, ebuserrors.ErrInvalidPayload)
	}
}

func toByteSlice(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case []byte:
		out := make([]byte, len(typed))
		copy(out, typed)
		return out, true
	case string:
		decoded, err := hex.DecodeString(strings.TrimSpace(typed))
		if err != nil || len(decoded) == 0 {
			return nil, false
		}
		return decoded, true
	case []any:
		out := make([]byte, len(typed))
		for i, entry := range typed {
			number, ok := toByte(entry)
			if !ok {
				return nil, false
			}
			out[i] = number
		}
		return out, true
	default:
		rv := reflect.ValueOf(value)
		if !rv.IsValid() {
			return nil, false
		}
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return nil, false
		}
		out := make([]byte, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			number, ok := toByte(rv.Index(i).Interface())
			if !ok {
				return nil, false
			}
			out[i] = number
		}
		return out, true
	}
}

func toByte(value any) (byte, bool) {
	switch typed := value.(type) {
	case byte:
		return typed, true
	case int:
		if typed < 0 || typed > 0xFF {
			return 0, false
		}
		return byte(typed), true
	case int64:
		if typed < 0 || typed > 0xFF {
			return 0, false
		}
		return byte(typed), true
	case float64:
		if typed < 0 || typed > 0xFF || math.Trunc(typed) != typed {
			return 0, false
		}
		return byte(typed), true
	default:
		return 0, false
	}
}

func confirmDecodableReadback(spec configFieldSpec, payload []byte) error {
	switch spec.valueType {
	case configValueFloat32:
		if len(payload) < 4 {
			return fmt.Errorf("float32 payload too short: %w", ebuserrors.ErrInvalidPayload)
		}
		raw := binary.LittleEndian.Uint32(payload[:4])
		value := float64(math.Float32frombits(raw))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("float32 payload invalid: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case configValueUint16:
		_, ok := decodePayloadUint16(payload)
		if !ok {
			return fmt.Errorf("uint16 payload invalid: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case configValueBoolU8:
		if len(payload) < 1 {
			return fmt.Errorf("bool payload too short: %w", ebuserrors.ErrInvalidPayload)
		}
		if payload[0] > 1 {
			return fmt.Errorf("bool payload out of range: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case configValueEnumU16:
		value, ok := decodePayloadUint16(payload)
		if !ok {
			return fmt.Errorf("enum payload invalid: %w", ebuserrors.ErrInvalidPayload)
		}
		for _, candidate := range spec.enum {
			if value == candidate {
				return nil
			}
		}
		return fmt.Errorf("enum payload unknown value %d: %w", value, ebuserrors.ErrInvalidPayload)
	default:
		return fmt.Errorf("unsupported readback decode: %w", ebuserrors.ErrInvalidPayload)
	}
}

func configReadbackMatchesWrite(spec configFieldSpec, written, readback []byte) bool {
	switch spec.valueType {
	case configValueFloat32:
		if len(written) < 4 || len(readback) < 4 {
			return false
		}
		return binary.LittleEndian.Uint32(readback[:4]) == binary.LittleEndian.Uint32(written[:4])
	case configValueUint16, configValueEnumU16:
		want, okWant := decodePayloadUint16(written)
		got, okGot := decodePayloadUint16(readback)
		return okWant && okGot && want == got
	case configValueBoolU8:
		return len(written) > 0 && len(readback) > 0 && (readback[0] == 0 || readback[0] == 1) && readback[0] == written[0]
	default:
		return false
	}
}

func decodePayloadUint16(payload []byte) (uint16, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	if len(payload) == 1 {
		return uint16(payload[0]), true
	}
	return binary.LittleEndian.Uint16(payload[:2]), true
}

func invokeResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker) (InvokeResult, error) {
	if registry == nil || invoker == nil {
		return InvokeResult{}, fmt.Errorf("graphql invoke missing dependencies: %w", ebuserrors.ErrInvalidPayload)
	}

	address, err := parseAddress(params.Args["address"])
	if err != nil {
		return InvokeResult{}, err
	}
	planeName, _ := params.Args["plane"].(string)
	methodName, _ := params.Args["method"].(string)

	paramMap, err := parseParams(params.Args["params"])
	if err != nil {
		return InvokeResult{}, err
	}

	entry, ok := registry.Lookup(address)
	if !ok || entry == nil {
		return InvokeResult{}, fmt.Errorf("graphql invoke missing device 0x%02x: %w", address, ebuserrors.ErrInvalidPayload)
	}

	plane, ok := findRegistryPlane(entry.Planes(), planeName)
	if !ok {
		return InvokeResult{}, fmt.Errorf("graphql invoke missing plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	routerPlane, ok := plane.(router.Plane)
	if !ok {
		return InvokeResult{}, fmt.Errorf("graphql invoke plane %q not routable: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	method, ok := findRegistryMethod(plane.Methods(), methodName)
	if !ok {
		return InvokeResult{}, fmt.Errorf("graphql invoke missing method %q: %w", methodName, ebuserrors.ErrInvalidPayload)
	}

	normalizedParams, err := validateInvokeParams(method, paramMap)
	if err != nil {
		return InvokeResult{}, err
	}

	ctx := params.Context
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := invoker.Invoke(ctx, routerPlane, methodName, normalizedParams)
	if err != nil {
		return InvokeResult{}, err
	}

	return InvokeResult{
		Ok:     true,
		Result: normalizeInvokeResult(result),
	}, nil
}

func parseParams(raw any) (map[string]any, error) {
	if raw == nil {
		return map[string]any{}, nil
	}
	if params, ok := raw.(map[string]any); ok {
		return params, nil
	}
	return nil, fmt.Errorf("graphql invoke invalid params: %w", ebuserrors.ErrInvalidPayload)
}

func validateInvokeParams(method registry.Method, params map[string]any) (map[string]any, error) {
	if method == nil {
		return nil, fmt.Errorf("graphql invoke missing method: %w", ebuserrors.ErrInvalidPayload)
	}
	if params == nil {
		params = map[string]any{}
	}

	template := method.Template()
	if template == nil {
		return nil, fmt.Errorf("graphql invoke missing template: %w", ebuserrors.ErrInvalidPayload)
	}

	if schemaProvider, ok := template.(paramSchemaProvider); ok {
		normalized, err := normalizeSchemaParams(schemaProvider.ParamSchema(), params)
		if err != nil {
			return nil, err
		}
		return normalized, nil
	}

	if builder, ok := template.(paramBuilder); ok {
		return validateBuildParams(builder, params)
	}

	if len(params) > 0 {
		return nil, fmt.Errorf("graphql invoke unexpected params: %w", ebuserrors.ErrInvalidPayload)
	}

	return params, nil
}

func normalizeSchemaParams(s schema.Schema, params map[string]any) (map[string]any, error) {
	if params == nil {
		return nil, fmt.Errorf("graphql invoke missing params: %w", ebuserrors.ErrInvalidPayload)
	}

	allowed := make(map[string]struct{}, len(s.Fields))
	out := make(map[string]any, len(s.Fields))
	for _, field := range s.Fields {
		if field.Type == nil {
			return nil, fmt.Errorf("graphql invoke field %q missing type: %w", field.Name, ebuserrors.ErrInvalidPayload)
		}
		allowed[field.Name] = struct{}{}
		value, ok := params[field.Name]
		if !ok {
			return nil, fmt.Errorf("graphql invoke field %q missing value: %w", field.Name, ebuserrors.ErrInvalidPayload)
		}

		if _, err := field.Type.Encode(value); err == nil {
			out[field.Name] = value
			continue
		}

		if coerced, ok := coerceFloatToInt(value); ok {
			if _, err := field.Type.Encode(coerced); err == nil {
				out[field.Name] = coerced
				continue
			}
		}

		return nil, fmt.Errorf("graphql invoke field %q invalid value: %w", field.Name, ebuserrors.ErrInvalidPayload)
	}

	for key := range params {
		if _, ok := allowed[key]; !ok {
			return nil, fmt.Errorf("graphql invoke field %q unexpected: %w", key, ebuserrors.ErrInvalidPayload)
		}
	}

	if _, err := s.Encode(out); err != nil {
		return nil, fmt.Errorf("graphql invoke params encode: %w", err)
	}

	return out, nil
}

func validateBuildParams(builder paramBuilder, params map[string]any) (map[string]any, error) {
	if _, err := builder.Build(params); err == nil {
		return params, nil
	}

	normalized := normalizeIntParams(params)
	if normalized == nil {
		return nil, fmt.Errorf("graphql invoke params invalid: %w", ebuserrors.ErrInvalidPayload)
	}
	if _, err := builder.Build(normalized); err != nil {
		return nil, fmt.Errorf("graphql invoke params invalid: %w", ebuserrors.ErrInvalidPayload)
	}
	return normalized, nil
}

func normalizeIntParams(params map[string]any) map[string]any {
	if params == nil {
		return nil
	}
	out := make(map[string]any, len(params))
	changed := false
	for key, value := range params {
		if coerced, ok := coerceFloatToInt(value); ok {
			out[key] = coerced
			changed = true
			continue
		}
		out[key] = value
	}
	if !changed {
		return params
	}
	return out
}

func coerceFloatToInt(value any) (int64, bool) {
	floatValue, ok := value.(float64)
	if !ok {
		return 0, false
	}
	if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
		return 0, false
	}
	if math.Trunc(floatValue) != floatValue {
		return 0, false
	}
	if floatValue > float64(math.MaxInt64) || floatValue < float64(math.MinInt64) {
		return 0, false
	}
	return int64(floatValue), true
}

func findRegistryPlane(planes []registry.Plane, name string) (registry.Plane, bool) {
	for _, plane := range planes {
		if plane.Name() == name {
			return plane, true
		}
	}
	return nil, false
}

func findRegistryMethod(methods []registry.Method, name string) (registry.Method, bool) {
	for _, method := range methods {
		if method.Name() == name {
			return method, true
		}
	}
	return nil, false
}

func mapInvokeError(err error) *InvokeError {
	if err == nil {
		return nil
	}

	code := "UNKNOWN"
	category := "UNKNOWN"

	switch {
	case errors.Is(err, ebuserrors.ErrInvalidPayload):
		code = "INVALID_PAYLOAD"
		category = "INVALID"
	case errors.Is(err, ebuserrors.ErrNoSuchDevice):
		code = "NO_SUCH_DEVICE"
		category = "DEFINITIVE"
	case errors.Is(err, ebuserrors.ErrNACK):
		code = "NACK"
		category = "DEFINITIVE"
	case errors.Is(err, ebuserrors.ErrTimeout):
		code = "TIMEOUT"
		category = "TRANSIENT"
	case errors.Is(err, ebuserrors.ErrBusCollision):
		code = "BUS_COLLISION"
		category = "TRANSIENT"
	case errors.Is(err, ebuserrors.ErrRetryExhausted):
		code = "RETRY_EXHAUSTED"
		category = "TRANSIENT"
	case errors.Is(err, ebuserrors.ErrCRCMismatch):
		code = "CRC_MISMATCH"
		category = "TRANSIENT"
	case errors.Is(err, ebuserrors.ErrTransportClosed):
		code = "TRANSPORT_CLOSED"
		category = "FATAL"
	}

	if category == "UNKNOWN" {
		switch {
		case ebuserrors.IsTransient(err):
			category = "TRANSIENT"
		case ebuserrors.IsDefinitive(err):
			category = "DEFINITIVE"
		case ebuserrors.IsFatal(err):
			category = "FATAL"
		}
	}

	return &InvokeError{
		Message:  err.Error(),
		Code:     code,
		Category: category,
	}
}

func normalizeInvokeResult(result any) any {
	switch typed := result.(type) {
	case map[string]types.Value:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			if value.Valid {
				out[key] = value.Value
			} else {
				out[key] = nil
			}
		}
		return out
	default:
		return result
	}
}

func invokeResultFromSource(params graphqlgo.ResolveParams) (InvokeResult, bool) {
	switch value := params.Source.(type) {
	case InvokeResult:
		return value, true
	case *InvokeResult:
		if value == nil {
			return InvokeResult{}, false
		}
		return *value, true
	default:
		return InvokeResult{}, false
	}
}

func invokeErrorFromSource(params graphqlgo.ResolveParams) (InvokeError, bool) {
	switch value := params.Source.(type) {
	case InvokeError:
		return value, true
	case *InvokeError:
		if value == nil {
			return InvokeError{}, false
		}
		return *value, true
	default:
		return InvokeError{}, false
	}
}

func boilerConfigResultFromSource(params graphqlgo.ResolveParams) (BoilerConfigMutationResult, bool) {
	switch value := params.Source.(type) {
	case BoilerConfigMutationResult:
		return value, true
	case *BoilerConfigMutationResult:
		if value == nil {
			return BoilerConfigMutationResult{}, false
		}
		return *value, true
	default:
		return BoilerConfigMutationResult{}, false
	}
}

func configResultFromSource(params graphqlgo.ResolveParams) (ConfigMutationResult, bool) {
	switch value := params.Source.(type) {
	case ConfigMutationResult:
		return value, true
	case *ConfigMutationResult:
		if value == nil {
			return ConfigMutationResult{}, false
		}
		return *value, true
	default:
		return ConfigMutationResult{}, false
	}
}

func jsonScalarType() *graphqlgo.Scalar {
	return jsonScalar
}

var jsonScalar = graphqlgo.NewScalar(graphqlgo.ScalarConfig{
	Name: "JSON",
	Serialize: func(value any) any {
		return value
	},
	ParseValue: func(value any) any {
		return value
	},
	ParseLiteral: func(valueAST ast.Value) any {
		return parseJSONLiteral(valueAST)
	},
},
)

func parseJSONLiteral(valueAST ast.Value) any {
	switch value := valueAST.(type) {
	case *ast.ObjectValue:
		out := make(map[string]any, len(value.Fields))
		for _, field := range value.Fields {
			out[field.Name.Value] = parseJSONLiteral(field.Value)
		}
		return out
	case *ast.ListValue:
		out := make([]any, len(value.Values))
		for i, entry := range value.Values {
			out[i] = parseJSONLiteral(entry)
		}
		return out
	case *ast.StringValue:
		return value.Value
	case *ast.IntValue:
		parsed, err := parseIntValue(value.Value)
		if err != nil {
			return nil
		}
		return parsed
	case *ast.FloatValue:
		parsed, err := parseFloatValue(value.Value)
		if err != nil {
			return nil
		}
		return parsed
	case *ast.BooleanValue:
		return value.Value
	default:
		return nil
	}
}

func parseIntValue(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}

func parseFloatValue(raw string) (float64, error) {
	return strconv.ParseFloat(raw, 64)
}
