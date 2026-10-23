package graphql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
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

type SystemConfigWriter interface {
	SetSystemConfig(ctx context.Context, fieldName string, rawValue string) ConfigMutationResult
}

type ScheduleWriter interface {
	SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error)
	SetDhwTimeProgram(ctx context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error)
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
	configValueCString
	configValueDateHDA3
)

// ConfigFieldSpec describes a B524 register for config mutations.
type ConfigFieldSpec = configFieldSpec

type configFieldSpec struct {
	conversionErr error
	group         byte
	addr          uint16
	valueType     configValueType
	min           float64
	max           float64
	maxLen        int
	enum          map[string]uint16
}

const (
	mutationControllerFallbackAddr = byte(0x15)
	mutationB524OpcodeLocal        = systemconfig.OpcodeLocal
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
	"holidaySetpoint":      {group: 0x03, addr: 0x0005, valueType: configValueFloat32, min: 5.0, max: 30.0},
}

var systemConfigFieldSpecs = SystemConfigFieldSpecs()

func adaptSystemConfigSpec(s systemconfig.ConfigFieldSpec) configFieldSpec {
	out := configFieldSpec{group: s.RegisterGroup, addr: s.RegisterAddr, min: s.Min, max: s.Max, maxLen: s.MaxLen, enum: s.Enum}
	switch s.ValueType {
	case systemconfig.ValueFloat32:
		out.valueType = configValueFloat32
	case systemconfig.ValueUint16:
		out.valueType = configValueUint16
	case systemconfig.ValueBoolU8:
		out.valueType = configValueBoolU8
	case systemconfig.ValueEnumU16:
		out.valueType = configValueEnumU16
	case systemconfig.ValueCString:
		out.valueType = configValueCString
	case systemconfig.ValueDateHDA3:
		out.valueType = configValueDateHDA3
	default:
		out.conversionErr = fmt.Errorf("unsupported native value type %d: %w", s.ValueType, ebuserrors.ErrInvalidPayload)
	}
	return out
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
		mutationType = buildMutationType(registry, invoker, builder.boilerConfigWriter(), builder.systemConfigWriter(), builder.scheduleWriter(), builder.mutationSourceForRequest, builder.systemConfigBound)
		// The one generic semantic operation route; it reads the builder's
		// injected router per request and never uses the eBUS invoker.
		mutationType.AddFieldConfig(semanticOperationFieldName, semanticOperationField(builder))
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

	return guardSemanticOperationRequests(handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false,
		RootObjectFn: func(_ context.Context, _ *http.Request) map[string]interface{} {
			return newGraphQLRootObject(builder)
		},
	})), nil
}

func buildMutationType(registry InvokeRegistry, invoker Invoker, boilerWriter BoilerConfigWriter, systemWriter SystemConfigWriter, scheduleWriter ScheduleWriter, mutationSourceProvider func() (byte, bool), boundProviders ...func() *ConfigBound) *graphqlgo.Object {
	if mutationSourceProvider == nil {
		mutationSourceProvider = func() (byte, bool) { return 0, false }
	}
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

	scheduleSlotResultType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ScheduleSlotResult",
		Fields: graphqlgo.Fields{
			"slotIndex": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleSlotResultFromSource(params)
					if !ok {
						return 0, nil
					}
					return result.SlotIndex, nil
				},
			},
			"accepted": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleSlotResultFromSource(params)
					if !ok {
						return false, nil
					}
					return result.Accepted, nil
				},
			},
			"errorCode": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleSlotResultFromSource(params)
					if !ok {
						return 0, nil
					}
					return result.ErrorCode, nil
				},
			},
			"errorDescription": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleSlotResultFromSource(params)
					if !ok || result.ErrorDesc == "" {
						return nil, nil
					}
					return result.ErrorDesc, nil
				},
			},
		},
	})

	scheduleWriteResultType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ScheduleWriteResult",
		Fields: graphqlgo.Fields{
			"success": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleWriteResultFromSource(params)
					if !ok {
						return false, nil
					}
					return result.Success, nil
				},
			},
			"slotResults": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(scheduleSlotResultType)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleWriteResultFromSource(params)
					if !ok {
						return nil, nil
					}
					return result.SlotResults, nil
				},
			},
			"error": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					result, ok := scheduleWriteResultFromSource(params)
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
					source, admitted := mutationSourceProvider()
					result, err := invokeResolve(params, registry, invoker, source, admitted)
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
			"set_boiler_config": &graphqlgo.Field{
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
					mutationSource, mutationSourceAdmitted := mutationSourceProvider()
					return setCircuitConfigResolve(params, registry, invoker, mutationSource, mutationSourceAdmitted), nil
				},
			},
			"set_circuit_config": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"index": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					mutationSource, mutationSourceAdmitted := mutationSourceProvider()
					return setCircuitConfigResolve(params, registry, invoker, mutationSource, mutationSourceAdmitted), nil
				},
			},
			"setSystemConfig": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					mutationSource, mutationSourceAdmitted := mutationSourceProvider()
					return setSystemConfigResolve(params, registry, invoker, systemWriter, mutationSource, mutationSourceAdmitted, boundProviders...), nil
				},
			},
			"set_system_config": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					mutationSource, mutationSourceAdmitted := mutationSourceProvider()
					return setSystemConfigResolve(params, registry, invoker, systemWriter, mutationSource, mutationSourceAdmitted, boundProviders...), nil
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
					mutationSource, mutationSourceAdmitted := mutationSourceProvider()
					return setZoneConfigResolve(params, registry, invoker, mutationSource, mutationSourceAdmitted), nil
				},
			},
			"set_zone_config": &graphqlgo.Field{
				Type: configResultType,
				Args: graphqlgo.FieldConfigArgument{
					"index": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"field": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
					"value": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					mutationSource, mutationSourceAdmitted := mutationSourceProvider()
					return setZoneConfigResolve(params, registry, invoker, mutationSource, mutationSourceAdmitted), nil
				},
			},
			"setZoneTimeProgram": &graphqlgo.Field{
				Type: scheduleWriteResultType,
				Args: graphqlgo.FieldConfigArgument{
					"zone":    &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"weekday": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"slots":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setZoneTimeProgramResolve(params, scheduleWriter), nil
				},
			},
			"set_zone_time_program": &graphqlgo.Field{
				Type: scheduleWriteResultType,
				Args: graphqlgo.FieldConfigArgument{
					"zone":    &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"weekday": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"slots":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setZoneTimeProgramResolve(params, scheduleWriter), nil
				},
			},
			"setDhwTimeProgram": &graphqlgo.Field{
				Type: scheduleWriteResultType,
				Args: graphqlgo.FieldConfigArgument{
					"weekday": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"slots":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setDhwTimeProgramResolve(params, scheduleWriter), nil
				},
			},
			"set_dhw_time_program": &graphqlgo.Field{
				Type: scheduleWriteResultType,
				Args: graphqlgo.FieldConfigArgument{
					"weekday": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"slots":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return setDhwTimeProgramResolve(params, scheduleWriter), nil
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

func setCircuitConfigResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker, mutationSource byte, mutationSourceAdmitted bool) ConfigMutationResult {
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
	return applyConfigMutation(params.Context, registry, invoker, mutationSource, mutationSourceAdmitted, spec, instance, fieldValue)
}

func setSystemConfigResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker, systemWriter SystemConfigWriter, mutationSource byte, mutationSourceAdmitted bool, boundProviders ...func() *ConfigBound) ConfigMutationResult {
	fieldName, _ := params.Args["field"].(string)
	fieldValue, _ := params.Args["value"].(string)
	if systemWriter != nil {
		ctx := params.Context
		if ctx == nil {
			ctx = context.Background()
		}
		return systemWriter.SetSystemConfig(ctx, fieldName, fieldValue)
	}
	return applySystemFieldMutation(params.Context, registry, invoker, mutationSource, mutationSourceAdmitted, fieldName, fieldValue, systemConfigFieldSpecs, suppliedSystemConfigBound(boundProviders))
}

func setZoneConfigResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker, mutationSource byte, mutationSourceAdmitted bool) ConfigMutationResult {
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
	return applyConfigMutation(params.Context, registry, invoker, mutationSource, mutationSourceAdmitted, spec, instance, fieldValue)
}

func setZoneTimeProgramResolve(params graphqlgo.ResolveParams, scheduleWriter ScheduleWriter) *mcp.TimeProgramWriteResult {
	if scheduleWriter == nil {
		return scheduleWriteMutationError(fmt.Errorf("schedule mutation missing writer: %w", ebuserrors.ErrInvalidPayload))
	}

	zone, err := parseScheduleMutationIntArg(params.Args["zone"], "zone")
	if err != nil {
		return scheduleWriteMutationError(err)
	}
	weekday, err := parseScheduleMutationIntArg(params.Args["weekday"], "weekday")
	if err != nil {
		return scheduleWriteMutationError(err)
	}
	rawSlots, _ := params.Args["slots"].(string)
	slots, err := parseTimeProgramSlotsJSON(rawSlots, true)
	if err != nil {
		return scheduleWriteMutationError(err)
	}

	ctx := params.Context
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := scheduleWriter.SetZoneTimeProgram(ctx, zone, weekday, slots)
	if err != nil {
		return scheduleWriteMutationError(err)
	}
	if result == nil {
		return scheduleWriteMutationError(fmt.Errorf("schedule writer returned nil result: %w", ebuserrors.ErrInvalidPayload))
	}
	return result
}

func setDhwTimeProgramResolve(params graphqlgo.ResolveParams, scheduleWriter ScheduleWriter) *mcp.TimeProgramWriteResult {
	if scheduleWriter == nil {
		return scheduleWriteMutationError(fmt.Errorf("schedule mutation missing writer: %w", ebuserrors.ErrInvalidPayload))
	}

	weekday, err := parseScheduleMutationIntArg(params.Args["weekday"], "weekday")
	if err != nil {
		return scheduleWriteMutationError(err)
	}
	rawSlots, _ := params.Args["slots"].(string)
	slots, err := parseTimeProgramSlotsJSON(rawSlots, false)
	if err != nil {
		return scheduleWriteMutationError(err)
	}

	ctx := params.Context
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := scheduleWriter.SetDhwTimeProgram(ctx, weekday, slots)
	if err != nil {
		return scheduleWriteMutationError(err)
	}
	if result == nil {
		return scheduleWriteMutationError(fmt.Errorf("schedule writer returned nil result: %w", ebuserrors.ErrInvalidPayload))
	}
	return result
}

func applyConfigMutation(ctx context.Context, registry InvokeRegistry, invoker Invoker, source byte, sourceAdmitted bool, spec configFieldSpec, instance byte, rawValue string, bound ...*ConfigBound) ConfigMutationResult {
	if registry == nil || invoker == nil {
		return configMutationError(fmt.Errorf("config mutation missing dependencies: %w", ebuserrors.ErrInvalidPayload))
	}
	if !sourceAdmitted || source == 0 {
		return configMutationError(fmt.Errorf("source selection not active: %w", ebuserrors.ErrInvalidPayload))
	}

	data, err := encodeConfigValue(spec, rawValue)
	if err != nil {
		return configMutationError(err)
	}

	plane, err := resolveControllerSystemPlane(registry, bound...)
	if err != nil {
		return configMutationError(err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	writeParams := map[string]any{
		"source":   source,
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
		"source":   source,
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

func parseScheduleMutationIntArg(raw any, name string) (int, error) {
	value, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("%s must be an integer: %w", name, ebuserrors.ErrInvalidPayload)
	}
	return value, nil
}

func parseTimeProgramSlotsJSON(raw string, tempRequired bool) ([]mcp.TimeProgramSlot, error) {
	var slotsRaw []map[string]any
	if err := json.Unmarshal([]byte(raw), &slotsRaw); err != nil {
		return nil, fmt.Errorf("invalid slots JSON: %w", err)
	}

	slots := make([]mcp.TimeProgramSlot, 0, len(slotsRaw))
	for i, slotRaw := range slotsRaw {
		slot, err := parseTimeProgramSlot(slotRaw, tempRequired)
		if err != nil {
			return nil, fmt.Errorf("slot %d: %w", i, err)
		}
		slots = append(slots, slot)
	}
	return slots, nil
}

func parseTimeProgramSlot(raw map[string]any, tempRequired bool) (mcp.TimeProgramSlot, error) {
	var slot mcp.TimeProgramSlot

	startHour, err := parseTimeProgramIntegerField(raw, "start_hour")
	if err != nil {
		return slot, err
	}
	startMinute, err := parseTimeProgramIntegerField(raw, "start_minute")
	if err != nil {
		return slot, err
	}
	endHour, err := parseTimeProgramIntegerField(raw, "end_hour")
	if err != nil {
		return slot, err
	}
	endMinute, err := parseTimeProgramIntegerField(raw, "end_minute")
	if err != nil {
		return slot, err
	}

	slot.StartHour = startHour
	slot.StartMinute = startMinute
	slot.EndHour = endHour
	slot.EndMinute = endMinute

	tempValue, ok := raw["temperature_c"]
	if ok && tempValue != nil {
		temp, ok := tempValue.(float64)
		if !ok {
			return slot, fmt.Errorf("temperature_c must be a number")
		}
		slot.TemperatureC = &temp
		return slot, nil
	}
	if tempRequired {
		return slot, fmt.Errorf("temperature_c is required")
	}

	return slot, nil
}

func parseTimeProgramIntegerField(raw map[string]any, field string) (int, error) {
	value, ok := raw[field]
	if !ok || value == nil {
		return 0, fmt.Errorf("%s is required", field)
	}
	coerced, ok := coerceFloatToInt(value)
	if !ok {
		return 0, fmt.Errorf("%s must be an integer, got %v", field, value)
	}
	return int(coerced), nil
}

func scheduleWriteMutationError(err error) *mcp.TimeProgramWriteResult {
	if err == nil {
		return &mcp.TimeProgramWriteResult{
			Success: false,
			Error:   "schedule mutation failed",
		}
	}
	return &mcp.TimeProgramWriteResult{
		Success: false,
		Error:   err.Error(),
	}
}

// ApplyConfigMutation performs a B524 set_ext_register write with read-back verification.
func ApplyConfigMutation(ctx context.Context, registry InvokeRegistry, invoker Invoker, source byte, sourceAdmitted bool, fieldName string, rawValue string, specs map[string]configFieldSpec, bounds ...*ConfigBound) ConfigMutationResult {
	return applySystemFieldMutation(ctx, registry, invoker, source, sourceAdmitted, fieldName, rawValue, specs, bounds...)
}

func suppliedSystemConfigBound(providers []func() *ConfigBound) *ConfigBound {
	if len(providers) == 0 || providers[0] == nil {
		return nil
	}
	return providers[0]()
}

// applySystemFieldMutation validates a supplied driver bound before native I/O.
func applySystemFieldMutation(ctx context.Context, registry InvokeRegistry, invoker Invoker, source byte, sourceAdmitted bool, fieldName string, rawValue string, specs map[string]configFieldSpec, bounds ...*ConfigBound) ConfigMutationResult {
	spec, err := resolveConfigFieldSpec("system", fieldName, specs)
	if err != nil {
		return configMutationError(err)
	}
	var expectedBound []*ConfigBound
	if fieldName == SystemConfigFieldDHWBivalencePoint {
		if len(bounds) == 0 || bounds[0] == nil {
			return configMutationError(fmt.Errorf("%s: missing resolved driver bound: %w", fieldName, ebuserrors.ErrInvalidPayload))
		}
		bound := *bounds[0]
		if math.IsNaN(bound.MinC) || math.IsNaN(bound.MaxC) || math.IsInf(bound.MinC, 0) || math.IsInf(bound.MaxC, 0) || bound.MinC > bound.MaxC {
			return configMutationError(fmt.Errorf("%s: invalid resolved driver bound: %w", fieldName, ebuserrors.ErrInvalidPayload))
		}
		if bound.MinC < spec.min || bound.MaxC > spec.max {
			return configMutationError(fmt.Errorf("%s: resolved driver bound exceeds conservative policy: %w", fieldName, ebuserrors.ErrInvalidPayload))
		}
		qualified := bound.Source == ConfigBoundSourceControllerProfile && bound.NarrowedOnly && bound.Profile != "" && bound.Reason == ""
		policy := bound.Source == ConfigBoundSourceUnmatchedPolicy && bound.MinC == spec.min && bound.MaxC == spec.max && bound.Profile == "" && !bound.NarrowedOnly
		if !qualified && !policy {
			return configMutationError(fmt.Errorf("%s: unqualified resolved driver bound: %w", fieldName, ebuserrors.ErrInvalidPayload))
		}
		if bound.ControllerAddress == 0 {
			return configMutationError(fmt.Errorf("%s: missing bound controller address: %w", fieldName, ebuserrors.ErrInvalidPayload))
		}
		expectedBound = []*ConfigBound{&bound}
		spec.min, spec.max = math.Max(spec.min, bound.MinC), math.Min(spec.max, bound.MaxC)
		if _, encodeErr := encodeConfigValue(spec, rawValue); encodeErr != nil {
			return configMutationError(fmt.Errorf("%s: %s: %w", fieldName, bound.Describe(), encodeErr))
		}
	}
	return applyConfigMutation(ctx, registry, invoker, source, sourceAdmitted, spec, systemconfig.Instance, rawValue, expectedBound...)
}

// SystemConfigFieldSpecs returns a copy of the system config field specs map.
func SystemConfigFieldSpecs() map[string]configFieldSpec {
	cp := make(map[string]configFieldSpec)
	for k, v := range systemconfig.SystemConfigFieldSpecs() {
		cp[k] = adaptSystemConfigSpec(v)
	}
	return cp
}

func EncodeSystemConfigValue(fieldName, rawValue string) ([]byte, configFieldSpec, error) {
	payload, spec, err := systemconfig.EncodeSystemConfigValue(fieldName, rawValue)
	if err != nil {
		return nil, configFieldSpec{}, err
	}
	adapted := adaptSystemConfigSpec(spec)
	if adapted.conversionErr != nil {
		return nil, adapted, adapted.conversionErr
	}
	return payload, adapted, nil
}

// ConfigFieldSpecGroup returns the B524 group byte for a field spec.
func (s configFieldSpec) Group() byte { return s.group }

// ConfigFieldSpecAddr returns the B524 register address for a field spec.
func (s configFieldSpec) Addr() uint16 { return s.addr }

// ConfirmDecodableReadback validates that a read-back payload is decodable.
func ConfirmDecodableReadback(spec configFieldSpec, payload []byte) error {
	return confirmDecodableReadback(spec, payload)
}

// ConfigReadbackMatchesWrite checks if the read-back matches the written payload.
func ConfigReadbackMatchesWrite(spec configFieldSpec, written, readback []byte) bool {
	return configReadbackMatchesWrite(spec, written, readback)
}

func resolveConfigFieldSpec(scope, fieldName string, specs map[string]configFieldSpec) (configFieldSpec, error) {
	if spec, ok := specs[fieldName]; ok {
		if spec.conversionErr != nil {
			return configFieldSpec{}, spec.conversionErr
		}
		return spec, nil
	}
	allowed := make([]string, 0, len(specs))
	for key := range specs {
		allowed = append(allowed, key)
	}
	sort.Strings(allowed)
	return configFieldSpec{}, fmt.Errorf("unknown %s field %q (allowed: %s): %w", scope, fieldName, strings.Join(allowed, ", "), ebuserrors.ErrInvalidPayload)
}

func nativeConfigSpec(s configFieldSpec) (systemconfig.ConfigFieldSpec, error) {
	if s.conversionErr != nil {
		return systemconfig.ConfigFieldSpec{}, s.conversionErr
	}
	out := systemconfig.ConfigFieldSpec{RegisterGroup: s.group, RegisterAddr: s.addr, Min: s.min, Max: s.max, MaxLen: s.maxLen, Enum: s.enum}
	switch s.valueType {
	case configValueFloat32:
		out.ValueType = systemconfig.ValueFloat32
	case configValueUint16:
		out.ValueType = systemconfig.ValueUint16
	case configValueBoolU8:
		out.ValueType = systemconfig.ValueBoolU8
	case configValueEnumU16:
		out.ValueType = systemconfig.ValueEnumU16
	case configValueCString:
		out.ValueType = systemconfig.ValueCString
	case configValueDateHDA3:
		out.ValueType = systemconfig.ValueDateHDA3
	default:
		return systemconfig.ConfigFieldSpec{}, fmt.Errorf("unsupported config value type %d: %w", s.valueType, ebuserrors.ErrInvalidPayload)
	}
	return out, nil
}

func encodeConfigValue(spec configFieldSpec, raw string) ([]byte, error) {
	native, err := nativeConfigSpec(spec)
	if err != nil {
		return nil, err
	}
	return systemconfig.EncodeValue(native, raw)
}

func resolveControllerSystemPlane(reg InvokeRegistry, bounds ...*ConfigBound) (router.Plane, error) {
	var expectedController []byte
	if len(bounds) != 0 && bounds[0] != nil {
		expectedController = []byte{bounds[0].ControllerAddress}
	}
	entry, err := findControllerEntry(reg, expectedController...)
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
	if len(bounds) != 0 && bounds[0] != nil {
		bound := bounds[0]
		current := systemconfig.CaptureBoundRegistryIdentity(reg, bound.ControllerAddress)
		if !bound.ControllerRegistryIdentity.Valid || !current.Valid || current != bound.ControllerRegistryIdentity {
			return nil, fmt.Errorf("bound controller registry identity or qualification changed: %w", ebuserrors.ErrInvalidPayload)
		}
	}

	return routerPlane, nil
}

func findControllerEntry(reg InvokeRegistry, expectedController ...byte) (registry.DeviceEntry, error) {
	if reg == nil {
		return nil, fmt.Errorf("registry missing: %w", ebuserrors.ErrInvalidPayload)
	}

	// P9.5 (operator-directed pivot 2026-05-09) — identifier-free
	// controller detection.
	//
	// The previous algorithm scanned the registry for any entry
	// whose DeviceID started with "BASV" and returned that as the
	// controller. Two problems made it brittle:
	//
	//  1. Identifier coupling: only Vaillant BASV2 controllers
	//     matched. Successor and sibling families (CTLV*, CTLS*,
	//     CTLR*, BASS*) and any future controller identifier got
	//     silently skipped, falling through to a hardcoded address
	//     fallback that may or may not be correct.
	//
	//  2. TOCTOU race with concurrent Register: the iteration's
	//     DeviceID read was lock-free, and even with snapshot-based
	//     re-validation (P9.5 pass 2 finding) a Register call between
	//     the snapshot identity-check and the live Lookup could swap
	//     the entry, returning a non-controller's Planes for
	//     mutation routing.
	//
	// New algorithm: try canonical regulator target addresses (eBUS
	// standard heating-regulator/heating-circuit-regulator companion
	// bytes) in priority order via plain Lookup. Each Lookup is
	// atomic under the registry RLock, so no TOCTOU window exists.
	// The first registered entry at one of these canonical addresses
	// is the controller — by construction the device sitting on the
	// bus at a regulator target address IS the regulator, regardless
	// of what identifier string the product-id catalog assigns to it.
	//
	// `mutationControllerFallbackAddr` (0x15) is the first address
	// in the canonical list, so the previous fallback behavior is
	// preserved as a no-op semantic when no other regulator is
	// registered.
	for _, addr := range systemconfig.CanonicalRegulatorTargetAddresses() {
		if entry, ok := reg.Lookup(addr); ok && entry != nil {
			if len(expectedController) != 0 && addr != expectedController[0] {
				return nil, fmt.Errorf("bound controller does not match routed controller: %w", ebuserrors.ErrInvalidPayload)
			}
			return entry, nil
		}
	}
	return nil, fmt.Errorf("no heating regulator at canonical eBUS target addresses: %w", ebuserrors.ErrInvalidPayload)
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
	native, err := nativeConfigSpec(spec)
	if err != nil {
		return err
	}
	return systemconfig.ConfirmDecodableReadback(native, payload)
}

func configReadbackMatchesWrite(spec configFieldSpec, written, readback []byte) bool {
	native, err := nativeConfigSpec(spec)
	return err == nil && systemconfig.ConfigReadbackMatchesWrite(native, written, readback)
}

func invokeResolve(params graphqlgo.ResolveParams, registry InvokeRegistry, invoker Invoker, source byte, sourceAdmitted bool) (InvokeResult, error) {
	if registry == nil || invoker == nil {
		return InvokeResult{}, fmt.Errorf("graphql invoke missing dependencies: %w", ebuserrors.ErrInvalidPayload)
	}
	if !sourceAdmitted || source == 0 {
		return InvokeResult{}, fmt.Errorf("source selection not active: %w", ebuserrors.ErrInvalidPayload)
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
	if _, ok := paramMap["source"]; ok {
		return InvokeResult{}, fmt.Errorf("graphql invoke field %q unsupported: %w", "source", ebuserrors.ErrInvalidPayload)
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
	normalizedParams["source"] = source

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

func scheduleWriteResultFromSource(params graphqlgo.ResolveParams) (mcp.TimeProgramWriteResult, bool) {
	switch value := params.Source.(type) {
	case mcp.TimeProgramWriteResult:
		return value, true
	case *mcp.TimeProgramWriteResult:
		if value == nil {
			return mcp.TimeProgramWriteResult{}, false
		}
		return *value, true
	default:
		return mcp.TimeProgramWriteResult{}, false
	}
}

func scheduleSlotResultFromSource(params graphqlgo.ResolveParams) (mcp.TimeProgramSlotResult, bool) {
	switch value := params.Source.(type) {
	case mcp.TimeProgramSlotResult:
		return value, true
	case *mcp.TimeProgramSlotResult:
		if value == nil {
			return mcp.TimeProgramSlotResult{}, false
		}
		return *value, true
	default:
		return mcp.TimeProgramSlotResult{}, false
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
