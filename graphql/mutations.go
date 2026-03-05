package graphql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

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
		mutationType = buildMutationType(registry, invoker)
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

func buildMutationType(registry InvokeRegistry, invoker Invoker) *graphqlgo.Object {
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
		},
	})
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
