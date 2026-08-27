package graphql

import (
	"context"
	"fmt"
	"net/http"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

func NewQuerySchema(builder *Builder) (graphqlgo.Schema, error) {
	if builder == nil {
		return graphqlgo.Schema{}, fmt.Errorf("graphql query schema missing builder: %w", ebuserrors.ErrInvalidPayload)
	}

	types := buildSchemaTypes()
	addEnergyTotalsToDevice(types.deviceType, types.energyTotals, builder)
	queryType := buildQueryType(builder, types)

	return graphqlgo.NewSchema(graphqlgo.SchemaConfig{Query: queryType})
}

func NewHandler(builder *Builder) (http.Handler, error) {
	schema, err := NewSchema(builder, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	return handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false,
		RootObjectFn: func(_ context.Context, _ *http.Request) map[string]interface{} {
			return newGraphQLRootObject(builder)
		},
	}), nil
}

func parseAddress(raw any) (byte, error) {
	switch value := raw.(type) {
	case int:
		return toAddress(value)
	case int64:
		return toAddress(int(value))
	case float64:
		return toAddress(int(value))
	default:
		return 0, fmt.Errorf("graphql query invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
}

func toAddress(value int) (byte, error) {
	if value < 0 || value > 0xFF {
		return 0, fmt.Errorf("graphql query invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
	return byte(value), nil
}

func findDevice(devices []Device, address byte) (Device, bool) {
	for _, device := range devices {
		if deviceHasAddress(device, address) {
			return device, true
		}
	}
	return Device{}, false
}

func deviceHasAddress(device Device, address byte) bool {
	for _, candidate := range normalizeDeviceAddresses(device.Address, device.Addresses) {
		if candidate == address {
			return true
		}
	}
	return false
}

func findPlane(planes []Plane, name string) (Plane, bool) {
	for _, plane := range planes {
		if plane.Name == name {
			return plane, true
		}
	}
	return Plane{}, false
}

func deviceFromSource(params graphqlgo.ResolveParams) (Device, bool) {
	switch value := params.Source.(type) {
	case Device:
		return value, true
	case *Device:
		if value == nil {
			return Device{}, false
		}
		return *value, true
	default:
		return Device{}, false
	}
}

func planeFromSource(params graphqlgo.ResolveParams) (Plane, bool) {
	switch value := params.Source.(type) {
	case Plane:
		return value, true
	case *Plane:
		if value == nil {
			return Plane{}, false
		}
		return *value, true
	default:
		return Plane{}, false
	}
}

func projectionFromSource(params graphqlgo.ResolveParams) (Projection, bool) {
	switch value := params.Source.(type) {
	case Projection:
		return value, true
	case *Projection:
		if value == nil {
			return Projection{}, false
		}
		return *value, true
	default:
		return Projection{}, false
	}
}

func projectionNodeFromSource(params graphqlgo.ResolveParams) (ProjectionNode, bool) {
	switch value := params.Source.(type) {
	case ProjectionNode:
		return value, true
	case *ProjectionNode:
		if value == nil {
			return ProjectionNode{}, false
		}
		return *value, true
	default:
		return ProjectionNode{}, false
	}
}

func projectionEdgeFromSource(params graphqlgo.ResolveParams) (ProjectionEdge, bool) {
	switch value := params.Source.(type) {
	case ProjectionEdge:
		return value, true
	case *ProjectionEdge:
		if value == nil {
			return ProjectionEdge{}, false
		}
		return *value, true
	default:
		return ProjectionEdge{}, false
	}
}

func methodFromSource(params graphqlgo.ResolveParams) (Method, bool) {
	switch value := params.Source.(type) {
	case Method:
		return value, true
	case *Method:
		if value == nil {
			return Method{}, false
		}
		return *value, true
	default:
		return Method{}, false
	}
}

func responseFromSource(params graphqlgo.ResolveParams) (ResponseSchema, bool) {
	switch value := params.Source.(type) {
	case ResponseSchema:
		return value, true
	case *ResponseSchema:
		if value == nil {
			return ResponseSchema{}, false
		}
		return *value, true
	default:
		return ResponseSchema{}, false
	}
}

func fieldFromSource(params graphqlgo.ResolveParams) (Field, bool) {
	switch value := params.Source.(type) {
	case Field:
		return value, true
	case *Field:
		if value == nil {
			return Field{}, false
		}
		return *value, true
	default:
		return Field{}, false
	}
}
