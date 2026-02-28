package graphql

import (
	"context"
	"fmt"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	graphqlgo "github.com/graphql-go/graphql"
)

func buildBroadcastType() *graphqlgo.Object {
	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BroadcastEvent",
		Fields: graphqlgo.Fields{
			"source": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					event, ok := broadcastEventFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(event.Source), nil
				},
			},
			"target": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					event, ok := broadcastEventFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(event.Target), nil
				},
			},
			"primary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					event, ok := broadcastEventFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(event.Primary), nil
				},
			},
			"secondary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					event, ok := broadcastEventFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(event.Secondary), nil
				},
			},
			"data": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Int))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					event, ok := broadcastEventFromSource(params)
					if !ok {
						return nil, nil
					}
					out := make([]int, len(event.Data))
					for i, value := range event.Data {
						out[i] = int(value)
					}
					return out, nil
				},
			},
		},
	})
}

func buildSubscriptionType(hub *BroadcastHub, types graphqlSchemaTypes) *graphqlgo.Object {
	if hub == nil {
		return nil
	}

	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Subscription",
		Fields: graphqlgo.Fields{
			"broadcast": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.broadcastType),
				Args: graphqlgo.FieldConfigArgument{
					"primary":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"secondary": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
				},
				Subscribe: func(params graphqlgo.ResolveParams) (any, error) {
					primary, err := parseAddress(params.Args["primary"])
					if err != nil {
						return nil, err
					}
					secondary, err := parseAddress(params.Args["secondary"])
					if err != nil {
						return nil, err
					}

					ctx := params.Context
					if ctx == nil {
						ctx = context.Background()
					}

					ch, err := hub.Subscribe(ctx, primary, secondary)
					if err != nil {
						return nil, err
					}
					return ch, nil
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if params.Source == nil {
						return nil, fmt.Errorf("graphql subscription missing broadcast payload: %w", ebuserrors.ErrInvalidPayload)
					}
					return params.Source, nil
				},
			},
			"zoneUpdate": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.zoneType),
				Subscribe: func(params graphqlgo.ResolveParams) (any, error) {
					ctx := params.Context
					if ctx == nil {
						ctx = context.Background()
					}
					return hub.SubscribeZones(ctx)
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if params.Source == nil {
						return nil, fmt.Errorf("graphql subscription missing zone payload: %w", ebuserrors.ErrInvalidPayload)
					}
					return params.Source, nil
				},
			},
			"dhwUpdate": &graphqlgo.Field{
				Type: types.dhwType,
				Subscribe: func(params graphqlgo.ResolveParams) (any, error) {
					ctx := params.Context
					if ctx == nil {
						ctx = context.Background()
					}
					return hub.SubscribeDHW(ctx)
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if params.Source == nil {
						return nil, fmt.Errorf("graphql subscription missing dhw payload: %w", ebuserrors.ErrInvalidPayload)
					}
					return params.Source, nil
				},
			},
			"energyUpdate": &graphqlgo.Field{
				Type: types.energyTotals,
				Subscribe: func(params graphqlgo.ResolveParams) (any, error) {
					ctx := params.Context
					if ctx == nil {
						ctx = context.Background()
					}
					return hub.SubscribeEnergy(ctx)
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if params.Source == nil {
						return nil, fmt.Errorf("graphql subscription missing energy payload: %w", ebuserrors.ErrInvalidPayload)
					}
					return params.Source, nil
				},
			},
			"boilerStatusUpdate": &graphqlgo.Field{
				Type: types.boilerStatusType,
				Subscribe: func(params graphqlgo.ResolveParams) (any, error) {
					ctx := params.Context
					if ctx == nil {
						ctx = context.Background()
					}
					return hub.SubscribeBoiler(ctx)
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if params.Source == nil {
						return nil, fmt.Errorf("graphql subscription missing boiler status payload: %w", ebuserrors.ErrInvalidPayload)
					}
					return params.Source, nil
				},
			},
		},
	})
}

func broadcastEventFromSource(params graphqlgo.ResolveParams) (BroadcastEvent, bool) {
	switch value := params.Source.(type) {
	case BroadcastEvent:
		return value, true
	case *BroadcastEvent:
		if value == nil {
			return BroadcastEvent{}, false
		}
		return *value, true
	default:
		return BroadcastEvent{}, false
	}
}
