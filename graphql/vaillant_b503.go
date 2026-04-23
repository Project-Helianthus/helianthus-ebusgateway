package graphql

import (
	"context"
	"fmt"

	graphqlgo "github.com/graphql-go/graphql"
)

// Vaillant B503 read-only GraphQL surface.
//
// Plan invariants:
//   - AD02: no GraphQL mutations for B503 (install-writes are MCP-only).
//   - AD05: no F.xxx derivation; firstActiveError surfaces raw decimal.
//   - AD14: EXPIRED MUST NEVER be a public B503Availability enum member; if
//     the underlying provider ever leaks "EXPIRED", the resolver maps it to
//     SESSION_BUSY before surfacing to clients.
//
// Resolvers wrap a VaillantB503Provider (implemented by the MCP layer) —
// the graphql package never touches b503session directly.

// VaillantB503Errors is the GraphQL view of a current-error or
// current-service payload. Slots that were 0xFFFF on the wire appear as nil
// entries in Slots (rendered as `null` in GraphQL).
type VaillantB503Errors struct {
	FirstActiveError *int
	Slots            []*int
}

// VaillantB503HistoryRecord is the GraphQL view of an error-history or
// service-history record.
type VaillantB503HistoryRecord struct {
	Index            int
	FirstActiveError *int
	Slots            []*int
}

// VaillantB503LiveMonitor is the GraphQL view of a live-monitor read/enable
// response. IssuerToken is populated on enable; RawHex on read.
type VaillantB503LiveMonitor struct {
	IssuerToken string
	RawHex      string
	Disabled    bool
}

// VaillantB503Provider abstracts the MCP B503 surface for GraphQL
// resolvers. Production wires this to an adapter that delegates to the MCP
// Server's handleVaillantB503Call path (never bypassing tool dispatch).
type VaillantB503Provider interface {
	Errors(ctx context.Context, target *byte) (VaillantB503Errors, error)
	ErrorHistory(ctx context.Context, target *byte, index *byte) (VaillantB503HistoryRecord, error)
	ServiceCurrent(ctx context.Context, target *byte) (VaillantB503Errors, error)
	ServiceHistory(ctx context.Context, target *byte, index *byte) (VaillantB503HistoryRecord, error)
	LiveMonitor(ctx context.Context, action string, issuerToken *string, target *byte) (VaillantB503LiveMonitor, error)
	Availability(ctx context.Context) string // returns string rendering of B503Availability
}

// SetVaillantB503Provider installs a provider. Nil providers are ignored so
// server bootstraps that omit B503 wiring still produce a valid schema
// (resolvers surface NOT_SUPPORTED-shaped errors on call).
func (b *Builder) SetVaillantB503Provider(provider VaillantB503Provider) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.vaillantB503 = provider
	b.mu.Unlock()
}

func (b *Builder) vaillantB503Provider() VaillantB503Provider {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.vaillantB503
}

// sanitizeAvailability enforces the plan AD14 invariant that EXPIRED never
// reaches clients. Any non-public value collapses to UNKNOWN.
func sanitizeAvailability(raw string) string {
	switch raw {
	case "AVAILABLE", "NOT_SUPPORTED", "TRANSPORT_DOWN", "SESSION_BUSY", "UNKNOWN":
		return raw
	case "EXPIRED":
		// Spec §7.1.1 / plan AD14: EXPIRED is internal-only.
		return "SESSION_BUSY"
	default:
		return "UNKNOWN"
	}
}

var b503AvailabilityEnum = graphqlgo.NewEnum(graphqlgo.EnumConfig{
	Name:        "B503Availability",
	Description: "Capability reason for the Vaillant B503 surface. EXPIRED is internal-only and never a member (plan AD14).",
	Values: graphqlgo.EnumValueConfigMap{
		"AVAILABLE":      {Value: "AVAILABLE"},
		"NOT_SUPPORTED":  {Value: "NOT_SUPPORTED"},
		"TRANSPORT_DOWN": {Value: "TRANSPORT_DOWN"},
		"SESSION_BUSY":   {Value: "SESSION_BUSY"},
		"UNKNOWN":        {Value: "UNKNOWN"},
	},
})

func resolveIntPtr(ptr *int) (any, error) {
	if ptr == nil {
		return nil, nil
	}
	return *ptr, nil
}

func resolveSlots(slots []*int) (any, error) {
	out := make([]any, len(slots))
	for i, v := range slots {
		if v == nil {
			out[i] = nil
		} else {
			out[i] = *v
		}
	}
	return out, nil
}

func buildVaillantB503Types() (
	errorsType *graphqlgo.Object,
	historyType *graphqlgo.Object,
	liveMonitorType *graphqlgo.Object,
	capabilityType *graphqlgo.Object,
	capabilitiesType *graphqlgo.Object,
) {
	errorsType = graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "VaillantB503Errors",
		Fields: graphqlgo.Fields{
			"firstActiveError": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503Errors)
					if !ok {
						return nil, nil
					}
					return resolveIntPtr(s.FirstActiveError)
				},
			},
			"slots": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.Int)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503Errors)
					if !ok {
						return []any{}, nil
					}
					return resolveSlots(s.Slots)
				},
			},
		},
	})

	historyType = graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "VaillantB503HistoryRecord",
		Fields: graphqlgo.Fields{
			"index": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503HistoryRecord)
					if !ok {
						return 0, nil
					}
					return s.Index, nil
				},
			},
			"firstActiveError": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503HistoryRecord)
					if !ok {
						return nil, nil
					}
					return resolveIntPtr(s.FirstActiveError)
				},
			},
			"slots": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.Int)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503HistoryRecord)
					if !ok {
						return []any{}, nil
					}
					return resolveSlots(s.Slots)
				},
			},
		},
	})

	liveMonitorType = graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "VaillantB503LiveMonitor",
		Fields: graphqlgo.Fields{
			"issuerToken": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503LiveMonitor)
					if !ok || s.IssuerToken == "" {
						return nil, nil
					}
					return s.IssuerToken, nil
				},
			},
			"rawHex": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503LiveMonitor)
					if !ok || s.RawHex == "" {
						return nil, nil
					}
					return s.RawHex, nil
				},
			},
			"disabled": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					s, ok := params.Source.(VaillantB503LiveMonitor)
					if !ok {
						return false, nil
					}
					return s.Disabled, nil
				},
			},
		},
	})

	capabilityType = graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "VaillantB503Capability",
		Fields: graphqlgo.Fields{
			"reason": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(b503AvailabilityEnum),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					reason, _ := params.Source.(string)
					return sanitizeAvailability(reason), nil
				},
			},
			"available": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					reason, _ := params.Source.(string)
					return sanitizeAvailability(reason) == "AVAILABLE", nil
				},
			},
		},
	})

	capabilitiesType = graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "VaillantCapabilities",
		Fields: graphqlgo.Fields{
			"vaillantB503": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(capabilityType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					// Source is the parent resolver's return; we expose the
					// raw (pre-sanitized) reason string and let the
					// capability-type's field resolvers apply sanitize.
					return params.Source, nil
				},
			},
		},
	})

	return errorsType, historyType, liveMonitorType, capabilityType, capabilitiesType
}

func parseTargetAddress(args map[string]any) (*byte, error) {
	raw, ok := args["targetAddress"]
	if !ok || raw == nil {
		return nil, nil
	}
	var v int
	switch typed := raw.(type) {
	case int:
		v = typed
	case int32:
		v = int(typed)
	case int64:
		v = int(typed)
	case float64:
		v = int(typed)
	default:
		return nil, fmt.Errorf("INVALID_ARGUMENT: targetAddress must be an integer")
	}
	if v < 0 || v > 255 {
		return nil, fmt.Errorf("INVALID_ARGUMENT: targetAddress must be 0-255, got %d", v)
	}
	b := byte(v)
	return &b, nil
}

func parseHistoryIndex(args map[string]any) (*byte, error) {
	raw, ok := args["index"]
	if !ok || raw == nil {
		return nil, nil
	}
	var v int
	switch typed := raw.(type) {
	case int:
		v = typed
	case int32:
		v = int(typed)
	case int64:
		v = int(typed)
	case float64:
		v = int(typed)
	default:
		return nil, fmt.Errorf("INVALID_ARGUMENT: index must be an integer")
	}
	if v < 0 || v > 255 {
		return nil, fmt.Errorf("INVALID_ARGUMENT: index must be 0-255, got %d", v)
	}
	b := byte(v)
	return &b, nil
}

func addVaillantB503Queries(fields graphqlgo.Fields, builder *Builder) {
	errorsType, historyType, liveMonitorType, _, capabilitiesType := buildVaillantB503Types()

	targetArg := graphqlgo.FieldConfigArgument{
		"targetAddress": &graphqlgo.ArgumentConfig{Type: graphqlgo.Int},
	}
	indexAndTargetArg := graphqlgo.FieldConfigArgument{
		"index":         &graphqlgo.ArgumentConfig{Type: graphqlgo.Int},
		"targetAddress": &graphqlgo.ArgumentConfig{Type: graphqlgo.Int},
	}

	providerOrErr := func() (VaillantB503Provider, error) {
		p := builder.vaillantB503Provider()
		if p == nil {
			return nil, fmt.Errorf("NOT_SUPPORTED: vaillantB503 provider not configured")
		}
		return p, nil
	}

	fields["vaillantErrors"] = &graphqlgo.Field{
		Type: errorsType,
		Args: targetArg,
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			target, err := parseTargetAddress(params.Args)
			if err != nil {
				return nil, err
			}
			p, err := providerOrErr()
			if err != nil {
				return nil, err
			}
			return p.Errors(params.Context, target)
		},
	}

	fields["vaillantErrorHistory"] = &graphqlgo.Field{
		Type: historyType,
		Args: indexAndTargetArg,
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			target, err := parseTargetAddress(params.Args)
			if err != nil {
				return nil, err
			}
			idx, err := parseHistoryIndex(params.Args)
			if err != nil {
				return nil, err
			}
			p, err := providerOrErr()
			if err != nil {
				return nil, err
			}
			return p.ErrorHistory(params.Context, target, idx)
		},
	}

	fields["vaillantServiceCurrent"] = &graphqlgo.Field{
		Type: errorsType,
		Args: targetArg,
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			target, err := parseTargetAddress(params.Args)
			if err != nil {
				return nil, err
			}
			p, err := providerOrErr()
			if err != nil {
				return nil, err
			}
			return p.ServiceCurrent(params.Context, target)
		},
	}

	fields["vaillantServiceHistory"] = &graphqlgo.Field{
		Type: historyType,
		Args: indexAndTargetArg,
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			target, err := parseTargetAddress(params.Args)
			if err != nil {
				return nil, err
			}
			idx, err := parseHistoryIndex(params.Args)
			if err != nil {
				return nil, err
			}
			p, err := providerOrErr()
			if err != nil {
				return nil, err
			}
			return p.ServiceHistory(params.Context, target, idx)
		},
	}

	fields["vaillantLiveMonitor"] = &graphqlgo.Field{
		Type: liveMonitorType,
		Args: graphqlgo.FieldConfigArgument{
			"action":        &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"issuerToken":   &graphqlgo.ArgumentConfig{Type: graphqlgo.String},
			"targetAddress": &graphqlgo.ArgumentConfig{Type: graphqlgo.Int},
		},
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			action, _ := params.Args["action"].(string)
			var issuerToken *string
			if raw, ok := params.Args["issuerToken"]; ok && raw != nil {
				if s, ok := raw.(string); ok {
					issuerToken = &s
				}
			}
			target, err := parseTargetAddress(params.Args)
			if err != nil {
				return nil, err
			}
			p, err := providerOrErr()
			if err != nil {
				return nil, err
			}
			return p.LiveMonitor(params.Context, action, issuerToken, target)
		},
	}

	fields["vaillantCapabilities"] = &graphqlgo.Field{
		Type: graphqlgo.NewNonNull(capabilitiesType),
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			p := builder.vaillantB503Provider()
			if p == nil {
				return "NOT_SUPPORTED", nil
			}
			raw := p.Availability(params.Context)
			return raw, nil
		},
	}
}
