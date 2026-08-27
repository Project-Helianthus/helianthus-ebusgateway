package graphql

import graphqlgo "github.com/graphql-go/graphql"

func addEnergyTotalsToDevice(deviceType *graphqlgo.Object, energyTotalsType *graphqlgo.Object, builder *Builder) {
	deviceType.AddFieldConfig("energyTotals", &graphqlgo.Field{
		Type: energyTotalsType,
		Resolve: func(params graphqlgo.ResolveParams) (any, error) {
			device, ok := deviceFromSource(params)
			if !ok {
				return nil, nil
			}
			if device.Role != "Regulator" {
				return nil, nil
			}
			return builder.semanticProvider().EnergyTotals(), nil
		},
	})
}

func buildQueryType(builder *Builder, types graphqlSchemaTypes) *graphqlgo.Object {
	fields := graphqlgo.Fields{}
	addVaillantB503Queries(fields, builder)
	obj := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Query",
		Fields: mergeQueryFields(fields, graphqlgo.Fields{
			"daemonStatus": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.statusType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.statusProvider().DaemonStatus(), nil
				},
			},
			"daemon_status": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.statusType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.statusProvider().DaemonStatus(), nil
				},
			},
			"gatewayIdentity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.gatewayIdentityType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.gatewayIdentityProvider().GatewayIdentity(), nil
				},
			},
			"gateway_identity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.gatewayIdentityType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.gatewayIdentityProvider().GatewayIdentity(), nil
				},
			},
			"adapterStatus": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.statusType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.statusProvider().AdapterStatus(), nil
				},
			},
			"adapter_status": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.statusType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.statusProvider().AdapterStatus(), nil
				},
			},
			"zones": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.zoneType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().Zones(), nil
				},
			},
			"dhw": &graphqlgo.Field{
				Type: types.dhwType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().DHW(), nil
				},
			},
			"energyTotals": &graphqlgo.Field{
				Type: types.energyTotals,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().EnergyTotals(), nil
				},
			},
			"energy_totals": &graphqlgo.Field{
				Type: types.energyTotals,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().EnergyTotals(), nil
				},
			},
			"circuits": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.circuitStatusType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().Circuits(), nil
				},
			},
			"radioDevices": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.radioDeviceType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().RadioDevices(), nil
				},
			},
			"radio_devices": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.radioDeviceType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().RadioDevices(), nil
				},
			},
			"fm5SemanticMode": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.fm5SemanticMode),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					mode := builder.semanticProvider().FM5SemanticMode()
					if mode == "" {
						mode = Fm5SemanticModeAbsent
					}
					return string(mode), nil
				},
			},
			"fm5Interpretation": &graphqlgo.Field{
				Type: types.fm5Interpretation,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					provider := builder.semanticProvider()
					if typed, ok := provider.(FM5InterpretationProvider); ok {
						verdict := typed.FM5Interpretation()
						if verdict == (Fm5Interpretation{}) {
							return nil, nil
						}
						if err := verdict.Validate(); err != nil {
							return nil, err
						}
						return verdict, nil
					}
					return nil, nil
				},
			},
			"fm5_semantic_mode": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.fm5SemanticMode),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					mode := builder.semanticProvider().FM5SemanticMode()
					if mode == "" {
						mode = Fm5SemanticModeAbsent
					}
					return string(mode), nil
				},
			},
			"solar": &graphqlgo.Field{
				Type: types.solarStatusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().Solar(), nil
				},
			},
			"cylinders": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.cylinderStatusType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					values := builder.semanticProvider().Cylinders()
					if len(values) == 0 {
						return []CylinderStatus{}, nil
					}
					return values, nil
				},
			},
			"boilerStatus": &graphqlgo.Field{
				Type: types.boilerStatusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().BoilerStatus(), nil
				},
			},
			"boiler_status": &graphqlgo.Field{
				Type: types.boilerStatusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().BoilerStatus(), nil
				},
			},
			"system": &graphqlgo.Field{
				Type: types.systemStatusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().System(), nil
				},
			},
			"schedules": &graphqlgo.Field{
				Type: types.scheduleStatusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().Schedules(), nil
				},
			},
			"adapterHardwareInfo": &graphqlgo.Field{
				Type: types.adapterHardwareInfoType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().AdapterHardwareInfo(), nil
				},
			},
			"adapter_hardware_info": &graphqlgo.Field{
				Type: types.adapterHardwareInfoType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().AdapterHardwareInfo(), nil
				},
			},
			"busSummary": &graphqlgo.Field{
				Type: types.busSummaryType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return resolveBusSummary(builder, params.Info.RootValue), nil
				},
			},
			"busMessages": &graphqlgo.Field{
				Type: types.busMessagesType,
				Args: graphqlgo.FieldConfigArgument{
					"limit": &graphqlgo.ArgumentConfig{Type: graphqlgo.Int},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					limit, err := parseBusObservabilityLimit(params.Args)
					if err != nil {
						return nil, err
					}
					return resolveBusMessages(builder, params.Info.RootValue, limit), nil
				},
			},
			"busPeriodicity": &graphqlgo.Field{
				Type: types.busPeriodicityType,
				Args: graphqlgo.FieldConfigArgument{
					"limit": &graphqlgo.ArgumentConfig{Type: graphqlgo.Int},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					limit, err := parseBusObservabilityLimit(params.Args)
					if err != nil {
						return nil, err
					}
					return resolveBusPeriodicity(builder, params.Info.RootValue, limit), nil
				},
			},
			"watchSummary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.watchSummaryType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return resolveWatchSummary(builder, params.Info.RootValue), nil
				},
			},
			"devices": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.deviceType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					snapshot := builder.FreshSchema()
					return snapshot.Devices, nil
				},
			},
			"device": &graphqlgo.Field{
				Type: types.deviceType,
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					address, err := parseAddress(params.Args["address"])
					if err != nil {
						return nil, err
					}
					snapshot := builder.FreshSchema()
					device, ok := findDevice(snapshot.Devices, address)
					if !ok {
						return nil, nil
					}
					return device, nil
				},
			},
			"planes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.planeType))),
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					address, err := parseAddress(params.Args["address"])
					if err != nil {
						return nil, err
					}
					snapshot := builder.FreshSchema()
					device, ok := findDevice(snapshot.Devices, address)
					if !ok {
						return []Plane{}, nil
					}
					return device.Planes, nil
				},
			},
			"methods": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.methodType))),
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"plane":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					address, err := parseAddress(params.Args["address"])
					if err != nil {
						return nil, err
					}
					planeName, _ := params.Args["plane"].(string)
					snapshot := builder.FreshSchema()
					device, ok := findDevice(snapshot.Devices, address)
					if !ok {
						return []Method{}, nil
					}
					plane, ok := findPlane(device.Planes, planeName)
					if !ok {
						return []Method{}, nil
					}
					return plane.Methods, nil
				},
			},
		}),
	})
	return obj
}

// mergeQueryFields merges b into a and returns a. On name collisions, b wins
// (so caller-supplied core fields always take precedence over the extension
// map).
func mergeQueryFields(a, b graphqlgo.Fields) graphqlgo.Fields {
	for k, v := range b {
		a[k] = v
	}
	return a
}
