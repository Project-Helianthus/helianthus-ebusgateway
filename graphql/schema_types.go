package graphql

import graphqlgo "github.com/graphql-go/graphql"

type graphqlSchemaTypes struct {
	fieldType                   *graphqlgo.Object
	responseType                *graphqlgo.Object
	methodType                  *graphqlgo.Object
	projectionNodeType          *graphqlgo.Object
	projectionEdgeType          *graphqlgo.Object
	projectionType              *graphqlgo.Object
	planeType                   *graphqlgo.Object
	deviceType                  *graphqlgo.Object
	broadcastType               *graphqlgo.Object
	statusType                  *graphqlgo.Object
	gatewayIdentityType         *graphqlgo.Object
	zoneType                    *graphqlgo.Object
	dhwType                     *graphqlgo.Object
	circuitStatusType           *graphqlgo.Object
	radioDeviceType             *graphqlgo.Object
	fm5SemanticMode             *graphqlgo.Enum
	vaillantRegulatorCapability *graphqlgo.Enum
	fm5Interpretation           *graphqlgo.Object
	solarStatusType             *graphqlgo.Object
	cylinderStatusType          *graphqlgo.Object
	energyTotals                *graphqlgo.Object
	boilerStatusType            *graphqlgo.Object
	systemStatusType            *graphqlgo.Object
	scheduleStatusType          *graphqlgo.Object
	adapterHardwareInfoType     *graphqlgo.Object
	busSummaryType              *graphqlgo.Object
	busMessagesType             *graphqlgo.Object
	busPeriodicityType          *graphqlgo.Object
	busIdentificationEvidence   *graphqlgo.Object
	watchSummaryType            *graphqlgo.Object
}

func buildSchemaTypes() graphqlSchemaTypes {
	zoneStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "zone_state",
		Fields: graphqlgo.Fields{
			"zone_room_temperature": &graphqlgo.Field{
				Description: "Zone room temperature; unit: celsius.",
				Type:        graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ZoneState)
					if !ok {
						return nil, nil
					}
					if state.CurrentTempC == nil {
						return nil, nil
					}
					return *state.CurrentTempC, nil
				},
			},

			"zone_room_humidity": &graphqlgo.Field{
				Description: "Zone room relative humidity; unit: percent.",
				Type:        graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ZoneState)
					if !ok {
						return nil, nil
					}
					if state.CurrentHumidityPct == nil {
						return nil, nil
					}
					return *state.CurrentHumidityPct, nil
				},
			},

			"hvac_action": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ZoneState)
					if !ok {
						return nil, nil
					}
					if state.HvacAction == "" {
						return nil, nil
					}
					return state.HvacAction, nil
				},
			},

			"special_function": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ZoneState)
					if !ok {
						return nil, nil
					}
					if state.SpecialFunction == "" {
						return nil, nil
					}
					return state.SpecialFunction, nil
				},
			},

			"heating_demand_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ZoneState)
					if !ok {
						return nil, nil
					}
					if state.HeatingDemandPct == nil {
						return nil, nil
					}
					return *state.HeatingDemandPct, nil
				},
			},

			"valve_position_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ZoneState)
					if !ok {
						return nil, nil
					}
					if state.ValvePositionPct == nil {
						return nil, nil
					}
					return *state.ValvePositionPct, nil
				},
			},
		},
	})

	zoneConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "zone_config",
		Fields: graphqlgo.Fields{
			"operation_mode_changeable": &graphqlgo.Field{Type: graphqlgo.Boolean, Resolve: func(params graphqlgo.ResolveParams) (any, error) {
				config, ok := params.Source.(ZoneConfig)
				if !ok || config.OperationModeChangeable == nil {
					return nil, nil
				}
				return *config.OperationModeChangeable, nil
			}},
			"source_label": &graphqlgo.Field{Type: graphqlgo.String, Resolve: func(params graphqlgo.ResolveParams) (any, error) {
				config, ok := params.Source.(ZoneConfig)
				if !ok || config.SourceLabel == nil {
					return nil, nil
				}
				return *config.SourceLabel, nil
			}},
			"operating_mode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.OperatingMode == "" {
						return nil, nil
					}
					return config.OperatingMode, nil
				},
			},

			"preset": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.Preset == "" {
						return nil, nil
					}
					return config.Preset, nil
				},
			},
			"target_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.TargetTempC == nil {
						return nil, nil
					}
					return *config.TargetTempC, nil
				},
			},

			"allowed_modes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return []string{}, nil
					}
					if len(config.AllowedModes) == 0 {
						return []string{"off", "auto", "heat"}, nil
					}
					return config.AllowedModes, nil
				},
			},

			"circuit_type": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.CircuitType == "" {
						return nil, nil
					}
					return config.CircuitType, nil
				},
			},

			"associated_circuit": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.AssociatedCircuit == nil {
						return nil, nil
					}
					return *config.AssociatedCircuit, nil
				},
			},

			"room_temperature_zone_mapping": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.RoomTemperatureZoneMapping == nil {
						return nil, nil
					}
					return *config.RoomTemperatureZoneMapping, nil
				},
			},

			"quick_veto": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return false, nil
					}
					return config.QuickVeto, nil
				},
			},

			"quick_veto_setpoint": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.QuickVetoSetpointC == nil {
						return nil, nil
					}
					return *config.QuickVetoSetpointC, nil
				},
			},

			"quick_veto_duration": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.QuickVetoDurationH == nil {
						return nil, nil
					}
					return *config.QuickVetoDurationH, nil
				},
			},

			"quick_veto_expiry": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.QuickVetoExpiry == "" {
						return nil, nil
					}
					return config.QuickVetoExpiry, nil
				},
			},

			"holiday_start_date": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidayStartDate == "" {
						return nil, nil
					}
					return config.HolidayStartDate, nil
				},
			},

			"holiday_end_date": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidayEndDate == "" {
						return nil, nil
					}
					return config.HolidayEndDate, nil
				},
			},

			"holiday_setpoint": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidaySetpointC == nil {
						return nil, nil
					}
					return *config.HolidaySetpointC, nil
				},
			},

			"holiday_start_time": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidayStartTime == "" {
						return nil, nil
					}
					return config.HolidayStartTime, nil
				},
			},

			"holiday_end_time": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidayEndTime == "" {
						return nil, nil
					}
					return config.HolidayEndTime, nil
				},
			},
		},
	})

	zoneType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "zone",
		Fields: graphqlgo.Fields{
			"id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					return zone.ID, nil
				},
			},
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					return zone.Name, nil
				},
			},
			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(zoneStateType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return ZoneState{}, nil
					}
					return zone.State, nil
				},
			},
			"config": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(zoneConfigType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return ZoneConfig{}, nil
					}
					return zone.Config, nil
				},
			},
		},
	})

	energyPointMetaType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "energy_point_meta",
		Fields: graphqlgo.Fields{
			"freshness_state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok {
						return string(EnergyFreshnessStateNeverSeen), nil
					}
					if meta.FreshnessState == "" {
						return string(EnergyFreshnessStateNeverSeen), nil
					}
					return string(meta.FreshnessState), nil
				},
			},

			"provenance": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok {
						return string(EnergyProvenanceNone), nil
					}
					if meta.Provenance == "" {
						return string(EnergyProvenanceNone), nil
					}
					return string(meta.Provenance), nil
				},
			},
			"last_observed_utc": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok || meta.LastObservedUTC == "" {
						return nil, nil
					}
					return meta.LastObservedUTC, nil
				},
			},

			"age_seconds": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok || meta.LastObservedUTC == "" {
						return nil, nil
					}
					return meta.AgeSeconds, nil
				},
			},

			"stale": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok {
						return false, nil
					}
					return meta.Stale, nil
				},
			},
		},
	})

	energySeriesType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "energy_series",
		Fields: graphqlgo.Fields{
			"today": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Float),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.Today, nil
				},
			},
			"yearly": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Float))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.Yearly, nil
				},
			},
			"monthly": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Float)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.Monthly, nil
				},
			},
			"today_meta": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyPointMetaType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return EnergyPointMeta{}, nil
					}
					return series.TodayMeta, nil
				},
			},

			"yearly_meta": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(energyPointMetaType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return []EnergyPointMeta{}, nil
					}
					if len(series.YearlyMeta) == 0 {
						return []EnergyPointMeta{}, nil
					}
					return series.YearlyMeta, nil
				},
			},

			"monthly_meta": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(energyPointMetaType)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.MonthlyMeta, nil
				},
			},
		},
	})

	energyChannelType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "energy_channel",
		Fields: graphqlgo.Fields{
			"dhw": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energySeriesType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					channel, ok := params.Source.(EnergyChannel)
					if !ok {
						return nil, nil
					}
					return channel.DHW, nil
				},
			},
			"climate": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energySeriesType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					channel, ok := params.Source.(EnergyChannel)
					if !ok {
						return nil, nil
					}
					return channel.Climate, nil
				},
			},
		},
	})

	energyTotalsType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "energy_totals",
		Fields: graphqlgo.Fields{
			"gas": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyChannelType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					totals, ok := params.Source.(*EnergyTotals)
					if !ok || totals == nil {
						return nil, nil
					}
					return totals.Gas, nil
				},
			},
			"electric": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyChannelType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					totals, ok := params.Source.(*EnergyTotals)
					if !ok || totals == nil {
						return nil, nil
					}
					return totals.Electric, nil
				},
			},
			"solar": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyChannelType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					totals, ok := params.Source.(*EnergyTotals)
					if !ok || totals == nil {
						return nil, nil
					}
					return totals.Solar, nil
				},
			},
		},
	})

	dhwStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "dhw_state",
		Fields: graphqlgo.Fields{
			"overrun_active": &graphqlgo.Field{Type: graphqlgo.Boolean, Resolve: func(params graphqlgo.ResolveParams) (any, error) {
				state, ok := params.Source.(DhwState)
				if !ok || state.OverrunActive == nil {
					return nil, nil
				}
				return *state.OverrunActive, nil
			}},
			"current_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(DhwState)
					if !ok {
						return nil, nil
					}
					if state.CurrentTempC == nil {
						return nil, nil
					}
					return *state.CurrentTempC, nil
				},
			},

			"special_function": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(DhwState)
					if !ok {
						return nil, nil
					}
					if state.SpecialFunction == "" {
						return nil, nil
					}
					return state.SpecialFunction, nil
				},
			},

			"heating_demand_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(DhwState)
					if !ok {
						return nil, nil
					}
					if state.HeatingDemandPct == nil {
						return nil, nil
					}
					return *state.HeatingDemandPct, nil
				},
			},
		},
	})

	dhwConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "dhw_config",
		Fields: graphqlgo.Fields{
			"operation_mode_changeable": &graphqlgo.Field{Type: graphqlgo.Boolean, Resolve: func(params graphqlgo.ResolveParams) (any, error) {
				config, ok := params.Source.(DhwConfig)
				if !ok || config.OperationModeChangeable == nil {
					return nil, nil
				}
				return *config.OperationModeChangeable, nil
			}},
			"operating_mode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(DhwConfig)
					if !ok {
						return nil, nil
					}
					if config.OperatingMode == "" {
						return nil, nil
					}
					return config.OperatingMode, nil
				},
			},

			"preset": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(DhwConfig)
					if !ok {
						return nil, nil
					}
					if config.Preset == "" {
						return nil, nil
					}
					return config.Preset, nil
				},
			},
			"target_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(DhwConfig)
					if !ok {
						return nil, nil
					}
					if config.TargetTempC == nil {
						return nil, nil
					}
					return *config.TargetTempC, nil
				},
			},

			"holiday_start_date": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(DhwConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidayStartDate == "" {
						return nil, nil
					}
					return config.HolidayStartDate, nil
				},
			},

			"holiday_end_date": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(DhwConfig)
					if !ok {
						return nil, nil
					}
					if config.HolidayEndDate == "" {
						return nil, nil
					}
					return config.HolidayEndDate, nil
				},
			},
		},
	})

	dhwType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "dhw_status",
		Fields: graphqlgo.Fields{
			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(dhwStateType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok || status == nil {
						return DhwState{}, nil
					}
					return status.State, nil
				},
			},
			"config": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(dhwConfigType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok || status == nil {
						return DhwConfig{}, nil
					}
					return status.Config, nil
				},
			},
		},
	})

	circuitStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "circuit_state",
		Fields: graphqlgo.Fields{
			"pump_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.PumpActive == nil {
						return nil, nil
					}
					return *state.PumpActive, nil
				},
			},

			"mixer_position_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.MixerPositionPct == nil {
						return nil, nil
					}
					return *state.MixerPositionPct, nil
				},
			},

			"flow_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.FlowTemperatureC == nil {
						return nil, nil
					}
					return *state.FlowTemperatureC, nil
				},
			},

			"flow_setpoint_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.FlowSetpointC == nil {
						return nil, nil
					}
					return *state.FlowSetpointC, nil
				},
			},

			"calc_flow_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.CalcFlowTempC == nil {
						return nil, nil
					}
					return *state.CalcFlowTempC, nil
				},
			},

			"circuit_state": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.CircuitState == "" {
						return nil, nil
					}
					return state.CircuitState, nil
				},
			},

			"humidity": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.Humidity == nil {
						return nil, nil
					}
					return *state.Humidity, nil
				},
			},
			"dew_point": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.DewPoint == nil {
						return nil, nil
					}
					return *state.DewPoint, nil
				},
			},

			"pump_hours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.PumpHours == nil {
						return nil, nil
					}
					return *state.PumpHours, nil
				},
			},

			"pump_starts": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.PumpStarts == nil {
						return nil, nil
					}
					return *state.PumpStarts, nil
				},
			},
		},
	})

	circuitConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "circuit_config",
		Fields: graphqlgo.Fields{
			"heating_curve": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.HeatingCurve == nil {
						return nil, nil
					}
					return *config.HeatingCurve, nil
				},
			},

			"flow_temp_max_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.FlowTempMaxC == nil {
						return nil, nil
					}
					return *config.FlowTempMaxC, nil
				},
			},

			"flow_temp_min_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.FlowTempMinC == nil {
						return nil, nil
					}
					return *config.FlowTempMinC, nil
				},
			},

			"summer_limit_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.SummerLimitC == nil {
						return nil, nil
					}
					return *config.SummerLimitC, nil
				},
			},

			"frost_prot_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.FrostProtC == nil {
						return nil, nil
					}
					return *config.FrostProtC, nil
				},
			},

			"room_temp_control": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.RoomTempControl == "" {
						return nil, nil
					}
					return config.RoomTempControl, nil
				},
			},

			"cooling_enabled": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.CoolingEnabled == nil {
						return nil, nil
					}
					return *config.CoolingEnabled, nil
				},
			},
		},
	})

	managingDeviceRoleType := graphqlgo.NewEnum(graphqlgo.EnumConfig{
		Name: "managing_device_role",
		Values: graphqlgo.EnumValueConfigMap{
			"REGULATOR":       &graphqlgo.EnumValueConfig{Value: string(ManagingDeviceRoleRegulator)},
			"FUNCTION_MODULE": &graphqlgo.EnumValueConfig{Value: string(ManagingDeviceRoleFunctionModule)},
			"UNKNOWN":         &graphqlgo.EnumValueConfig{Value: string(ManagingDeviceRoleUnknown)},
		},
	})

	vaillantRegulatorCapabilityType := graphqlgo.NewEnum(graphqlgo.EnumConfig{
		Name: "vaillant_regulator_capability",
		Values: graphqlgo.EnumValueConfigMap{
			"UNKNOWN": &graphqlgo.EnumValueConfig{Value: string(VaillantRegulatorCapabilityUnknown)},
			"NONE":    &graphqlgo.EnumValueConfig{Value: string(VaillantRegulatorCapabilityNone)},
			"PRESENT": &graphqlgo.EnumValueConfig{Value: string(VaillantRegulatorCapabilityPresent)},
		},
	})

	circuitManagingDeviceType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "circuit_managing_device",
		Fields: graphqlgo.Fields{
			"role": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(managingDeviceRoleType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(ManagingDevice)
					if !ok || device.Role == "" {
						return string(ManagingDeviceRoleUnknown), nil
					}
					return string(device.Role), nil
				},
			},
			"device_id": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(ManagingDevice)
					if !ok || device.DeviceID == nil {
						return nil, nil
					}
					return *device.DeviceID, nil
				},
			},

			"address": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(ManagingDevice)
					if !ok || device.Address == nil {
						return nil, nil
					}
					return *device.Address, nil
				},
			},
		},
	})

	circuitStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "circuit_status",
		Fields: graphqlgo.Fields{
			"index": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return nil, nil
					}
					return status.Index, nil
				},
			},
			"circuit_type": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return nil, nil
					}
					return status.CircuitType, nil
				},
			},

			"has_mixer": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return nil, nil
					}
					return status.HasMixer, nil
				},
			},

			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(circuitStateType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return CircuitState{}, nil
					}
					return status.State, nil
				},
			},
			"config": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(circuitConfigType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return CircuitConfig{}, nil
					}
					return status.Config, nil
				},
			},
			"managing_device": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(circuitManagingDeviceType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return ManagingDevice{Role: ManagingDeviceRoleUnknown}, nil
					}
					return status.ManagingDevice, nil
				},
			},
		},
	})

	radioDeviceType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "radio_device",
		Fields: graphqlgo.Fields{
			"group": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok {
						return nil, nil
					}
					return device.Group, nil
				},
			},
			"instance": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok {
						return nil, nil
					}
					return device.Instance, nil
				},
			},
			"slot_mode": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok {
						return nil, nil
					}
					if device.SlotMode == "" {
						return "active", nil
					}
					return device.SlotMode, nil
				},
			},

			"device_connected": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DeviceConnected == nil {
						return nil, nil
					}
					return *device.DeviceConnected, nil
				},
			},

			"device_class_address": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DeviceClassAddress == nil {
						return nil, nil
					}
					return *device.DeviceClassAddress, nil
				},
			},

			"device_model": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DeviceModel == "" {
						return nil, nil
					}
					return device.DeviceModel, nil
				},
			},

			"firmware_version": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.FirmwareVersion == nil {
						return nil, nil
					}
					return *device.FirmwareVersion, nil
				},
			},

			"hardware_identifier": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.HardwareIdentifier == nil {
						return nil, nil
					}
					return *device.HardwareIdentifier, nil
				},
			},

			"remote_control_address": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.RemoteControlAddress == nil {
						return nil, nil
					}
					return *device.RemoteControlAddress, nil
				},
			},

			"device_paired": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DevicePaired == nil {
						return nil, nil
					}
					return *device.DevicePaired, nil
				},
			},

			"reception_strength": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.ReceptionStrength == nil {
						return nil, nil
					}
					return *device.ReceptionStrength, nil
				},
			},

			"zone_assignment": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.ZoneAssignment == nil {
						return nil, nil
					}
					return *device.ZoneAssignment, nil
				},
			},

			"room_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.RoomTemperatureC == nil {
						return nil, nil
					}
					return *device.RoomTemperatureC, nil
				},
			},

			"room_humidity_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.RoomHumidityPct == nil {
						return nil, nil
					}
					return *device.RoomHumidityPct, nil
				},
			},
		},
	})

	fm5SemanticModeType := graphqlgo.NewEnum(graphqlgo.EnumConfig{
		Name: "fm5_semantic_mode",
		Values: graphqlgo.EnumValueConfigMap{
			string(Fm5SemanticModeInterpreted): &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticModeInterpreted)},
			string(Fm5SemanticModeGPIOOnly):    &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticModeGPIOOnly)},
			string(Fm5SemanticModeAbsent):      &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticModeAbsent)},
		},
	})
	fm5SemanticDegradedReasonType := graphqlgo.NewEnum(graphqlgo.EnumConfig{
		Name: "fm5_semantic_degraded_reason",
		Values: graphqlgo.EnumValueConfigMap{
			string(Fm5SemanticDegradedReasonControllerUnreachable):         &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonControllerUnreachable)},
			string(Fm5SemanticDegradedReasonConfigurationUnavailable):      &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonConfigurationUnavailable)},
			string(Fm5SemanticDegradedReasonConfigurationNotInterpretable): &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonConfigurationNotInterpretable)},
			string(Fm5SemanticDegradedReasonSolarAcquisitionFailed):        &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonSolarAcquisitionFailed)},
			string(Fm5SemanticDegradedReasonCylinderAcquisitionFailed):     &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonCylinderAcquisitionFailed)},
			string(Fm5SemanticDegradedReasonEvidenceStale):                 &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonEvidenceStale)},
			string(Fm5SemanticDegradedReasonIncoherentAcquisition):         &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticDegradedReasonIncoherentAcquisition)},
		},
	})
	fm5InterpretationType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "fm5_interpretation",
		Fields: graphqlgo.Fields{
			"mode": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(fm5SemanticModeType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return string(params.Source.(Fm5Interpretation).Mode), nil
				},
			},
			"degraded_reason": &graphqlgo.Field{
				Type: fm5SemanticDegradedReasonType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					reason := params.Source.(Fm5Interpretation).DegradedReason
					if reason == "" {
						return nil, nil
					}
					return string(reason), nil
				},
			},
			"evidence_revision": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return params.Source.(Fm5Interpretation).EvidenceRevision, nil
				},
			},
		},
	})

	solarStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "solar_status",
		Fields: graphqlgo.Fields{
			"collector_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.CollectorTemperatureC == nil {
						return nil, nil
					}
					return *status.CollectorTemperatureC, nil
				},
			},

			"return_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.ReturnTemperatureC == nil {
						return nil, nil
					}
					return *status.ReturnTemperatureC, nil
				},
			},

			"pump_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.PumpActive == nil {
						return nil, nil
					}
					return *status.PumpActive, nil
				},
			},

			"current_yield": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.CurrentYield == nil {
						return nil, nil
					}
					return *status.CurrentYield, nil
				},
			},

			"pump_hours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.PumpHours == nil {
						return nil, nil
					}
					return *status.PumpHours, nil
				},
			},

			"solar_enabled": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.SolarEnabled == nil {
						return nil, nil
					}
					return *status.SolarEnabled, nil
				},
			},

			"function_mode": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.FunctionMode == nil {
						return nil, nil
					}
					return *status.FunctionMode, nil
				},
			},
		},
	})

	cylinderStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "cylinder_status",
		Fields: graphqlgo.Fields{
			"index": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok {
						return nil, nil
					}
					return status.Index, nil
				},
			},
			"temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.TemperatureC == nil {
						return nil, nil
					}
					return *status.TemperatureC, nil
				},
			},

			"max_setpoint_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.MaxSetpointC == nil {
						return nil, nil
					}
					return *status.MaxSetpointC, nil
				},
			},

			"charge_hysteresis_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.ChargeHysteresisC == nil {
						return nil, nil
					}
					return *status.ChargeHysteresisC, nil
				},
			},

			"charge_offset_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.ChargeOffsetC == nil {
						return nil, nil
					}
					return *status.ChargeOffsetC, nil
				},
			},
		},
	})

	boilerStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "boiler_state",
		Fields: graphqlgo.Fields{
			"flow_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok {
						return nil, nil
					}
					if state.FlowTemperatureC == nil {
						return nil, nil
					}
					return *state.FlowTemperatureC, nil
				},
			},

			"return_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok {
						return nil, nil
					}
					if state.ReturnTemperatureC == nil {
						return nil, nil
					}
					return *state.ReturnTemperatureC, nil
				},
			},

			"central_heating_pump_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok {
						return nil, nil
					}
					if state.CentralHeatingPumpActive == nil {
						return nil, nil
					}
					return *state.CentralHeatingPumpActive, nil
				},
			},

			"water_pressure_bar": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.WaterPressureBar == nil {
						return nil, nil
					}
					return *state.WaterPressureBar, nil
				},
			},

			"external_pump_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.ExternalPumpActive == nil {
						return nil, nil
					}
					return *state.ExternalPumpActive, nil
				},
			},

			"circulation_pump_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.CirculationPumpActive == nil {
						return nil, nil
					}
					return *state.CirculationPumpActive, nil
				},
			},

			"gas_valve_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.GasValveActive == nil {
						return nil, nil
					}
					return *state.GasValveActive, nil
				},
			},

			"flame_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.FlameActive == nil {
						return nil, nil
					}
					return *state.FlameActive, nil
				},
			},

			"diverter_valve_position_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DiverterValvePositionPct == nil {
						return nil, nil
					}
					return *state.DiverterValvePositionPct, nil
				},
			},

			"fan_speed_rpm": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.FanSpeedRpm == nil {
						return nil, nil
					}
					return *state.FanSpeedRpm, nil
				},
			},

			"target_fan_speed_rpm": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.TargetFanSpeedRpm == nil {
						return nil, nil
					}
					return *state.TargetFanSpeedRpm, nil
				},
			},

			"ionisation_voltage_ua": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.IonisationVoltageUa == nil {
						return nil, nil
					}
					return *state.IonisationVoltageUa, nil
				},
			},

			"dhw_water_flow_lpm": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwWaterFlowLpm == nil {
						return nil, nil
					}
					return *state.DhwWaterFlowLpm, nil
				},
			},

			"dhw_demand_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwDemandActive == nil {
						return nil, nil
					}
					return *state.DhwDemandActive, nil
				},
			},

			"heating_switch_active": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.HeatingSwitchActive == nil {
						return nil, nil
					}
					return *state.HeatingSwitchActive, nil
				},
			},

			"storage_load_pump_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.StorageLoadPumpPct == nil {
						return nil, nil
					}
					return *state.StorageLoadPumpPct, nil
				},
			},

			"modulation_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.ModulationPct == nil {
						return nil, nil
					}
					return *state.ModulationPct, nil
				},
			},

			"primary_circuit_flow_lpm": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.PrimaryCircuitFlowLpm == nil {
						return nil, nil
					}
					return *state.PrimaryCircuitFlowLpm, nil
				},
			},

			"flow_temp_desired_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.FlowTempDesiredC == nil {
						return nil, nil
					}
					return *state.FlowTempDesiredC, nil
				},
			},

			"dhw_temp_desired_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwTempDesiredC == nil {
						return nil, nil
					}
					return *state.DhwTempDesiredC, nil
				},
			},

			"state_number": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.StateNumber == nil {
						return nil, nil
					}
					return *state.StateNumber, nil
				},
			},

			"dhw_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwTemperatureC == nil {
						return nil, nil
					}
					return *state.DhwTemperatureC, nil
				},
			},

			"dhw_target_temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwTargetTemperatureC == nil {
						return nil, nil
					}
					return *state.DhwTargetTemperatureC, nil
				},
			},
		},
	})

	boilerConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "boiler_config",
		Fields: graphqlgo.Fields{
			"dhw_operating_mode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.DhwOperatingMode == nil {
						return nil, nil
					}
					return *config.DhwOperatingMode, nil
				},
			},

			"flowset_hc_max_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.FlowsetHcMaxC == nil {
						return nil, nil
					}
					return *config.FlowsetHcMaxC, nil
				},
			},

			"flowset_hwc_max_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.FlowsetHwcMaxC == nil {
						return nil, nil
					}
					return *config.FlowsetHwcMaxC, nil
				},
			},

			"partload_hc_kw": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.PartloadHcKW == nil {
						return nil, nil
					}
					return *config.PartloadHcKW, nil
				},
			},

			"partload_hwc_kw": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.PartloadHwcKW == nil {
						return nil, nil
					}
					return *config.PartloadHwcKW, nil
				},
			},

			"installer_menu_code": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.InstallerMenuCode == nil {
						return nil, nil
					}
					return *config.InstallerMenuCode, nil
				},
			},

			"phone_number": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.PhoneNumber == nil {
						return nil, nil
					}
					return *config.PhoneNumber, nil
				},
			},

			"hours_till_service": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.HoursTillService == nil {
						return nil, nil
					}
					return *config.HoursTillService, nil
				},
			},
		},
	})

	boilerDiagnosticsType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "boiler_diagnostics",
		Fields: graphqlgo.Fields{
			"heating_status_raw": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok {
						return nil, nil
					}
					if diag.HeatingStatusRaw == nil {
						return nil, nil
					}
					return *diag.HeatingStatusRaw, nil
				},
			},

			"dhw_status_raw": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DhwStatusRaw == nil {
						return nil, nil
					}
					return *diag.DhwStatusRaw, nil
				},
			},

			"central_heating_hours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.CentralHeatingHours == nil {
						return nil, nil
					}
					return *diag.CentralHeatingHours, nil
				},
			},

			"dhw_hours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DhwHours == nil {
						return nil, nil
					}
					return *diag.DhwHours, nil
				},
			},

			"central_heating_starts": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.CentralHeatingStarts == nil {
						return nil, nil
					}
					return *diag.CentralHeatingStarts, nil
				},
			},

			"dhw_starts": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DhwStarts == nil {
						return nil, nil
					}
					return *diag.DhwStarts, nil
				},
			},

			"pump_hours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.PumpHours == nil {
						return nil, nil
					}
					return *diag.PumpHours, nil
				},
			},

			"fan_hours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.FanHours == nil {
						return nil, nil
					}
					return *diag.FanHours, nil
				},
			},

			"deactivations_ifc": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DeactivationsIFC == nil {
						return nil, nil
					}
					return *diag.DeactivationsIFC, nil
				},
			},

			"deactivations_templimiter": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DeactivationsTemplimiter == nil {
						return nil, nil
					}
					return *diag.DeactivationsTemplimiter, nil
				},
			},
		},
	})

	boilerStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "boiler_status",
		Fields: graphqlgo.Fields{
			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(boilerStateType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BoilerStatus)
					if !ok || status == nil {
						return BoilerState{}, nil
					}
					return status.State, nil
				},
			},
			"config": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(boilerConfigType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BoilerStatus)
					if !ok || status == nil {
						return BoilerConfig{}, nil
					}
					return status.Config, nil
				},
			},
			"diagnostics": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(boilerDiagnosticsType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BoilerStatus)
					if !ok || status == nil {
						return BoilerDiagnostics{}, nil
					}
					return status.Diagnostics, nil
				},
			},
		},
	})

	systemStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "system_state",
		Fields: graphqlgo.Fields{
			"system_off": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.SystemOff == nil {
						return nil, nil
					}
					return *state.SystemOff, nil
				},
			},

			"system_water_pressure": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.SystemWaterPressure == nil {
						return nil, nil
					}
					return *state.SystemWaterPressure, nil
				},
			},

			"system_flow_temperature": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.SystemFlowTemperature == nil {
						return nil, nil
					}
					return *state.SystemFlowTemperature, nil
				},
			},

			"outdoor_temperature": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.OutdoorTemperature == nil {
						return nil, nil
					}
					return *state.OutdoorTemperature, nil
				},
			},

			"outdoor_temperature_avg24h": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.OutdoorTemperatureAvg24h == nil {
						return nil, nil
					}
					return *state.OutdoorTemperatureAvg24h, nil
				},
			},

			"maintenance_due": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.MaintenanceDue == nil {
						return nil, nil
					}
					return *state.MaintenanceDue, nil
				},
			},

			"hwc_cylinder_temperature_top": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.HwcCylinderTemperatureTop == nil {
						return nil, nil
					}
					return *state.HwcCylinderTemperatureTop, nil
				},
			},

			"hwc_cylinder_temperature_bottom": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.HwcCylinderTemperatureBottom == nil {
						return nil, nil
					}
					return *state.HwcCylinderTemperatureBottom, nil
				},
			},
		},
	})

	systemConfigBoundType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "system_config_bound",
		Fields: graphqlgo.Fields{
			"narrowed_only": &graphqlgo.Field{
				Type:        graphqlgo.NewNonNull(graphqlgo.Boolean),
				Description: "Qualified controller-profile evidence can only narrow the released -20..50 policy window; unmatched controllers retain that window. A profile match does not qualify the target installation.",
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if bound, ok := params.Source.(ConfigBound); ok {
						return bound.NarrowedOnly, nil
					}
					return nil, nil
				},
			},

			"min_c": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Float),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if bound, ok := params.Source.(ConfigBound); ok {
						return bound.MinC, nil
					}
					return nil, nil
				},
			},

			"max_c": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Float),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if bound, ok := params.Source.(ConfigBound); ok {
						return bound.MaxC, nil
					}
					return nil, nil
				},
			},

			"source": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if bound, ok := params.Source.(ConfigBound); ok {
						return bound.Source, nil
					}
					return nil, nil
				},
			},
			"reason": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if bound, ok := params.Source.(ConfigBound); ok && bound.Reason != "" {
						return bound.Reason, nil
					}
					return nil, nil
				},
			},
			"profile": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					if bound, ok := params.Source.(ConfigBound); ok && bound.Profile != "" {
						return bound.Profile, nil
					}
					return nil, nil
				},
			},
		},
	})

	systemConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "system_config",
		Fields: graphqlgo.Fields{
			"adaptive_heating_curve": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.AdaptiveHeatingCurve == nil {
						return nil, nil
					}
					return *config.AdaptiveHeatingCurve, nil
				},
			},

			"alternative_point": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.AlternativePoint == nil {
						return nil, nil
					}
					return *config.AlternativePoint, nil
				},
			},

			"heating_circuit_bivalence_point": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.HeatingCircuitBivalencePoint == nil {
						return nil, nil
					}
					return *config.HeatingCircuitBivalencePoint, nil
				},
			},

			"dhw_bivalence_point": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.DhwBivalencePoint == nil {
						return nil, nil
					}
					return *config.DhwBivalencePoint, nil
				},
			},
			"dhw_bivalence_point_bound": &graphqlgo.Field{
				Type: systemConfigBoundType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.DhwBivalenceBound == nil {
						return nil, nil
					}
					return *config.DhwBivalenceBound, nil
				},
			},

			"hc_emergency_temperature": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.HcEmergencyTemperature == nil {
						return nil, nil
					}
					return *config.HcEmergencyTemperature, nil
				},
			},

			"hwc_max_flow_temp_desired": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.HwcMaxFlowTempDesired == nil {
						return nil, nil
					}
					return *config.HwcMaxFlowTempDesired, nil
				},
			},

			"max_room_humidity": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.MaxRoomHumidity == nil {
						return nil, nil
					}
					return *config.MaxRoomHumidity, nil
				},
			},

			"maintenance_date": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.MaintenanceDate == nil {
						return nil, nil
					}
					return *config.MaintenanceDate, nil
				},
			},

			"installer_name": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.InstallerName == nil {
						return nil, nil
					}
					return *config.InstallerName, nil
				},
			},

			"installer_phone": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.InstallerPhone == nil {
						return nil, nil
					}
					return *config.InstallerPhone, nil
				},
			},

			"installer_menu_code": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.InstallerMenuCode == nil {
						return nil, nil
					}
					return *config.InstallerMenuCode, nil
				},
			},
		},
	})

	systemPropertiesType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "system_properties",
		Fields: graphqlgo.Fields{
			"system_scheme": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					props, ok := params.Source.(SystemProperties)
					if !ok || props.SystemScheme == nil {
						return nil, nil
					}
					return *props.SystemScheme, nil
				},
			},

			"module_configuration_vr71": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					props, ok := params.Source.(SystemProperties)
					if !ok || props.ModuleConfigurationVR71 == nil {
						return nil, nil
					}
					return *props.ModuleConfigurationVR71, nil
				},
			},
		},
	})

	systemStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "system_status",
		Fields: graphqlgo.Fields{
			"gateway_brand": &graphqlgo.Field{Type: graphqlgo.String, Resolve: func(params graphqlgo.ResolveParams) (any, error) {
				status, ok := params.Source.(*SystemStatus)
				if !ok || status == nil || status.GatewayBrand == nil {
					return nil, nil
				}
				return *status.GatewayBrand, nil
			}},
			"gateway_vendor": &graphqlgo.Field{Type: graphqlgo.String, Resolve: func(params graphqlgo.ResolveParams) (any, error) {
				status, ok := params.Source.(*SystemStatus)
				if !ok || status == nil || status.GatewayVendor == nil {
					return nil, nil
				}
				return *status.GatewayVendor, nil
			}},
			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(systemStateType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SystemStatus)
					if !ok || status == nil {
						return SystemState{}, nil
					}
					return status.State, nil
				},
			},
			"config": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(systemConfigType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SystemStatus)
					if !ok || status == nil {
						return SystemConfig{}, nil
					}
					return status.Config, nil
				},
			},
			"properties": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(systemPropertiesType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SystemStatus)
					if !ok || status == nil {
						return SystemProperties{}, nil
					}
					return status.Properties, nil
				},
			},
		},
	})

	statusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "service_status",
		Fields: graphqlgo.Fields{
			"status": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.Status, nil
				},
			},
			"firmware_version": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.FirmwareVersion, nil
				},
			},

			"updates_available": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.UpdatesAvailable, nil
				},
			},

			"initiator_address": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.InitiatorAddress, nil
				},
			},
		},
	})

	gatewayIdentityType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "gateway_identity",
		Fields: graphqlgo.Fields{
			"instance_guid": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					identity, ok := params.Source.(GatewayIdentity)
					if !ok {
						return nil, nil
					}
					if identity.InstanceGUID == "" {
						return nil, nil
					}
					return identity.InstanceGUID, nil
				},
			},
		},
	})

	fieldType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "field",
		Fields: graphqlgo.Fields{
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					field, ok := fieldFromSource(params)
					if !ok {
						return nil, nil
					}
					return field.Name, nil
				},
			},
			"type": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					field, ok := fieldFromSource(params)
					if !ok {
						return nil, nil
					}
					return field.Type, nil
				},
			},
			"size": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					field, ok := fieldFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(field.Size), nil
				},
			},
		},
	})

	responseType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "response_schema",
		Fields: graphqlgo.Fields{
			"fields": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(fieldType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					response, ok := responseFromSource(params)
					if !ok {
						return nil, nil
					}
					return response.Fields, nil
				},
			},
		},
	})

	methodType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "method",
		Fields: graphqlgo.Fields{
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return method.Name, nil
				},
			},
			"read_only": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return method.ReadOnly, nil
				},
			},
			"primary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(method.Primary), nil
				},
			},
			"secondary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(method.Secondary), nil
				},
			},
			"response": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(responseType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return method.Response, nil
				},
			},
		},
	})

	projectionNodeType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "projection_node",
		Fields: graphqlgo.Fields{
			"id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					node, ok := projectionNodeFromSource(params)
					if !ok {
						return nil, nil
					}
					return node.ID, nil
				},
			},
			"path": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					node, ok := projectionNodeFromSource(params)
					if !ok {
						return nil, nil
					}
					return node.Path, nil
				},
			},
			"canonical_path": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					node, ok := projectionNodeFromSource(params)
					if !ok {
						return nil, nil
					}
					return node.CanonicalPath, nil
				},
			},
		},
	})

	projectionEdgeType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "projection_edge",
		Fields: graphqlgo.Fields{
			"id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					edge, ok := projectionEdgeFromSource(params)
					if !ok {
						return nil, nil
					}
					return edge.ID, nil
				},
			},
			"from": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					edge, ok := projectionEdgeFromSource(params)
					if !ok {
						return nil, nil
					}
					return edge.From, nil
				},
			},
			"to": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					edge, ok := projectionEdgeFromSource(params)
					if !ok {
						return nil, nil
					}
					return edge.To, nil
				},
			},
		},
	})

	projectionType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "projection",
		Fields: graphqlgo.Fields{
			"plane": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					projection, ok := projectionFromSource(params)
					if !ok {
						return nil, nil
					}
					return projection.Plane, nil
				},
			},
			"nodes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(projectionNodeType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					projection, ok := projectionFromSource(params)
					if !ok {
						return nil, nil
					}
					return projection.Nodes, nil
				},
			},
			"edges": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(projectionEdgeType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					projection, ok := projectionFromSource(params)
					if !ok {
						return nil, nil
					}
					return projection.Edges, nil
				},
			},
		},
	})

	planeType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "plane",
		Fields: graphqlgo.Fields{
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					plane, ok := planeFromSource(params)
					if !ok {
						return nil, nil
					}
					return plane.Name, nil
				},
			},
			"methods": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(methodType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					plane, ok := planeFromSource(params)
					if !ok {
						return nil, nil
					}
					return plane.Methods, nil
				},
			},
		},
	})

	deviceType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "device",
		Fields: graphqlgo.Fields{
			"address": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(device.Address), nil
				},
			},
			"discovery_source": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.DiscoverySource == "" {
						return nil, nil
					}
					return device.DiscoverySource, nil
				},
			},
			"verification_state": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.VerificationState == "" {
						return nil, nil
					}
					return device.VerificationState, nil
				},
			},
			"addresses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Int))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					addresses := normalizeDeviceAddresses(device.Address, device.Addresses)
					values := make([]int, len(addresses))
					for index, address := range addresses {
						values[index] = int(address)
					}
					return values, nil
				},
			},
			"manufacturer": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.Manufacturer, nil
				},
			},
			"device_id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.DeviceID, nil
				},
			},

			"serial_number": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.SerialNumber, nil
				},
			},

			"mac_address": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.MacAddress, nil
				},
			},

			"software_version": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.SoftwareVersion, nil
				},
			},

			"hardware_version": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.HardwareVersion, nil
				},
			},

			"display_name": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.DisplayName == "" {
						return nil, nil
					}
					return device.DisplayName, nil
				},
			},

			"product_family": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.ProductFamily == "" {
						return nil, nil
					}
					return device.ProductFamily, nil
				},
			},

			"product_model": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.ProductModel == "" {
						return nil, nil
					}
					return device.ProductModel, nil
				},
			},

			"part_number": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.PartNumber == "" {
						return nil, nil
					}
					return device.PartNumber, nil
				},
			},

			"role": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.Role == "" {
						return nil, nil
					}
					return device.Role, nil
				},
			},
			"planes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(planeType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.Planes, nil
				},
			},
			"projections": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(projectionType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.Projections, nil
				},
			},
		},
	})

	scheduleTimerSlotType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "schedule_timer_slot",
		Fields: graphqlgo.Fields{
			"start_hour": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.StartHour, nil
				},
			},

			"start_minute": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.StartMinute, nil
				},
			},

			"end_hour": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.EndHour, nil
				},
			},

			"end_minute": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.EndMinute, nil
				},
			},

			"temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok || slot.TemperatureC == nil {
						return nil, nil
					}
					return *slot.TemperatureC, nil
				},
			},

			"temperature_raw": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok || slot.TemperatureRaw == nil {
						return nil, nil
					}
					return *slot.TemperatureRaw, nil
				},
			},
		},
	})

	scheduleDayProgramType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "schedule_day_program",
		Fields: graphqlgo.Fields{
			"weekday": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					day, ok := params.Source.(ScheduleDayProgram)
					if !ok {
						return nil, nil
					}
					return day.Weekday, nil
				},
			},
			"slots": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(scheduleTimerSlotType)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					day, ok := params.Source.(ScheduleDayProgram)
					if !ok {
						return nil, nil
					}
					return day.Slots, nil
				},
			},
		},
	})

	scheduleConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "schedule_config",
		Fields: graphqlgo.Fields{
			"max_slots": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.MaxSlots, nil
				},
			},

			"time_resolution": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.TimeResolution, nil
				},
			},

			"min_duration": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.MinDuration, nil
				},
			},

			"has_temperature": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.HasTemperature, nil
				},
			},

			"temp_slots": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.TempSlots, nil
				},
			},

			"min_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil || cfg.MinTempC == nil {
						return nil, nil
					}
					return *cfg.MinTempC, nil
				},
			},

			"max_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil || cfg.MaxTempC == nil {
						return nil, nil
					}
					return *cfg.MaxTempC, nil
				},
			},
		},
	})

	scheduleProgramType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "schedule_program",
		Fields: graphqlgo.Fields{
			"zone": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					prog, ok := params.Source.(ScheduleProgram)
					if !ok {
						return nil, nil
					}
					return prog.Zone, nil
				},
			},
			"hc": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					prog, ok := params.Source.(ScheduleProgram)
					if !ok {
						return nil, nil
					}
					return prog.HC, nil
				},
			},
			"config": &graphqlgo.Field{
				Type: scheduleConfigType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					prog, ok := params.Source.(ScheduleProgram)
					if !ok {
						return nil, nil
					}
					return prog.Config, nil
				},
			},
			"slots_used": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Int)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					prog, ok := params.Source.(ScheduleProgram)
					if !ok {
						return nil, nil
					}
					return prog.SlotsUsed, nil
				},
			},

			"days": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(scheduleDayProgramType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					prog, ok := params.Source.(ScheduleProgram)
					if !ok {
						return nil, nil
					}
					return prog.Days, nil
				},
			},
		},
	})

	scheduleStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "schedule_status",
		Fields: graphqlgo.Fields{
			"programs": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(scheduleProgramType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*ScheduleStatus)
					if !ok || status == nil {
						return nil, nil
					}
					return status.Programs, nil
				},
			},
		},
	})

	adapterHardwareInfoType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "adapter_hardware_info",
		Fields: graphqlgo.Fields{
			"firmware_version": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},

			"firmware_checksum": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},

			"bootloader_version": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},

			"bootloader_checksum": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},

			"hardware_id": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},

			"hardware_config": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},

			"features": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"jumpers":  &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"jumper_flags": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return []string{}, nil
					}
					if info.JumperFlags == nil {
						return []string{}, nil
					}
					return info.JumperFlags, nil
				},
			},

			"is_wifi": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},

			"is_ethernet": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},

			"temperature_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.TemperatureC == nil {
						return nil, nil
					}
					return *info.TemperatureC, nil
				},
			},

			"supply_voltage_mv": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.SupplyVoltageMV == nil {
						return nil, nil
					}
					return *info.SupplyVoltageMV, nil
				},
			},

			"bus_voltage_max_dv": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.BusVoltageMaxDV == nil {
						return nil, nil
					}
					return *info.BusVoltageMaxDV, nil
				},
			},

			"bus_voltage_min_dv": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.BusVoltageMinDV == nil {
						return nil, nil
					}
					return *info.BusVoltageMinDV, nil
				},
			},

			"reset_cause": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.ResetCause == nil {
						return nil, nil
					}
					return *info.ResetCause, nil
				},
			},

			"reset_cause_code": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.ResetCauseCode == nil {
						return nil, nil
					}
					return int(*info.ResetCauseCode), nil
				},
			},

			"restart_count": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.RestartCount == nil {
						return nil, nil
					}
					return int(*info.RestartCount), nil
				},
			},

			"wifi_rssi_dbm": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.WiFiRSSIDBm == nil {
						return nil, nil
					}
					return *info.WiFiRSSIDBm, nil
				},
			},

			"last_identity_query": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.LastIdentityQuery == nil {
						return nil, nil
					}
					return info.LastIdentityQuery.UTC().Format("2006-01-02T15:04:05Z"), nil
				},
			},

			"last_telemetry_query": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.LastTelemetryQuery == nil {
						return nil, nil
					}
					return info.LastTelemetryQuery.UTC().Format("2006-01-02T15:04:05Z"), nil
				},
			},

			"version_response_len": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},

			"info_supported": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},
		},
	})

	busSummaryType, busMessagesType, busPeriodicityType := buildBusObservabilityTypes()
	watchSummaryType := buildWatchSummaryType()

	return graphqlSchemaTypes{
		fieldType:                   fieldType,
		responseType:                responseType,
		methodType:                  methodType,
		projectionNodeType:          projectionNodeType,
		projectionEdgeType:          projectionEdgeType,
		projectionType:              projectionType,
		planeType:                   planeType,
		deviceType:                  deviceType,
		broadcastType:               buildBroadcastType(),
		statusType:                  statusType,
		gatewayIdentityType:         gatewayIdentityType,
		zoneType:                    zoneType,
		dhwType:                     dhwType,
		circuitStatusType:           circuitStatusType,
		radioDeviceType:             radioDeviceType,
		fm5SemanticMode:             fm5SemanticModeType,
		vaillantRegulatorCapability: vaillantRegulatorCapabilityType,
		fm5Interpretation:           fm5InterpretationType,
		solarStatusType:             solarStatusType,
		cylinderStatusType:          cylinderStatusType,
		energyTotals:                energyTotalsType,
		boilerStatusType:            boilerStatusType,
		systemStatusType:            systemStatusType,
		scheduleStatusType:          scheduleStatusType,
		adapterHardwareInfoType:     adapterHardwareInfoType,
		busSummaryType:              busSummaryType,
		busMessagesType:             busMessagesType,
		busPeriodicityType:          busPeriodicityType,
		busIdentificationEvidence:   buildIdentificationEvidenceType(),
		watchSummaryType:            watchSummaryType,
	}
}
