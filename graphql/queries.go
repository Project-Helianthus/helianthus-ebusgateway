package graphql

import (
	"context"
	"fmt"
	"net/http"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

type graphqlSchemaTypes struct {
	fieldType               *graphqlgo.Object
	responseType            *graphqlgo.Object
	methodType              *graphqlgo.Object
	projectionNodeType      *graphqlgo.Object
	projectionEdgeType      *graphqlgo.Object
	projectionType          *graphqlgo.Object
	planeType               *graphqlgo.Object
	deviceType              *graphqlgo.Object
	broadcastType           *graphqlgo.Object
	statusType              *graphqlgo.Object
	gatewayIdentityType     *graphqlgo.Object
	zoneType                *graphqlgo.Object
	dhwType                 *graphqlgo.Object
	circuitStatusType       *graphqlgo.Object
	radioDeviceType         *graphqlgo.Object
	fm5SemanticMode         *graphqlgo.Enum
	solarStatusType         *graphqlgo.Object
	cylinderStatusType      *graphqlgo.Object
	energyTotals            *graphqlgo.Object
	boilerStatusType        *graphqlgo.Object
	systemStatusType        *graphqlgo.Object
	scheduleStatusType      *graphqlgo.Object
	adapterHardwareInfoType *graphqlgo.Object
	busSummaryType          *graphqlgo.Object
	busMessagesType         *graphqlgo.Object
	busPeriodicityType      *graphqlgo.Object
	watchSummaryType        *graphqlgo.Object
}

func buildSchemaTypes() graphqlSchemaTypes {
	zoneStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ZoneState",
		Fields: graphqlgo.Fields{
			"currentTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
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
			"current_temp_c": &graphqlgo.Field{
				Type: graphqlgo.Float,
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
			"currentHumidityPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
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
			"current_humidity_pct": &graphqlgo.Field{
				Type: graphqlgo.Float,
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
			"hvacAction": &graphqlgo.Field{
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
			"specialFunction": &graphqlgo.Field{
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
			"heatingDemandPct": &graphqlgo.Field{
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
			"valvePositionPct": &graphqlgo.Field{
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
		Name: "ZoneConfig",
		Fields: graphqlgo.Fields{
			"operatingMode": &graphqlgo.Field{
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
			"targetTempC": &graphqlgo.Field{
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
			"allowedModes": &graphqlgo.Field{
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
			"circuitType": &graphqlgo.Field{
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
			"associatedCircuit": &graphqlgo.Field{
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
			"roomTemperatureZoneMapping": &graphqlgo.Field{
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
			"quickVeto": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(ZoneConfig)
					if !ok {
						return false, nil
					}
					return config.QuickVeto, nil
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
			"quickVetoSetpoint": &graphqlgo.Field{
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
			"quickVetoDuration": &graphqlgo.Field{
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
			"quickVetoExpiry": &graphqlgo.Field{
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
			"holidayStartDate": &graphqlgo.Field{
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
			"holidayEndDate": &graphqlgo.Field{
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
			"holidaySetpoint": &graphqlgo.Field{
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
			"holidayStartTime": &graphqlgo.Field{
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
			"holidayEndTime": &graphqlgo.Field{
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
		Name: "Zone",
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
		Name: "EnergyPointMeta",
		Fields: graphqlgo.Fields{
			"freshnessState": &graphqlgo.Field{
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
			"lastObservedUtc": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok || meta.LastObservedUTC == "" {
						return nil, nil
					}
					return meta.LastObservedUTC, nil
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
			"ageSeconds": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					meta, ok := params.Source.(EnergyPointMeta)
					if !ok || meta.LastObservedUTC == "" {
						return nil, nil
					}
					return meta.AgeSeconds, nil
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
		Name: "EnergySeries",
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
			"todayMeta": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyPointMetaType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return EnergyPointMeta{}, nil
					}
					return series.TodayMeta, nil
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
			"yearlyMeta": &graphqlgo.Field{
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
			"monthlyMeta": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(energyPointMetaType)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.MonthlyMeta, nil
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
		Name: "EnergyChannel",
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
		Name: "EnergyTotals",
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
		Name: "DhwState",
		Fields: graphqlgo.Fields{
			"currentTempC": &graphqlgo.Field{
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
			"specialFunction": &graphqlgo.Field{
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
			"heatingDemandPct": &graphqlgo.Field{
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
		Name: "DhwConfig",
		Fields: graphqlgo.Fields{
			"operatingMode": &graphqlgo.Field{
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
			"targetTempC": &graphqlgo.Field{
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
			"holidayStartDate": &graphqlgo.Field{
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
			"holidayEndDate": &graphqlgo.Field{
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
		Name: "DhwStatus",
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
		Name: "CircuitState",
		Fields: graphqlgo.Fields{
			"pumpActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.PumpActive == nil {
						return nil, nil
					}
					return *state.PumpActive, nil
				},
			},
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
			"mixerPositionPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.MixerPositionPct == nil {
						return nil, nil
					}
					return *state.MixerPositionPct, nil
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
			"flowTemperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.FlowTemperatureC == nil {
						return nil, nil
					}
					return *state.FlowTemperatureC, nil
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
			"flowSetpointC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.FlowSetpointC == nil {
						return nil, nil
					}
					return *state.FlowSetpointC, nil
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
			"calcFlowTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.CalcFlowTempC == nil {
						return nil, nil
					}
					return *state.CalcFlowTempC, nil
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
			"circuitState": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.CircuitState == "" {
						return nil, nil
					}
					return state.CircuitState, nil
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
			"dewPoint": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.DewPoint == nil {
						return nil, nil
					}
					return *state.DewPoint, nil
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
			"pumpHours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.PumpHours == nil {
						return nil, nil
					}
					return *state.PumpHours, nil
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
			"pumpStarts": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(CircuitState)
					if !ok || state.PumpStarts == nil {
						return nil, nil
					}
					return *state.PumpStarts, nil
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
		Name: "CircuitConfig",
		Fields: graphqlgo.Fields{
			"heatingCurve": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.HeatingCurve == nil {
						return nil, nil
					}
					return *config.HeatingCurve, nil
				},
			},
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
			"flowTempMaxC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.FlowTempMaxC == nil {
						return nil, nil
					}
					return *config.FlowTempMaxC, nil
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
			"flowTempMinC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.FlowTempMinC == nil {
						return nil, nil
					}
					return *config.FlowTempMinC, nil
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
			"summerLimitC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.SummerLimitC == nil {
						return nil, nil
					}
					return *config.SummerLimitC, nil
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
			"frostProtC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.FrostProtC == nil {
						return nil, nil
					}
					return *config.FrostProtC, nil
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
			"roomTempControl": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.RoomTempControl == "" {
						return nil, nil
					}
					return config.RoomTempControl, nil
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
			"coolingEnabled": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(CircuitConfig)
					if !ok || config.CoolingEnabled == nil {
						return nil, nil
					}
					return *config.CoolingEnabled, nil
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
		Name: "ManagingDeviceRole",
		Values: graphqlgo.EnumValueConfigMap{
			"REGULATOR":       &graphqlgo.EnumValueConfig{Value: string(ManagingDeviceRoleRegulator)},
			"FUNCTION_MODULE": &graphqlgo.EnumValueConfig{Value: string(ManagingDeviceRoleFunctionModule)},
			"UNKNOWN":         &graphqlgo.EnumValueConfig{Value: string(ManagingDeviceRoleUnknown)},
		},
	})

	circuitManagingDeviceType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "CircuitManagingDevice",
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
			"deviceId": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(ManagingDevice)
					if !ok || device.DeviceID == nil {
						return nil, nil
					}
					return *device.DeviceID, nil
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
		Name: "CircuitStatus",
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
			"circuitType": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return nil, nil
					}
					return status.CircuitType, nil
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
			"hasMixer": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return nil, nil
					}
					return status.HasMixer, nil
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
			"managingDevice": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(circuitManagingDeviceType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CircuitStatus)
					if !ok {
						return ManagingDevice{Role: ManagingDeviceRoleUnknown}, nil
					}
					return status.ManagingDevice, nil
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
		Name: "RadioDevice",
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
			"slotMode": &graphqlgo.Field{
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
			"deviceConnected": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DeviceConnected == nil {
						return nil, nil
					}
					return *device.DeviceConnected, nil
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
			"deviceClassAddress": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DeviceClassAddress == nil {
						return nil, nil
					}
					return *device.DeviceClassAddress, nil
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
			"deviceModel": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DeviceModel == "" {
						return nil, nil
					}
					return device.DeviceModel, nil
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
			"firmwareVersion": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.FirmwareVersion == nil {
						return nil, nil
					}
					return *device.FirmwareVersion, nil
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
			"hardwareIdentifier": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.HardwareIdentifier == nil {
						return nil, nil
					}
					return *device.HardwareIdentifier, nil
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
			"remoteControlAddress": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.RemoteControlAddress == nil {
						return nil, nil
					}
					return *device.RemoteControlAddress, nil
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
			"devicePaired": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.DevicePaired == nil {
						return nil, nil
					}
					return *device.DevicePaired, nil
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
			"receptionStrength": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.ReceptionStrength == nil {
						return nil, nil
					}
					return *device.ReceptionStrength, nil
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
			"zoneAssignment": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.ZoneAssignment == nil {
						return nil, nil
					}
					return *device.ZoneAssignment, nil
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
			"roomTemperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.RoomTemperatureC == nil {
						return nil, nil
					}
					return *device.RoomTemperatureC, nil
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
			"roomHumidityPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := params.Source.(RadioDevice)
					if !ok || device.RoomHumidityPct == nil {
						return nil, nil
					}
					return *device.RoomHumidityPct, nil
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
		Name: "Fm5SemanticMode",
		Values: graphqlgo.EnumValueConfigMap{
			string(Fm5SemanticModeInterpreted): &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticModeInterpreted)},
			string(Fm5SemanticModeGPIOOnly):    &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticModeGPIOOnly)},
			string(Fm5SemanticModeAbsent):      &graphqlgo.EnumValueConfig{Value: string(Fm5SemanticModeAbsent)},
		},
	})

	solarStatusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "SolarStatus",
		Fields: graphqlgo.Fields{
			"collectorTemperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.CollectorTemperatureC == nil {
						return nil, nil
					}
					return *status.CollectorTemperatureC, nil
				},
			},
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
			"returnTemperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.ReturnTemperatureC == nil {
						return nil, nil
					}
					return *status.ReturnTemperatureC, nil
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
			"pumpActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.PumpActive == nil {
						return nil, nil
					}
					return *status.PumpActive, nil
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
			"currentYield": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.CurrentYield == nil {
						return nil, nil
					}
					return *status.CurrentYield, nil
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
			"pumpHours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.PumpHours == nil {
						return nil, nil
					}
					return *status.PumpHours, nil
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
			"solarEnabled": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.SolarEnabled == nil {
						return nil, nil
					}
					return *status.SolarEnabled, nil
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
			"functionMode": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*SolarStatus)
					if !ok || status == nil || status.FunctionMode == nil {
						return nil, nil
					}
					return *status.FunctionMode, nil
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
		Name: "CylinderStatus",
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
			"temperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.TemperatureC == nil {
						return nil, nil
					}
					return *status.TemperatureC, nil
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
			"maxSetpointC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.MaxSetpointC == nil {
						return nil, nil
					}
					return *status.MaxSetpointC, nil
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
			"chargeHysteresisC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.ChargeHysteresisC == nil {
						return nil, nil
					}
					return *status.ChargeHysteresisC, nil
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
			"chargeOffsetC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(CylinderStatus)
					if !ok || status.ChargeOffsetC == nil {
						return nil, nil
					}
					return *status.ChargeOffsetC, nil
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
		Name: "BoilerState",
		Fields: graphqlgo.Fields{
			"flowTemperatureC": &graphqlgo.Field{
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
			"returnTemperatureC": &graphqlgo.Field{
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
			"centralHeatingPumpActive": &graphqlgo.Field{
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
			"waterPressureBar": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.WaterPressureBar == nil {
						return nil, nil
					}
					return *state.WaterPressureBar, nil
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
			"externalPumpActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.ExternalPumpActive == nil {
						return nil, nil
					}
					return *state.ExternalPumpActive, nil
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
			"circulationPumpActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.CirculationPumpActive == nil {
						return nil, nil
					}
					return *state.CirculationPumpActive, nil
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
			"gasValveActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.GasValveActive == nil {
						return nil, nil
					}
					return *state.GasValveActive, nil
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
			"flameActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.FlameActive == nil {
						return nil, nil
					}
					return *state.FlameActive, nil
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
			"diverterValvePositionPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DiverterValvePositionPct == nil {
						return nil, nil
					}
					return *state.DiverterValvePositionPct, nil
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
			"fanSpeedRpm": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.FanSpeedRpm == nil {
						return nil, nil
					}
					return *state.FanSpeedRpm, nil
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
			"targetFanSpeedRpm": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.TargetFanSpeedRpm == nil {
						return nil, nil
					}
					return *state.TargetFanSpeedRpm, nil
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
			"ionisationVoltageUa": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.IonisationVoltageUa == nil {
						return nil, nil
					}
					return *state.IonisationVoltageUa, nil
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
			"dhwWaterFlowLpm": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwWaterFlowLpm == nil {
						return nil, nil
					}
					return *state.DhwWaterFlowLpm, nil
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
			"dhwDemandActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwDemandActive == nil {
						return nil, nil
					}
					return *state.DhwDemandActive, nil
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
			"heatingSwitchActive": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.HeatingSwitchActive == nil {
						return nil, nil
					}
					return *state.HeatingSwitchActive, nil
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
			"storageLoadPumpPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.StorageLoadPumpPct == nil {
						return nil, nil
					}
					return *state.StorageLoadPumpPct, nil
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
			"modulationPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.ModulationPct == nil {
						return nil, nil
					}
					return *state.ModulationPct, nil
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
			"primaryCircuitFlowLpm": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.PrimaryCircuitFlowLpm == nil {
						return nil, nil
					}
					return *state.PrimaryCircuitFlowLpm, nil
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
			"flowTempDesiredC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.FlowTempDesiredC == nil {
						return nil, nil
					}
					return *state.FlowTempDesiredC, nil
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
			"dhwTempDesiredC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwTempDesiredC == nil {
						return nil, nil
					}
					return *state.DhwTempDesiredC, nil
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
			"stateNumber": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.StateNumber == nil {
						return nil, nil
					}
					return *state.StateNumber, nil
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
			"dhwTemperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwTemperatureC == nil {
						return nil, nil
					}
					return *state.DhwTemperatureC, nil
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
			"dhwTargetTemperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(BoilerState)
					if !ok || state.DhwTargetTemperatureC == nil {
						return nil, nil
					}
					return *state.DhwTargetTemperatureC, nil
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
		Name: "BoilerConfig",
		Fields: graphqlgo.Fields{
			"dhwOperatingMode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.DhwOperatingMode == nil {
						return nil, nil
					}
					return *config.DhwOperatingMode, nil
				},
			},
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
			"flowsetHcMaxC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.FlowsetHcMaxC == nil {
						return nil, nil
					}
					return *config.FlowsetHcMaxC, nil
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
			"flowsetHwcMaxC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.FlowsetHwcMaxC == nil {
						return nil, nil
					}
					return *config.FlowsetHwcMaxC, nil
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
			"partloadHcKW": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.PartloadHcKW == nil {
						return nil, nil
					}
					return *config.PartloadHcKW, nil
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
			"partloadHwcKW": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.PartloadHwcKW == nil {
						return nil, nil
					}
					return *config.PartloadHwcKW, nil
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
			"installerMenuCode": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.InstallerMenuCode == nil {
						return nil, nil
					}
					return *config.InstallerMenuCode, nil
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
			"phoneNumber": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.PhoneNumber == nil {
						return nil, nil
					}
					return *config.PhoneNumber, nil
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
			"hoursTillService": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(BoilerConfig)
					if !ok || config.HoursTillService == nil {
						return nil, nil
					}
					return *config.HoursTillService, nil
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
		Name: "BoilerDiagnostics",
		Fields: graphqlgo.Fields{
			"heatingStatusRaw": &graphqlgo.Field{
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
			"dhwStatusRaw": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DhwStatusRaw == nil {
						return nil, nil
					}
					return *diag.DhwStatusRaw, nil
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
			"centralHeatingHours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.CentralHeatingHours == nil {
						return nil, nil
					}
					return *diag.CentralHeatingHours, nil
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
			"dhwHours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DhwHours == nil {
						return nil, nil
					}
					return *diag.DhwHours, nil
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
			"centralHeatingStarts": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.CentralHeatingStarts == nil {
						return nil, nil
					}
					return *diag.CentralHeatingStarts, nil
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
			"dhwStarts": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DhwStarts == nil {
						return nil, nil
					}
					return *diag.DhwStarts, nil
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
			"pumpHours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.PumpHours == nil {
						return nil, nil
					}
					return *diag.PumpHours, nil
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
			"fanHours": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.FanHours == nil {
						return nil, nil
					}
					return *diag.FanHours, nil
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
			"deactivationsIFC": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DeactivationsIFC == nil {
						return nil, nil
					}
					return *diag.DeactivationsIFC, nil
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
			"deactivationsTemplimiter": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					diag, ok := params.Source.(BoilerDiagnostics)
					if !ok || diag.DeactivationsTemplimiter == nil {
						return nil, nil
					}
					return *diag.DeactivationsTemplimiter, nil
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
		Name: "BoilerStatus",
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
		Name: "SystemState",
		Fields: graphqlgo.Fields{
			"systemOff": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.SystemOff == nil {
						return nil, nil
					}
					return *state.SystemOff, nil
				},
			},
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
			"systemWaterPressure": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.SystemWaterPressure == nil {
						return nil, nil
					}
					return *state.SystemWaterPressure, nil
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
			"systemFlowTemperature": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.SystemFlowTemperature == nil {
						return nil, nil
					}
					return *state.SystemFlowTemperature, nil
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
			"outdoorTemperature": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.OutdoorTemperature == nil {
						return nil, nil
					}
					return *state.OutdoorTemperature, nil
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
			"outdoorTemperatureAvg24h": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.OutdoorTemperatureAvg24h == nil {
						return nil, nil
					}
					return *state.OutdoorTemperatureAvg24h, nil
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
			"maintenanceDue": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.MaintenanceDue == nil {
						return nil, nil
					}
					return *state.MaintenanceDue, nil
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
			"hwcCylinderTemperatureTop": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.HwcCylinderTemperatureTop == nil {
						return nil, nil
					}
					return *state.HwcCylinderTemperatureTop, nil
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
			"hwcCylinderTemperatureBottom": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(SystemState)
					if !ok || state.HwcCylinderTemperatureBottom == nil {
						return nil, nil
					}
					return *state.HwcCylinderTemperatureBottom, nil
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

	systemConfigType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "SystemConfig",
		Fields: graphqlgo.Fields{
			"adaptiveHeatingCurve": &graphqlgo.Field{
				Type: graphqlgo.Boolean,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.AdaptiveHeatingCurve == nil {
						return nil, nil
					}
					return *config.AdaptiveHeatingCurve, nil
				},
			},
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
			"alternativePoint": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.AlternativePoint == nil {
						return nil, nil
					}
					return *config.AlternativePoint, nil
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
			"heatingCircuitBivalencePoint": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.HeatingCircuitBivalencePoint == nil {
						return nil, nil
					}
					return *config.HeatingCircuitBivalencePoint, nil
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
			"dhwBivalencePoint": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.DhwBivalencePoint == nil {
						return nil, nil
					}
					return *config.DhwBivalencePoint, nil
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
			"hcEmergencyTemperature": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.HcEmergencyTemperature == nil {
						return nil, nil
					}
					return *config.HcEmergencyTemperature, nil
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
			"hwcMaxFlowTempDesired": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.HwcMaxFlowTempDesired == nil {
						return nil, nil
					}
					return *config.HwcMaxFlowTempDesired, nil
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
			"maxRoomHumidity": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.MaxRoomHumidity == nil {
						return nil, nil
					}
					return *config.MaxRoomHumidity, nil
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
			"maintenanceDate": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.MaintenanceDate == nil {
						return nil, nil
					}
					return *config.MaintenanceDate, nil
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
			"installerName": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.InstallerName == nil {
						return nil, nil
					}
					return *config.InstallerName, nil
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
			"installerPhone": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.InstallerPhone == nil {
						return nil, nil
					}
					return *config.InstallerPhone, nil
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
			"installerMenuCode": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					config, ok := params.Source.(SystemConfig)
					if !ok || config.InstallerMenuCode == nil {
						return nil, nil
					}
					return *config.InstallerMenuCode, nil
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
		Name: "SystemProperties",
		Fields: graphqlgo.Fields{
			"systemScheme": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					props, ok := params.Source.(SystemProperties)
					if !ok || props.SystemScheme == nil {
						return nil, nil
					}
					return *props.SystemScheme, nil
				},
			},
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
			"moduleConfigurationVR71": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					props, ok := params.Source.(SystemProperties)
					if !ok || props.ModuleConfigurationVR71 == nil {
						return nil, nil
					}
					return *props.ModuleConfigurationVR71, nil
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
		Name: "SystemStatus",
		Fields: graphqlgo.Fields{
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
		Name: "ServiceStatus",
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
			"firmwareVersion": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.FirmwareVersion, nil
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
			"updatesAvailable": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.UpdatesAvailable, nil
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
			"initiatorAddress": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.InitiatorAddress, nil
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
		Name: "GatewayIdentity",
		Fields: graphqlgo.Fields{
			"instanceGuid": &graphqlgo.Field{
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
		Name: "Field",
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
		Name: "ResponseSchema",
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
		Name: "Method",
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
			"readOnly": &graphqlgo.Field{
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
		Name: "ProjectionNode",
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
			"canonicalPath": &graphqlgo.Field{
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
		Name: "ProjectionEdge",
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
		Name: "Projection",
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
		Name: "Plane",
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
		Name: "Device",
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
			"deviceId": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.DeviceID, nil
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
			"serialNumber": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.SerialNumber, nil
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
			"macAddress": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.MacAddress, nil
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
			"softwareVersion": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.SoftwareVersion, nil
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
			"hardwareVersion": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.HardwareVersion, nil
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
			"displayName": &graphqlgo.Field{
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
			"productFamily": &graphqlgo.Field{
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
			"productModel": &graphqlgo.Field{
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
			"partNumber": &graphqlgo.Field{
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
		Name: "ScheduleTimerSlot",
		Fields: graphqlgo.Fields{
			"startHour": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.StartHour, nil
				},
			},
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
			"startMinute": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.StartMinute, nil
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
			"endHour": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.EndHour, nil
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
			"endMinute": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok {
						return nil, nil
					}
					return slot.EndMinute, nil
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
			"temperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok || slot.TemperatureC == nil {
						return nil, nil
					}
					return *slot.TemperatureC, nil
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
			"temperatureRaw": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					slot, ok := params.Source.(ScheduleTimerSlot)
					if !ok || slot.TemperatureRaw == nil {
						return nil, nil
					}
					return *slot.TemperatureRaw, nil
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
		Name: "ScheduleDayProgram",
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
		Name: "ScheduleConfig",
		Fields: graphqlgo.Fields{
			"maxSlots": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.MaxSlots, nil
				},
			},
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
			"timeResolution": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.TimeResolution, nil
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
			"minDuration": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.MinDuration, nil
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
			"hasTemperature": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.HasTemperature, nil
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
			"tempSlots": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil {
						return nil, nil
					}
					return cfg.TempSlots, nil
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
			"minTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil || cfg.MinTempC == nil {
						return nil, nil
					}
					return *cfg.MinTempC, nil
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
			"maxTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					cfg, ok := params.Source.(*ScheduleConfig)
					if !ok || cfg == nil || cfg.MaxTempC == nil {
						return nil, nil
					}
					return *cfg.MaxTempC, nil
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
		Name: "ScheduleProgram",
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
			"slotsUsed": &graphqlgo.Field{
				Type: graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Int)),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					prog, ok := params.Source.(ScheduleProgram)
					if !ok {
						return nil, nil
					}
					return prog.SlotsUsed, nil
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
		Name: "ScheduleStatus",
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
		Name: "AdapterHardwareInfo",
		Fields: graphqlgo.Fields{
			"firmwareVersion":    &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"firmware_version": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return "", nil
					}
					return info.FirmwareVersion, nil
				},
			},
			"firmwareChecksum":   &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"firmware_checksum": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return "", nil
					}
					return info.FirmwareChecksum, nil
				},
			},
			"bootloaderVersion":  &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"bootloader_version": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return "", nil
					}
					return info.BootloaderVersion, nil
				},
			},
			"bootloaderChecksum": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"bootloader_checksum": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return "", nil
					}
					return info.BootloaderChecksum, nil
				},
			},
			"hardwareID":         &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"hardware_id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return "", nil
					}
					return info.HardwareID, nil
				},
			},
			"hardwareConfig":     &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"hardware_config": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return "", nil
					}
					return info.HardwareConfig, nil
				},
			},
			"features":           &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"jumpers":            &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"jumperFlags": &graphqlgo.Field{
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
			"isWifi":     &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},
			"is_wifi": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return false, nil
					}
					return info.IsWiFi, nil
				},
			},
			"isEthernet": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},
			"is_ethernet": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return false, nil
					}
					return info.IsEthernet, nil
				},
			},
			"temperatureC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.TemperatureC == nil {
						return nil, nil
					}
					return *info.TemperatureC, nil
				},
			},
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
			"supplyVoltageMv": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.SupplyVoltageMV == nil {
						return nil, nil
					}
					return *info.SupplyVoltageMV, nil
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
			"busVoltageMaxDv": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.BusVoltageMaxDV == nil {
						return nil, nil
					}
					return *info.BusVoltageMaxDV, nil
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
			"busVoltageMinDv": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.BusVoltageMinDV == nil {
						return nil, nil
					}
					return *info.BusVoltageMinDV, nil
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
			"resetCause": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.ResetCause == nil {
						return nil, nil
					}
					return *info.ResetCause, nil
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
			"resetCauseCode": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.ResetCauseCode == nil {
						return nil, nil
					}
					return int(*info.ResetCauseCode), nil
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
			"restartCount": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.RestartCount == nil {
						return nil, nil
					}
					return int(*info.RestartCount), nil
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
			"wifiRssiDbm": &graphqlgo.Field{
				Type: graphqlgo.Int,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.WiFiRSSIDBm == nil {
						return nil, nil
					}
					return *info.WiFiRSSIDBm, nil
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
			"lastIdentityQuery": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.LastIdentityQuery == nil {
						return nil, nil
					}
					return info.LastIdentityQuery.UTC().Format("2006-01-02T15:04:05Z"), nil
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
			"lastTelemetryQuery": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil || info.LastTelemetryQuery == nil {
						return nil, nil
					}
					return info.LastTelemetryQuery.UTC().Format("2006-01-02T15:04:05Z"), nil
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
			"versionResponseLen": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"version_response_len": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return 0, nil
					}
					return info.VersionResponseLen, nil
				},
			},
			"infoSupported":      &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},
			"info_supported": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					info, ok := params.Source.(*AdapterHardwareInfo)
					if !ok || info == nil {
						return false, nil
					}
					return info.InfoSupported, nil
				},
			},
		},
	})

	busSummaryType, busMessagesType, busPeriodicityType := buildBusObservabilityTypes()
	watchSummaryType := buildWatchSummaryType()

	return graphqlSchemaTypes{
		fieldType:               fieldType,
		responseType:            responseType,
		methodType:              methodType,
		projectionNodeType:      projectionNodeType,
		projectionEdgeType:      projectionEdgeType,
		projectionType:          projectionType,
		planeType:               planeType,
		deviceType:              deviceType,
		broadcastType:           buildBroadcastType(),
		statusType:              statusType,
		gatewayIdentityType:     gatewayIdentityType,
		zoneType:                zoneType,
		dhwType:                 dhwType,
		circuitStatusType:       circuitStatusType,
		radioDeviceType:         radioDeviceType,
		fm5SemanticMode:         fm5SemanticModeType,
		solarStatusType:         solarStatusType,
		cylinderStatusType:      cylinderStatusType,
		energyTotals:            energyTotalsType,
		boilerStatusType:        boilerStatusType,
		systemStatusType:        systemStatusType,
		scheduleStatusType:      scheduleStatusType,
		adapterHardwareInfoType: adapterHardwareInfoType,
		busSummaryType:          busSummaryType,
		busMessagesType:         busMessagesType,
		busPeriodicityType:      busPeriodicityType,
		watchSummaryType:        watchSummaryType,
	}
}

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
	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Query",
		Fields: graphqlgo.Fields{
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
		},
	})
}

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
