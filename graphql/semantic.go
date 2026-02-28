package graphql

type ZoneState struct {
	CurrentTempC       *float64
	CurrentHumidityPct *float64
	HvacAction         string
	SpecialFunction    string
	HeatingDemandPct   *float64
	ValvePositionPct   *float64
}

type ZoneConfig struct {
	OperatingMode     string
	Preset            string
	TargetTempC       *float64
	AllowedModes      []string
	CircuitType       string
	AssociatedCircuit *int
}

type Zone struct {
	ID     string
	Name   string
	State  ZoneState
	Config ZoneConfig
}

type DhwState struct {
	CurrentTempC     *float64
	SpecialFunction  string
	HeatingDemandPct *float64
}

type DhwConfig struct {
	OperatingMode string
	Preset        string
	TargetTempC   *float64
}

type DhwStatus struct {
	State  DhwState
	Config DhwConfig
}

type EnergySeries struct {
	Today  float64
	Yearly []float64
}

type EnergyChannel struct {
	DHW     EnergySeries
	Climate EnergySeries
}

type EnergyTotals struct {
	Gas      EnergyChannel
	Electric EnergyChannel
	Solar    EnergyChannel
}

type BoilerState struct {
	FlowTemperatureC         *float64
	ReturnTemperatureC       *float64
	CentralHeatingPumpActive *bool
	DhwTemperatureC          *float64
	DhwTargetTemperatureC    *float64
}

type BoilerConfig struct {
	DhwOperatingMode *string
}

type BoilerDiagnostics struct {
	HeatingStatusRaw *int
	DhwStatusRaw     *int
}

type BoilerStatus struct {
	State       BoilerState
	Config      BoilerConfig
	Diagnostics BoilerDiagnostics
}

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
	EnergyTotals() *EnergyTotals
	BoilerStatus() *BoilerStatus
}

type staticSemanticProvider struct{}

func (staticSemanticProvider) Zones() []Zone {
	return nil
}

func (staticSemanticProvider) DHW() *DhwStatus {
	return nil
}

func (staticSemanticProvider) EnergyTotals() *EnergyTotals {
	return nil
}

func (staticSemanticProvider) BoilerStatus() *BoilerStatus {
	return nil
}
