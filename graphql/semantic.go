package graphql

type Zone struct {
	ID                     string
	Name                   string
	OperatingMode          string
	Preset                 string
	HvacAction             string
	AllowedModes           []string
	CurrentTempC           *float64
	TargetTempC            *float64
	CurrentHumidityPct     *float64
	HeatingDemand          *float64
	SpecialFunction        string
	CircuitTypeRaw         string
	ZoneCircuitIndexRaw    string
	ZoneOperationModeRaw   string
	ZoneValveStatusRaw     string
	ZoneSpecialFunctionRaw string
}

type DhwStatus struct {
	OperatingMode         string
	Preset                string
	CurrentTempC          *float64
	TargetTempC           *float64
	HeatingDemand         *float64
	SpecialFunction       string
	DhwOperationModeRaw   string
	DhwSpecialFunctionRaw string
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

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
	EnergyTotals() *EnergyTotals
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
