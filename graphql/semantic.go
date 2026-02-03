package graphql

type Zone struct {
	ID            string
	Name          string
	OperatingMode string
	Preset        string
	CurrentTempC  *float64
	TargetTempC   *float64
	HeatingDemand *float64
}

type DhwStatus struct {
	OperatingMode string
	Preset        string
	CurrentTempC  *float64
	TargetTempC   *float64
	HeatingDemand *float64
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
