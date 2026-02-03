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

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
}

type staticSemanticProvider struct{}

func (staticSemanticProvider) Zones() []Zone {
	return nil
}

func (staticSemanticProvider) DHW() *DhwStatus {
	return nil
}
