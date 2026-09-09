package ebusgateway

import "time"

// ModbusTCPConfig is the disabled-by-default gateway composition boundary.
// Add-on and CLI configuration are introduced separately in FMV3-M4-03.
type ModbusTCPConfig struct {
	Enabled         bool
	Endpoint        string
	DialTimeout     time.Duration
	GrowattBMSRS485 GrowattBMSRS485Config
}

// GrowattBMSRS485Config is the explicit, disabled-by-default composition
// boundary for the exact read-only 1xSxxP ESS V2.02 RTU observer. Source and
// lifecycle identity are supplied by configuration; decoded register values
// never establish either identity.
type GrowattBMSRS485Config struct {
	Enabled          bool
	SourceID         string
	SourceEpoch      string
	DriverGeneration uint64
	UnitID           byte
	SerialPath       string
	Baud             uint32
	Parity           string
	StopBits         uint8
	ResponseTimeout  time.Duration
	MaxResponseDelay time.Duration
	MaxQuiescence    time.Duration
}
