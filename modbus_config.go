package ebusgateway

import "time"

// ModbusTCPConfig is the disabled-by-default gateway composition boundary.
// Add-on and CLI configuration are introduced separately in FMV3-M4-03.
type ModbusTCPConfig struct {
	Enabled         bool
	Endpoint        string
	DialTimeout     time.Duration
	GrowattBMSRS485 GrowattBMSRS485Config
	TeslaGen3HSC    TeslaGen3HSCRetainedConfig
}

// TeslaGen3HSCRetainedConfig enables the non-send owner for completed,
// transport-correlated WC3 current-limit outcomes. EndpointID is a public
// configured label. No serial path or operation authority is part of this
// configuration.
type TeslaGen3HSCRetainedConfig struct {
	Enabled          bool
	EndpointID       string
	AssetID          string
	SourceID         string
	SourceEpoch      string
	ClockEpoch       string
	EVSEID           string
	ConnectorID      string
	Profile          string
	DriverGeneration uint64
	Node             byte
}

// GrowattBMSRS485Config is the explicit, disabled-by-default composition
// boundary for the exact read-only 1xSxxP ESS V2.02 RTU observer. Source and
// lifecycle identity are supplied by configuration; decoded register values
// never establish either identity.
type GrowattBMSRS485Config struct {
	Enabled bool
	// AssetID is the stable, operator-configured semantic asset identity. It is
	// deliberately separate from SourceID: no native tuple, unit, version or
	// decoded observation is allowed to manufacture an asset identity.
	AssetID          string
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
