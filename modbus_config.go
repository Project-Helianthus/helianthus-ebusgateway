package ebusgateway

import "time"

// ModbusTCPConfig is the disabled-by-default gateway composition boundary.
// Add-on and CLI configuration are introduced separately in FMV3-M4-03.
type ModbusTCPConfig struct {
	Enabled     bool
	Endpoint    string
	DialTimeout time.Duration
}
