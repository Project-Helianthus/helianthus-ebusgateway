package ebus_standard

import "time"

const (
	EnvelopeContractName  = "helianthus-ebus-mcp"
	EnvelopeContractMajor = 1
	EnvelopeContractMinor = 0
)

// NewEnvelope stub — RED. Impl lands in next commit.
func NewEnvelope(_ any, _ error, _ time.Time) map[string]any { return nil }

// DataHash stub — RED.
func DataHash(_ any) string { return "" }
