package mcp

import (
	"context"
	"errors"
)

// TeslaHSCV1CorrelatedResponse is one inbound RTU exchange already correlated
// by the transport. It has no request-construction or transmit capability.
type TeslaHSCV1CorrelatedResponse struct {
	Function byte
	Payloads [][]byte
}

// TeslaHSCV1ResponseProvider supplies only completed correlated responses.
type TeslaHSCV1ResponseProvider interface {
	TeslaHSCV1Responses(context.Context) ([]TeslaHSCV1CorrelatedResponse, error)
}

// TeslaHSCV1Runtime projects injected Tesla response metadata into the
// existing read-only MCP profile status surface.
type TeslaHSCV1Runtime struct{ provider TeslaHSCV1ResponseProvider }

func NewTeslaHSCV1Runtime(provider TeslaHSCV1ResponseProvider) (*TeslaHSCV1Runtime, error) {
	if provider == nil {
		return nil, errors.New("Tesla HSC response provider is required")
	}
	return &TeslaHSCV1Runtime{provider: provider}, nil
}

// TeslaHSCV1 validates only inbound response multiplicity and always projects
// a fail-closed read-only result. It performs no I/O beyond calling its
// injected response provider.
func (runtime *TeslaHSCV1Runtime) TeslaHSCV1(ctx context.Context) (TeslaHSCV1Result, error) {
	if runtime == nil || runtime.provider == nil || ctx == nil {
		return TeslaHSCV1Result{}, errors.New("Tesla HSC runtime is unavailable")
	}
	responses, err := runtime.provider.TeslaHSCV1Responses(ctx)
	if err != nil {
		return TeslaHSCV1Result{}, err
	}
	if len(responses) == 0 {
		return TeslaHSCV1Result{}, errors.New("Tesla HSC correlated response batch is empty")
	}
	for _, response := range responses {
		if !validTeslaHSCV1CorrelatedResponse(response) {
			return TeslaHSCV1Result{}, errors.New("Tesla HSC correlated response is invalid")
		}
	}
	return TeslaHSCV1Result{Disposition: "framing_only", Compatibility: "correlated_response", OutboundAllowed: false}, nil
}

func validTeslaHSCV1CorrelatedResponse(response TeslaHSCV1CorrelatedResponse) bool {
	if len(response.Payloads) == 0 || len(response.Payloads) > 8 {
		return false
	}
	switch response.Function {
	case 100:
		return true
	case 101, 102:
		return len(response.Payloads) == 1
	default:
		return false
	}
}
