package mcp

import (
	"context"
	"errors"
)

// TeslaHSCV1CorrelatedResponse is one RTU exchange already correlated by the
// transport. Native records are copied into the MCP result without invoking
// transport or constructing a request.
type TeslaHSCV1CorrelatedResponse struct {
	Function        byte
	OutboundAllowed bool
	Records         []TeslaHSCV1NativeRecord
}

// TeslaHSCV1ResponseProvider supplies only completed correlated responses.
type TeslaHSCV1ResponseProvider interface {
	TeslaHSCV1Responses(context.Context) ([]TeslaHSCV1CorrelatedResponse, error)
}

// TeslaHSCV1Runtime projects injected Tesla native records into the MCP
// profile status surface.
type TeslaHSCV1Runtime struct{ provider TeslaHSCV1ResponseProvider }

func NewTeslaHSCV1Runtime(provider TeslaHSCV1ResponseProvider) (*TeslaHSCV1Runtime, error) {
	if provider == nil {
		return nil, errors.New("tesla HSC response provider is required")
	}
	return &TeslaHSCV1Runtime{provider: provider}, nil
}

// TeslaHSCV1 validates only already-correlated native record multiplicity. It
// performs no I/O beyond calling its injected response provider.
func (runtime *TeslaHSCV1Runtime) TeslaHSCV1(ctx context.Context) (TeslaHSCV1Result, error) {
	if runtime == nil || runtime.provider == nil || ctx == nil {
		return TeslaHSCV1Result{}, errors.New("tesla HSC runtime is unavailable")
	}
	responses, err := runtime.provider.TeslaHSCV1Responses(ctx)
	if err != nil {
		return TeslaHSCV1Result{}, err
	}
	if len(responses) == 0 {
		return TeslaHSCV1Result{}, errors.New("tesla HSC correlated response batch is empty")
	}
	result := TeslaHSCV1Result{Disposition: "native_records", Compatibility: "correlated_response"}
	for _, response := range responses {
		if !validTeslaHSCV1CorrelatedResponse(response) {
			return TeslaHSCV1Result{}, errors.New("tesla HSC correlated response is invalid")
		}
		result.OutboundAllowed = result.OutboundAllowed || response.OutboundAllowed
		for _, record := range response.Records {
			result.NativeRecords = append(result.NativeRecords, copyTeslaHSCV1NativeRecord(record))
		}
	}
	return result, nil
}

func validTeslaHSCV1CorrelatedResponse(response TeslaHSCV1CorrelatedResponse) bool {
	if len(response.Records) == 0 || len(response.Records) > 8 {
		return false
	}
	switch response.Function {
	case 100:
		// FC100 may retain a bounded echo/result sequence.
	case 101, 102:
		if len(response.Records) != 1 {
			return false
		}
	default:
		return false
	}
	for _, record := range response.Records {
		if record.Function != response.Function || len(record.Payload) > 252 ||
			record.Compatibility == "" || record.Provenance == "" {
			return false
		}
	}
	return true
}

func copyTeslaHSCV1NativeRecord(record TeslaHSCV1NativeRecord) TeslaHSCV1NativeRecord {
	record.Payload = append([]byte(nil), record.Payload...)
	record.FieldNames = append([]string(nil), record.FieldNames...)
	return record
}
