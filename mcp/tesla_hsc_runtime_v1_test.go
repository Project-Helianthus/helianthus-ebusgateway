package mcp

import (
	"context"
	"reflect"
	"testing"
)

func TestTeslaHSCV1RuntimeInjectsOnlyRedactedCorrelatedResponses(t *testing.T) {
	responseType := reflect.TypeOf(TeslaHSCV1CorrelatedResponse{})
	for field := 0; field < responseType.NumField(); field++ {
		if typeContainsBytes(responseType.Field(field).Type) {
			t.Fatalf("correlated response accepts raw bytes through %s", responseType.Field(field).Name)
		}
	}
	runtime, err := NewTeslaHSCV1Runtime(&teslaHSCV1RuntimeFixture{responses: []TeslaHSCV1CorrelatedResponse{
		{Function: 100, PayloadCount: 2},
		{Function: 101, PayloadCount: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.TeslaHSCV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.OutboundAllowed || result.RetainedLength != 0 || result.RetainedDigest != "" {
		t.Fatalf("unsafe result: %#v", result)
	}
	if result.Disposition != "framing_only" || result.Compatibility != "correlated_response" {
		t.Fatalf("result = %#v", result)
	}
}

func typeContainsBytes(typ reflect.Type) bool {
	if typ.Kind() == reflect.Array || typ.Kind() == reflect.Slice {
		return typ.Elem().Kind() == reflect.Uint8 || typeContainsBytes(typ.Elem())
	}
	return false
}

func TestTeslaHSCV1RuntimeRejectsUncorrelatedOrOpaqueOutboundPaths(t *testing.T) {
	for _, response := range []TeslaHSCV1CorrelatedResponse{
		{Function: 101, PayloadCount: 2},
		{Function: 102},
	} {
		runtime, err := NewTeslaHSCV1Runtime(&teslaHSCV1RuntimeFixture{responses: []TeslaHSCV1CorrelatedResponse{response}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.TeslaHSCV1(context.Background()); err == nil {
			t.Fatalf("invalid correlated response accepted: %#v", response)
		}
	}
}

type teslaHSCV1RuntimeFixture struct {
	responses []TeslaHSCV1CorrelatedResponse
}

func (fixture *teslaHSCV1RuntimeFixture) TeslaHSCV1Responses(context.Context) ([]TeslaHSCV1CorrelatedResponse, error) {
	return fixture.responses, nil
}
