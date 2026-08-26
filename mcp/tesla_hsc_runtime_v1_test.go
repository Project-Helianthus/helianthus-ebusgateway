package mcp

import (
	"bytes"
	"context"
	"testing"
)

func TestTeslaHSCV1RuntimeRetainsInjectedNativeRecords(t *testing.T) {
	payload := []byte{0x32, 0x02, 0x2a, 0x00}
	runtime, err := NewTeslaHSCV1Runtime(&teslaHSCV1RuntimeFixture{responses: []TeslaHSCV1CorrelatedResponse{
		{Function: 100, Records: []TeslaHSCV1NativeRecord{{
			Function: 100, Payload: payload, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay",
			Family: 6, RequestTag: 5, ResponseTag: 6, RequestName: "GetConfig", ResponseName: "WCConfig",
			FieldNames: []string{"settings", "wifi_config"},
		}}},
		{Function: 101, OutboundAllowed: true, Records: []TeslaHSCV1NativeRecord{{
			Function: 101, Payload: []byte{}, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay",
		}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.TeslaHSCV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != "native_records" || result.Compatibility != "correlated_response" || !result.OutboundAllowed || len(result.NativeRecords) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if record := result.NativeRecords[0]; !bytes.Equal(record.Payload, payload) || record.RequestName != "GetConfig" || record.ResponseName != "WCConfig" || len(record.FieldNames) != 2 {
		t.Fatalf("record = %#v", record)
	}
	payload[0] = 0
	if result.NativeRecords[0].Payload[0] != 0x32 {
		t.Fatalf("native payload was not copied: %#v", result.NativeRecords[0])
	}
}

func TestTeslaHSCV1RuntimeRejectsInvalidCorrelatedRecords(t *testing.T) {
	valid := TeslaHSCV1NativeRecord{Function: 101, Payload: []byte{}, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay"}
	for _, response := range []TeslaHSCV1CorrelatedResponse{
		{Function: 101, Records: []TeslaHSCV1NativeRecord{valid, valid}},
		{Function: 102},
		{Function: 101, Records: []TeslaHSCV1NativeRecord{{Function: 102, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay"}}},
		{Function: 102, Records: []TeslaHSCV1NativeRecord{{Function: 102, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay", ResponseName: "unqualified"}}},
		{Function: 100, Records: []TeslaHSCV1NativeRecord{{Function: 100, Payload: bytes.Repeat([]byte{1}, 253), Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay"}}},
		{Function: 100, Records: []TeslaHSCV1NativeRecord{{Function: 100, Compatibility: "", Provenance: "synthetic-replay"}}},
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
