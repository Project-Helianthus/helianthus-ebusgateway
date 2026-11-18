package mcp

import (
	"context"
	"encoding/json"
	"testing"
)

func TestGrowattStorageEvaluatedTimestampUsesAuthoritativeEvaluation(t *testing.T) {
	timestamp, err := growattStorageEvaluatedTimestamp(map[string]any{
		"evaluation": map[string]any{"context": map[string]any{"evaluated_at": map[string]any{"unix_nanoseconds": "1800000000123456789"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if timestamp != "2027-01-15T08:00:00.123456789Z" {
		t.Fatalf("timestamp=%q", timestamp)
	}
}

// growattStorageGraphProvider serves one fixed storage publication.
type growattStorageGraphProvider struct {
	ModbusV1Provider
	value json.RawMessage
}

func (p growattStorageGraphProvider) GrowattStorageSemanticCurrent(context.Context) (any, error) {
	return p.value, nil
}

// TestGrowattStorageSemanticReadCarriesThePackResource: the storage read
// returns the publication's battery-pack resource and the pack dimension
// value that names it exactly as published; it derives, rewrites and drops
// nothing, and its timestamp is the evaluation's.
func TestGrowattStorageSemanticReadCarriesThePackResource(t *testing.T) {
	const published = `{"evaluation":{"context":{"evaluated_at":{"unix_nanoseconds":"1800000000000000007"}}},` +
		`"projection":{"dispositions":[{"item_id":"storage.pack.voltage","source_keys":[{"fact_id":"storage.pack.voltage","dimensions":[{"id":"storage.dimension.pack","value":{"kind":"text","text":"resource:synthetic-pack"}}]}]}]},` +
		`"selections":[],` +
		`"snapshot":{"asset_id":"asset:synthetic-storage","facts":[{"key":{"fact_id":"storage.pack.voltage","dimensions":[{"id":"storage.dimension.pack","value":{"kind":"text","text":"resource:synthetic-pack"}}]}}],` +
		`"resources":[{"resource_id":"resource:synthetic-pack","kind":"resource.battery_pack","anchor":{"class":"equipment","basis":"operator_slot","slot":"synthetic-slot"},"state":"current","revision":"1"}]}}`
	server := &Server{}
	registerGrowattStorageSemanticTool(server, growattStorageGraphProvider{value: json.RawMessage(published)})
	t.Cleanup(func() {
		growattStorageSemanticProviders.Lock()
		delete(growattStorageSemanticProviders.byServer, server)
		growattStorageSemanticProviders.Unlock()
	})
	result, handled := server.handleGrowattStorageSemanticCall(context.Background(), SemanticV1GrowattStorageCurrentGetTool, map[string]any{})
	if !handled || result["isError"] != false {
		t.Fatalf("handled=%t result=%v", handled, result)
	}
	var envelope struct {
		Meta struct {
			DataTimestamp string `json:"data_timestamp"`
		} `json:"meta"`
		Data  json.RawMessage `json:"data"`
		Error any             `json:"error"`
	}
	if err := json.Unmarshal([]byte(result["content"].([]map[string]any)[0]["text"].(string)), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil || envelope.Meta.DataTimestamp != "2027-01-15T08:00:00.000000007Z" {
		t.Fatalf("envelope meta=%+v error=%v", envelope.Meta, envelope.Error)
	}
	var want, got any
	if err := json.Unmarshal([]byte(published), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(envelope.Data, &got); err != nil {
		t.Fatal(err)
	}
	wantBytes, _ := json.Marshal(want)
	gotBytes, _ := json.Marshal(got)
	if string(wantBytes) != string(gotBytes) {
		t.Fatalf("read rewrote the publication:\n got %s\nwant %s", gotBytes, wantBytes)
	}
	var view struct {
		Snapshot struct {
			Facts []struct {
				Key struct {
					Dimensions []struct {
						Value struct {
							Text string `json:"text"`
						} `json:"value"`
					} `json:"dimensions"`
				} `json:"key"`
			} `json:"facts"`
			Resources []struct {
				ResourceID string `json:"resource_id"`
				Kind       string `json:"kind"`
			} `json:"resources"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(envelope.Data, &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot.Resources) != 1 || view.Snapshot.Resources[0].Kind != "resource.battery_pack" || len(view.Snapshot.Facts) != 1 || view.Snapshot.Facts[0].Key.Dimensions[0].Value.Text != view.Snapshot.Resources[0].ResourceID {
		t.Fatalf("read lost the pack resource: %+v", view.Snapshot)
	}
}
