package feedv1

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestSchemaKeepsPublicObjectsClosedAndMatchesWireMembers(t *testing.T) {
	raw, err := os.ReadFile("../../docs/schemas/matter-binding-feed-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	definitions := schemaMap(t, schema["$defs"], "$defs")
	wantDefinitions := []string{"causal", "changesResponse", "cursor", "dimension", "disposition", "evidence", "execution", "factKey", "fence", "id", "intent", "ledger", "loss", "manifest", "matterAttribute", "matterDocument", "matterResult", "operation", "operationClaim", "origin", "pack", "portalAction", "portalSource", "report", "requested", "resource", "revisions", "snapshot", "snapshotResponse", "target", "timePoint"}
	if got := sortedKeys(definitions); !reflect.DeepEqual(got, wantDefinitions) {
		t.Fatalf("definitions=%v want=%v", got, wantDefinitions)
	}
	wantMembers := map[string][]string{
		"cursor":           {"instance", "sequence"},
		"target":           {"mapping_revision", "projection_contract", "target_id", "target_version"},
		"fence":            {"binding_id", "driver_generation", "revisions", "snapshot_id", "source_epoch"},
		"resource":         {"asset_id", "binding_id", "causal", "driver_generation", "last_fence", "projected", "resource_id", "revisions", "snapshot_id", "source_epoch", "state"},
		"operationClaim":   {"action_id", "binding_id", "capability_id", "catalog_revision", "digest", "driver_generation", "driver_id", "manifest_id", "manifest_version", "operation_id", "resource_id", "revision", "snapshot_id", "source_epoch"},
		"operation":        {"action", "asset_id", "claim"},
		"snapshot":         {"contract", "ledger", "operations", "resources", "target"},
		"snapshotResponse": {"contract", "cursor", "snapshot"},
		"changesResponse":  {"changes", "contract", "cursor", "resync_required"},
		"intent":           {"arguments", "asset_id", "causal", "claim", "deadline", "idempotency_key"},
		"execution":        {"ack", "contract", "outcome", "readback"},
	}
	for name, want := range wantMembers {
		definition := schemaMap(t, definitions[name], name)
		if definition["additionalProperties"] != false {
			t.Fatalf("%s is not closed", name)
		}
		properties := schemaMap(t, definition["properties"], name+".properties")
		if got := sortedKeys(properties); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s properties=%v want=%v", name, got, want)
		}
	}
	intent := schemaMap(t, definitions["intent"], "intent")
	properties := schemaMap(t, intent["properties"], "intent.properties")
	for _, forbidden := range []string{"principal", "route", "native_route", "endpoint"} {
		if _, ok := properties[forbidden]; ok {
			t.Fatalf("intent exposes forbidden member %q", forbidden)
		}
	}
}

func schemaMap(t *testing.T, value any, path string) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object", path)
	}
	return result
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
