package portal

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func i35JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func i35Select(t *testing.T, raw []byte, selection InvestigationSelection) InvestigationPublicSelection {
	t.Helper()
	result, err := SelectInvestigationPublic(raw, selection)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func i35Fixture() map[string]any {
	return map[string]any{
		"title": "PRIVATE_MARKER", "markdown": "```json\nPRIVATE_MARKER\n```", "generated_at": "PRIVATE_MARKER",
		"evidence": map[string]any{
			"captured_at": "PRIVATE_MARKER", "unknown": map[string]any{"nested": "PRIVATE_MARKER"},
			"registry_devices": []any{map[string]any{"serial_number": "PRIVATE_MARKER"}},
			"snapshots":        []any{map[string]any{"id": "PRIVATE_SNAPSHOT", "label": "PRIVATE_MARKER", "captured_at": "PRIVATE_MARKER", "payload": map[string]any{"semantic": map[string]any{"has_dhw": true}, "timeline": map[string]any{"items": []any{"PRIVATE_MARKER"}}, "raw_hex": "PRIVATE_MARKER"}}},
			"timeline": []any{map[string]any{
				"type": "snapshot", "layer": "composite", "correlation_id": "PRIVATE_EVENT", "at": "PRIVATE_MARKER", "error": "PRIVATE_MARKER",
				"payload": map[string]any{
					"semantic":    map[string]any{"zones_count": 0, "has_dhw": false, "has_energy": true, "energy_series": true, "captured_utc": "PRIVATE_MARKER", "nested": map[string]any{"has_dhw": "PRIVATE_MARKER"}},
					"registry":    map[string]any{"device_count": 2, "items": []any{"PRIVATE_MARKER"}},
					"projection":  map[string]any{"device_count": 3, "projection_count": 4, "secret": "PRIVATE_MARKER"},
					"PRIVATE_KEY": map[string]any{"semantic": map[string]any{"has_dhw": true}},
				},
				"provenance": map[string]any{"source": "poll:composite", "dropped": 0, "interval_ms": 1000, "error": "PRIVATE_MARKER"},
			}},
			"provenance": []any{map[string]any{
				"correlation_id": "PRIVATE_EVENT", "layer": "composite", "source": "poll:composite", "at": "PRIVATE_MARKER", "confidence": 0.7, "dropped": 0, "interval_ms": 1000,
				"payload_keys": []string{"PRIVATE_KEY"}, "decode_path": []string{"source:poll:composite", "PRIVATE_MARKER", "gateway.portal.stream"},
			}},
		},
	}
}

func i35All() InvestigationSelection {
	return InvestigationSelection{Snapshots: true, Timeline: true, Provenance: true, IncludeDropped: true, IncludeInterval: true, Diagnostics: []string{"semantic.zones_count", "semantic.has_dhw", "semantic.has_energy", "semantic.energy_series", "registry.device_count", "projection.device_count", "projection.projection_count"}}
}

func TestI35AllowlistAndDuplicateNarrative(t *testing.T) {
	fixture := i35Fixture()
	fixture["markdown"] = "```json\n" + string(i35JSON(t, fixture["evidence"])) + "\n```"
	result := i35Select(t, i35JSON(t, fixture), i35All())
	raw := string(i35JSON(t, result))
	for _, forbidden := range []string{"PRIVATE", "captured", "generated_at", "\"at\"", "confidence", "markdown", "raw_hex", "serial_number", "error"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("leaked %s: %s", forbidden, raw)
		}
	}
	want := map[string]any{"semantic": map[string]any{"zones_count": int64(0), "has_dhw": false, "has_energy": true, "energy_series": true}, "registry": map[string]any{"device_count": int64(2)}, "projection": map[string]any{"device_count": int64(3), "projection_count": int64(4)}}
	if !reflect.DeepEqual(result.Evidence["timeline"][0]["payload"], want) {
		t.Fatalf("payload: %#v", result)
	}
	provenance := result.Evidence["provenance"][0]
	if !reflect.DeepEqual(provenance["payload_keys"], []string{"projection", "registry", "semantic"}) {
		t.Fatal(provenance)
	}
	if provenance["correlation_id"] != "event-1" || result.Evidence["snapshots"][0]["id"] != "snapshot-1" {
		t.Fatal(result)
	}
	if !reflect.DeepEqual(provenance["decode_path"], []string{"source:poll:composite", "gateway.portal.stream"}) {
		t.Fatal(provenance)
	}
	if result.Freshness != "Unknown" || result.Components["registry_devices"].Omitted != 1 {
		t.Fatal(result)
	}
	if !reflect.DeepEqual(result.Evidence["snapshots"][0]["payload"], map[string]any{}) {
		t.Fatal(result)
	}
}

func TestI35ExplicitSelectionAndOrder(t *testing.T) {
	raw := i35JSON(t, i35Fixture())
	empty := i35Select(t, raw, InvestigationSelection{})
	if len(empty.Evidence) != 0 || len(empty.Diagnostics) != 0 {
		t.Fatal(empty)
	}
	a := InvestigationSelection{Timeline: true, Provenance: true, Diagnostics: []string{"semantic.has_dhw", "registry.device_count"}}
	b := a
	b.Diagnostics = []string{"registry.device_count", "semantic.has_dhw", "semantic.has_dhw"}
	first := i35Select(t, raw, a)
	if !reflect.DeepEqual(first, i35Select(t, raw, b)) {
		t.Fatal("selection order or duplication changed output")
	}
	if !reflect.DeepEqual(first.Evidence["timeline"][0]["provenance"], map[string]any{"source": "poll:composite"}) {
		t.Fatal(first)
	}
	if _, err := SelectInvestigationPublic(raw, InvestigationSelection{Diagnostics: []string{"PRIVATE_KEY"}}); err == nil {
		t.Fatal("unknown selection accepted")
	}
}

// Each summary path is varied independently: removing its exact lookup/type
// guard or defaulting its absent value to zero/false must fail its variant.
func TestI35EveryDiagnosticField(t *testing.T) {
	paths := []string{"semantic.zones_count", "semantic.has_dhw", "semantic.has_energy", "semantic.energy_series", "registry.device_count", "projection.device_count", "projection.projection_count"}
	for _, path := range paths {
		for _, variant := range []string{"present", "alternate", "absent", "invalid", "nested", "null", "negative", "fraction"} {
			t.Run(path+"/"+variant, func(t *testing.T) {
				parts := strings.Split(path, ".")
				isBool := path == "semantic.has_dhw" || path == "semantic.has_energy" || path == "semantic.energy_series"
				value := any(9)
				expected := any(int64(9))
				if isBool {
					value = false
					expected = false
				}
				group := map[string]any{}
				switch variant {
				case "alternate":
					if isBool {
						group[parts[1]] = true
						expected = true
					} else {
						group[parts[1]] = 0
						expected = int64(0)
					}
				case "present":
					group[parts[1]] = value
				case "invalid":
					group[parts[1]] = "PRIVATE_MARKER"
				case "nested":
					group[parts[1]] = map[string]any{parts[1]: value}
				case "null":
					group[parts[1]] = nil
				case "negative":
					group[parts[1]] = -1
				case "fraction":
					group[parts[1]] = 0.5
				}
				raw := i35JSON(t, map[string]any{"evidence": map[string]any{"timeline": []any{map[string]any{"payload": map[string]any{parts[0]: group}}}}})
				result := i35Select(t, raw, InvestigationSelection{Timeline: true, Diagnostics: []string{path}})
				payload := result.Evidence["timeline"][0]["payload"]
				want := map[string]any{}
				loss := InvestigationDiagnosticLoss{}
				switch variant {
				case "present", "alternate":
					want[parts[0]] = map[string]any{parts[1]: expected}
				case "absent":
					loss.Missing = 1
				default:
					loss.Invalid = 1
				}
				if !reflect.DeepEqual(payload, want) || result.Diagnostics[path] != loss {
					t.Fatalf("payload=%#v losses=%#v", payload, result.Diagnostics)
				}
			})
		}
	}
}

func TestI35AliasesAndDetachedBuffers(t *testing.T) {
	original := i35Fixture()
	raw := i35JSON(t, original)
	result := i35Select(t, raw, i35All())
	before := i35JSON(t, result)
	for i := range raw {
		raw[i] = 'x'
	}
	if !bytes.Equal(before, i35JSON(t, result)) {
		t.Fatal("input buffer retained")
	}
	// Vary every identifier field and every private category/text field separately.
	// Copying/hashing each original identity, time, error, or category must fail.
	for _, field := range []string{"id", "correlation_id", "at", "captured_at", "label", "error", "type", "layer", "source", "payload_keys", "decode_path"} {
		t.Run(field, func(t *testing.T) {
			fixture := i35Fixture()
			evidence := fixture["evidence"].(map[string]any)
			for _, name := range []string{"snapshots", "timeline", "provenance"} {
				record := evidence[name].([]any)[0].(map[string]any)
				if _, ok := record[field]; ok {
					if field == "payload_keys" || field == "decode_path" {
						record[field] = []string{"SECOND_PRIVATE_MARKER"}
					} else {
						record[field] = "SECOND_PRIVATE_MARKER"
					}
				}
			}
			altered := i35Select(t, i35JSON(t, fixture), i35All())
			if strings.Contains(string(i35JSON(t, altered)), "PRIVATE") {
				t.Fatal(altered)
			}
			if field == "id" || field == "correlation_id" || field == "at" || field == "captured_at" || field == "label" || field == "error" || field == "payload_keys" {
				if !bytes.Equal(before, i35JSON(t, altered)) {
					t.Fatal("private value affected aliases/output")
				}
			}
		})
	}
	result.Evidence["timeline"][0]["payload"].(map[string]any)["semantic"].(map[string]any)["has_dhw"] = true
	again := i35Select(t, i35JSON(t, original), i35All())
	if !bytes.Equal(before, i35JSON(t, again)) {
		t.Fatal("result aliases another call")
	}
}

func TestI35BoundsAndWholeRecords(t *testing.T) {
	raw := i35JSON(t, i35Fixture())
	full := i35Select(t, raw, i35All())
	selection := i35All()
	selection.MaxBytes = len(i35JSON(t, full)) - 1
	bounded := i35Select(t, raw, selection)
	if len(i35JSON(t, bounded)) > selection.MaxBytes {
		t.Fatal("output budget exceeded")
	}
	if len(bounded.Evidence["provenance"]) != 0 || bounded.Components["provenance"].SizeLimit != 1 || bounded.Components["provenance"].Omitted != 1 {
		t.Fatal(bounded)
	}
	if !reflect.DeepEqual(full.Evidence["timeline"], bounded.Evidence["timeline"]) {
		t.Fatal("record split")
	}
	selection.MaxBytes = 1
	if _, err := SelectInvestigationPublic(raw, selection); err == nil {
		t.Fatal("minimal artifact exceeds budget")
	}
	// Malformed oversized bytes must take the pre-parse size rejection branch.
	oversized := bytes.Repeat([]byte("["), investigationSelectionMaxBytes+1)
	if _, err := SelectInvestigationPublic(oversized, InvestigationSelection{}); err == nil || err.Error() != "investigation input size limit" {
		t.Fatal(err)
	}
	fixture := i35Fixture()
	evidence := fixture["evidence"].(map[string]any)
	event := evidence["timeline"].([]any)[0]
	snapshot := evidence["snapshots"].([]any)[0]
	evidence["timeline"] = make([]any, 23)
	evidence["snapshots"] = make([]any, 5)
	for i := range evidence["timeline"].([]any) {
		evidence["timeline"].([]any)[i] = event
	}
	for i := range evidence["snapshots"].([]any) {
		evidence["snapshots"].([]any)[i] = snapshot
	}
	capped := i35Select(t, i35JSON(t, fixture), i35All())
	if capped.Components["timeline"].Retained != 20 || capped.Components["timeline"].Omitted != 3 || capped.Components["snapshots"].Retained != 3 || capped.Components["snapshots"].Omitted != 2 {
		t.Fatal(capped)
	}
	if len(capped.Evidence["provenance"]) != 0 {
		t.Fatal("ambiguous event linked")
	}
	for _, record := range capped.Evidence["timeline"] {
		if _, ok := record["correlation_id"]; ok {
			t.Fatal("ambiguous alias")
		}
	}
}

func TestI35PartialAndMissingComponents(t *testing.T) {
	for _, variant := range []string{"absent", "empty", "malformed"} {
		t.Run(variant, func(t *testing.T) {
			fixture := i35Fixture()
			evidence := fixture["evidence"].(map[string]any)
			switch variant {
			case "absent":
				delete(evidence, "snapshots")
			case "empty":
				evidence["snapshots"] = []any{}
			case "malformed":
				evidence["snapshots"] = "PRIVATE_MARKER"
			}
			result := i35Select(t, i35JSON(t, fixture), i35All())
			loss := result.Components["snapshots"]
			if loss.Present != (variant != "absent") || loss.Invalid != (variant == "malformed") || loss.Supplied != 0 {
				t.Fatal(loss)
			}
			if len(result.Evidence["timeline"]) != 1 || result.Freshness != "Unknown" {
				t.Fatal("partial failure erased valid data", result)
			}
		})
	}
}

func TestI35DetachedRepeatedSelection(t *testing.T) {
	raw := i35JSON(t, i35Fixture())
	first := i35Select(t, raw, i35All())
	before := i35JSON(t, first)
	second := i35Select(t, raw, i35All())
	second.Evidence["timeline"][0]["payload"].(map[string]any)["semantic"].(map[string]any)["has_dhw"] = true
	second.Evidence["provenance"][0]["payload_keys"].([]string)[0] = "changed"
	second.Components["timeline"] = InvestigationComponentLoss{}
	second.Notes[0] = "changed"
	if !bytes.Equal(before, i35JSON(t, first)) {
		t.Fatal("results share caller-mutable memory")
	}
}

func TestI35CorrelationMustMatchExactly(t *testing.T) {
	for _, component := range []string{"timeline", "provenance"} {
		t.Run(component, func(t *testing.T) {
			fixture := i35Fixture()
			fixture["evidence"].(map[string]any)[component].([]any)[0].(map[string]any)["correlation_id"] = "DIFFERENT_PRIVATE_EVENT"
			result := i35Select(t, i35JSON(t, fixture), i35All())
			if len(result.Evidence["provenance"]) != 0 || len(result.Evidence["timeline"]) != 1 {
				t.Fatal("unmatched lineage rebound", result)
			}
		})
	}
}

func TestI35SourceDiagnostics(t *testing.T) {
	for _, component := range []string{"timeline", "provenance"} {
		for _, field := range []string{"source", "dropped", "interval_ms"} {
			variants := []string{"valid", "absent", "invalid"}
			if field != "source" {
				variants = append(variants, "zero")
			}
			for _, variant := range variants {
				t.Run(component+"/"+field+"/"+variant, func(t *testing.T) {
					fixture := i35Fixture()
					record := fixture["evidence"].(map[string]any)[component].([]any)[0].(map[string]any)
					if component == "timeline" {
						record = record["provenance"].(map[string]any)
					}
					delete(record, field)
					if variant == "valid" {
						if field == "source" {
							record[field] = "poll:composite"
						} else {
							record[field] = 7
						}
					}
					if variant == "zero" {
						record[field] = 0
					}
					if variant == "invalid" {
						record[field] = "PRIVATE_MARKER"
					}
					result := i35Select(t, i35JSON(t, fixture), i35All())
					out := result.Evidence[component][0]
					if component == "timeline" {
						out = out["provenance"].(map[string]any)
					}
					value, present := out[field]
					if present != (variant == "valid" || variant == "zero") {
						t.Fatal(out)
					}
					if present && field != "source" {
						expected := int64(7)
						if variant == "zero" {
							expected = 0
						}
						if value != expected {
							t.Fatal(out)
						}
					}
				})
			}
		}
	}
}

func TestI35DeepUnknownAndInvalidInput(t *testing.T) {
	// A nested allowlisted-looking key in an opaque extension is never promoted.
	nested := strings.Repeat(`{"extension":`, 1000) + `{"semantic":{"has_dhw":true},"error":"PRIVATE_MARKER"}` + strings.Repeat(`}`, 1000)
	raw := []byte(`{"markdown":"PRIVATE_MARKER","evidence":{"timeline":[{"payload":{"extension":` + nested + `}}]}}`)
	result := i35Select(t, raw, InvestigationSelection{Timeline: true, Diagnostics: []string{"semantic.has_dhw"}})
	if !reflect.DeepEqual(result.Evidence["timeline"][0]["payload"], map[string]any{}) || result.Diagnostics["semantic.has_dhw"].Missing != 1 {
		t.Fatal(result)
	}
	for _, raw := range []string{`null`, `[]`, `{"evidence":null}`, `{"evidence":[]}`, `{"evidence":{}`} {
		if _, err := SelectInvestigationPublic([]byte(raw), InvestigationSelection{}); err == nil {
			t.Fatalf("accepted malformed root: %s", raw)
		}
	}
	for _, limit := range []int{-1, investigationSelectionMaxBytes + 1} {
		if _, err := SelectInvestigationPublic([]byte(`{"evidence":{}}`), InvestigationSelection{MaxBytes: limit}); err == nil {
			t.Fatal("invalid byte limit accepted")
		}
	}
}

func TestI35CounterOptIns(t *testing.T) {
	for _, field := range []string{"dropped", "interval_ms"} {
		t.Run(field, func(t *testing.T) {
			selection := InvestigationSelection{Timeline: true, Provenance: true, IncludeDropped: field == "dropped", IncludeInterval: field == "interval_ms"}
			result := i35Select(t, i35JSON(t, i35Fixture()), selection)
			for _, component := range []string{"timeline", "provenance"} {
				source := result.Evidence[component][0]
				if component == "timeline" {
					source = source["provenance"].(map[string]any)
				}
				for _, counter := range []string{"dropped", "interval_ms"} {
					_, present := source[counter]
					if present != (counter == field) {
						t.Fatalf("%s / %s: %#v", component, field, source)
					}
				}
			}
		})
	}
}

func TestP19UnicodeCorrelations(t *testing.T) {
	cases := []struct {
		name, event, provenance string
		match, alias            bool
	}{
		{"escaped-bmp", `"\u00e9"`, `"é"`, true, true},
		{"literal-bmp", `"é"`, `"\u00e9"`, true, true},
		{"surrogate-pair", `"\ud83d\ude00"`, `"😀"`, true, true},
		{"replacement", `"�"`, `"\ufffd"`, true, true},
		{"escaped-backslash", `"\\ud800"`, `"\\ud800"`, true, true},
		{"distinct-codepoints", `"é"`, `"e\u0301"`, false, true},
		{"distinct-event-codepoints", `"e\u0301"`, `"é"`, false, true},
		{"no-trimming", `" é "`, `"é"`, false, true},
		{"no-provenance-trimming", `"é"`, `" é "`, false, true},
		{"high-pair", `"\ud800"`, `"\ud801"`, false, false},
		{"low-pair", `"\udc00"`, `"\udc01"`, false, false},
		{"high-before-text", `"\ud800x"`, `"�x"`, false, false},
		{"high-before-bmp", `"\ud800\u0041"`, `"�A"`, false, false},
		{"invalid-utf8", "\"\xff\"", "\"\xfe\"", false, false},
		{"invalid-event", "\"\xff\"", `"�"`, false, false},
		{"invalid-provenance", `"�"`, "\"\xff\"", false, true},
		{"surrogate-event", `"\ud800"`, `"�"`, false, false},
		{"surrogate-provenance", `"�"`, `"\ud800"`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(`{"evidence":{"timeline":[{"correlation_id":` + tc.event + `,"payload":{}}],"provenance":[{"correlation_id":` + tc.provenance + `}]}}`)
			result := i35Select(t, raw, InvestigationSelection{Timeline: true, Provenance: true})
			event := result.Evidence["timeline"][0]
			alias, present := event["correlation_id"]
			if present != tc.alias || (present && alias != "event-1") {
				t.Fatal(event)
			}
			wantCount := 0
			if tc.match {
				wantCount = 1
			}
			if len(result.Evidence["provenance"]) != wantCount {
				t.Fatal(result)
			}
			if tc.match && result.Evidence["provenance"][0]["correlation_id"] != "event-1" {
				t.Fatal(result)
			}
			// Direct oracle covers validation independently of occurrence/matching guards.
			if !tc.alias && investigationString(json.RawMessage(tc.event)) != "" {
				t.Fatal("malformed event decoded")
			}
		})
	}
	// An invalid event must not steal the unique alias of a valid replacement ID.
	raw := []byte(`{"evidence":{"timeline":[{"correlation_id":"\ud800"},{"correlation_id":"�"}],"provenance":[{"correlation_id":"�"}]}}`)
	result := i35Select(t, raw, InvestigationSelection{Timeline: true, Provenance: true})
	if _, ok := result.Evidence["timeline"][0]["correlation_id"]; ok {
		t.Fatal(result)
	}
	if result.Evidence["timeline"][1]["correlation_id"] != "event-2" || len(result.Evidence["provenance"]) != 1 || result.Evidence["provenance"][0]["correlation_id"] != "event-2" {
		t.Fatal(result)
	}
}

func TestP19DiagnosticExamination(t *testing.T) {
	for _, timeline := range []bool{false, true} {
		for _, group := range []string{`"x"`, `null`, `[]`, `true`, `3`, `{}`, `{"has_dhw":false}`} {
			t.Run(group+map[bool]string{false: "/unexamined", true: "/examined"}[timeline], func(t *testing.T) {
				raw := []byte(`{"evidence":{"timeline":[{"payload":{"semantic":` + group + `}}]}}`)
				result := i35Select(t, raw, InvestigationSelection{Timeline: timeline, Diagnostics: []string{"semantic.has_dhw"}})
				if !timeline {
					if len(result.Diagnostics) != 0 {
						t.Fatal("unexamined diagnostics", result.Diagnostics)
					}
					return
				}
				want := InvestigationDiagnosticLoss{Invalid: 1}
				if group == `{}` {
					want = InvestigationDiagnosticLoss{Missing: 1}
				}
				if group == `{"has_dhw":false}` {
					want = InvestigationDiagnosticLoss{}
				}
				if got, ok := result.Diagnostics["semantic.has_dhw"]; !ok || got != want {
					t.Fatal(result)
				}
			})
		}
	}
	result := i35Select(t, []byte(`{"evidence":{"timeline":[{"payload":{}}]}}`), InvestigationSelection{Timeline: true, Diagnostics: []string{"semantic.has_dhw"}})
	if result.Diagnostics["semantic.has_dhw"] != (InvestigationDiagnosticLoss{Missing: 1}) {
		t.Fatal(result)
	}
}

func TestP19DecodePaths(t *testing.T) {
	for _, stage := range []string{"source:poll:composite", "layer:composite", "gateway.portal.stream", "gateway.portal.timeline"} {
		t.Run(stage, func(t *testing.T) {
			fixture := i35Fixture()
			fixture["evidence"].(map[string]any)["provenance"].([]any)[0].(map[string]any)["decode_path"] = []string{"unreviewed", stage, "other"}
			result := i35Select(t, i35JSON(t, fixture), i35All())
			if !reflect.DeepEqual(result.Evidence["provenance"][0]["decode_path"], []string{stage}) {
				t.Fatal(result)
			}
		})
	}
}

func TestP19BudgetRemovalSequence(t *testing.T) {
	events, provenance, snapshots := []any{}, []any{}, []any{}
	for _, id := range []string{"alpha", "beta", "gamma"} {
		events = append(events, map[string]any{"correlation_id": id, "payload": map[string]any{}})
		provenance = append(provenance, map[string]any{"correlation_id": id})
		snapshots = append(snapshots, map[string]any{})
	}
	raw := i35JSON(t, map[string]any{"evidence": map[string]any{"timeline": events, "provenance": provenance, "snapshots": snapshots}})
	selection := InvestigationSelection{Timeline: true, Provenance: true, Snapshots: true}
	full := i35Select(t, raw, selection)
	for _, name := range []string{"provenance", "snapshots", "timeline"} {
		if len(full.Evidence[name]) != 3 || full.Components[name] != (InvestigationComponentLoss{Present: true, Supplied: 3, Retained: 3}) {
			t.Fatal(full)
		}
	}
	for i := 0; i < 3; i++ {
		alias := []string{"event-1", "event-2", "event-3"}[i]
		if full.Evidence["timeline"][i]["correlation_id"] != alias || full.Evidence["provenance"][i]["correlation_id"] != alias || full.Evidence["snapshots"][i]["id"] != []string{"snapshot-1", "snapshot-2", "snapshot-3"}[i] {
			t.Fatal(full)
		}
	}
	// Independent expected survivor counts cover every transition and each tail.
	for _, counts := range [][3]int{{2, 3, 3}, {1, 3, 3}, {0, 3, 3}, {0, 2, 3}, {0, 1, 3}, {0, 0, 3}, {0, 0, 2}, {0, 0, 1}, {0, 0, 0}} {
		expected := i35Select(t, raw, selection)
		for i, name := range []string{"provenance", "snapshots", "timeline"} {
			expected.Evidence[name] = expected.Evidence[name][:counts[i]]
			expected.Components[name] = InvestigationComponentLoss{Present: true, Supplied: 3, Retained: counts[i], Omitted: 3 - counts[i], SizeLimit: 3 - counts[i]}
		}
		boundedSelection := selection
		boundedSelection.MaxBytes = len(i35JSON(t, expected))
		result := i35Select(t, raw, boundedSelection)
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("survivors %v: got %#v want %#v", counts, result, expected)
		}
	}
}
