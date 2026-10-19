package portal

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const investigationSelectionMaxBytes = 256 * 1024

// InvestigationSelection selects reviewed fields, never arbitrary paths or text.
// MaxBytes bounds this detached result, not a future rendered archive.
type InvestigationSelection struct {
	Snapshots       bool
	Timeline        bool
	Provenance      bool
	Diagnostics     []string
	IncludeDropped  bool
	IncludeInterval bool
	MaxBytes        int
}

// InvestigationComponentLoss describes selection, not acquisition or freshness.
// Present distinguishes an absent component from a supplied empty array.
type InvestigationComponentLoss struct {
	Present   bool `json:"present"`
	Invalid   bool `json:"invalid"`
	Supplied  int  `json:"supplied"`
	Retained  int  `json:"retained"`
	Omitted   int  `json:"omitted"`
	SizeLimit int  `json:"size_limit"`
}

type InvestigationDiagnosticLoss struct {
	Missing int `json:"missing"`
	Invalid int `json:"invalid"`
}

// InvestigationPublicSelection is renderer input, not a new public bundle schema.
// No caller-owned buffers, private identifiers, or narrative are retained.
type InvestigationPublicSelection struct {
	Evidence    map[string][]map[string]any            `json:"evidence"`
	Components  map[string]InvestigationComponentLoss  `json:"components"`
	Diagnostics map[string]InvestigationDiagnosticLoss `json:"diagnostics"`
	Freshness   string                                 `json:"freshness"`
	Notes       []string                               `json:"notes"`
}

var investigationDiagnosticTypes = map[string]bool{
	"semantic.zones_count":        false,
	"semantic.has_dhw":            true,
	"semantic.has_energy":         true,
	"semantic.energy_series":      true,
	"registry.device_count":       false,
	"projection.device_count":     false,
	"projection.projection_count": false,
}

// SelectInvestigationPublic filters a serialized existing export supplied in
// memory. The byte limit is checked before JSON parsing. Unknown subtrees are
// opaque: only exact, typed summary leaves are inspected, never recursively copied.
func SelectInvestigationPublic(input []byte, selection InvestigationSelection) (InvestigationPublicSelection, error) {
	fail := func() (InvestigationPublicSelection, error) {
		return InvestigationPublicSelection{}, errors.New("invalid investigation selection input")
	}
	if len(input) > investigationSelectionMaxBytes {
		return InvestigationPublicSelection{}, errors.New("investigation input size limit")
	}
	if len(selection.Diagnostics) > 32 {
		return fail()
	}
	maxBytes := selection.MaxBytes
	if maxBytes == 0 {
		maxBytes = investigationSelectionMaxBytes
	}
	if maxBytes < 0 || maxBytes > investigationSelectionMaxBytes {
		return fail()
	}
	selected := map[string]bool{}
	for _, path := range selection.Diagnostics {
		if _, known := investigationDiagnosticTypes[path]; !known {
			return fail()
		}
		selected[path] = true
	}
	root := investigationObject(input)
	if root == nil {
		return fail()
	}
	evidence := investigationObject(root["evidence"])
	if evidence == nil {
		return fail()
	}
	result := InvestigationPublicSelection{
		Evidence:    map[string][]map[string]any{},
		Components:  map[string]InvestigationComponentLoss{},
		Diagnostics: map[string]InvestigationDiagnosticLoss{},
		Freshness:   "Unknown",
		Notes:       []string{"withheld_time", "withheld_identity", "withheld_coordinate", "withheld_secret", "unsafe_raw_evidence", "unreviewed_field", "lineage_unknown", "collection_unknown"},
	}
	for path := range selected {
		result.Diagnostics[path] = InvestigationDiagnosticLoss{}
	}
	arrays := map[string][]json.RawMessage{}
	for _, name := range []string{"snapshots", "timeline", "provenance", "registry_devices"} {
		raw, present := evidence[name]
		var records []json.RawMessage
		invalid := present && (len(raw) == 0 || raw[0] != '[' || json.Unmarshal(raw, &records) != nil)
		arrays[name] = records
		result.Components[name] = InvestigationComponentLoss{Present: present, Invalid: invalid, Supplied: len(records), Omitted: len(records)}
	}
	// A correlation is retained only if it identifies exactly one supplied event.
	occurrences := map[string]int{}
	for _, raw := range arrays["timeline"] {
		if id := investigationString(investigationObject(raw)["correlation_id"]); id != "" {
			occurrences[id]++
		}
	}
	events := map[string]map[string]any{}
	if selection.Timeline {
		for i, raw := range arrays["timeline"] {
			if len(result.Evidence["timeline"]) == 20 {
				break
			}
			record := investigationObject(raw)
			if record == nil {
				continue
			}
			out := map[string]any{"payload": investigationPayload(record["payload"], selected, result.Diagnostics)}
			investigationLiteral(record, out, "type", "snapshot")
			investigationLiteral(record, out, "layer", "composite")
			if source := investigationObject(record["provenance"]); source != nil {
				out["provenance"] = investigationSource(source, selection)
			}
			id := investigationString(record["correlation_id"])
			if id != "" && occurrences[id] == 1 {
				out["correlation_id"] = fmt.Sprintf("event-%d", i+1)
				events[id] = out
			}
			result.Evidence["timeline"] = append(result.Evidence["timeline"], out)
		}
	}
	if selection.Snapshots {
		for i, raw := range arrays["snapshots"] {
			if len(result.Evidence["snapshots"]) == 3 {
				break
			}
			if investigationObject(raw) == nil {
				continue
			}
			result.Evidence["snapshots"] = append(result.Evidence["snapshots"], map[string]any{"id": fmt.Sprintf("snapshot-%d", i+1), "payload": map[string]any{}})
		}
	}
	if selection.Provenance {
		for _, raw := range arrays["provenance"] {
			if len(result.Evidence["provenance"]) == 20 {
				break
			}
			record := investigationObject(raw)
			event := events[investigationString(record["correlation_id"])]
			if event == nil {
				continue
			}
			out := investigationSource(record, selection)
			out["correlation_id"] = event["correlation_id"]
			investigationLiteral(record, out, "layer", "composite")
			keys := []string{}
			for key := range event["payload"].(map[string]any) {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			out["payload_keys"] = keys
			var stages []string
			if json.Unmarshal(record["decode_path"], &stages) == nil {
				safe := []string{}
				for _, stage := range stages {
					switch stage {
					case "source:poll:composite", "layer:composite", "gateway.portal.stream", "gateway.portal.timeline":
						safe = append(safe, stage)
					}
				}
				out["decode_path"] = safe
			}
			result.Evidence["provenance"] = append(result.Evidence["provenance"], out)
		}
	}
	// Remove entire records, provenance first so no retained record points to an
	// event removed by the byte budget. Recount after every removal, including loss.
	for {
		for name, loss := range result.Components {
			loss.Retained = len(result.Evidence[name])
			loss.Omitted = loss.Supplied - loss.Retained
			result.Components[name] = loss
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return fail()
		}
		if len(encoded) <= maxBytes {
			return result, nil
		}
		removed := false
		for _, name := range []string{"provenance", "snapshots", "timeline"} {
			records := result.Evidence[name]
			if len(records) == 0 {
				continue
			}
			result.Evidence[name] = records[:len(records)-1]
			loss := result.Components[name]
			loss.SizeLimit++
			result.Components[name] = loss
			removed = true
			break
		}
		if !removed {
			return InvestigationPublicSelection{}, errors.New("investigation selection size limit")
		}
	}
}

func investigationObject(raw json.RawMessage) map[string]json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return nil
	}
	return object
}

func investigationString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func investigationLiteral(in map[string]json.RawMessage, out map[string]any, key, literal string) {
	if investigationString(in[key]) == literal {
		out[key] = literal
	}
}

func investigationCounter(raw json.RawMessage) (int64, bool) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	return value, err == nil && value >= 0
}

func investigationSource(in map[string]json.RawMessage, selection InvestigationSelection) map[string]any {
	out := map[string]any{}
	investigationLiteral(in, out, "source", "poll:composite")
	for _, key := range []string{"dropped", "interval_ms"} {
		if (key == "dropped" && !selection.IncludeDropped) || (key == "interval_ms" && !selection.IncludeInterval) {
			continue
		}
		if value, ok := investigationCounter(in[key]); ok {
			out[key] = value
		}
	}
	return out
}

func investigationPayload(raw json.RawMessage, selected map[string]bool, losses map[string]InvestigationDiagnosticLoss) map[string]any {
	in := investigationObject(raw)
	out := map[string]any{}
	for path := range selected {
		parts := strings.Split(path, ".")
		value, exists := investigationObject(in[parts[0]])[parts[1]]
		loss := losses[path]
		if !exists {
			loss.Missing++
			losses[path] = loss
			continue
		}
		var safe any
		if investigationDiagnosticTypes[path] {
			// Unmarshalling null into bool succeeds; require an actual JSON boolean.
			if string(value) == "true" {
				safe = true
			} else if string(value) == "false" {
				safe = false
			}
		} else if count, ok := investigationCounter(value); ok {
			safe = count
		}
		if safe == nil {
			loss.Invalid++
			losses[path] = loss
			continue
		}
		group, ok := out[parts[0]].(map[string]any)
		if !ok {
			group = map[string]any{}
			out[parts[0]] = group
		}
		group[parts[1]] = safe
	}
	return out
}
