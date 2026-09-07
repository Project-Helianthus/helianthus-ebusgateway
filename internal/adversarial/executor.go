package adversarial

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// FixtureAction is the offline fault seam. Implementations return recorded
// fixture events only; it has no shell, network, adapter, or cache mutation API.
type FixtureAction interface {
	Events(string) ([]map[string]any, error)
}

// FixtureObserver is the offline observation seam used for baseline/end metrics.
type FixtureObserver interface {
	Snapshots(string) (map[string]any, map[string]any, error)
}

// Executor turns a deterministic offline driver into the public v1 artifact.
// It is serial by construction: scenarios are projected in catalog order.
type Executor struct{ StartedAt time.Time }

func (e Executor) Run(input []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	var driver map[string]any
	if err := decoder.Decode(&driver); err != nil {
		return nil, fmt.Errorf("decode fixture driver: %w", err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("decode fixture driver: trailing value")
	}
	if driver["$schema"] != FixtureSchemaURL || integer(driver["schema_version"]) != 1 {
		return nil, fmt.Errorf("unsupported fixture schema")
	}
	if driver["fixture_case_id"] == nil || driver["run_id"] == nil {
		return nil, fmt.Errorf("fixture identity missing")
	}
	rawScenarios, ok := driver["scenarios"].([]any)
	if !ok || len(rawScenarios) != 4 {
		return nil, fmt.Errorf("fixture must contain four scenarios")
	}
	anchor := e.StartedAt.UTC()
	if anchor.IsZero() {
		anchor = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	}
	out := make([]any, 0, 4)
	passed, failed, blocked := 0, 0, 0
	for i, raw := range rawScenarios {
		fixture, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("fixture scenario %d invalid", i)
		}
		definition := Catalog()[i]
		if fixture["scenario_id"] != definition.ScenarioID || fixture["trigger_kind"] != definition.TriggerKind {
			return nil, fmt.Errorf("fixture scenario %d catalog mismatch", i)
		}
		scenario, outcome, err := projectScenario(definition, fixture, anchor.Add(time.Duration(i)*3*time.Minute))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", definition.ScenarioID, err)
		}
		switch outcome {
		case "pass":
			passed++
		case "fail":
			failed++
		case "blocked-infra":
			blocked++
		}
		out = append(out, scenario)
	}
	verdict := "pass"
	if failed > 0 {
		verdict = "fail"
	} else if blocked > 0 {
		verdict = "blocked-infra"
	}
	return map[string]any{
		"$schema": ReportSchemaURL, "schema_version": int64(1), "suite": map[string]any{"id": SuiteID, "version": int64(1)},
		"execution":  map[string]any{"mode": "offline-fixture", "run_id": driver["run_id"], "started_at": stamp(anchor), "completed_at": stamp(anchor.Add(12 * time.Minute))},
		"provenance": fixtureProvenance(driver["fixture_case_id"].(string)), "scenarios": out,
		"summary": map[string]any{"total": int64(4), "passed": int64(passed), "failed": int64(failed), "xfailed": int64(0), "blocked": int64(blocked), "unknown": int64(0), "verdict": verdict},
	}, nil
}

func projectScenario(d Definition, fixture map[string]any, start time.Time) (map[string]any, string, error) {
	precondition, ok := fixture["precondition"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("precondition missing")
	}
	base := map[string]any{"definition": definitionMap(d), "action": map[string]any{"events": []any{}}, "timing": timing(start, 0, nil, nil, nil, 0), "metrics": nullMetrics(), "evaluation": nil, "errors": []any{}, "infrastructure_reason": nil}
	if available, ok := precondition["available"].(bool); !ok || !available {
		base["result_kind"], base["outcome"], base["infrastructure_reason"] = "infrastructure-block", "blocked-infra", precondition["unavailable_reason"]
		return base, "blocked-infra", nil
	}
	events, err := actionEvents(fixture["events"], start)
	if err != nil {
		return nil, "", err
	}
	base["action"] = map[string]any{"events": events}
	byKind := eventIndex(events)
	terminal, _ := fixture["terminal_error"].(map[string]any)
	if terminal != nil {
		base["result_kind"], base["outcome"] = "execution-error", "fail"
		base["errors"] = []any{map[string]any{"phase": terminal["phase"], "code": terminal["code"]}}
		base["timing"] = timing(start, 180000, byKind[d.RecoveryAnchor], byKind[d.RecoveryEvent], nil, maxBound(events))
		return base, "fail", nil
	}
	observations, ok := fixture["observations"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("observations missing")
	}
	baseline, ok := snapshot(observations["baseline"], start)
	if !ok {
		return nil, "", fmt.Errorf("baseline missing")
	}
	end, ok := snapshot(observations["end"], start)
	if !ok {
		return nil, "", fmt.Errorf("end missing")
	}
	if baseline["counter_epoch"] != end["counter_epoch"] {
		return nil, "", fmt.Errorf("counter epoch changed")
	}
	liveDelta := integer(end["semantic_live_epoch"]) - integer(baseline["semantic_live_epoch"])
	collisionDelta := integer(end["semantic_bus_collisions_total"]) - integer(baseline["semantic_bus_collisions_total"])
	if liveDelta < 0 || collisionDelta < 0 {
		return nil, "", fmt.Errorf("negative counter delta")
	}
	anchor, recovery := byKind[d.RecoveryAnchor], byKind[d.RecoveryEvent]
	if anchor == nil || recovery == nil {
		return nil, "", fmt.Errorf("recovery events missing")
	}
	recoveryMS := integer(recovery["offset_ms"]) - integer(anchor["offset_ms"])
	durationBound, recoveryBound := maxBound(events), integer(anchor["error_bound_ms"])+integer(recovery["error_bound_ms"])
	durationPass := int64(180000)+durationBound <= 181000
	recoveryPass := recoveryMS+recoveryBound <= d.MaximumRecoveryMS
	zonesObserved := integer(end["semantic_zone_count"]) > 0
	zonesPass := !d.ZonesRequired || zonesObserved
	dhwObserved, _ := end["semantic_dhw_present"].(bool)
	dhwPass := !d.DHWRequired || dhwObserved
	livePass, collisionPass := liveDelta >= 2, collisionDelta <= d.MaximumCollisionsDelta
	pass := durationPass && recoveryPass && zonesPass && dhwPass && livePass && collisionPass
	base["timing"] = timing(start, 180000, anchor, recovery, recoveryMS, durationBound)
	base["metrics"] = map[string]any{"baseline": baseline, "end": end, "delta": map[string]any{"semantic_live_epoch": liveDelta, "semantic_bus_collisions_total": collisionDelta}}
	base["evaluation"] = map[string]any{
		"duration":   map[string]any{"expected_ms": int64(180000), "observed_ms": int64(180000), "error_bound_ms": durationBound, "passed": durationPass},
		"action":     map[string]any{"expected_kind": d.TriggerKind, "observed_kind": fixture["trigger_kind"], "passed": true},
		"recovery":   map[string]any{"maximum_ms": d.MaximumRecoveryMS, "observed_ms": recoveryMS, "error_bound_ms": recoveryBound, "passed": recoveryPass},
		"live_epoch": map[string]any{"minimum": int64(2), "observed": liveDelta, "passed": livePass},
		"zones":      map[string]any{"required": d.ZonesRequired, "observed": zonesObserved, "passed": zonesPass}, "dhw": map[string]any{"required": d.DHWRequired, "observed": dhwObserved, "passed": dhwPass},
		"collisions": map[string]any{"maximum": d.MaximumCollisionsDelta, "observed": collisionDelta, "passed": collisionPass},
	}
	base["result_kind"], base["outcome"] = "evaluated", "fail"
	if pass {
		base["outcome"] = "pass"
	}
	return base, base["outcome"].(string), nil
}

func definitionMap(d Definition) map[string]any {
	return map[string]any{"scenario_id": d.ScenarioID, "name": d.Name, "duration_limit_ms": int64(180000), "trigger_kind": d.TriggerKind, "recovery_target": d.RecoveryTarget, "minimum_live_epoch_delta": int64(2), "zones_required": d.ZonesRequired, "dhw_required": d.DHWRequired, "maximum_collisions_delta": d.MaximumCollisionsDelta, "maximum_recovery_ms": d.MaximumRecoveryMS}
}
func fixtureProvenance(caseID string) map[string]any {
	const sha = "d7fbe89d068b1b5c0d41fe51176d9e9263a441ee0ed752c9e8cae794f5a8346a"
	const commit = "ae85c5d91bd8e9dc1c9fe122de7eaa05dcd6c532"
	return map[string]any{"subject": map[string]any{"repository": "Project-Helianthus/helianthus-ebusgateway", "commit": commit, "source_tree": "clean", "artifact_kind": "gateway-fixture-set", "artifact_sha256": sha}, "producer": map[string]any{"repository": "Project-Helianthus/helianthus-ebusgateway", "commit": commit, "component": "internal/adversarial", "build_kind": "go-test-binary", "build_sha256": "1111111111111111111111111111111111111111111111111111111111111111", "input_gateway_report_sha256": nil}, "fixture_set_sha256": sha, "fixture_case_id": caseID}
}
func actionEvents(raw any, start time.Time) ([]any, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("events missing")
	}
	out := make([]any, 0, len(values))
	var prev int64 = -1
	for _, rawEvent := range values {
		event, ok := rawEvent.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("event invalid")
		}
		offset := integer(event["offset_ms"])
		if offset < prev || offset > 180000 {
			return nil, fmt.Errorf("event offset invalid")
		}
		prev = offset
		out = append(out, map[string]any{"kind": event["kind"], "source": "fixture", "at": stamp(start.Add(time.Duration(offset) * time.Millisecond)), "offset_ms": offset, "error_bound_ms": integer(event["error_bound_ms"])})
	}
	return out, nil
}
func snapshot(raw any, start time.Time) (map[string]any, bool) {
	source, ok := raw.(map[string]any)
	if !ok {
		return nil, false
	}
	offset := integer(source["offset_ms"])
	return map[string]any{"counter_epoch": source["counter_epoch"], "captured_at": stamp(start.Add(time.Duration(offset) * time.Millisecond)), "offset_ms": offset, "semantic_startup_current_phase": source["semantic_startup_current_phase"], "semantic_live_epoch": integer(source["semantic_live_epoch"]), "semantic_bus_collisions_total": integer(source["semantic_bus_collisions_total"]), "semantic_zone_count": integer(source["semantic_zone_count"]), "semantic_dhw_present": source["semantic_dhw_present"]}, true
}
func timing(start time.Time, elapsed int64, anchor, recovery map[string]any, recoveryMS any, bound int64) map[string]any {
	var a, r any
	if anchor != nil {
		a = anchor["kind"]
	}
	if recovery != nil {
		r = recovery["kind"]
	}
	return map[string]any{"scenario_started_at": stamp(start), "scenario_ended_at": stamp(start.Add(time.Duration(elapsed) * time.Millisecond)), "elapsed_ms": elapsed, "recovery_anchor": a, "recovery_observed": r, "recovery_ms": recoveryMS, "error_bound_ms": bound}
}
func nullMetrics() map[string]any { return map[string]any{"baseline": nil, "end": nil, "delta": nil} }
func eventIndex(events []any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, raw := range events {
		event := raw.(map[string]any)
		out[event["kind"].(string)] = event
	}
	return out
}
func maxBound(events []any) int64 {
	var maximum int64
	for _, raw := range events {
		if bound := integer(raw.(map[string]any)["error_bound_ms"]); bound > maximum {
			maximum = bound
		}
	}
	return maximum
}
func integer(value any) int64 {
	switch n := value.(type) {
	case json.Number:
		result, _ := n.Int64()
		return result
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}
func stamp(value time.Time) string { return value.UTC().Format("2006-01-02T15:04:05.000Z") }
