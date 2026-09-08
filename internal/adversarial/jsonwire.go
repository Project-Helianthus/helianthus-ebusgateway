package adversarial

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

const maximumJSONBytes = 1024 * 1024

var integerToken = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

func strictUnmarshal(raw []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := requireEOF(d); err != nil {
		return err
	}
	return nil
}

func scanJSON(raw []byte, label string) error {
	if len(raw) > maximumJSONBytes {
		return fmt.Errorf("%s size", label)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("%s utf8", label)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := scanValue(d, 0); err != nil {
		return err
	}
	return requireEOF(d)
}

func requireEOF(d *json.Decoder) error {
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanValue(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting exceeds 64")
	}
	tok, err := d.Token()
	if err != nil {
		return err
	}
	switch v := tok.(type) {
	case json.Number:
		s := string(v)
		if !integerToken.MatchString(s) || len(strings.TrimPrefix(s, "-")) > 16 {
			return errors.New("invalid integer token")
		}
	case json.Delim:
		switch v {
		case '{':
			seen := map[string]struct{}{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return errors.New("invalid object key")
				}
				if _, ok := seen[name]; ok {
					return errors.New("duplicate object key")
				}
				seen[name] = struct{}{}
				if err := scanValue(d, depth+1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid object end")
			}
		case '[':
			for d.More() {
				if err := scanValue(d, depth+1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid array end")
			}
		default:
			return errors.New("unexpected delimiter")
		}
	}
	return nil
}

func decodeRaw(raw []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func exactObject(v any, keys ...string) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("object required")
	}
	if len(m) != len(keys) {
		return nil, errors.New("closed object shape")
	}
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return nil, errors.New("required member missing")
		}
	}
	return m, nil
}
func nonNull(m map[string]any, keys ...string) error {
	for _, k := range keys {
		if m[k] == nil {
			return errors.New("non-null member is null")
		}
	}
	return nil
}
func array(v any) ([]any, error) {
	a, ok := v.([]any)
	if !ok {
		return nil, errors.New("array required")
	}
	return a, nil
}

func fixtureShape(raw map[string]any) error {
	r, err := exactObject(raw, "$schema", "schema_version", "suite", "fixture_case_id", "run_id", "scenarios")
	if err != nil {
		return err
	}
	if err = nonNull(r, "$schema", "schema_version", "suite", "fixture_case_id", "run_id", "scenarios"); err != nil {
		return err
	}
	s, err := exactObject(r["suite"], "id", "version")
	if err != nil {
		return err
	}
	if err = nonNull(s, "id", "version"); err != nil {
		return err
	}
	scenarios, err := array(r["scenarios"])
	if err != nil {
		return err
	}
	for _, rv := range scenarios {
		x, err := exactObject(rv, "scenario_id", "trigger_kind", "precondition", "events", "observations", "terminal_error", "resource_artifact_ids")
		if err != nil {
			return err
		}
		if err = nonNull(x, "scenario_id", "trigger_kind", "precondition", "events", "observations", "resource_artifact_ids"); err != nil {
			return err
		}
		p, err := exactObject(x["precondition"], "available", "unavailable_reason")
		if err != nil {
			return err
		}
		if err = nonNull(p, "available"); err != nil {
			return err
		}
		events, err := array(x["events"])
		if err != nil {
			return err
		}
		for _, ev := range events {
			e, err := exactObject(ev, "kind", "offset_ms", "error_bound_ms")
			if err != nil {
				return err
			}
			if err = nonNull(e, "kind", "offset_ms", "error_bound_ms"); err != nil {
				return err
			}
		}
		o, err := exactObject(x["observations"], "baseline", "end")
		if err != nil {
			return err
		}
		for _, k := range []string{"baseline", "end"} {
			if o[k] != nil {
				q, err := exactObject(o[k], "counter_epoch", "offset_ms", "semantic_startup_current_phase", "semantic_live_epoch", "semantic_bus_collisions_total", "semantic_zone_count", "semantic_dhw_present")
				if err != nil {
					return err
				}
				if err = nonNull(q, "counter_epoch", "offset_ms", "semantic_startup_current_phase", "semantic_live_epoch", "semantic_bus_collisions_total", "semantic_zone_count", "semantic_dhw_present"); err != nil {
					return err
				}
			}
		}
		if x["terminal_error"] != nil {
			e, err := exactObject(x["terminal_error"], "phase", "code")
			if err != nil {
				return err
			}
			if err = nonNull(e, "phase", "code"); err != nil {
				return err
			}
		}
		if _, err = array(x["resource_artifact_ids"]); err != nil {
			return err
		}
	}
	return nil
}

func reportShape(raw map[string]any) error {
	r, err := exactObject(raw, "$schema", "schema_version", "suite", "execution", "provenance", "scenarios", "summary")
	if err != nil {
		return err
	}
	if err = nonNull(r, "$schema", "schema_version", "suite", "execution", "provenance", "scenarios", "summary"); err != nil {
		return err
	}
	s, err := exactObject(r["suite"], "id", "version")
	if err != nil {
		return err
	}
	if err = nonNull(s, "id", "version"); err != nil {
		return err
	}
	e, err := exactObject(r["execution"], "mode", "run_id", "started_at", "completed_at")
	if err != nil {
		return err
	}
	if err = nonNull(e, "mode", "run_id", "started_at", "completed_at"); err != nil {
		return err
	}
	p, err := exactObject(r["provenance"], "subject", "producer", "fixture_set_sha256", "fixture_case_id")
	if err != nil {
		return err
	}
	if err = nonNull(p, "subject", "producer", "fixture_set_sha256", "fixture_case_id"); err != nil {
		return err
	}
	sub, err := exactObject(p["subject"], "repository", "commit", "source_tree", "artifact_kind", "artifact_sha256")
	if err != nil {
		return err
	}
	if err = nonNull(sub, "repository", "commit", "source_tree", "artifact_kind", "artifact_sha256"); err != nil {
		return err
	}
	prod, err := exactObject(p["producer"], "repository", "commit", "component", "build_kind", "build_sha256", "input_gateway_report_sha256")
	if err != nil {
		return err
	}
	if err = nonNull(prod, "repository", "commit", "component", "build_kind", "build_sha256"); err != nil {
		return err
	}
	scenarios, err := array(r["scenarios"])
	if err != nil {
		return err
	}
	for _, rv := range scenarios {
		if err := scenarioShape(rv); err != nil {
			return err
		}
	}
	sum, err := exactObject(r["summary"], "total", "passed", "failed", "xfailed", "blocked", "unknown", "verdict")
	if err != nil {
		return err
	}
	return nonNull(sum, "total", "passed", "failed", "xfailed", "blocked", "unknown", "verdict")
}

func scenarioShape(rv any) error {
	x, err := exactObject(rv, "definition", "action", "timing", "metrics", "evaluation", "errors", "infrastructure_reason", "result_kind", "outcome")
	if err != nil {
		return err
	}
	if err = nonNull(x, "definition", "action", "timing", "metrics", "errors", "result_kind", "outcome"); err != nil {
		return err
	}
	d, err := exactObject(x["definition"], "scenario_id", "name", "duration_limit_ms", "trigger_kind", "recovery_target", "minimum_live_epoch_delta", "zones_required", "dhw_required", "maximum_collisions_delta", "maximum_recovery_ms")
	if err != nil {
		return err
	}
	if err = nonNull(d, "scenario_id", "name", "duration_limit_ms", "trigger_kind", "recovery_target", "minimum_live_epoch_delta", "zones_required", "dhw_required", "maximum_collisions_delta", "maximum_recovery_ms"); err != nil {
		return err
	}
	a, err := exactObject(x["action"], "events")
	if err != nil {
		return err
	}
	if err = nonNull(a, "events"); err != nil {
		return err
	}
	evs, err := array(a["events"])
	if err != nil {
		return err
	}
	for _, rv := range evs {
		e, err := exactObject(rv, "kind", "source", "at", "offset_ms", "error_bound_ms")
		if err != nil {
			return err
		}
		if err = nonNull(e, "kind", "source", "at", "offset_ms", "error_bound_ms"); err != nil {
			return err
		}
	}
	t, err := exactObject(x["timing"], "scenario_started_at", "scenario_ended_at", "elapsed_ms", "recovery_anchor", "recovery_observed", "recovery_ms", "error_bound_ms")
	if err != nil {
		return err
	}
	if err = nonNull(t, "scenario_started_at", "scenario_ended_at", "elapsed_ms", "error_bound_ms"); err != nil {
		return err
	}
	m, err := exactObject(x["metrics"], "baseline", "end", "delta")
	if err != nil {
		return err
	}
	for _, k := range []string{"baseline", "end"} {
		if m[k] != nil {
			q, err := exactObject(m[k], "counter_epoch", "captured_at", "offset_ms", "semantic_startup_current_phase", "semantic_live_epoch", "semantic_bus_collisions_total", "semantic_zone_count", "semantic_dhw_present")
			if err != nil {
				return err
			}
			if err = nonNull(q, "counter_epoch", "captured_at", "offset_ms", "semantic_startup_current_phase", "semantic_live_epoch", "semantic_bus_collisions_total", "semantic_zone_count", "semantic_dhw_present"); err != nil {
				return err
			}
		}
	}
	if m["delta"] != nil {
		q, err := exactObject(m["delta"], "semantic_live_epoch", "semantic_bus_collisions_total")
		if err != nil {
			return err
		}
		if err = nonNull(q, "semantic_live_epoch", "semantic_bus_collisions_total"); err != nil {
			return err
		}
	}
	if x["evaluation"] != nil {
		ev, err := exactObject(x["evaluation"], "duration", "action", "recovery", "live_epoch", "zones", "dhw", "collisions")
		if err != nil {
			return err
		}
		if err = nonNull(ev, "duration", "action", "recovery", "live_epoch", "zones", "dhw", "collisions"); err != nil {
			return err
		}
		specs := map[string][]string{"duration": {"expected_ms", "observed_ms", "error_bound_ms", "passed"}, "action": {"expected_kind", "observed_kind", "passed"}, "recovery": {"maximum_ms", "observed_ms", "error_bound_ms", "passed"}, "live_epoch": {"minimum", "observed", "passed"}, "zones": {"required", "observed", "passed"}, "dhw": {"required", "observed", "passed"}, "collisions": {"maximum", "observed", "passed"}}
		for k, keys := range specs {
			q, err := exactObject(ev[k], keys...)
			if err != nil {
				return err
			}
			if err = nonNull(q, keys...); err != nil {
				return err
			}
		}
	}
	errs, err := array(x["errors"])
	if err != nil {
		return err
	}
	for _, rv := range errs {
		q, err := exactObject(rv, "phase", "code")
		if err != nil {
			return err
		}
		if err = nonNull(q, "phase", "code"); err != nil {
			return err
		}
	}
	return nil
}

func ParseFixtureV1(raw []byte) (FixtureV1, error) {
	if err := scanJSON(raw, "fixture"); err != nil {
		return FixtureV1{}, fmt.Errorf("%w: structural_shape", ErrInvalidFixture)
	}
	v, err := decodeRaw(raw)
	if err != nil || fixtureShape(v) != nil {
		return FixtureV1{}, fmt.Errorf("%w: structural_shape", ErrInvalidFixture)
	}
	var out FixtureV1
	if err := strictUnmarshal(raw, &out); err != nil {
		return FixtureV1{}, fmt.Errorf("%w: structural_shape", ErrInvalidFixture)
	}
	if rule := validateFixture(out); rule != "" {
		return FixtureV1{}, fmt.Errorf("%w: %s", ErrInvalidFixture, rule)
	}
	return out, nil
}

func ParseReportV1(raw []byte) (ReportV1, error) {
	if err := scanJSON(raw, "report"); err != nil {
		return ReportV1{}, fmt.Errorf("%w: structural_shape", ErrInvalidReport)
	}
	v, err := decodeRaw(raw)
	if err != nil || reportShape(v) != nil {
		return ReportV1{}, fmt.Errorf("%w: structural_shape", ErrInvalidReport)
	}
	var out ReportV1
	if err := strictUnmarshal(raw, &out); err != nil {
		return ReportV1{}, fmt.Errorf("%w: structural_shape", ErrInvalidReport)
	}
	if err := ValidateRuntimeReportV1(out); err != nil {
		return ReportV1{}, err
	}
	return out, nil
}
