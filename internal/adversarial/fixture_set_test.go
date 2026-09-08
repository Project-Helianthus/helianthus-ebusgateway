package adversarial

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManifestRejectsBindingMutations(t *testing.T) {
	type mutation struct {
		name  string
		apply func(*manifestV1, map[string][]byte)
	}
	mutations := []mutation{
		{"path_escape", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].Path = "../escape" }},
		{"artifact_sort", func(m *manifestV1, _ map[string][]byte) {
			m.Artifacts[0], m.Artifacts[1] = m.Artifacts[1], m.Artifacts[0]
		}},
		{"size_mismatch", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].SizeBytes++ }},
		{"digest_mismatch", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].SHA256 = strings.Repeat("0", 64) }},
		{"unreferenced_resource", func(m *manifestV1, _ map[string][]byte) { m.Cases[0].ResourceArtifactIDs = []string{} }},
		{"duplicate_resource", func(m *manifestV1, _ map[string][]byte) {
			id := m.Cases[0].ResourceArtifactIDs[0]
			m.Cases[0].ResourceArtifactIDs = append(m.Cases[0].ResourceArtifactIDs, id)
		}},
		{"wrong_adv04_resource", func(m *manifestV1, _ map[string][]byte) {
			m.Cases[0].ResourceArtifactIDs = []string{m.Cases[1].ResourceArtifactIDs[0]}
		}},
		{"duplicate_case", func(m *manifestV1, _ map[string][]byte) { m.Cases = append(m.Cases, m.Cases[len(m.Cases)-1]) }},
		{"per_file_limit", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].SizeBytes = maximumJSONBytes + 1 }},
		{"aggregate_limit", func(m *manifestV1, files map[string][]byte) {
			for i := range m.Artifacts {
				if m.Artifacts[i].Role == "cache-image" {
					data := make([]byte, maximumJSONBytes)
					files[m.Artifacts[i].Path] = data
					m.Artifacts[i].SizeBytes = int64(len(data))
					m.Artifacts[i].SHA256 = digest(data)
				}
			}
		}},
		{"duplicate_run", func(m *manifestV1, files map[string][]byte) {
			var first, second *manifestArtifact
			for i := range m.Artifacts {
				if m.Artifacts[i].ArtifactID == "driver-evaluated-fail" {
					first = &m.Artifacts[i]
				}
				if m.Artifacts[i].ArtifactID == "driver-execution-error" {
					second = &m.Artifacts[i]
				}
			}
			var a, b map[string]any
			_ = json.Unmarshal(files[first.Path], &a)
			_ = json.Unmarshal(files[second.Path], &b)
			b["run_id"] = a["run_id"]
			raw, _ := json.MarshalIndent(b, "", "  ")
			raw = append(raw, '\n')
			files[second.Path] = raw
			second.SizeBytes = int64(len(raw))
			second.SHA256 = digest(raw)
		}},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			m, files := freshManifest(t)
			tc.apply(&m, files)
			raw, _ := json.MarshalIndent(m, "", "  ")
			raw = append(raw, '\n')
			_, err := buildFixtureSetData(raw, func(p string) ([]byte, error) {
				b, ok := files[p]
				if !ok {
					return nil, errors.New("missing")
				}
				return b, nil
			}, false)
			if err == nil {
				t.Fatal("mutated manifest accepted")
			}
		})
	}
}

func TestFixtureResourceIdentifiersAndScenarioBinding(t *testing.T) {
	raw := fixtureBytes(t, "inputs", "offline-all-pass")
	base, err := ParseFixtureV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*FixtureV1)
	}{
		{"two_ids", func(f *FixtureV1) {
			f.Scenarios[3].ResourceArtifactIDs = []string{"cache-offline-all-pass-adv04", "cache-evaluated-fail-adv04"}
		}},
		{"malformed", func(f *FixtureV1) { f.Scenarios[3].ResourceArtifactIDs = []string{"Bad_ID"} }},
		{"overlong", func(f *FixtureV1) { f.Scenarios[3].ResourceArtifactIDs = []string{strings.Repeat("a", 65)} }},
		{"wrong_scenario", func(f *FixtureV1) { f.Scenarios[0].ResourceArtifactIDs = []string{"cache-offline-all-pass-adv04"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := cloneFixtureV1(base)
			tc.mutate(&f)
			changed, _ := json.Marshal(f)
			if _, err := ParseFixtureV1(changed); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("ParseFixtureV1 accepted mutation: %v", err)
			}
			if _, err := BindFixtureDriverV1(changed); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("BindFixtureDriverV1 accepted mutation: %v", err)
			}
		})
	}

	t.Run("wrong_bound_case_resource", func(t *testing.T) {
		f := cloneFixtureV1(base)
		f.Scenarios[3].ResourceArtifactIDs = []string{"cache-evaluated-fail-adv04"}
		changed, _ := json.Marshal(f)
		if _, err := ParseFixtureV1(changed); err != nil {
			t.Fatalf("structurally valid resource rejected: %v", err)
		}
		if _, err := BindFixtureDriverV1(changed); !errors.Is(err, ErrInvalidFixture) {
			t.Fatalf("manifest binding accepted wrong case resource: %v", err)
		}
	})
	for _, name := range fixtureCases {
		if _, err := ParseFixtureV1(fixtureBytes(t, "inputs", name)); err != nil {
			t.Fatalf("positive %s: %v", name, err)
		}
	}
}

func TestTerminalFixtureValidatesEveryPresentSnapshot(t *testing.T) {
	terminal, err := ParseFixtureV1(fixtureBytes(t, "inputs", "execution-error"))
	if err != nil {
		t.Fatal(err)
	}
	positive, err := ParseFixtureV1(fixtureBytes(t, "inputs", "offline-all-pass"))
	if err != nil {
		t.Fatal(err)
	}
	baseline := *positive.Scenarios[0].Observations.Baseline
	mutations := []struct {
		name   string
		mutate func(*FixtureSnapshot)
	}{{"malformed_uuid", func(s *FixtureSnapshot) { s.CounterEpoch = "not-a-uuid" }}, {"negative_offset", func(s *FixtureSnapshot) { s.OffsetMS = -1 }}, {"invalid_counter", func(s *FixtureSnapshot) { s.SemanticBusCollisionsTotal = -1 }}}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			f := cloneFixtureV1(terminal)
			s := baseline
			tc.mutate(&s)
			f.Scenarios[0].Observations.Baseline = &s
			raw, _ := json.Marshal(f)
			if _, err := ParseFixtureV1(raw); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("terminal fixture accepted malformed snapshot: %v", err)
			}
		})
	}
}

func TestFixtureSemanticZoneCountUsesSafeIntegerRange(t *testing.T) {
	base, err := ParseFixtureV1(fixtureBytes(t, "inputs", "offline-all-pass"))
	if err != nil {
		t.Fatal(err)
	}

	zone21 := cloneFixtureV1(base)
	zone21.Scenarios[0].Observations.Baseline.SemanticZoneCount = 21
	zone21.Scenarios[0].Observations.End.SemanticZoneCount = 21
	if raw, err := json.Marshal(zone21); err != nil {
		t.Fatal(err)
	} else if _, err := ParseFixtureV1(raw); err != nil {
		t.Fatalf("zone count 21 rejected: %v", err)
	}

	for _, tc := range []struct {
		name  string
		value int64
	}{{"negative", -1}, {"above_safe_integer", safeInteger + 1}} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := cloneFixtureV1(base)
			fixture.Scenarios[0].Observations.End.SemanticZoneCount = tc.value
			raw, _ := json.Marshal(fixture)
			if _, err := ParseFixtureV1(raw); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("ParseFixtureV1 accepted %d: %v", tc.value, err)
			}
		})
	}
}

func TestFixtureTerminalErrorAdmissionMatchesExecution(t *testing.T) {
	base, err := ParseFixtureV1(fixtureBytes(t, "inputs", "offline-all-pass"))
	if err != nil {
		t.Fatal(err)
	}

	negative := []struct {
		name   string
		mutate func(*FixtureV1)
	}{
		{"trigger_observer_code", func(f *FixtureV1) { f.Scenarios[0].TerminalError = &TerminalError{"trigger", "observer_failed"} }},
		{"observer_trigger_code", func(f *FixtureV1) { f.Scenarios[0].TerminalError = &TerminalError{"observer", "trigger_failed"} }},
		{"evaluation_observer_code", func(f *FixtureV1) { f.Scenarios[0].TerminalError = &TerminalError{"evaluation", "observer_timeout"} }},
		{"artifact_continuity_code", func(f *FixtureV1) { f.Scenarios[0].TerminalError = &TerminalError{"artifact", "counter_epoch_changed"} }},
		{"available_precondition_error", func(f *FixtureV1) {
			f.Scenarios[0].TerminalError = &TerminalError{"precondition", "precondition_unavailable"}
		}},
		{"unavailable_precondition_error", func(f *FixtureV1) {
			reason := "ha_harness_unavailable"
			s := &f.Scenarios[0]
			s.Precondition = Precondition{Available: false, UnavailableReason: &reason}
			s.Events = []FixtureEvent{}
			s.Observations = FixtureObservations{}
			s.TerminalError = &TerminalError{"precondition", "precondition_unavailable"}
		}},
		{"duration_error_wrong_scenario", func(f *FixtureV1) {
			f.Scenarios[0].TerminalError = &TerminalError{"evaluation", "action_duration_out_of_bounds"}
		}},
		{"duration_error_with_valid_partition", func(f *FixtureV1) {
			f.Scenarios[2].TerminalError = &TerminalError{"evaluation", "action_duration_out_of_bounds"}
		}},
	}
	for _, tc := range negative {
		t.Run("reject_"+tc.name, func(t *testing.T) {
			fixture := cloneFixtureV1(base)
			tc.mutate(&fixture)
			raw, _ := json.Marshal(fixture)
			if _, err := ParseFixtureV1(raw); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("ParseFixtureV1 accepted terminal mismatch: %v", err)
			}
		})
	}

	accepted := []struct {
		name       string
		scenario   int
		terminal   TerminalError
		mutateData func(*FixtureScenario)
	}{
		{"trigger_rejected", 0, TerminalError{"trigger", "trigger_rejected"}, nil},
		{"trigger_timeout", 0, TerminalError{"trigger", "trigger_timeout"}, nil},
		{"trigger_failed", 0, TerminalError{"trigger", "trigger_failed"}, nil},
		{"observer_timeout", 0, TerminalError{"observer", "observer_timeout"}, nil},
		{"observer_failed", 0, TerminalError{"observer", "observer_failed"}, nil},
		{"counter_epoch_changed", 0, TerminalError{"evaluation", "counter_epoch_changed"}, func(s *FixtureScenario) { s.Observations.End.CounterEpoch = "00000000-0000-4000-8000-000000000099" }},
		{"negative_counter_delta", 0, TerminalError{"evaluation", "negative_counter_delta"}, func(s *FixtureScenario) {
			s.Observations.End.SemanticLiveEpoch = s.Observations.Baseline.SemanticLiveEpoch - 1
		}},
		{"evaluation_evidence_incomplete", 0, TerminalError{"evaluation", "evidence_incomplete"}, func(s *FixtureScenario) { s.Observations.End = nil }},
		{"artifact_evidence_incomplete", 0, TerminalError{"artifact", "evidence_incomplete"}, func(s *FixtureScenario) { s.Observations.Baseline, s.Observations.End = nil, nil }},
		{"action_duration_out_of_bounds", 2, TerminalError{"evaluation", "action_duration_out_of_bounds"}, func(s *FixtureScenario) { s.Events[2].OffsetMS = 58000 }},
	}
	for _, tc := range accepted {
		t.Run("accept_"+tc.name, func(t *testing.T) {
			fixture := cloneFixtureV1(base)
			scenario := &fixture.Scenarios[tc.scenario]
			scenario.TerminalError = &TerminalError{tc.terminal.Phase, tc.terminal.Code}
			if tc.mutateData != nil {
				tc.mutateData(scenario)
			}
			raw, _ := json.Marshal(fixture)
			parsed, err := ParseFixtureV1(raw)
			if err != nil {
				t.Fatalf("ParseFixtureV1 rejected supported terminal: %v", err)
			}
			result, err := canonicalRunner().runScenario(FixtureContext{}, catalogV1[tc.scenario], parsed.Scenarios[tc.scenario], canonicalRunner().StartedAt.Add(time.Duration(tc.scenario)*3*time.Minute))
			if err != nil {
				t.Fatalf("supported terminal not executable: %v", err)
			}
			if len(result.Errors) != 1 || result.Errors[0] != (ScenarioError{tc.terminal.Phase, tc.terminal.Code}) {
				t.Fatalf("terminal projection mismatch: %#v", result.Errors)
			}
		})
	}
}

func freshManifest(t *testing.T) (manifestV1, map[string][]byte) {
	t.Helper()
	raw, e := fixtureFiles.ReadFile("fixtures/v1/fixture-input-manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var m manifestV1
	if e = json.Unmarshal(raw, &m); e != nil {
		t.Fatal(e)
	}
	files := map[string][]byte{}
	for _, a := range m.Artifacts {
		b, e := fixtureFiles.ReadFile("fixtures/v1/" + a.Path)
		if e != nil {
			t.Fatal(e)
		}
		files[a.Path] = append([]byte(nil), b...)
	}
	return m, files
}
