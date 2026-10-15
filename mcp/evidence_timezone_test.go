package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestTeslaEvidenceTimezoneDigests(t *testing.T) {
	base := time.Unix(1_800_000_000, 123).UTC()
	publish := func(t *testing.T, at time.Time) map[string]json.RawMessage {
		t.Helper()
		p, err := newTestTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
		if err != nil {
			t.Fatal(err)
		}
		p.SetClock(func() time.Time { return base.Add(time.Second) }, func() (uint64, error) { return 0, nil })
		source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
		evidence := TeslaGen3EVSESemanticEvidence{ObservationID: "observation:timezone", ObservedAt: at, EvaluatedAt: at.Add(time.Second), PersistentObservedAt: at, ProvisionalObservedAt: at, MonotonicNS: 1, PersistentMonotonicNS: 1, ProvisionalMonotonicNS: 1, EvaluatedMonotonicNS: 1_000_000_001, Sequence: 1}
		before := evidence
		sourceBefore, err := json.Marshal([]any{teslaGen3EVSEPersistentRecord(source.Persistent, evidence), teslaGen3EVSEProvisionalRecord(source.Provisional, evidence)})
		if err != nil {
			t.Fatal(err)
		}
		input := p.inputDigest(source, evidence)
		if err := p.Publish(source, evidence); err != nil {
			t.Fatal(err)
		}
		public, err := p.TeslaGen3EVSESemanticCurrent(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(public)
		if err != nil {
			t.Fatal(err)
		}
		var view map[string]json.RawMessage
		if err := json.Unmarshal(raw, &view); err != nil {
			t.Fatal(err)
		}
		view["input"], err = json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		sourceAfter, err := json.Marshal([]any{teslaGen3EVSEPersistentRecord(source.Persistent, evidence), teslaGen3EVSEProvisionalRecord(source.Provisional, evidence)})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(evidence, before) || !bytes.Equal(sourceBefore, sourceAfter) {
			t.Fatal("publication changed caller-owned input")
		}
		var snapshot struct {
			ID      json.RawMessage `json:"snapshot_id"`
			Cursors []struct {
				Digest json.RawMessage `json:"last_batch_digest"`
			} `json:"cursors"`
		}
		var evaluation struct {
			Digest json.RawMessage `json:"evaluation_digest"`
		}
		if err := json.Unmarshal(view["snapshot"], &snapshot); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(view["evaluation"], &evaluation); err != nil {
			t.Fatal(err)
		}
		if len(snapshot.ID) == 0 || len(snapshot.Cursors) != 1 || len(snapshot.Cursors[0].Digest) == 0 || len(evaluation.Digest) == 0 {
			t.Fatal("publication lacks digest coordinates")
		}
		view["snapshot_id"] = snapshot.ID
		view["last_batch_digest"] = snapshot.Cursors[0].Digest
		view["evaluation_digest"] = evaluation.Digest
		return view
	}
	want := publish(t, base)
	for _, tc := range []struct {
		name string
		at   time.Time
	}{
		{"UTC", base}, {"positive", base.In(time.FixedZone("positive", 19800))}, {"negative", base.In(time.FixedZone("negative", -25200))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := publish(t, tc.at)
			for _, key := range []string{"input", "snapshot", "evaluation", "projection", "selections", "snapshot_id", "last_batch_digest", "evaluation_digest"} {
				if !bytes.Equal(got[key], want[key]) {
					t.Errorf("same instant changed %s", key)
				}
			}
		})
	}
}

func TestTeslaEvidenceTimezoneZeroAndMonotonic(t *testing.T) {
	p, err := newTestTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
	if err != nil {
		t.Fatal(err)
	}
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	anchor := time.Now()
	a := TeslaGen3EVSESemanticEvidence{ObservedAt: anchor, EvaluatedAt: anchor, MonotonicNS: 1}
	b := a
	b.ObservedAt, b.EvaluatedAt = anchor.Round(0), anchor.Round(0)
	if p.inputDigest(source, a) != p.inputDigest(source, b) {
		t.Error("Go monotonic metadata changed input digest")
	}
	b.MonotonicNS++
	if p.inputDigest(source, a) == p.inputDigest(source, b) {
		t.Error("explicit monotonic coordinate was lost")
	}
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("positive", 3600), time.FixedZone("negative", -3600)} {
		zero := time.Time{}.In(loc)
		evidence := TeslaGen3EVSESemanticEvidence{ObservationID: "observation:zero", ObservedAt: zero, EvaluatedAt: zero, Sequence: 1}
		before := evidence
		if err := p.Publish(source, evidence); err == nil {
			t.Error("zero receipt was admitted")
		}
		if evidence != before {
			t.Error("zero caller input changed")
		}
	}
}
