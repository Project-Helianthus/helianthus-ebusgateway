package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	"reflect"
	"testing"
	"time"
)

func TestGrowattEvidenceTimezoneDigests(t *testing.T) {
	base := time.Unix(1_800_000_000, 3).UTC()
	publish := func(t *testing.T, at time.Time) map[string]json.RawMessage {
		t.Helper()
		fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 4}
		runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
		native, err := runtime.GrowattBMSRS485V202(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		evidence, ok := runtime.LastObservationEvidence()
		if !ok {
			t.Fatal("missing retained evidence")
		}
		evidence.ReceiptWall = at
		before := evidence.clone()
		// Canonical decimals keep the #1029 receipt-wall fixture explicit;
		// the separate Decimal defect was fixed in #1051.
		native.Status.PackVoltageVolts = 52
		native.Status.PackCurrentAmps = -12.5
		native.Status.SOCPercent = 75
		native.Status.TemperatureCelsius = 24
		native.Status.CumulativeChargeAmpHours = 123.5
		native.Status.CumulativeDischargeAmpHours = 42.25
		// JSON captures public scalar fields independently; retained native
		// evidence is checked separately through its public accessor.
		statusBefore, err := json.Marshal(native.Status)
		if err != nil {
			t.Fatal(err)
		}
		nativeBefore := native.Status.NativeObservation()
		slicesBefore := nativeBefore.Slices()
		public, err := runtime.storage.Publish(native.Status, evidence)
		if err != nil {
			t.Fatalf("canonical fixture publication: %v", err)
		}
		statusAfter, err := json.Marshal(native.Status)
		if err != nil {
			t.Fatal(err)
		}
		nativeAfter := native.Status.NativeObservation()
		if !reflect.DeepEqual(evidence, before) || !bytes.Equal(statusAfter, statusBefore) ||
			nativeAfter.UnitID() != nativeBefore.UnitID() || nativeAfter.Revision() != nativeBefore.Revision() ||
			!reflect.DeepEqual(nativeAfter.Slices(), slicesBefore) {
			t.Fatal("publication changed caller-owned input")
		}
		encoded, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		ref := growattStorageEvidence("native.growatt.bms.rs485.v202.observation", evidence)
		if at.Location() == base.Location() && at.Equal(base) {
			sum := sha256.Sum256(encoded)
			if string(ref.Digest) != fmt.Sprintf("sha256:%x", sum) {
				t.Error("UTC evidence baseline bytes changed")
			}
		}
		var view map[string]json.RawMessage
		raw, err := json.Marshal(public)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &view); err != nil {
			t.Fatal(err)
		}
		// Snapshot includes last_batch_digest, snapshot_id and all fact evidence;
		// evaluation includes evaluation_digest. Compare each independently.
		view["evidence"], err = json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
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
		{"UTC", base},
		{"positive", base.In(time.FixedZone("positive", 5*3600+1800))},
		{"negative", base.In(time.FixedZone("negative", -7*3600))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := publish(t, tc.at)
			for _, key := range []string{"evidence", "snapshot", "evaluation", "projection", "selections", "snapshot_id", "last_batch_digest", "evaluation_digest"} {
				if !bytes.Equal(got[key], want[key]) {
					t.Errorf("same instant changed %s", key)
				}
			}
		})
	}
}

func TestGrowattEvidenceTimezoneZeroAndMonotonic(t *testing.T) {
	// Explicit receipt monotonic coordinates remain evidence, independent of
	// Go's process-local monotonic metadata, which JSON intentionally omits.
	anchor := time.Now()
	a := GrowattBMSRS485ObservationEvidence{ReceiptWall: anchor, ReceiptMonotonic: time.Second}
	b := a
	b.ReceiptWall = anchor.Round(0)
	if growattStorageEvidence("native.fixture", a) != growattStorageEvidence("native.fixture", b) {
		t.Error("Go monotonic metadata changed digest")
	}
	b.ReceiptMonotonic++
	if growattStorageEvidence("native.fixture", a) == growattStorageEvidence("native.fixture", b) {
		t.Error("explicit receipt monotonic coordinate was lost")
	}
	zero := time.Time{}
	if !zero.In(time.FixedZone("positive", 3600)).IsZero() {
		t.Fatal("zero fixture is not zero")
	}
	for _, at := range []time.Time{zero, zero.In(time.FixedZone("positive", 3600)), zero.In(time.FixedZone("negative", -3600))} {
		evidence := GrowattBMSRS485ObservationEvidence{ReceiptWall: at}
		if err := validateGrowattStorageInput(modbusreg.GrowattBMSTypedReadOnlyStatus{}, evidence, "asset:fixture", "source:fixture"); err == nil || err.Error() != "native_evidence_missing" {
			t.Fatalf("zero receipt must fail closed: %v", err)
		}
	}
}
