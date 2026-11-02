package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"strconv"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// All identities, clock coordinates and admission-positive controls are
// synthetic. They establish adapter behavior, never physical qualification.
func thermalSolarPumpFixture() (thermalSolarPumpContext, *thermalSolarPumpObservation, *thermalSolarPumpObservation) {
	identity := thermalSolarPumpIdentity{Asset: "asset:solar", Service: "service:solar", Owner: "loop:a",
		Source: "source:a", SourceEpoch: "epoch:a", DriverGeneration: "1", Binding: "binding:a", BindingRevision: "1", AssociationRevision: "1"}
	wall := semreg.TimePoint{UnixNanoseconds: "10", ClockID: "clock.utc", UncertaintyNS: "0"}
	ctx := thermalSolarPumpContext{Identity: identity, Qualification: semreg.QualificationQualified, FM5: semreg.AvailabilityAvailable,
		Evaluation: semreg.EvaluationContext{EvaluatedAt: wall, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "100"}}}
	sum := sha256.Sum256([]byte("synthetic solar value evidence"))
	statusValue, hoursValue := uint8(0), uint32(42)
	status := &thermalSolarPumpObservation{Identity: identity,
		Selector: thermalSolarPumpSelector{PB: 0xB5, SB: 0x24, Opcode: 0x02, OperationType: "read", Group: 0x04, Instance: 0, Register: 0x0008, Direction: "target_to_initiator"},
		Status:   &statusValue, Request: []byte{0x02, 0x04, 0, 0x08, 0}, Reply: []byte{0}, Family: "synthetic-controller", Firmware: "unknown",
		Evidence:   []semreg.EvidenceRef{{Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:]))}},
		ReceivedAt: wall, ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "90"},
		Policy:   semreg.FreshnessPolicy{PolicyID: "policy:synthetic-status", Version: "1.0.0", FreshForNS: "20", RetainForNS: "80", MaxWallUncertaintyNS: "0"},
		Validity: semreg.ValidityGood, Qualification: semreg.QualificationQualified, Acquisition: semreg.AvailabilityAvailable}
	hours := *status
	hours.Status, hours.Hours = nil, &hoursValue
	hours.Selector.Register = 0x000B
	hours.Request, hours.Reply = []byte{0x02, 0x04, 0, 0x0B, 0}, []byte{42, 0, 0, 0}
	hours.Evidence = append([]semreg.EvidenceRef(nil), status.Evidence...)
	hours.Policy.PolicyID, hours.Policy.FreshForNS, hours.Policy.RetainForNS = "policy:synthetic-hours", "50", "100"
	return ctx, status, &hours
}

func TestThermalSolarPumpMappingValues(t *testing.T) {
	for _, value := range []uint8{0, 1, 2, 0xFF} {
		t.Run("status-"+strconv.Itoa(int(value)), func(t *testing.T) {
			ctx, status, hours := thermalSolarPumpFixture()
			*status.Status, *hours.Hours = value, 0
			status.Reply[0] = value
			binary.LittleEndian.PutUint32(hours.Reply, 0)
			got := mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
			if (got.Status.Retained != nil) != (value <= 1) {
				t.Fatalf("status code %d admission: %+v", value, got.Status)
			}
			if value <= 1 && (*got.Status.Retained.Status != value || !got.Status.Usable) {
				t.Fatal("lost false/on native value")
			}
			if got.Hours.Retained == nil || *got.Hours.Retained.Hours != 0 || !got.Hours.Usable || len(got.Facts) != 0 {
				t.Fatal("zero lost, sibling affected or inferred fact")
			}
		})
	}
	for _, kind := range []string{"missing", "empty", "invalid", "sentinel", "wrong-value-kind"} {
		t.Run(kind, func(t *testing.T) {
			ctx, oldStatus, oldHours := thermalSolarPumpFixture()
			_, status, hours := thermalSolarPumpFixture()
			switch kind {
			case "missing":
				status, hours = nil, nil
			case "empty":
				status.Status, hours.Hours = nil, nil
			case "invalid":
				status.Validity, hours.Validity = semreg.ValidityBad, semreg.ValiditySuspect
			case "sentinel":
				*status.Status, *hours.Hours = 0xFF, ^uint32(0)
				status.Reply[0] = 0xFF
				binary.LittleEndian.PutUint32(hours.Reply, ^uint32(0))
			case "wrong-value-kind":
				status.Hours, hours.Status = hours.Hours, status.Status
			}
			got := mapThermalSolarPump(ctx, thermalSolarPumpState{oldStatus, oldHours}, status, hours)
			if !reflect.DeepEqual(got.Status.Retained, oldStatus) || !reflect.DeepEqual(got.Hours.Retained, oldHours) || got.Status.Problem == "" || got.Hours.Problem == "" {
				t.Fatal("failed observation replaced or renewed last good fields")
			}
			if kind != "missing" && len(got.Attempted) != 2 {
				t.Fatal("failed native evidence lost")
			}
		})
	}
}

func TestThermalSolarPumpMappingDecreaseAndNoInference(t *testing.T) {
	for _, oldValue := range []uint32{42, ^uint32(0) - 1} {
		ctx, status, hours := thermalSolarPumpFixture()
		*hours.Hours = oldValue
		binary.LittleEndian.PutUint32(hours.Reply, oldValue)
		previous := mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
		_, nextStatus, nextHours := thermalSolarPumpFixture()
		*nextStatus.Status, *nextHours.Hours = 1, 0
		nextStatus.Reply[0] = 1
		binary.LittleEndian.PutUint32(nextHours.Reply, 0)
		nextStatus.ReceiptMonotonic.Nanoseconds, nextHours.ReceiptMonotonic.Nanoseconds = "95", "95"
		got := mapThermalSolarPump(ctx, thermalSolarPumpState{previous.Status.Retained, previous.Hours.Retained}, nextStatus, nextHours)
		if *got.Hours.Retained.Hours != 0 || *got.Status.Retained.Status != 1 || len(got.History) != 2 || *got.History[1].Hours != oldValue || len(got.Facts) != 0 {
			t.Fatal("counter decrease unwrapped or status edge turned into a fact")
		}
		wantLoss := []thermalSolarPumpLoss{{"pump-active", "Hypothesis", "no_thermal_pack_definition"}, {"operating-hours", "Hypothesis", "no_thermal_pack_definition_absolute_hours_only"}, {"starts", "Unknown", "no_native_selector_or_thermal_pack_definition"}}
		if !reflect.DeepEqual(got.Loss, wantLoss) {
			t.Fatalf("native loss changed: %+v", got.Loss)
		}
	}
	// Existing pack definitions stay upstream-owned. Solar loss supplies neither
	// placeholder definition IDs nor requested-item dispositions to merge.
	ctx, _, _ := thermalSolarPumpFixture()
	missing := mapThermalSolarPump(ctx, thermalSolarPumpState{}, nil, nil)
	if len(missing.Facts) != 0 || missing.Status.Retained != nil || missing.Hours.Retained != nil || len(missing.Loss) != 3 {
		t.Fatal("missing source created facts or lost source loss")
	}
}

// Each mutation is exercised against both retained and incoming observations;
// the other field must remain usable. Removing any corresponding identity or
// selector comparison would admit a cross-lineage/cross-selector value.
func TestThermalSolarPumpMappingIdentity(t *testing.T) {
	mutations := map[string]func(*thermalSolarPumpObservation){
		"asset":                func(o *thermalSolarPumpObservation) { o.Identity.Asset = "asset:other" },
		"service":              func(o *thermalSolarPumpObservation) { o.Identity.Service = "service:other" },
		"owner":                func(o *thermalSolarPumpObservation) { o.Identity.Owner = "loop:b" },
		"source":               func(o *thermalSolarPumpObservation) { o.Identity.Source = "source:b" },
		"source-epoch":         func(o *thermalSolarPumpObservation) { o.Identity.SourceEpoch = "epoch:b" },
		"driver-generation":    func(o *thermalSolarPumpObservation) { o.Identity.DriverGeneration = "2" },
		"binding":              func(o *thermalSolarPumpObservation) { o.Identity.Binding = "binding:b" },
		"binding-revision":     func(o *thermalSolarPumpObservation) { o.Identity.BindingRevision = "2" },
		"association-revision": func(o *thermalSolarPumpObservation) { o.Identity.AssociationRevision = "2" },
		"pb":                   func(o *thermalSolarPumpObservation) { o.Selector.PB = 0xB4 },
		"sb":                   func(o *thermalSolarPumpObservation) { o.Selector.SB = 0x23 },
		"opcode":               func(o *thermalSolarPumpObservation) { o.Selector.Opcode = 0x06 },
		"operation-type":       func(o *thermalSolarPumpObservation) { o.Selector.OperationType = "write" },
		"group":                func(o *thermalSolarPumpObservation) { o.Selector.Group = 0x02 },
		"instance":             func(o *thermalSolarPumpObservation) { o.Selector.Instance = 1 },
		"register":             func(o *thermalSolarPumpObservation) { o.Selector.Register = 0x0025 },
		"direction":            func(o *thermalSolarPumpObservation) { o.Selector.Direction = "initiator_to_target" },
	}
	for name, mutate := range mutations {
		for _, target := range []string{"status", "hours"} {
			t.Run(name+"/"+target, func(t *testing.T) {
				ctx, status, hours := thermalSolarPumpFixture()
				if target == "status" {
					mutate(status)
				} else {
					mutate(hours)
				}
				for _, retained := range []bool{false, true} {
					var got thermalSolarPumpMapping
					if retained {
						got = mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, nil, nil)
					} else {
						got = mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
					}
					bad, sibling := got.Status, got.Hours
					if target == "hours" {
						bad, sibling = got.Hours, got.Status
					}
					if bad.Retained != nil || bad.Usable || !sibling.Usable {
						t.Fatalf("identity comparison omitted: %+v", got)
					}
					if retained && len(got.History) != 1 {
						t.Fatal("old lineage history lost")
					}
				}
			})
		}
	}
}

func TestThermalSolarPumpMappingReplacement(t *testing.T) {
	ctx, status, hours := thermalSolarPumpFixture()
	previous := thermalSolarPumpState{status, hours}
	ctx.Identity.Owner = "loop:b"
	_, replacement, _ := thermalSolarPumpFixture()
	replacement.Identity = ctx.Identity
	*replacement.Status = 0
	got := mapThermalSolarPump(ctx, previous, replacement, nil)
	if got.Status.Retained == nil || *got.Status.Retained.Status != 0 || got.Hours.Retained != nil || len(got.History) != 2 {
		t.Fatal("replacement inherited old owner")
	}
	if !reflect.DeepEqual(got.History[0], *status) || !reflect.DeepEqual(got.History[1], *hours) {
		t.Fatal("history relabeled")
	}
}

func TestThermalSolarPumpMappingLifecycle(t *testing.T) {
	ctx, status, hours := thermalSolarPumpFixture()
	ctx.Evaluation.EvaluateMonotonic.Nanoseconds = "120"
	newHours := *hours
	newHours.ReceiptMonotonic.Nanoseconds = "115"
	got := mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, nil, &newHours)
	if got.Status.Freshness != semreg.FreshnessStale || got.Status.Usable || got.Hours.Freshness != semreg.FreshnessFresh || !got.Hours.Usable {
		t.Fatal("hours renewed status")
	}
	if got.Status.Retained.ReceiptMonotonic.Nanoseconds != "90" || got.Status.Retained.ReceivedAt.UnixNanoseconds != "10" {
		t.Fatal("evaluation became receipt")
	}
	for _, mode := range []string{"qualification", "binding-qualification", "validity", "acquisition", "expired", "clock-epoch", "future", "policy", "unknown-identity"} {
		t.Run(mode, func(t *testing.T) {
			ctx, status, hours := thermalSolarPumpFixture()
			switch mode {
			case "qualification":
				status.Qualification = semreg.QualificationUnknown
			case "binding-qualification":
				ctx.Qualification = semreg.QualificationCandidate
			case "validity":
				status.Validity = semreg.ValidityUnknown
			case "acquisition":
				status.Acquisition = semreg.AvailabilityUnavailable
			case "expired":
				status.ReceiptMonotonic.Nanoseconds = "0"
			case "clock-epoch":
				status.ReceiptMonotonic.ClockEpochID = "clock:other"
			case "future":
				status.ReceiptMonotonic.Nanoseconds = "101"
			case "policy":
				status.Policy.FreshForNS = "0"
			case "unknown-identity":
				ctx.Identity.AssociationRevision = ""
			}
			got := mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
			if got.Status.Usable {
				t.Fatal("invalid lifecycle admitted")
			}
			if mode != "binding-qualification" && mode != "unknown-identity" && !got.Hours.Usable {
				t.Fatal("sibling lifecycle affected")
			}
			if mode == "qualification" && (got.Status.Retained == nil || got.Status.Retained.Validity != semreg.ValidityGood || got.Status.Freshness != semreg.FreshnessFresh) {
				t.Fatal("qualification collapsed validity/freshness")
			}
		})
	}
	for _, availability := range []semreg.Availability{semreg.AvailabilityDegraded, semreg.AvailabilityWithdrawn, semreg.AvailabilityUnavailable, ""} {
		t.Run(string(availability), func(t *testing.T) {
			ctx, status, hours := thermalSolarPumpFixture()
			ctx.FM5 = availability
			_, next, _ := thermalSolarPumpFixture()
			*next.Status, next.ReceiptMonotonic.Nanoseconds = 1, "95"
			next.Reply[0] = 1
			got := mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, next, nil)
			if !reflect.DeepEqual(got.Status.Retained, status) || !reflect.DeepEqual(got.Hours.Retained, hours) || got.Availability != availability {
				t.Fatal("FM5 lost coherent fields")
			}
			if got.Status.Usable != (availability == semreg.AvailabilityDegraded) || got.Hours.Usable != (availability == semreg.AvailabilityDegraded) {
				t.Fatal("structural absence became false/zero")
			}
		})
	}
}

func TestThermalSolarPumpMappingReceiptOrdering(t *testing.T) {
	for _, tick := range []semreg.Uint64{"89", "90"} {
		ctx, status, hours := thermalSolarPumpFixture()
		_, next, _ := thermalSolarPumpFixture()
		*next.Status, next.ReceiptMonotonic.Nanoseconds = 1, tick
		next.Reply[0] = 1
		got := mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, next, nil)
		if *got.Status.Retained.Status != 0 || got.Status.Retained.ReceiptMonotonic.Nanoseconds != "90" || got.Status.Problem != "nonadvancing_or_incomparable_receipt" {
			t.Fatal("arrival order replaced receipt order")
		}
	}
}

func TestThermalSolarPumpMappingIndependentFields(t *testing.T) {
	ctx, status, hours := thermalSolarPumpFixture()
	_, invalidStatus, newHours := thermalSolarPumpFixture()
	*invalidStatus.Status = 2
	*newHours.Hours = 43
	invalidStatus.Reply[0] = 2
	binary.LittleEndian.PutUint32(newHours.Reply, 43)
	invalidStatus.ReceiptMonotonic.Nanoseconds, newHours.ReceiptMonotonic.Nanoseconds = "95", "95"
	got := mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, invalidStatus, newHours)
	if *got.Status.Retained.Status != 0 || got.Status.Retained.ReceiptMonotonic.Nanoseconds != "90" ||
		*got.Hours.Retained.Hours != 43 || got.Hours.Retained.ReceiptMonotonic.Nanoseconds != "95" {
		t.Fatal("invalid status renewed itself or blocked independent hours")
	}
	// The reverse update must not renew hours, even though its policy is slower.
	ctx.Evaluation.EvaluateMonotonic.Nanoseconds = "145"
	_, newStatus, _ := thermalSolarPumpFixture()
	newStatus.ReceiptMonotonic.Nanoseconds = "140"
	got = mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, newStatus, nil)
	if !got.Status.Usable || got.Hours.Usable || got.Hours.Freshness != semreg.FreshnessStale ||
		got.Hours.Retained.ReceiptMonotonic.Nanoseconds != "90" {
		t.Fatal("status renewed sibling hours")
	}
	for _, boundary := range []struct {
		tick semreg.Uint64
		want semreg.Freshness
	}{
		{"109", semreg.FreshnessFresh}, {"110", semreg.FreshnessStale},
		{"169", semreg.FreshnessStale}, {"170", semreg.FreshnessExpired},
	} {
		ctx.Evaluation.EvaluateMonotonic.Nanoseconds = boundary.tick
		got = mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, nil, nil)
		if got.Status.Freshness != boundary.want {
			t.Fatalf("policy boundary %s: %s", boundary.tick, got.Status.Freshness)
		}
	}
}

func TestThermalSolarPumpMappingDetached(t *testing.T) {
	ctx, status, hours := thermalSolarPumpFixture()
	got := mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
	baseline := mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
	*status.Status, *hours.Hours, status.Request[0], hours.Reply[0] = 1, 9, 9, 9
	status.Evidence[0].Digest = "changed"
	if !reflect.DeepEqual(got, baseline) {
		t.Fatal("input aliases output")
	}
	*got.Status.Retained.Status, *got.Hours.Retained.Hours = 1, 5
	got.Status.Retained.Request[0], got.Status.Retained.Reply[0] = 7, 7
	got.Status.Retained.Evidence[0].Digest = "output-changed"
	if *baseline.Status.Retained.Status != 0 || *baseline.Hours.Retained.Hours != 42 || baseline.Status.Retained.Request[0] != 2 || baseline.Status.Retained.Reply[0] != 0 || baseline.Status.Retained.Evidence[0].Digest == "output-changed" {
		t.Fatal("outputs alias other results")
	}
	if *got.Attempted[0].Status != 0 || got.Attempted[0].Request[0] != 2 || got.Attempted[0].Reply[0] != 0 || got.Attempted[0].Evidence[0].Digest == "output-changed" {
		t.Fatal("retained aliases attempted")
	}
	ctx, status, hours = thermalSolarPumpFixture()
	_, wantStatus, wantHours := thermalSolarPumpFixture()
	got = mapThermalSolarPump(ctx, thermalSolarPumpState{}, status, hours)
	*got.Status.Retained.Status, *got.Hours.Retained.Hours = 1, 7
	got.Status.Retained.Request[0], got.Hours.Retained.Reply[0] = 8, 8
	got.Status.Retained.Evidence[0].Digest = "mutated-output"
	if !reflect.DeepEqual(status, wantStatus) || !reflect.DeepEqual(hours, wantHours) {
		t.Fatal("output mutation changed caller input")
	}
	ctx, status, hours = thermalSolarPumpFixture()
	_, next, _ := thermalSolarPumpFixture()
	next.ReceiptMonotonic.Nanoseconds = "95"
	got = mapThermalSolarPump(ctx, thermalSolarPumpState{status, hours}, next, nil)
	*got.History[0].Status = 1
	got.History[0].Reply[0] = 8
	if *status.Status != 0 || status.Reply[0] != 0 || *got.Status.Retained.Status != 0 {
		t.Fatal("history aliases input/current")
	}
	if !reflect.DeepEqual(mapThermalSolarPump(ctx, thermalSolarPumpState{}, next, hours), mapThermalSolarPump(ctx, thermalSolarPumpState{}, next, hours)) {
		t.Fatal("nondeterministic mapping")
	}
}
