package matter

import (
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestManifestAndLedgerCoverPinnedMetadataExactlyOnce(t *testing.T) {
	manifest := Manifest()
	if manifest.TargetID != TargetID || manifest.TargetVersion != TargetVersion || manifest.MappingRevision != MappingRevision || len(manifest.PackVersions) != 5 {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	requested, dispositions := Ledger()
	if len(requested) == 0 || len(requested) != len(dispositions) {
		t.Fatalf("ledger counts requested=%d dispositions=%d", len(requested), len(dispositions))
	}
	seen := map[projection.RequestedItem]struct{}{}
	for i, item := range requested {
		if _, duplicate := seen[item]; duplicate {
			t.Fatalf("duplicate requested item: %#v", item)
		}
		seen[item] = struct{}{}
		if dispositions[i].Kind != item.Kind || dispositions[i].ItemID != item.ItemID || dispositions[i].Outcome != projection.ProjectionUnknown || dispositions[i].Reason == nil || len(dispositions[i].SourceKeys) != 0 {
			t.Fatalf("non-positive disposition %d: %#v", i, dispositions[i])
		}
	}
	if _, ok := seen[projection.RequestedItem{Kind: projection.ItemFact, ItemID: "evse.ac.current"}]; !ok {
		t.Fatal("EVSE current absent")
	}
	if _, ok := seen[projection.RequestedItem{Kind: projection.ItemOperation, ItemID: "evse.operation.set_allocated_current"}]; !ok {
		t.Fatal("EVSE operation absent")
	}
}

func TestLedgerIsDeterministicAndDoesNotExposeOperations(t *testing.T) {
	firstRequested, firstDispositions := Ledger()
	secondRequested, secondDispositions := Ledger()
	if !reflect.DeepEqual(firstRequested, secondRequested) || !reflect.DeepEqual(firstDispositions, secondDispositions) {
		t.Fatal("ledger is nondeterministic")
	}
	for _, disposition := range firstDispositions {
		if disposition.Kind == projection.ItemOperation && (len(disposition.SourceKeys) != 0 || disposition.Outcome != projection.ProjectionUnknown) {
			t.Fatalf("operation became actionable: %#v", disposition)
		}
	}
}

func TestEVSEActiveCurrentUsesPinnedNumericTargetAndExactConversion(t *testing.T) {
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "32", Exponent10: -1}, Unit: "unit.ampere"}}
	got, err := amperesToMilliampere(value)
	if err != nil || got != "3200" {
		t.Fatalf("A to mA = %q, %v", got, err)
	}
	if evseActiveCurrent != (TargetElement{DeviceType: 0x0510, Cluster: 0x0090, Kind: ElementAttribute, ElementID: 0x0005}) {
		t.Fatalf("unexpected target: %#v", evseActiveCurrent)
	}
}

func TestEVSECurrentFailsClosedOnWrongUnitOrPrecision(t *testing.T) {
	for _, value := range []semreg.Value{
		{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "1", Exponent10: 0}, Unit: "unit.volt"}},
		{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "1", Exponent10: -4}, Unit: "unit.ampere"}},
	} {
		if _, err := amperesToMilliampere(value); err == nil {
			t.Fatalf("accepted malformed or lossy value: %#v", value)
		}
	}
}

func TestEvaluationEligibilityFailsClosedForStaleAndUnavailable(t *testing.T) {
	for _, item := range []semreg.EvaluatedFact{
		{Freshness: semreg.FreshnessStale, EffectiveAvailability: semreg.AvailabilityAvailable},
		{Freshness: semreg.FreshnessFresh, EffectiveAvailability: semreg.AvailabilityUnavailable},
	} {
		if err := eligibleEvaluation(semreg.EvaluationView{Facts: []semreg.EvaluatedFact{item}}); err == nil {
			t.Fatalf("accepted ineligible evaluation: %#v", item)
		}
	}
}

func TestCanonicalJSONIsDeterministicForEquivalentResult(t *testing.T) {
	result := Result{Document: Document{Contract: Contract, TargetID: TargetID, TargetVersion: TargetVersion, MappingRevision: MappingRevision, Attributes: []TargetAttribute{}}}
	// A report is deliberately required. This is a fail-closed output boundary.
	if _, err := CanonicalJSON(result); err == nil {
		t.Fatal("accepted result without a validated report")
	}
}
