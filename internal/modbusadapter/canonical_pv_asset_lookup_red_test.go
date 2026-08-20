package modbusadapter

import (
	"testing"
	"time"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

func TestCanonicalPVSnapshotByAsset_IsReadOnlyAndReturnsDetachedRetainedSnapshot(t *testing.T) {
	one := m2mAssetSnapshot("pv-asset-one", "11")
	two := m2mAssetSnapshot("pv-asset-two", "22")
	adapter := &Adapter{qualifications: map[string]sunSpecQualificationRecord{
		"first":  {canonical: one, producedAt: time.Unix(100, 0).UTC()},
		"second": {canonical: two, producedAt: time.Unix(200, 0).UTC()},
	}}
	if _, _, ok := adapter.CanonicalPVSnapshotByAsset("pv-asset-unknown"); ok {
		t.Fatal("unknown asset returned a canonical snapshot")
	}
	got, producedAt, ok := adapter.CanonicalPVSnapshotByAsset("pv-asset-two")
	if !ok || got.AssetRef != "pv-asset-two" || got.Generation != 22 || !producedAt.Equal(time.Unix(200, 0).UTC()) {
		t.Fatalf("asset lookup = %#v, %s, %v", got, producedAt, ok)
	}
	key := pv.NewFactKey(pv.FactACActivePower, pv.Dimensions{Scope: pv.ScopeTotal})
	mutated := got.Facts[key]
	mutated.Value.Decimal.Coefficient = "999"
	got.Facts[key] = mutated
	again, _, ok := adapter.CanonicalPVSnapshotByAsset("pv-asset-two")
	if !ok || again.Facts[pv.NewFactKey(pv.FactACActivePower, pv.Dimensions{Scope: pv.ScopeTotal})].Value.Decimal.Coefficient != "22" {
		t.Fatal("asset lookup returned an attached retained snapshot")
	}
}

func m2mAssetSnapshot(asset string, coefficient string) pv.Snapshot {
	d := pv.MustDecimal(coefficient, 0)
	dimensions := pv.Dimensions{Scope: pv.ScopeTotal}
	key := pv.NewFactKey(pv.FactACActivePower, dimensions)
	return pv.Snapshot{AssetRef: asset, Generation: uint64(len(coefficient)) * 11, Facts: map[pv.FactKey]pv.Fact{key: {ID: pv.FactACActivePower, Dimensions: dimensions, Value: pv.DecimalFactValue(d)}}}
}
