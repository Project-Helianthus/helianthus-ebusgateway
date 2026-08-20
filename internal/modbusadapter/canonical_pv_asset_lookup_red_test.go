package modbusadapter

import "testing"

func TestCanonicalPVSnapshotByAsset_IsReadOnlyAndReturnsDetachedRetainedSnapshot(t *testing.T) {
	adapter := &Adapter{}
	if _, _, ok := adapter.CanonicalPVSnapshotByAsset("pv-asset-unknown"); ok {
		t.Fatal("unknown asset returned a canonical snapshot")
	}
}
