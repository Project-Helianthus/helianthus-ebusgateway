package mcp

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func msp06ReviewProjection(t *testing.T) eebusV1Projection {
	t.Helper()
	projection, err := eebusV1ProjectSnapshot(msp06Snapshot(t, "runtime-a"), bytes.Repeat([]byte{0x6b}, sha256.Size))
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func TestMSP06StoreSamplesClockUnderMutex(t *testing.T) {
	base := time.Now()
	store := newEEBusV1SnapshotStore(func() time.Time { return base }, &msp06EntropyReader{})
	captured, code := store.capture(msp06ReviewProjection(t))
	if code != "" {
		t.Fatal(code)
	}
	store.now = func() time.Time {
		if store.mu.TryLock() {
			store.mu.Unlock()
			t.Error("clock sampled outside store mutex")
		}
		return base.Add(time.Minute)
	}
	_ = store.lookup(captured.EvidenceRefs.ServicesListRef, eebusV1ServicesListTool, "services")
}

func TestMSP06ProviderCollectionBoundsFailClosed(t *testing.T) {
	snapshot := msp06Snapshot(t, "runtime-a")
	snapshot.Features = make([]eebusruntime.FeatureV1, eebusV1MaxCollectionSize+1)
	if err := eebusV1ValidateProviderCollectionBounds(snapshot); err == nil {
		t.Fatal("oversized provider collection passed")
	}
}
