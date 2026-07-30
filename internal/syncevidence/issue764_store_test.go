package syncevidence

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type issue764RejectEntropy struct{}

func (issue764RejectEntropy) Read([]byte) (int, error) {
	return 0, errors.New("unexpected entropy read")
}

func issue764FiveSourceBundle(t *testing.T, actionRef EvidenceRefV1, entropy byte) []byte {
	t.Helper()
	return issue764FiveSourceBundleWithCloudRef(t, actionRef, actionRef, entropy)
}

func issue764FiveSourceBundleWithCloudRef(
	t *testing.T,
	actionRef EvidenceRefV1,
	cloudRef EvidenceRefV1,
	entropy byte,
) []byte {
	t.Helper()
	observed := time.Date(2026, 7, 30, 12, 0, 2, 0, time.UTC)
	sources := []RegisteredSource{
		issue764TerminalEBusSource(SourceEBusB509, PhasePre, "ebus-b509", "dbe91a10a208613183f890849c634f8d13661194aad937a03ae2a4143070bf2d"),
		{
			Phase: PhasePre, SourceKind: SourceEEBus, SourceContract: M625EEBusContractV1, SourceVersion: 1,
			RuntimeInstance: "eebus-runtime", OperationVersion: issue764OperationVersion, OperationScope: "feature-data",
			Admission: allowedAdmission("eebus.raw.read"),
			EvidenceRefs: []EvidenceRefV1{
				contentEvidenceRefForTest("0a2885d01d6703389541e246db59bcd845a332e7ed296abca2d49b4f8de31811"),
			},
			EEBusM625Reader: issue764M625ReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
				return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: issue764M625Payload()}, nil
			}),
		},
		issue764TerminalEBusSource(SourceEBusB524, PhaseAction, "ebus-b524", "1002e09890801c9032548af407b13b58d889217dfc83e58bfe2a28df6bc33b78"),
		cloudRegistration(PrecapturedCloudInput{
			SourceObservedAt: observed,
			NormalizedEvidence: cloudPayload(
				observed,
				"JJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJ",
				"21.5",
			),
			EvidenceRef: cloudRef,
		}, PhaseAction, "cloud-runtime"),
		issue764TerminalEBusSource(SourceEBusB555, PhasePost, "ebus-b555", "a3237e344f5c3582ceaf1ca947eabf200bede8fe9388ec85cf647331f026c72d"),
	}
	recorder := testRecorder(t, sources, func(options *RecorderOptions) {
		options.Entropy = bytes.NewReader(bytes.Repeat([]byte{entropy}, 1<<20))
	})
	bundle, err := recorder.Capture(context.Background(), ActionMarker{EvidenceRef: actionRef})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	return bundle
}

func issue764RetainedCloudContentRef(t *testing.T, raw []byte) EvidenceRefV1 {
	t.Helper()
	bundle, err := verifyBundle(raw)
	if err != nil {
		t.Fatalf("verify retained bundle: %v", err)
	}
	for _, artifact := range bundle.Artifacts {
		if artifact.SourceBinding.SourceKind == SourceCloudApp {
			return EvidenceRefV1{
				Kind:            EvidenceKindContent,
				DigestAlgorithm: DigestAlgorithmContentBytes,
				Digest:          HashContentBytes(artifact.NormalizedEvidence),
			}
		}
	}
	t.Fatal("retained bundle has no cloud artifact")
	return EvidenceRefV1{}
}

func TestIssue764RetainedLookupSurvivesRestartWithoutEntropyOrStaging(t *testing.T) {
	root := filepath.Join(storeTestRoot(t), "store")
	actionRef := redEvidenceRef('a')
	bundle := issue764FiveSourceBundle(t, actionRef, 0x61)
	replay, err := Replay(bundle)
	if err != nil {
		t.Fatal(err)
	}
	lockProof := bytes.Repeat([]byte{0x4c}, 32)
	store, err := OpenFileStore(FileStoreConfig{
		Root: root, QuotaBytes: 1 << 27, LockProof: lockProof,
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveCapture(int64(DefaultLimitsV1().MaxBundleBytes))
	if err != nil {
		t.Fatal(err)
	}
	id, err := reservation.PublishVerified(bundle, replay)
	if err != nil {
		t.Fatalf("PublishVerified: %v", err)
	}
	if id != bundleIDFromBytes(t, bundle) {
		t.Fatalf("published id = %q", id)
	}
	retained, err := store.lookupOneShot(actionRef, SourceTupleV1{
		SourceKind: SourceEEBus, Contract: M625EEBusContractV1, Version: 1,
	})
	if err != nil || retained.Status != OneShotLookupExisting ||
		!bytes.Equal(retained.Bundle, bundle) || !bytes.Equal(retained.Replay, replay) {
		t.Fatalf("first lookup = %#v, %v", retained, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := OpenFileStore(FileStoreConfig{
		Root: root, QuotaBytes: 1 << 27, Entropy: issue764RejectEntropy{}, LockProof: lockProof,
	})
	if err != nil {
		t.Fatalf("restart requested entropy or failed: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	retained, err = restarted.lookupOneShot(actionRef, SourceTupleV1{
		SourceKind: SourceEEBus, Contract: M625EEBusContractV1, Version: 1,
	})
	if err != nil || retained.Status != OneShotLookupExisting ||
		!bytes.Equal(retained.Bundle, bundle) || !bytes.Equal(retained.Replay, replay) {
		t.Fatalf("restart lookup = %#v, %v", retained, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != storeLockName && entry.Name() != id+".json" {
			t.Fatalf("repeat created unexpected store entry %q", entry.Name())
		}
	}
}

func TestIssue764RetainedLookupConflictsOnMultipleCandidates(t *testing.T) {
	root := filepath.Join(storeTestRoot(t), "store")
	actionRef := redEvidenceRef('b')
	store, err := OpenFileStore(FileStoreConfig{
		Root: root, QuotaBytes: 1 << 27, LockProof: bytes.Repeat([]byte{0x4c}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, entropy := range []byte{0x61, 0x62} {
		bundle := issue764FiveSourceBundle(t, actionRef, entropy)
		if _, err := store.Publish(bundle); err != nil {
			t.Fatal(err)
		}
	}
	retained, err := store.lookupOneShot(actionRef, SourceTupleV1{
		SourceKind: SourceEEBus, Contract: M625EEBusContractV1, Version: 1,
	})
	if err != nil || retained.Status != OneShotLookupConflict ||
		retained.Bundle != nil || retained.Replay != nil {
		t.Fatalf("conflicting lookup = %#v, %v", retained, err)
	}
}

func TestIssue764RetainedLookupRejectsLegacyCloudContentMismatch(t *testing.T) {
	root := filepath.Join(storeTestRoot(t), "store")
	request, err := parseOneShotRequest(issue764RequestBytes(t, 'a'))
	if err != nil {
		t.Fatal(err)
	}
	actionRef := request.ActionEvidenceRef
	const entropy = byte(0x61)
	provisional := issue764FiveSourceBundle(t, redEvidenceRef('f'), entropy)
	cloudRef := issue764RetainedCloudContentRef(t, provisional)
	if evidenceRefKey(cloudRef) == evidenceRefKey(actionRef) {
		t.Fatal("legacy mismatch fixture refs unexpectedly match")
	}
	legacy := issue764FiveSourceBundleWithCloudRef(t, actionRef, cloudRef, entropy)
	if got := issue764RetainedCloudContentRef(t, legacy); evidenceRefKey(got) != evidenceRefKey(cloudRef) {
		t.Fatalf("legacy cloud digest = %#v, want %#v", got, cloudRef)
	}
	if _, err := Replay(legacy); err != nil {
		t.Fatalf("legacy bundle no longer satisfies replay contract: %v", err)
	}
	lockProof := bytes.Repeat([]byte{0x4c}, 32)
	store, err := OpenFileStore(FileStoreConfig{
		Root: root, QuotaBytes: 1 << 27, LockProof: lockProof,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Publish(legacy); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := OpenFileStore(FileStoreConfig{
		Root: root, QuotaBytes: 1 << 27, Entropy: issue764RejectEntropy{}, LockProof: lockProof,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	retained, err := restarted.lookupOneShot(actionRef, SourceTupleV1{
		SourceKind: SourceEEBus, Contract: M625EEBusContractV1, Version: 1,
	})
	if err != nil || retained.Status != OneShotLookupNone ||
		retained.Bundle != nil || retained.Replay != nil {
		t.Fatalf("legacy mismatch lookup = %#v, %v", retained, err)
	}
}

func TestIssue764VerifiedPublishReopensExactFinalBytes(t *testing.T) {
	root := filepath.Join(storeTestRoot(t), "store")
	store, err := OpenFileStore(FileStoreConfig{
		Root: root, QuotaBytes: 1 << 27, LockProof: bytes.Repeat([]byte{0x4c}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	bundle := issue764FiveSourceBundle(t, redEvidenceRef('c'), 0x61)
	replay, err := Replay(bundle)
	if err != nil {
		t.Fatal(err)
	}
	store.beforePublishedReopen = func(name string) {
		path := filepath.Join(root, name)
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		raw[len(raw)-1] ^= 1
		if writeErr := os.WriteFile(path, raw, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	reservation, err := store.ReserveCapture(int64(DefaultLimitsV1().MaxBundleBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.PublishVerified(bundle, replay); !errors.Is(err, ErrDurability) {
		t.Fatalf("replaced final publish error = %v", err)
	}
}

var _ io.Reader = issue764RejectEntropy{}
