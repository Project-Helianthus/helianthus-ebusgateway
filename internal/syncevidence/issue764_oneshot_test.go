package syncevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type issue764CountingReader struct {
	reader io.Reader
	bytes  int
}

func (reader *issue764CountingReader) Read(target []byte) (int, error) {
	count, err := reader.reader.Read(target)
	reader.bytes += count
	return count, err
}

func issue764OneShotOptions(
	t *testing.T,
	root string,
	reader EEBusM625Reader,
	entropy io.Reader,
	clockCalls *int,
	versionCalls *int,
) OneShotExecutionOptions {
	t.Helper()
	return OneShotExecutionOptions{
		Root:    root,
		Reader:  reader,
		Entropy: entropy,
		ClockFactory: func() Clock {
			*clockCalls++
			return &redClock{wall: time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)}
		},
		BuildIdentity: func() (OneShotBuildIdentity, error) {
			*versionCalls++
			return OneShotBuildIdentity{
				RecorderVersion:  testBuildVersion,
				ReplayVersion:    testBuildVersion,
				OperationVersion: issue764OperationVersion,
			}, nil
		},
	}
}

func TestIssue764OneShotPublishesThenRepeatsAndRestartsWithoutAcquisition(t *testing.T) {
	root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
	requestBytes := issue764RequestBytes(t, 'a')
	issue764WriteRequest(t, root, requestBytes, 0o600)
	readerCalls := 0
	reader := issue764M625ReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		readerCalls++
		return AcquiredEvidence{
			SourceObservedAt:   time.Date(2026, 7, 30, 12, 0, 2, 0, time.UTC),
			NormalizedEvidence: issue764M625Payload(),
		}, nil
	})
	entropy := &issue764CountingReader{
		reader: bytes.NewReader(bytes.Repeat([]byte{0x61}, 1<<20)),
	}
	clockCalls := 0
	versionCalls := 0
	options := issue764OneShotOptions(t, root, reader, entropy, &clockCalls, &versionCalls)
	replayCalls := 0
	options.replay = func(bundle []byte) ([]byte, error) {
		replayCalls++
		return Replay(bundle)
	}

	receipt := ExecuteOneShot(context.Background(), options)
	if receipt != (OneShotReceiptV1{Category: OneShotPublished}) {
		t.Fatalf("first receipt = %#v", receipt)
	}
	if readerCalls != 1 || clockCalls != 1 || versionCalls != 1 || replayCalls != 2 || entropy.bytes == 0 {
		t.Fatalf(
			"first calls reader=%d clock=%d version=%d replay=%d entropy=%d",
			readerCalls, clockCalls, versionCalls, replayCalls, entropy.bytes,
		)
	}
	firstEntropy := entropy.bytes
	storeRoot := filepath.Join(root, "store")
	entries, err := os.ReadDir(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	var bundlePath string
	for _, entry := range entries {
		switch {
		case entry.Name() == storeLockName:
		case strings.HasSuffix(entry.Name(), ".json"):
			if bundlePath != "" {
				t.Fatalf("multiple published bundles: %q and %q", bundlePath, entry.Name())
			}
			bundlePath = filepath.Join(storeRoot, entry.Name())
		default:
			t.Fatalf("unexpected durable entry %q", entry.Name())
		}
	}
	raw, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := verifyBundle(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateOneShotBundle(bundle, options.sourceTuple()); err != nil {
		t.Fatalf("published bundle: %v", err)
	}
	if len(bundle.Sources) != 5 {
		t.Fatalf("source count = %d", len(bundle.Sources))
	}
	exactIdentities := map[SourceKind]*EBusSourceIdentityV1{
		SourceEBusB509: {
			Family: EBusFamilyB509, TargetAddress: 0x08, TargetProduct: "BAI00",
			RegisterFamily: "system", RegisterID: 512, UnitScaleSource: "gateway-catalog-v1",
			EvidenceRole: "AUTHORITATIVE",
		},
		SourceEBusB524: {
			Family: EBusFamilyB524, TargetAddress: 0x15, SourceAddress: 0xf7,
			Opcode: 2, GG: 3, II: 0, RR: 28, GroupMeaning: "zones",
			InstanceGate: "index-not-ff", RegisterCategory: "STATE",
			UnitScaleSource: "vrc-explorer-v1",
		},
		SourceEBusB555: {
			Family: EBusFamilyB555, DeviceFamily: "VRC",
			ScheduleProgram: "heating-program-1", SlotIndex: 0, DayOfWeek: "MONDAY",
			TimeIdentity: "06:00:00", OperationModeContext: "AUTO",
			UnitScaleSource: "source-native",
		},
	}
	for _, source := range bundle.Sources {
		want := exactIdentities[source.SourceBinding.SourceKind]
		if want == nil {
			continue
		}
		got := cloneIdentity(source.EBusIdentity)
		if got == nil || got.TargetPseudonym == "" {
			t.Fatalf("exact identity missing for %s", source.SourceBinding.SourceKind)
		}
		got.TargetPseudonym = ""
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("identity for %s = %#v, want %#v", source.SourceBinding.SourceKind, got, want)
		}
	}
	lockPath := filepath.Join(storeRoot, storeLockName)
	lockBefore, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lockInfoBefore, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}

	receipt = ExecuteOneShot(context.Background(), options)
	if receipt != (OneShotReceiptV1{Category: OneShotExisting}) {
		t.Fatalf("repeat receipt = %#v", receipt)
	}
	if readerCalls != 1 || clockCalls != 1 || versionCalls != 1 || replayCalls != 2 || entropy.bytes != firstEntropy {
		t.Fatalf(
			"repeat performed work reader=%d clock=%d version=%d replay=%d entropy=%d/%d",
			readerCalls, clockCalls, versionCalls, replayCalls, entropy.bytes, firstEntropy,
		)
	}
	lockAfter, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lockInfoAfter, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(lockAfter, lockBefore) || !lockInfoAfter.ModTime().Equal(lockInfoBefore.ModTime()) {
		t.Fatalf("repeat rewrote store lock proof")
	}

	mutatedRequest := issue764MutateRequestCloudValue(t, requestBytes)
	issue764WriteRequest(t, root, mutatedRequest, 0o600)
	receipt = ExecuteOneShot(context.Background(), options)
	if receipt != (OneShotReceiptV1{Category: OneShotInvalidRequest}) {
		t.Fatalf("mutated retained-ref receipt = %#v", receipt)
	}
	if readerCalls != 1 || clockCalls != 1 || versionCalls != 1 || replayCalls != 2 || entropy.bytes != firstEntropy {
		t.Fatalf(
			"mutated retained-ref request performed work reader=%d clock=%d version=%d replay=%d entropy=%d/%d",
			readerCalls, clockCalls, versionCalls, replayCalls, entropy.bytes, firstEntropy,
		)
	}
	issue764WriteRequest(t, root, requestBytes, 0o600)

	restartOptions := options
	restartOptions.Reader = issue764M625ReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		t.Fatal("restart acquired a source")
		return AcquiredEvidence{}, errors.New("unreachable")
	})
	restartOptions.Entropy = issue764RejectEntropy{}
	restartOptions.ClockFactory = func() Clock {
		t.Fatal("restart created a clock")
		return nil
	}
	restartOptions.BuildIdentity = func() (OneShotBuildIdentity, error) {
		t.Fatal("restart resolved build identity")
		return OneShotBuildIdentity{}, errors.New("unreachable")
	}
	restartOptions.replay = func([]byte) ([]byte, error) {
		t.Fatal("restart ran a new-capture replay")
		return nil, errors.New("unreachable")
	}
	receipt = ExecuteOneShot(context.Background(), restartOptions)
	if receipt != (OneShotReceiptV1{Category: OneShotExisting}) {
		t.Fatalf("restart receipt = %#v", receipt)
	}
	entries, err = os.ReadDir(storeRoot)
	if err != nil || len(entries) != 2 {
		t.Fatalf("restart entries = %v, %v", entries, err)
	}
}

func TestIssue764OneShotReplayMismatchPublishesNothing(t *testing.T) {
	root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
	issue764WriteRequest(t, root, issue764RequestBytes(t, 'b'), 0o600)
	reader := issue764M625ReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{
			SourceObservedAt:   time.Date(2026, 7, 30, 12, 0, 2, 0, time.UTC),
			NormalizedEvidence: issue764M625Payload(),
		}, nil
	})
	clockCalls, versionCalls := 0, 0
	options := issue764OneShotOptions(
		t,
		root,
		reader,
		bytes.NewReader(bytes.Repeat([]byte{0x62}, 1<<20)),
		&clockCalls,
		&versionCalls,
	)
	replayCalls := 0
	options.replay = func([]byte) ([]byte, error) {
		replayCalls++
		return []byte{byte(replayCalls)}, nil
	}
	receipt := ExecuteOneShot(context.Background(), options)
	if receipt != (OneShotReceiptV1{Category: OneShotReplayMismatch}) || replayCalls != 2 {
		t.Fatalf("receipt/calls = %#v/%d", receipt, replayCalls)
	}
	entries, err := os.ReadDir(filepath.Join(root, "store"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != storeLockName {
			t.Fatalf("replay mismatch left %q", entry.Name())
		}
	}
}

func TestIssue764OneShotInvalidRequestIsCategoryOnlyAndDoesNotOpenStore(t *testing.T) {
	root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
	issue764WriteRequest(t, root, issue764RequestBytes(t, 'c'), 0o640)
	receipt := ExecuteOneShot(context.Background(), OneShotExecutionOptions{Root: root})
	if receipt != (OneShotReceiptV1{Category: OneShotInvalidRequest}) {
		t.Fatalf("receipt = %#v", receipt)
	}
	raw, err := json.Marshal(receipt)
	if err != nil || string(raw) != `{"category":"INVALID_REQUEST"}` {
		t.Fatalf("receipt JSON = %q, %v", raw, err)
	}
	if _, err := os.Stat(filepath.Join(root, "store")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid request opened store: %v", err)
	}
}

func TestIssue764OneShotRejectsStoreSymlinkBeforeAnyNewCaptureWork(t *testing.T) {
	base := issue764SecureTempDir(t)
	root := filepath.Join(base, "synchronized-evidence")
	issue764WriteRequest(t, root, issue764RequestBytes(t, 'd'), 0o600)
	target := filepath.Join(base, "store-target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "store")); err != nil {
		t.Fatal(err)
	}
	receipt := ExecuteOneShot(context.Background(), OneShotExecutionOptions{
		Root:    root,
		Entropy: issue764RejectEntropy{},
		Reader: issue764M625ReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
			t.Fatal("unsafe store triggered acquisition")
			return AcquiredEvidence{}, nil
		}),
		ClockFactory: func() Clock {
			t.Fatal("unsafe store created a clock")
			return nil
		},
		BuildIdentity: func() (OneShotBuildIdentity, error) {
			t.Fatal("unsafe store resolved build identity")
			return OneShotBuildIdentity{}, nil
		},
	})
	if receipt != (OneShotReceiptV1{Category: OneShotPublishFailed}) {
		t.Fatalf("receipt = %#v", receipt)
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		t.Fatalf("symlink target entries = %v, %v", entries, err)
	}
}
