package syncevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type redClock struct {
	wall   time.Time
	offset uint64
}

func (clock *redClock) Observe() ClockObservation {
	result := ClockObservation{Wall: clock.wall, OffsetNS: clock.offset, UncertaintyNS: 10}
	clock.wall = clock.wall.Add(time.Second)
	clock.offset += uint64(time.Second)
	return result
}

type redSource struct {
	calls  int
	result SourceCapture
}

func (source *redSource) Capture(context.Context, SourceRequest) SourceCapture {
	source.calls++
	return source.result
}

func redEvidenceRef(digit byte) EvidenceRefV1 {
	return EvidenceRefV1{
		Kind:            EvidenceKindContent,
		DigestAlgorithm: DigestAlgorithmContentBytes,
		Digest:          "sha256:" + string(bytes.Repeat([]byte{digit}, 64)),
	}
}

func TestMSP065CaptureAndReplayAreDeterministicAndOffline(t *testing.T) {
	started := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	clock := &redClock{wall: started}
	eebus := &redSource{result: SourceCapture{
		RuntimeKind:         RuntimeEEBus,
		SourceKind:          SourceEEBus,
		SourceContract:      "helianthus-eebus-mcp",
		SourceSchemaVersion: 1,
		OperationID:         "eebus.v1.services.list",
		OperationVersion:    "git:520b6439441cb6e8ef9ff291bde28f4efa4db254",
		OperationScope:      "services",
		SnapshotMode:        SnapshotLiveRead,
		AuthPermissions:     []string{"eebus.raw.read"},
		State:               StatePresent,
		SourceObservedAt:    started.Add(time.Second),
		EvidenceRefs:        []EvidenceRefV1{redEvidenceRef('2')},
		NormalizedEvidence:  json.RawMessage(`{"meta":{"contract":{"name":"helianthus-eebus-mcp","major":1,"minor":0},"tool":"eebus.v1.services.list","scope":"services","mask_tier":"redacted","auth_scope":"eebus.raw.read","mode":"evidence","data_timestamp":"2026-07-19T12:00:01Z","data_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","runtime":{"state":"ready"}},"data":{"services":[]},"error":null}`),
	}}
	ebus := &redSource{result: SourceCapture{
		RuntimeKind:         RuntimeEBus,
		SourceKind:          SourceEBusB524,
		SourceContract:      "helianthus.ebus.b524.evidence.v1",
		SourceSchemaVersion: 1,
		OperationID:         "ebus.v1.snapshot.capture",
		OperationVersion:    "git:520b6439441cb6e8ef9ff291bde28f4efa4db254",
		OperationScope:      "ebus-b524",
		SnapshotMode:        SnapshotFrozen,
		AuthPermissions:     []string{"ebus.read"},
		State:               StateNotTested,
		ErrorCategory:       ErrorExactIdentityMissing,
		EvidenceRefs:        []EvidenceRefV1{redEvidenceRef('3')},
	}}

	recorder, err := NewRecorder(RecorderOptions{
		Clock:   clock,
		Entropy: bytes.NewReader(bytes.Repeat([]byte{0x44}, 4096)),
		Limits:  DefaultLimitsV1(),
		Sources: []RegisteredSource{
			{Phase: PhasePre, Source: eebus},
			{Phase: PhaseAction, Source: ebus},
		},
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	bundle, err := recorder.Capture(context.Background(), ActionMarker{
		ID:          "marker-99999999999999999999999999999999",
		EvidenceRef: redEvidenceRef('1'),
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if eebus.calls != 1 || ebus.calls != 1 {
		t.Fatalf("capture calls eeBUS/eBUS = %d/%d, want 1/1", eebus.calls, ebus.calls)
	}

	first, err := Replay(bundle)
	if err != nil {
		t.Fatalf("Replay #1: %v", err)
	}
	second, err := Replay(bundle)
	if err != nil {
		t.Fatalf("Replay #2: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("replay differs\nfirst:  %s\nsecond: %s", first, second)
	}
	if eebus.calls != 1 || ebus.calls != 1 {
		t.Fatalf("offline replay called sources: eeBUS/eBUS = %d/%d", eebus.calls, ebus.calls)
	}

	var decoded map[string]any
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatalf("decode replay: %v", err)
	}
	if decoded["contract"] != ReplayContractV1 {
		t.Fatalf("replay contract = %v", decoded["contract"])
	}
}

func TestMSP065CaptureRejectsReflectedErrorsAndNonCanonicalSourceData(t *testing.T) {
	secret := "token=do-not-reflect"
	source := &redSource{result: SourceCapture{
		RuntimeKind:         RuntimeCloudApp,
		SourceKind:          SourceCloudApp,
		SourceContract:      "helianthus.cloud-app.precaptured.evidence.v1",
		SourceSchemaVersion: 1,
		OperationID:         "cloud.precaptured.import",
		OperationVersion:    "git:11d2f2c18218495577b630b5970b7fe2b2fd72e8",
		OperationScope:      "cloud-app",
		SnapshotMode:        SnapshotPrecaptured,
		AuthPermissions:     []string{"cloud.read"},
		State:               StateUnavailable,
		ErrorCategory:       ErrorAcquisitionFailed,
		NativeError:         errors.New(secret),
		EvidenceRefs:        []EvidenceRefV1{redEvidenceRef('4')},
	}}
	recorder, err := NewRecorder(RecorderOptions{
		Clock:   &redClock{wall: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)},
		Entropy: bytes.NewReader(bytes.Repeat([]byte{0x55}, 1024)),
		Limits:  DefaultLimitsV1(),
		Sources: []RegisteredSource{{Phase: PhaseAction, Source: source}},
	})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := recorder.Capture(context.Background(), ActionMarker{ID: "marker-88888888888888888888888888888888", EvidenceRef: redEvidenceRef('1')})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bundle, []byte(secret)) {
		t.Fatalf("bundle reflected native error: %s", bundle)
	}
	if !bytes.Contains(bundle, []byte(`"error_category":"ACQUISITION_FAILED"`)) {
		t.Fatalf("bundle lacks category-only terminal state: %s", bundle)
	}
}

func TestMSP065StableOrderingDoesNotDependOnRegistrationOrder(t *testing.T) {
	a := RegisteredSource{Phase: PhasePost, Source: &redSource{result: SourceCapture{RuntimeKind: RuntimeCloudApp, SourceKind: SourceCloudApp}}}
	b := RegisteredSource{Phase: PhasePre, Source: &redSource{result: SourceCapture{RuntimeKind: RuntimeEEBus, SourceKind: SourceEEBus}}}
	got := CanonicalSourceOrder([]RegisteredSource{a, b})
	want := []RegisteredSource{b, a}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical order = %#v, want %#v", got, want)
	}
}
