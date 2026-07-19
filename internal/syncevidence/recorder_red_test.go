package syncevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const testBuildVersion = "1.0.0+git.61bc62fa1d08dcf7b677c3dc08855beb5c68ceb1"

type redClock struct {
	mu     sync.Mutex
	wall   time.Time
	offset uint64
}

func (clock *redClock) Observe() ClockObservation {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	result := ClockObservation{Wall: clock.wall, OffsetNS: clock.offset, UncertaintyNS: 10}
	clock.wall = clock.wall.Add(time.Second)
	clock.offset += uint64(time.Second)
	return result
}

type eebusReaderFunc func(context.Context, SourceRequest) (AcquiredEvidence, error)

func (reader eebusReaderFunc) ListServices(ctx context.Context, request SourceRequest) (AcquiredEvidence, error) {
	return reader(ctx, request)
}

type ebusReaderFunc func(context.Context, SourceRequest) (AcquiredEvidence, error)

func (reader ebusReaderFunc) ReadSnapshot(ctx context.Context, request SourceRequest) (AcquiredEvidence, error) {
	return reader(ctx, request)
}

type manualTimer struct {
	ch      chan time.Time
	stopped bool
}

func (timer *manualTimer) C() <-chan time.Time { return timer.ch }
func (timer *manualTimer) Stop() bool {
	timer.stopped = true
	return true
}

func redEvidenceRef(digit byte) EvidenceRefV1 {
	return EvidenceRefV1{
		Kind:            EvidenceKindContent,
		DigestAlgorithm: DigestAlgorithmContentBytes,
		Digest:          "sha256:" + string(bytes.Repeat([]byte{digit}, 64)),
	}
}

func allowedAdmission(permission string) SourceAdmission {
	return SourceAdmission{
		Selection: SelectionIncluded, Policy: PolicyAllowed, Backend: BackendUnknown,
		EffectivePermissions: []string{permission}, RequiredPermissions: []string{permission},
	}
}

func testRecorder(t *testing.T, sources []RegisteredSource, options func(*RecorderOptions)) *Recorder {
	t.Helper()
	configuration := RecorderOptions{
		Clock:   &redClock{wall: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)},
		Entropy: bytes.NewReader(bytes.Repeat([]byte{0x44}, 1<<20)),
		Limits:  DefaultLimitsV1(), Sources: sources,
		RecorderVersion: testBuildVersion, ReplayVersion: testBuildVersion,
	}
	if options != nil {
		options(&configuration)
	}
	recorder, err := NewRecorder(configuration)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	return recorder
}

func captureBundle(t *testing.T, recorder *Recorder, marker ActionMarker) SynchronizedEvidenceBundleV1 {
	t.Helper()
	raw, err := recorder.Capture(context.Background(), marker)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	bundle, err := verifyBundle(raw)
	if err != nil {
		t.Fatalf("verifyBundle: %v\n%s", err, raw)
	}
	return bundle
}

func eebusPayload(t *testing.T, observed time.Time, services []any) json.RawMessage {
	t.Helper()
	data := map[string]any{"services": services}
	payload := map[string]any{
		"meta": map[string]any{
			"contract": map[string]any{"name": "helianthus-eebus-mcp", "major": json.Number("1"), "minor": json.Number("0")},
			"tool":     "eebus.v1.services.list", "scope": "services", "mask_tier": "redacted",
			"auth_scope": "eebus.raw.read", "mode": "evidence", "data_timestamp": observed.Format(time.RFC3339Nano),
			"data_hash": "sha256:" + strings.Repeat("0", 64), "runtime": map[string]any{"state": "ready"},
		},
		"data": data, "error": nil,
	}
	hash, err := eebusEnvelopeDataHash(payload)
	if err != nil {
		t.Fatal(err)
	}
	payload["meta"].(map[string]any)["data_hash"] = hash
	encoded, err := canonicalJSONValue(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func cloudPayload(observed time.Time, subject, value string) json.RawMessage {
	return json.RawMessage(`{"contract":"helianthus.cloud-app.precaptured.evidence.v1","observation_type":"ROOM_TEMPERATURE","schema_version":1,"source_observed_at":"` + observed.Format(time.RFC3339Nano) + `","subject_pseudonym":"` + subject + `","unit":"degC","value":"` + value + `"}`)
}

func eebusRegistration(reader EEBusServicesReader, phase Phase, instance string) RegisteredSource {
	return RegisteredSource{
		Phase: phase, SourceKind: SourceEEBus, RuntimeInstance: instance,
		OperationVersion: "git:520b6439441cb6e8ef9ff291bde28f4efa4db254", OperationScope: "services",
		Admission: allowedAdmission("eebus.raw.read"), EvidenceRefs: []EvidenceRefV1{redEvidenceRef('2')}, EEBusReader: reader,
	}
}

func cloudRegistration(input PrecapturedCloudInput, phase Phase, instance string) RegisteredSource {
	return RegisteredSource{
		Phase: phase, SourceKind: SourceCloudApp, RuntimeInstance: instance,
		OperationVersion: "git:61bc62fa1d08dcf7b677c3dc08855beb5c68ceb1", OperationScope: "cloud-app",
		Admission: allowedAdmission("cloud.read"), PrecapturedCloud: &input,
	}
}

func b524Registration(reader EBusSnapshotReader, phase Phase, instance string) RegisteredSource {
	return RegisteredSource{
		Phase: phase, SourceKind: SourceEBusB524, RuntimeInstance: instance,
		OperationVersion: "git:61bc62fa1d08dcf7b677c3dc08855beb5c68ceb1", OperationScope: "ebus-b524",
		Admission: allowedAdmission("ebus.read"), EvidenceRefs: []EvidenceRefV1{redEvidenceRef('4')}, EBusReader: reader,
		EBusIdentity: &EBusSourceIdentityV1{
			Family: EBusFamilyB524, Opcode: 2, GG: 3, II: 0, RR: 28, TargetAddress: 21, SourceAddress: 247,
			GroupMeaning: "zones", InstanceGate: "index-not-ff", RegisterCategory: "STATE", UnitScaleSource: "vrc-explorer-v1",
		},
	}
}

func b524Payload(t *testing.T, observed time.Time, target string) json.RawMessage {
	t.Helper()
	identity := map[string]any{
		"family": "B524", "target_pseudonym": target, "opcode": json.Number("2"), "GG": json.Number("3"), "II": json.Number("0"),
		"RR": json.Number("28"), "target_address": json.Number("21"), "source_address": json.Number("247"), "group_meaning": "zones",
		"instance_gate": "index-not-ff", "register_category": "STATE", "unit_scale_source": "vrc-explorer-v1",
	}
	payload := map[string]any{
		"contract": "helianthus.ebus.b524.evidence.v1", "schema_version": json.Number("1"),
		"source_observed_at": observed.Format(time.RFC3339Nano), "identity": identity,
		"observations": []any{map[string]any{"value": "21.25", "unit": "degC", "quality": "OBSERVED"}},
	}
	encoded, err := canonicalJSONValue(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestMSP065TypedReadOnlySourcesAndValueOnlyCloudReplayOffline(t *testing.T) {
	observed := time.Date(2026, 7, 19, 11, 59, 59, 0, time.UTC)
	calls := 0
	eebus := eebusRegistration(eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		calls++
		return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: eebusPayload(t, observed, []any{})}, nil
	}), PhasePre, "eebus-runtime-a")
	cloud := cloudRegistration(PrecapturedCloudInput{
		SourceObservedAt: observed, NormalizedEvidence: cloudPayload(observed, strings.Repeat("A", 43), "21.5"), EvidenceRef: redEvidenceRef('3'),
	}, PhaseAction, "cloud-runtime-a")
	recorder := testRecorder(t, []RegisteredSource{cloud, eebus}, nil)
	raw, err := recorder.Capture(context.Background(), ActionMarker{ID: "external-secret-marker", EvidenceRef: redEvidenceRef('1')})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || bytes.Contains(raw, []byte("external-secret-marker")) {
		t.Fatalf("typed acquisition calls=%d or external marker leaked", calls)
	}
	first, err := Replay(raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Replay(raw)
	if err != nil || !bytes.Equal(first, second) || calls != 1 {
		t.Fatalf("offline replay mismatch err=%v calls=%d", err, calls)
	}
}

func TestMSP065TargetPseudonymIsMintedInternally(t *testing.T) {
	observed := time.Date(2026, 7, 19, 11, 59, 59, 0, time.UTC)
	inputTarget := "target-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	reader := ebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: b524Payload(t, observed, inputTarget)}, nil
	})
	bundle := captureBundle(t, testRecorder(t, []RegisteredSource{b524Registration(reader, PhasePre, "ebus-runtime")}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if len(bundle.Artifacts) != 1 || bundle.Artifacts[0].EBusIdentity == nil {
		t.Fatalf("missing eBUS artifact: %#v", bundle.Artifacts)
	}
	minted := bundle.Artifacts[0].EBusIdentity.TargetPseudonym
	if minted == inputTarget || !targetIDPattern.MatchString(minted) || bytes.Contains(bundle.Artifacts[0].NormalizedEvidence, []byte(inputTarget)) {
		t.Fatalf("target pseudonym was not internally remasked: %q", minted)
	}
}

func TestMSP065RecorderOwnsPolicyAuthorizationAndErrorPrecedence(t *testing.T) {
	called := 0
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		called++
		return AcquiredEvidence{}, errors.New("native text must not be reflected")
	})
	excluded := eebusRegistration(reader, PhasePre, "excluded")
	excluded.Admission.Selection = SelectionExcluded
	withheld := eebusRegistration(reader, PhaseAction, "withheld")
	withheld.Admission.Policy = PolicyWithheld
	denied := eebusRegistration(reader, PhasePost, "denied")
	denied.Admission.EffectivePermissions = []string{"other.read"}
	bundle := captureBundle(t, testRecorder(t, []RegisteredSource{denied, withheld, excluded}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if called != 0 {
		t.Fatalf("policy-gated reader called %d times", called)
	}
	want := []ErrorCategory{ErrorNotSelected, ErrorPolicyWithheld, ErrorAuthorizationDenied}
	for index, source := range bundle.Sources {
		if source.ErrorCategory == nil || *source.ErrorCategory != want[index] {
			t.Fatalf("source[%d] category=%v want=%s", index, source.ErrorCategory, want[index])
		}
	}
}

func TestMSP065OneRemaskScopePerBundleAndEEBusHashAfterRemask(t *testing.T) {
	observed := time.Date(2026, 7, 19, 11, 59, 59, 0, time.UTC)
	evidenceDigest := "sha256:" + strings.Repeat("d", 64)
	services := []any{
		map[string]any{
			"id": map[string]any{"kind": "service", "digest": strings.Repeat("A", 43)}, "kind": "local", "visible": true, "paired": true,
			"evidence": []any{
				map[string]any{"kind": "service", "digest": evidenceDigest, "size": json.Number("2"), "data_timestamp": observed.Format(time.RFC3339Nano)},
				map[string]any{"kind": "service", "digest": evidenceDigest, "size": json.Number("10"), "data_timestamp": observed.Format(time.RFC3339Nano)},
			},
		},
		map[string]any{"id": map[string]any{"kind": "service", "digest": strings.Repeat("B", 43)}, "kind": "remote", "visible": true, "paired": false},
	}
	eebus := eebusRegistration(eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: eebusPayload(t, observed, services)}, nil
	}), PhasePre, "eebus-runtime")
	cloud := cloudRegistration(PrecapturedCloudInput{
		SourceObservedAt: observed, NormalizedEvidence: cloudPayload(observed, strings.Repeat("C", 43), "21.5"), EvidenceRef: redEvidenceRef('3'),
	}, PhaseAction, "cloud-runtime")
	bundle := captureBundle(t, testRecorder(t, []RegisteredSource{eebus, cloud}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if len(bundle.Artifacts) != 2 || bundle.Artifacts[0].Remasking.ScopeID != bundle.Artifacts[1].Remasking.ScopeID {
		t.Fatalf("remask scopes are not shared: %#v", bundle.Artifacts)
	}
	eebusArtifact := bundle.Artifacts[0]
	parsed, _, err := parseJSON(eebusArtifact.NormalizedEvidence, DefaultLimitsV1(), false)
	if err != nil {
		t.Fatal(err)
	}
	payload := parsed.(map[string]any)
	data := payload["data"].(map[string]any)
	meta := payload["meta"].(map[string]any)
	expectedHash, err := eebusEnvelopeDataHash(payload)
	if err != nil {
		t.Fatal(err)
	}
	if meta["data_hash"] != expectedHash {
		t.Fatalf("data_hash not recomputed: %v", meta["data_hash"])
	}
	rows := data["services"].([]any)
	keys := []string{serviceDigest(rows[0]), serviceDigest(rows[1])}
	if !sort.StringsAreSorted(keys) || keys[0] == strings.Repeat("A", 43) || keys[1] == strings.Repeat("B", 43) {
		t.Fatalf("remasked service ordering invalid: %v", keys)
	}
	for _, raw := range rows {
		service := raw.(map[string]any)
		if evidence, ok := service["evidence"].([]any); ok {
			for _, descriptor := range evidence {
				if descriptor.(map[string]any)["digest"] != evidenceDigest {
					t.Fatalf("evidence hash digest was incorrectly remasked: %#v", descriptor)
				}
			}
		}
	}
}

func TestMSP065MalformedEEBusOrderingAndDuplicateBecomeRedactionFailure(t *testing.T) {
	observed := time.Date(2026, 7, 19, 11, 59, 59, 0, time.UTC)
	service := map[string]any{"id": map[string]any{"kind": "service", "digest": strings.Repeat("A", 43)}, "kind": "local", "visible": true, "paired": true}
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: eebusPayload(t, observed, []any{service, service})}, nil
	})
	bundle := captureBundle(t, testRecorder(t, []RegisteredSource{eebusRegistration(reader, PhasePre, "eebus")}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if len(bundle.Artifacts) != 0 || bundle.Sources[0].State != StateWithheld || *bundle.Sources[0].ErrorCategory != ErrorRedactionFailed {
		t.Fatalf("malformed eeBUS terminal=%#v artifacts=%d", bundle.Sources[0], len(bundle.Artifacts))
	}
}

func TestMSP065TimeoutPanicAndNoncooperativeCallsAreCategoryOnlyAndBounded(t *testing.T) {
	observed := time.Date(2026, 7, 19, 11, 59, 59, 0, time.UTC)
	panicReader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) { panic("private panic text") })
	panicBundle := captureBundle(t, testRecorder(t, []RegisteredSource{eebusRegistration(panicReader, PhasePre, "panic")}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if panicBundle.Sources[0].State != StateUnavailable || *panicBundle.Sources[0].ErrorCategory != ErrorAcquisitionFailed {
		t.Fatalf("panic terminal=%#v", panicBundle.Sources[0])
	}
	cancelReader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{}, context.Canceled
	})
	cancelBundle := captureBundle(t, testRecorder(t, []RegisteredSource{eebusRegistration(cancelReader, PhasePre, "cancel")}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if cancelBundle.Sources[0].State != StateUnavailable || *cancelBundle.Sources[0].ErrorCategory != ErrorAcquisitionFailed {
		t.Fatalf("source cancellation terminal=%#v", cancelBundle.Sources[0])
	}

	started := make(chan struct{})
	release := make(chan struct{})
	blocking := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		close(started)
		<-release
		return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: eebusPayload(t, observed, nil)}, nil
	})
	sourceTimer := &manualTimer{ch: make(chan time.Time, 1)}
	totalTimer := &manualTimer{ch: make(chan time.Time, 1)}
	recorder := testRecorder(t, []RegisteredSource{eebusRegistration(blocking, PhasePre, "blocked")}, func(options *RecorderOptions) {
		options.NewTimer = func(duration time.Duration) StoppableTimer {
			if duration == time.Duration(options.Limits.MaxCaptureDurationNS) {
				return totalTimer
			}
			return sourceTimer
		}
	})
	result := make(chan []byte, 1)
	errs := make(chan error, 1)
	go func() {
		raw, err := recorder.Capture(context.Background(), ActionMarker{EvidenceRef: redEvidenceRef('1')})
		result <- raw
		errs <- err
	}()
	<-started
	sourceTimer.ch <- time.Now()
	raw := <-result
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	bundle, err := verifyBundle(raw)
	if err != nil || bundle.Sources[0].State != StateUnavailable || *bundle.Sources[0].ErrorCategory != ErrorTimeout {
		t.Fatalf("timeout bundle err=%v source=%#v", err, bundle.Sources[0])
	}
	if _, err := recorder.Capture(context.Background(), ActionMarker{EvidenceRef: redEvidenceRef('1')}); !errors.Is(err, ErrCapturePending) {
		t.Fatalf("second capture error=%v want=%v", err, ErrCapturePending)
	}
	close(release)
}

func TestMSP065TotalDeadlineStopsBeforeAndBetweenSources(t *testing.T) {
	calls := 0
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		calls++
		return AcquiredEvidence{}, nil
	})
	total := &manualTimer{ch: make(chan time.Time, 1)}
	total.ch <- time.Now()
	recorder := testRecorder(t, []RegisteredSource{
		eebusRegistration(reader, PhasePre, "one"), eebusRegistration(reader, PhaseAction, "two"),
	}, func(options *RecorderOptions) {
		options.NewTimer = func(duration time.Duration) StoppableTimer {
			if duration == time.Duration(options.Limits.MaxCaptureDurationNS) {
				return total
			}
			return &manualTimer{ch: make(chan time.Time)}
		}
	})
	bundle := captureBundle(t, recorder, ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if calls != 0 {
		t.Fatalf("deadline allowed %d calls", calls)
	}
	for _, source := range bundle.Sources {
		if source.State != StateNotTested || *source.ErrorCategory != ErrorBudgetExhausted {
			t.Fatalf("deadline terminal=%#v", source)
		}
	}
	if !total.stopped {
		t.Fatal("total timer was not stopped")
	}
}

func TestMSP065RuntimeInstancesDistinctAndBuildVersionsInjected(t *testing.T) {
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{}, ErrBackendUnavailable
	})
	a := eebusRegistration(reader, PhasePre, "instance-a")
	b := eebusRegistration(reader, PhaseAction, "instance-b")
	c := eebusRegistration(reader, PhasePost, "instance-a")
	bundle := captureBundle(t, testRecorder(t, []RegisteredSource{a, b, c}, nil), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	ids := []string{bundle.Sources[0].SourceBinding.RuntimePseudonym, bundle.Sources[1].SourceBinding.RuntimePseudonym, bundle.Sources[2].SourceBinding.RuntimePseudonym}
	if ids[0] == ids[1] || ids[0] != ids[2] {
		t.Fatalf("runtime instance pseudonyms=%v", ids)
	}
	if bundle.RecorderVersion != testBuildVersion || bundle.ReplayVersion != testBuildVersion {
		t.Fatalf("build versions=%s/%s", bundle.RecorderVersion, bundle.ReplayVersion)
	}
	options := RecorderOptions{Clock: &redClock{}, Limits: DefaultLimitsV1(), Sources: []RegisteredSource{a}}
	if _, err := NewRecorder(options); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing build versions error=%v", err)
	}
}

func TestMSP065WholeObjectPrivacyAndCanonicalTimestampLexemes(t *testing.T) {
	repository, commit, path := "Project-Helianthus/evidence", strings.Repeat("a", 40), "captures/192.168.1.10.json"
	markerRef := EvidenceRefV1{Kind: EvidenceKindGitBlob, DigestAlgorithm: DigestAlgorithmGitBlobV1, Digest: "sha256:" + strings.Repeat("b", 64), Repository: &repository, Commit: &commit, Path: &path}
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{}, ErrBackendUnavailable
	})
	recorder := testRecorder(t, []RegisteredSource{eebusRegistration(reader, PhasePre, "eebus")}, nil)
	if _, err := recorder.Capture(context.Background(), ActionMarker{EvidenceRef: markerRef}); err == nil || err.Error() != "privacy.prohibited" {
		t.Fatalf("embedded IP privacy error=%v", err)
	}

	fixture, err := os.ReadFile(filepath.Join("testdata", "canonical", "positive", "bundle.json"))
	if err != nil {
		t.Fatal(err)
	}
	noncanonical := bytes.Replace(fixture, []byte(`2026-07-19T12:00:10Z`), []byte(`2026-07-19T12:00:10.0Z`), 1)
	if bytes.Equal(noncanonical, fixture) {
		t.Fatal("timestamp mutation did not change fixture")
	}
	if _, err := Replay(noncanonical); err == nil || err.Error() != "schema.bundle" {
		t.Fatalf("noncanonical timestamp error=%v", err)
	}
	for _, prohibited := range []string{"/tmp/public", `C:\\Users\\public`, "-----BEGIN CERTIFICATE-----", "native object dump", "access_token=value"} {
		if err := validatePrivacy(map[string]any{"note": prohibited}); err == nil || err.Error() != "privacy.prohibited" {
			t.Errorf("privacy value %q error=%v", prohibited, err)
		}
	}
}

func TestMSP065ParserStructuralBudgetIsIndependentOfSourceItems(t *testing.T) {
	limits := DefaultLimitsV1()
	limits.MaxItemsPerSource = 1
	raw := []byte(`{"values":[0,1,2]}`)
	value, stats, err := parseJSON(raw, limits, false)
	if err != nil || stats.arrayItems != 3 || value == nil {
		t.Fatalf("structural parse value=%v stats=%#v err=%v", value, stats, err)
	}
}

func TestMSP065CanonicalFixturesPositiveReplayAndAllNegatives(t *testing.T) {
	if got := digestHex(mustReadContract("synchronized-evidence-replay-v1.schema.json")); got != "95573cdb0c5baa39596d3fbdee4ca483c9c94187dc38eb3c2f0dd8b7fd4d672e" {
		t.Fatalf("pinned replay schema digest=%s", got)
	}
	positive, err := os.ReadFile(filepath.Join("testdata", "canonical", "positive", "bundle.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Replay(positive)
	if err != nil {
		t.Fatalf("canonical positive: %v", err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "canonical", "positive", "replay-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := CanonicalizeJSON(want)
	if err != nil || !bytes.Equal(got, wantCanonical) {
		t.Fatalf("canonical replay mismatch err=%v\ngot=%s\nwant=%s", err, got, wantCanonical)
	}
	negative, err := filepath.Glob(filepath.Join("testdata", "canonical", "negative", "*.json"))
	if err != nil || len(negative) == 0 {
		t.Fatalf("negative fixtures: %v count=%d", err, len(negative))
	}
	for _, fixture := range negative {
		fixture := fixture
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			raw, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Replay(raw); err == nil {
				t.Fatal("negative fixture unexpectedly replayed")
			}
		})
	}
}

func TestMSP065MarkerClockAndArtifactIngestionPhaseAreValidated(t *testing.T) {
	positive, err := os.ReadFile(filepath.Join("testdata", "canonical", "positive", "bundle.json"))
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := parseJSON(positive, DefaultLimitsV1(), true)
	if err != nil {
		t.Fatal(err)
	}
	root := value.(map[string]any)
	window := root["capture_window"].(map[string]any)
	action := window["action"].(map[string]any)
	action["marker_captured_at"] = "2026-07-19T12:00:09Z"
	mutated, _ := canonicalJSONValue(root)
	if _, err := Replay(mutated); err == nil || err.Error() != "clock.skew" {
		t.Fatalf("marker clock error=%v", err)
	}

	value, _, _ = parseJSON(positive, DefaultLimitsV1(), true)
	root = value.(map[string]any)
	artifact := root["artifacts"].([]any)[0].(map[string]any)
	artifact["recorder_ingested_offset_ns"] = json.Number("5000000000")
	mutated, _ = canonicalJSONValue(root)
	if _, err := Replay(mutated); err == nil {
		t.Fatal("out-of-phase artifact ingestion accepted")
	}
}

func TestMSP065StableOrderingDoesNotDependOnRegistrationOrder(t *testing.T) {
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{}, ErrBackendUnavailable
	})
	a := eebusRegistration(reader, PhasePost, "z")
	b := eebusRegistration(reader, PhasePre, "a")
	got := CanonicalSourceOrder([]RegisteredSource{a, b})
	if len(got) != 2 || got[0].Phase != b.Phase || got[0].RuntimeInstance != b.RuntimeInstance || got[1].Phase != a.Phase || got[1].RuntimeInstance != a.RuntimeInstance {
		t.Fatalf("canonical order=%#v", got)
	}
}

func serviceDigest(value any) string {
	service := value.(map[string]any)
	identity := service["id"].(map[string]any)
	return identity["digest"].(string)
}
