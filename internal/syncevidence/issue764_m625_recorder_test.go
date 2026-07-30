package syncevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type issue764M625ReaderFunc func(context.Context, SourceRequest) (AcquiredEvidence, error)

const issue764OperationVersion = "git:61bc62fa1d08dcf7b677c3dc08855beb5c68ceb1"

func (reader issue764M625ReaderFunc) ReadFeatureData(ctx context.Context, request SourceRequest) (AcquiredEvidence, error) {
	return reader(ctx, request)
}

func issue764M625Payload() json.RawMessage {
	return json.RawMessage(`{
		"contract":"helianthus.eebus.m625.public-redacted-evidence.v1",
		"schema_version":1,
		"source_observed_at":"2026-07-30T12:00:02Z",
		"services":["BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"],
		"feature_paths":[
			{
				"service":"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				"entity":"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
				"feature":"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
				"feature_path":[
					{"kind":"SERVICE","selector":"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
					{"kind":"ENTITY","selector":"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"},
					{"kind":"FEATURE","selector":"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD"},
					{"kind":"FIELD","selector":"EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"}
				]
			},
			{
				"service":"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				"entity":"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
				"feature":"GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG",
				"feature_path":[
					{"kind":"SERVICE","selector":"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
					{"kind":"ENTITY","selector":"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"},
					{"kind":"FEATURE","selector":"GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"}
				]
			}
		],
		"observations":[
			{
				"observation_ref":"obs-HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH",
				"path_index":0,
				"feature_type":"Measurement",
				"feature_role":"server",
				"function":"measurementListData",
				"source_observed_at":"2026-07-30T12:00:01Z",
				"terminal_classification":"SUCCESS",
				"value_type":"DECIMAL",
				"value":"21.5",
				"unit":"degC",
				"quality":"OBSERVED"
			},
			{
				"observation_ref":"obs-IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII",
				"path_index":1,
				"feature_type":"HVAC",
				"feature_role":"server",
				"function":"hvacSystemFunctionListData",
				"source_observed_at":"2026-07-30T12:00:02Z",
				"terminal_classification":"DECODE_ERROR",
				"value_type":null,
				"value":null,
				"unit":null,
				"quality":null
			}
		]
	}`)
}

func issue764ExactIdentity(kind SourceKind) *EBusSourceIdentityV1 {
	switch kind {
	case SourceEBusB509:
		return &EBusSourceIdentityV1{
			Family: EBusFamilyB509, TargetAddress: 0x15, SourceAddress: 0xf7,
			TargetProduct: "BASV2", RegisterFamily: "status", RegisterID: 1,
			UnitScaleSource: "catalog-v1", EvidenceRole: "AUTHORITATIVE",
		}
	case SourceEBusB524:
		return &EBusSourceIdentityV1{
			Family: EBusFamilyB524, TargetAddress: 0x15, SourceAddress: 0xf7,
			Opcode: 2, GG: 3, II: 0, RR: 28, GroupMeaning: "zones",
			InstanceGate: "index-not-ff", RegisterCategory: "STATE",
			UnitScaleSource: "catalog-v1",
		}
	case SourceEBusB555:
		return &EBusSourceIdentityV1{
			Family: EBusFamilyB555, TargetAddress: 0x15, SourceAddress: 0xf7,
			DeviceFamily: "BASV2", ScheduleProgram: "heating", SlotIndex: 0,
			DayOfWeek: "MONDAY", TimeIdentity: "06:00:00",
			OperationModeContext: "AUTO", UnitScaleSource: "catalog-v1",
		}
	default:
		return nil
	}
}

func issue764TerminalEBusSource(kind SourceKind, phase Phase, scope, digest string) RegisteredSource {
	return RegisteredSource{
		Phase: phase, SourceKind: kind, RuntimeInstance: "ebus-runtime",
		OperationVersion: issue764OperationVersion, OperationScope: scope,
		Admission:    allowedAdmission("ebus.read"),
		EvidenceRefs: []EvidenceRefV1{contentEvidenceRefForTest(digest)},
		EBusIdentity: issue764ExactIdentity(kind),
		EBusReader: ebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
			return AcquiredEvidence{}, ErrBackendUnavailable
		}),
	}
}

func contentEvidenceRefForTest(digest string) EvidenceRefV1 {
	return EvidenceRefV1{
		Kind:            EvidenceKindContent,
		DigestAlgorithm: DigestAlgorithmContentBytes,
		Digest:          "sha256:" + digest,
	}
}

func TestIssue764M625FiveSourceCaptureRemasksAndReplays(t *testing.T) {
	observed := time.Date(2026, 7, 30, 12, 0, 2, 0, time.UTC)
	m625Reader := issue764M625ReaderFunc(func(_ context.Context, request SourceRequest) (AcquiredEvidence, error) {
		if request.OperationID != "eebus.v1.features.data.get" ||
			request.OperationScope != "feature-data" ||
			request.MaskTier != "redacted" ||
			!permissionsContain(request.AuthScope.Permissions, []string{"eebus.raw.read"}) {
			t.Fatalf("M6.25 request binding = %#v", request)
		}
		return AcquiredEvidence{SourceObservedAt: observed, NormalizedEvidence: issue764M625Payload()}, nil
	})
	actionRef := redEvidenceRef('a')
	sources := []RegisteredSource{
		issue764TerminalEBusSource(SourceEBusB509, PhasePre, "ebus-b509", "dbe91a10a208613183f890849c634f8d13661194aad937a03ae2a4143070bf2d"),
		{
			Phase: PhasePre, SourceKind: SourceEEBus, SourceContract: M625EEBusContractV1, SourceVersion: 1,
			RuntimeInstance: "eebus-runtime", OperationVersion: issue764OperationVersion, OperationScope: "feature-data",
			Admission: allowedAdmission("eebus.raw.read"),
			EvidenceRefs: []EvidenceRefV1{
				contentEvidenceRefForTest("0a2885d01d6703389541e246db59bcd845a332e7ed296abca2d49b4f8de31811"),
			},
			EEBusM625Reader: m625Reader,
		},
		issue764TerminalEBusSource(SourceEBusB524, PhaseAction, "ebus-b524", "1002e09890801c9032548af407b13b58d889217dfc83e58bfe2a28df6bc33b78"),
		cloudRegistration(PrecapturedCloudInput{
			SourceObservedAt: observed,
			NormalizedEvidence: cloudPayload(
				observed,
				strings.Repeat("J", 43),
				"21.5",
			),
			EvidenceRef: actionRef,
		}, PhaseAction, "cloud-runtime"),
		issue764TerminalEBusSource(SourceEBusB555, PhasePost, "ebus-b555", "a3237e344f5c3582ceaf1ca947eabf200bede8fe9388ec85cf647331f026c72d"),
	}
	recorder := testRecorder(t, sources, func(options *RecorderOptions) {
		options.Entropy = bytes.NewReader(bytes.Repeat([]byte{0x61}, 1<<20))
	})
	raw, err := recorder.Capture(context.Background(), ActionMarker{EvidenceRef: actionRef})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if _, err := Replay(raw); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	var bundle SynchronizedEvidenceBundleV1
	if err := json.Unmarshal(raw, &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Sources) != 5 || len(bundle.Artifacts) != 2 {
		t.Fatalf("source/artifact count = %d/%d, want 5/2", len(bundle.Sources), len(bundle.Artifacts))
	}
	for _, source := range bundle.Sources {
		switch source.SourceContract {
		case M625EEBusContractV1:
			if source.State != StatePresent || source.SourceSchemaVersion != 1 ||
				source.SourceBinding.OperationID != "eebus.v1.features.data.get" {
				t.Fatalf("M6.25 source = %#v", source)
			}
		case "helianthus.cloud-app.precaptured.evidence.v1":
			if source.State != StatePresent {
				t.Fatalf("cloud source = %#v", source)
			}
		default:
			if source.State != StateUnavailable || source.EBusIdentity == nil {
				t.Fatalf("eBUS terminal source = %#v", source)
			}
		}
	}

	var m625 json.RawMessage
	for _, artifact := range bundle.Artifacts {
		if artifact.SourceContract == M625EEBusContractV1 {
			m625 = artifact.NormalizedEvidence
			if len(artifact.Remasking.Entries) != 16 {
				t.Fatalf("M6.25 remasking entry count = %d, want 16", len(artifact.Remasking.Entries))
			}
		}
	}
	if len(m625) == 0 {
		t.Fatal("M6.25 artifact absent")
	}
	for _, forbidden := range []string{
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
		"obs-HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH",
		"candidate_ref",
		"remote_ski",
		"ship_id",
		"device_address",
		"raw_request",
	} {
		if bytes.Contains(m625, []byte(forbidden)) {
			t.Fatalf("M6.25 artifact leaks %q: %s", forbidden, m625)
		}
	}
}

func TestIssue764M625ValidatorRejectsWrongOrderingAndRawMetadata(t *testing.T) {
	observed := time.Date(2026, 7, 30, 12, 0, 2, 0, time.UTC)
	base := sourceCapture{
		sourceKind:          SourceEEBus,
		sourceContract:      M625EEBusContractV1,
		sourceSchemaVersion: 1,
		sourceObservedAt:    observed,
		operationID:         "eebus.v1.features.data.get",
		operationScope:      "feature-data",
	}
	valid, _, err := parseJSON(issue764M625Payload(), DefaultLimitsV1(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSourcePayload(valid.(map[string]any), base); err != nil {
		t.Fatalf("valid M6.25 payload rejected: %v", err)
	}

	for name, mutate := range map[string]func(map[string]any){
		"raw identity": func(root map[string]any) {
			root["remote_ski"] = strings.Repeat("a", 40)
		},
		"wrong ordering": func(root map[string]any) {
			paths := root["feature_paths"].([]any)
			paths[0], paths[1] = paths[1], paths[0]
		},
		"wrong value matrix": func(root map[string]any) {
			observation := root["observations"].([]any)[0].(map[string]any)
			observation["value_type"] = "BOOLEAN"
			observation["value"] = "true"
			observation["unit"] = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			value, _, err := parseJSON(issue764M625Payload(), DefaultLimitsV1(), false)
			if err != nil {
				t.Fatal(err)
			}
			root := value.(map[string]any)
			mutate(root)
			if err := validateSourcePayload(root, base); err == nil {
				t.Fatal("invalid M6.25 payload accepted")
			}
		})
	}
}
