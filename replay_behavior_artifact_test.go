package ebusgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

const replayBehaviorArtifactSchema = "observe_first_replay_behavior_v1"

type replayBehaviorArtifact struct {
	Schema     string                    `json:"schema"`
	CapturedAt string                    `json:"captured_at"`
	Source     string                    `json:"source"`
	OK         bool                      `json:"ok"`
	Summary    replayBehaviorSummary     `json:"summary"`
	Cases      []replayBehaviorCaseEntry `json:"cases"`
}

type replayBehaviorSummary struct {
	TotalCases         int `json:"total_cases"`
	LockedCases        int `json:"locked_cases"`
	Observed           int `json:"observed_cases"`
	ObservationFailure int `json:"observation_failure_cases"`
}

type replayBehaviorCaseEntry struct {
	Name     string                 `json:"name"`
	Observed replayBehaviorObserved `json:"observed"`
	Status   string                 `json:"status"`
	Reason   string                 `json:"reason,omitempty"`
}

type replayBehaviorObserved struct {
	DirectApply           bool   `json:"direct_apply"`
	Disposition           string `json:"disposition"`
	RawDisposition        string `json:"raw_disposition,omitempty"`
	ThirdPartyEligible    bool   `json:"third_party_eligible,omitempty"`
	DirectApplyPolicy     string `json:"direct_apply_policy,omitempty"`
	TransactionEvents     int    `json:"transaction_events,omitempty"`
	ObservedSymbols       int    `json:"observed_symbols,omitempty"`
	CompletedTransactions int    `json:"completed_transactions,omitempty"`
	PassiveState          string `json:"passive_state,omitempty"`
	ReplayHarness         string `json:"replay_harness,omitempty"`
}

func TestReplayBehaviorArtifact(t *testing.T) {
	outPath := strings.TrimSpace(os.Getenv("REPLAY_BEHAVIOR_ARTIFACT_PATH"))
	if outPath == "" {
		t.Skip("REPLAY_BEHAVIOR_ARTIFACT_PATH not set")
	}

	artifact := buildReplayBehaviorArtifact(t)
	payload, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent replay behavior artifact error = %v", err)
	}
	payload = append(payload, '\n')
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(outPath), err)
	}
	if err := os.WriteFile(outPath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", outPath, err)
	}
}

func buildReplayBehaviorArtifact(t *testing.T) replayBehaviorArtifact {
	t.Helper()

	locked := make([]replayBehaviorCaseEntry, 0, 3)
	locked = append(locked, observeReplayB524ValueBearingEnh(t))
	locked = append(locked, observeReplayCollisionEpisode(t))
	locked = append(locked, observeReplayTimeoutNoProgress(t))

	return replayBehaviorArtifact{
		Schema:     replayBehaviorArtifactSchema,
		CapturedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Source:     "go_replay_harness",
		OK:         true,
		Summary: replayBehaviorSummary{
			TotalCases:         len(locked),
			LockedCases:        len(locked),
			Observed:           len(locked),
			ObservationFailure: 0,
		},
		Cases: locked,
	}
}

func observeReplayB524ValueBearingEnh(t *testing.T) replayBehaviorCaseEntry {
	t.Helper()

	deduplicator := newTestDeduplicator(t)
	subscription, err := deduplicator.Subscribe("replay-behavior", DedupSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	forceHealthyDedup(deduplicator)
	stateKey := NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C)
	deduplicator.cfg.WatchObserver = staticWatchObserver{
		byCanonical: map[string]WatchObservation{
			stateKey.Canonical(): {
				State: WatchObservationStateActive,
				Descriptor: WatchDescriptor{
					Key:               stateKey,
					SemanticClass:     WatchSemanticClassState,
					CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
					DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
				},
				HasDescriptor: true,
			},
		},
	}

	base := time.Unix(0, 0)
	deduplicator.nowFunc = func() time.Time { return base }

	request := protocol.Frame{
		Source:    0x31,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
	}
	response := protocol.Frame{
		Source:    request.Target,
		Target:    request.Source,
		Primary:   request.Primary,
		Secondary: request.Secondary,
		Data:      []byte{0x42, 0x01, 0x03, 0x1C, 0x00, 0x22},
	}

	deduplicator.OnPassiveClassifiedEvent(passiveTransactionEvent(base.Add(100*time.Millisecond), request, response))
	event := requireAdjudicatedEvent(t, subscription, DedupDispositionUnmatchedThirdParty)
	if !event.ThirdPartyEligible {
		t.Fatal("ThirdPartyEligible = false; want true for B524 runtime observer fallback")
	}
	if event.FamilyPolicy.DirectApplyPolicy != ObserveFirstDirectApplyPolicyStateDefault {
		t.Fatalf("DirectApplyPolicy = %q; want %q", event.FamilyPolicy.DirectApplyPolicy, ObserveFirstDirectApplyPolicyStateDefault)
	}

	return replayBehaviorCaseEntry{
		Name: "b524_value_bearing_enh",
		Observed: replayBehaviorObserved{
			DirectApply:        false,
			Disposition:        "ambiguity",
			RawDisposition:     string(event.Disposition),
			ThirdPartyEligible: event.ThirdPartyEligible,
			DirectApplyPolicy:  string(event.FamilyPolicy.DirectApplyPolicy),
			ReplayHarness:      "active_passive_deduplicator",
		},
		Status: "observed",
		Reason: "observed B524 runtime observer fallback produced unmatched third-party disposition",
	}
}

func observeReplayCollisionEpisode(t *testing.T) replayBehaviorCaseEntry {
	t.Helper()

	currentProxyModeledObserverSymbols := mustLoadProxyObserverFixture(t, "testdata/p03_proxy_single_combined_artifact_observer.hex")
	result := runProxyENSObserverHarness(t, []proxyObserverWrite{
		{delay: 25 * time.Millisecond, logicalSymbols: currentProxyModeledObserverSymbols},
	}, false, 1)

	transactionEvents := result.countEvents(PassiveClassifiedEventTransaction)
	if transactionEvents != 0 {
		t.Fatalf("transaction events = %d; want 0 for collision replay", transactionEvents)
	}
	if result.completedTransactions != 0 {
		t.Fatalf("completedTransactions = %d; want 0 for collision replay", result.completedTransactions)
	}

	return replayBehaviorCaseEntry{
		Name: "collision_episode",
		Observed: replayBehaviorObserved{
			DirectApply:           false,
			Disposition:           "falsification",
			TransactionEvents:     transactionEvents,
			ObservedSymbols:       int(result.snapshot.TapStatus.ObservedSymbolCount),
			CompletedTransactions: result.completedTransactions,
			PassiveState:          result.passiveState,
			ReplayHarness:         "proxy_ens_observer",
		},
		Status: "observed",
		Reason: "observed proxy-observer collision stream produced no direct-apply replay path",
	}
}

func observeReplayTimeoutNoProgress(t *testing.T) replayBehaviorCaseEntry {
	t.Helper()

	request := protocol.Frame{
		Source:    0xF7,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x08, 0x10},
	}
	partial := proxyObserverTransactionBytes(request, []byte{0x01, 0x42})
	result := runProxyENSObserverHarness(t, []proxyObserverWrite{
		{delay: 25 * time.Millisecond, logicalSymbols: partial[:2]},
	}, false, 1)

	transactionEvents := result.countEvents(PassiveClassifiedEventTransaction)
	if transactionEvents != 0 {
		t.Fatalf("transaction events = %d; want 0 for timeout/no-progress replay", transactionEvents)
	}
	if result.completedTransactions != 0 {
		t.Fatalf("completedTransactions = %d; want 0 for timeout/no-progress replay", result.completedTransactions)
	}

	return replayBehaviorCaseEntry{
		Name: "timeout_no_progress",
		Observed: replayBehaviorObserved{
			DirectApply:           false,
			Disposition:           "falsification",
			TransactionEvents:     transactionEvents,
			ObservedSymbols:       int(result.snapshot.TapStatus.ObservedSymbolCount),
			CompletedTransactions: result.completedTransactions,
			PassiveState:          result.passiveState,
			ReplayHarness:         "proxy_ens_observer_timeout",
		},
		Status: "observed",
		Reason: "observed truncated observer stream produced no progress and no direct-apply path",
	}
}
