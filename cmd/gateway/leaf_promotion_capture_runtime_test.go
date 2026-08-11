package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

func TestIssue784CheckpointPublicationIsImmutableAndRestartIdempotent(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "eebus")
	if err := os.Mkdir(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := openLeafPromotionCheckpointRoot(stateRoot)
	if err != nil {
		t.Fatalf("openLeafPromotionCheckpointRoot: %v", err)
	}
	acquisitions := 0
	runtime := &leafPromotionCaptureRuntime{
		root: root, source: &leafPromotionLiveSource{}, processInstanceID: "process-one",
		now: time.Now, entropy: bytes.NewReader(bytes.Repeat([]byte{0x42}, 1024)),
	}
	runtime.acquire = func(_ context.Context, request mcp.LeafPromotionCaptureRequest, _ *promotioncapture.WindowCheckpoint) (promotioncapture.WindowCheckpoint, error) {
		acquisitions++
		return issue784Checkpoint(t, request.CampaignID, promotioncapture.WindowPhase(request.Phase), "process-one"), nil
	}
	request := mcp.LeafPromotionCaptureRequest{CampaignID: "m8-live-01", Phase: string(promotioncapture.PhasePreRestart)}
	first := runtime.CaptureLeafPromotion(t.Context(), request)
	if first.Category != "PUBLISHED" || first.CampaignID != request.CampaignID || first.WindowHash == "" || first.ReceiptHash == "" {
		t.Fatalf("first receipt = %+v", first)
	}
	second := runtime.CaptureLeafPromotion(t.Context(), request)
	if second.Category != "EXISTING" || second.WindowHash != first.WindowHash || second.ReceiptHash != first.ReceiptHash || acquisitions != 1 {
		t.Fatalf("second receipt/acquisitions = %+v/%d", second, acquisitions)
	}
	path := runtime.checkpointPath(request.CampaignID, promotioncapture.PhasePreRestart)
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("checkpoint mode = %v, err=%v", info, err)
	}

	restarted := &leafPromotionCaptureRuntime{
		root: root, source: &leafPromotionLiveSource{}, processInstanceID: "process-two",
		now: time.Now, entropy: bytes.NewReader(bytes.Repeat([]byte{0x24}, 1024)),
		acquire: func(context.Context, mcp.LeafPromotionCaptureRequest, *promotioncapture.WindowCheckpoint) (promotioncapture.WindowCheckpoint, error) {
			t.Fatal("restart idempotency reacquired an immutable PRE window")
			return promotioncapture.WindowCheckpoint{}, nil
		},
	}
	third := restarted.CaptureLeafPromotion(t.Context(), request)
	if third.Category != "EXISTING" || third.WindowHash != first.WindowHash || third.ReceiptHash != first.ReceiptHash {
		t.Fatalf("restart receipt = %+v", third)
	}
}

func TestIssue784RestartContinuityRequiresStableBindingsAndNewProcess(t *testing.T) {
	before := issue784Checkpoint(t, "campaign", promotioncapture.PhasePreRestart, "process-one")
	before.Window.EndedAt = "2026-08-11T10:00:10Z"
	after := before.Window
	after.Phase = promotioncapture.PhasePostRestart
	after.StartedAt = "2026-08-11T10:01:00Z"
	after.EndedAt = "2026-08-11T10:01:01Z"
	after.ProcessInstanceHash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	restart := time.Date(2026, 8, 11, 10, 0, 30, 0, time.UTC)
	request := mcp.LeafPromotionCaptureRequest{CampaignID: "campaign", Phase: string(promotioncapture.PhasePostRestart), RestartCompletedAt: &restart}
	if err := leafPromotionValidateRestart(before, after, request); err != nil {
		t.Fatalf("valid restart rejected: %v", err)
	}

	for name, mutate := range map[string]func(*promotioncapture.Window){
		"same process": func(window *promotioncapture.Window) { window.ProcessInstanceHash = before.Window.ProcessInstanceHash },
		"local identity": func(window *promotioncapture.Window) {
			window.LocalIdentityHash = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		},
		"trust state": func(window *promotioncapture.Window) {
			window.TrustStateHash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
		},
		"peer binding": func(window *promotioncapture.Window) {
			window.PeerBindingHash = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		},
		"admitted source": func(window *promotioncapture.Window) { window.AdmittedSource++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := after
			mutate(&changed)
			if err := leafPromotionValidateRestart(before, changed, request); err == nil {
				t.Fatal("changed restart binding accepted")
			}
		})
	}
}

func TestIssue784CheckpointRootRejectsUnsafePermissionsAndSymlink(t *testing.T) {
	unsafeRoot := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(unsafeRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := openLeafPromotionCheckpointRoot(unsafeRoot); err == nil {
		t.Fatal("group-readable state root accepted")
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := openLeafPromotionCheckpointRoot(link); err == nil {
		t.Fatal("symlink state root accepted")
	}
}

func issue784Checkpoint(t *testing.T, campaign string, phase promotioncapture.WindowPhase, process string) promotioncapture.WindowCheckpoint {
	t.Helper()
	candidates := make([]promotioncapture.CapturedCandidateWindow, 18)
	for index := range candidates {
		candidates[index] = promotioncapture.CapturedCandidateWindow{
			CandidateID: "fixed", Evaluation: promotioncapture.WindowEvaluation{CandidateID: "fixed", Outcome: promotioncapture.OutcomeNotTested, Fixed: true},
		}
	}
	checkpoint := promotioncapture.WindowCheckpoint{
		Contract: leafPromotionCheckpointContract, SchemaVersion: 1, CampaignID: campaign,
		ProcessInstanceID: process, TrustStateID: "trust", PeerBindingID: "peer",
		Window: promotioncapture.Window{
			WindowID: "window", Phase: phase, StartedAt: "2026-08-11T10:00:00Z", EndedAt: "2026-08-11T10:00:01Z",
			CaptureGeneration: "capture", ProcessInstanceHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			LocalIdentityHash: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			TrustStateHash:    "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			PeerBindingHash:   "sha256:3333333333333333333333333333333333333333333333333333333333333333",
			AdmittedSource:    0x31, EEBusRuntimeEpoch: 1, ConnectionGeneration: 1,
			EBusPollGeneration: "poll", M8NoDrift: true, RollbackExact: true,
		},
		Candidates: candidates, CapturedAt: "2026-08-11T10:00:02Z",
	}
	if err := promotioncapture.BindCheckpointHash(&checkpoint); err != nil {
		t.Fatalf("BindCheckpointHash: %v", err)
	}
	return checkpoint
}
