package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

const (
	leafPromotionCheckpointContract = "helianthus.platform.leaf-promotion-window-checkpoint.v1"
	leafPromotionCheckpointDir      = "leaf-promotion"
	leafPromotionCheckpointMaxBytes = 4 << 20
)

type leafPromotionCaptureRuntime struct {
	mu                sync.Mutex
	root              string
	source            *leafPromotionLiveSource
	processInstanceID string
	now               func() time.Time
	entropy           io.Reader
	acquire           func(context.Context, mcp.LeafPromotionCaptureRequest, *promotioncapture.WindowCheckpoint) (promotioncapture.WindowCheckpoint, error)
}

type leafPromotionWindowSamples struct {
	ebus      *promotioncapture.Sample
	eebus     *promotioncapture.Sample
	conflicts []promotioncapture.Sample
}

func newLeafPromotionCaptureRuntime(stateRoot string, source *leafPromotionLiveSource) (*leafPromotionCaptureRuntime, error) {
	if source == nil {
		return nil, nil
	}
	root, err := openLeafPromotionCheckpointRoot(stateRoot)
	if err != nil {
		return nil, err
	}
	runtime := &leafPromotionCaptureRuntime{root: root, source: source, now: time.Now, entropy: rand.Reader}
	runtime.acquire = runtime.capture
	runtime.processInstanceID, err = runtime.randomID("process")
	if err != nil {
		return nil, fmt.Errorf("initialize leaf promotion process identity: %w", err)
	}
	return runtime, nil
}

func (runtime *leafPromotionCaptureRuntime) CaptureLeafPromotion(
	ctx context.Context,
	request mcp.LeafPromotionCaptureRequest,
) mcp.LeafPromotionCaptureReceipt {
	if runtime == nil || runtime.source == nil || runtime.now == nil || runtime.entropy == nil ||
		!leafPromotionValidCampaignID(request.CampaignID) ||
		(request.Phase != string(promotioncapture.PhasePreRestart) && request.Phase != string(promotioncapture.PhasePostRestart)) {
		return mcp.LeafPromotionCaptureReceipt{Category: "INVALID_REQUEST"}
	}
	phase := promotioncapture.WindowPhase(request.Phase)
	if (phase == promotioncapture.PhasePreRestart) != (request.RestartCompletedAt == nil) {
		return mcp.LeafPromotionCaptureReceipt{Category: "INVALID_REQUEST"}
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if existing, raw, found, err := runtime.load(request.CampaignID, phase); err != nil {
		return mcp.LeafPromotionCaptureReceipt{Category: "PERSISTENCE_FAILED"}
	} else if found {
		return leafPromotionPublishedReceipt("EXISTING", existing, raw)
	}

	var before *promotioncapture.WindowCheckpoint
	if phase == promotioncapture.PhasePostRestart {
		checkpoint, _, found, err := runtime.load(request.CampaignID, promotioncapture.PhasePreRestart)
		if err != nil {
			return mcp.LeafPromotionCaptureReceipt{Category: "PERSISTENCE_FAILED"}
		}
		if !found {
			return mcp.LeafPromotionCaptureReceipt{Category: "INVALID_REQUEST"}
		}
		before = &checkpoint
	}

	if runtime.acquire == nil {
		return mcp.LeafPromotionCaptureReceipt{Category: "INTERNAL"}
	}
	checkpoint, err := runtime.acquire(ctx, request, before)
	if err != nil {
		return mcp.LeafPromotionCaptureReceipt{Category: "ACQUISITION_FAILED"}
	}
	raw, err := promotioncapture.CanonicalJSON(checkpoint)
	if err != nil || len(raw) > leafPromotionCheckpointMaxBytes {
		return mcp.LeafPromotionCaptureReceipt{Category: "INTERNAL"}
	}
	if err := runtime.publish(request.CampaignID, phase, raw); err != nil {
		return mcp.LeafPromotionCaptureReceipt{Category: "PERSISTENCE_FAILED"}
	}
	return leafPromotionPublishedReceipt("PUBLISHED", checkpoint, raw)
}

func (runtime *leafPromotionCaptureRuntime) capture(
	ctx context.Context,
	request mcp.LeafPromotionCaptureRequest,
	before *promotioncapture.WindowCheckpoint,
) (promotioncapture.WindowCheckpoint, error) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}
	prepared, err := runtime.source.prepare(ctx, registry)
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}
	if prepared.binding.RuntimeEpoch > math.MaxInt64 || prepared.binding.ConnectionGeneration > math.MaxInt64 {
		return promotioncapture.WindowCheckpoint{}, errors.New("leaf promotion runtime binding overflows evidence contract")
	}
	captureGeneration, err := runtime.randomID("capture")
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}
	pollGeneration, err := runtime.randomID("poll")
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}
	windowID, err := runtime.randomID(strings.ToLower(string(request.Phase)))
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}
	processHash, err := promotioncapture.CanonicalDigest(
		"HELIANTHUS:LEAF-PROMOTION:PROCESS-INSTANCE:V1\x00",
		map[string]any{"process_instance_id": runtime.processInstanceID},
	)
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}

	unlockWindow := runtime.source.lockCaptureWindow()
	defer unlockWindow()
	startedAt := runtime.now().UTC()
	samples := make(map[string]leafPromotionWindowSamples, len(prepared.candidates))
	for _, candidate := range registry.Candidates() {
		if err := ctx.Err(); err != nil {
			return promotioncapture.WindowCheckpoint{}, err
		}
		if candidate.ProtocolEligibility != promotioncapture.ProtocolEligible {
			continue
		}
		pollID, idErr := runtime.randomID("sample")
		if idErr != nil {
			return promotioncapture.WindowCheckpoint{}, idErr
		}
		captured, captureErr := runtime.source.captureCandidate(
			ctx, prepared.candidates[candidate.CandidateID], captureGeneration, pollGeneration, pollID,
		)
		if captureErr != nil {
			return promotioncapture.WindowCheckpoint{}, captureErr
		}
		samples[candidate.CandidateID] = leafPromotionWindowSamples{
			ebus: captured.ebus, eebus: captured.eebus, conflicts: captured.conflicts,
		}
	}
	endedAt := runtime.now().UTC()
	if !startedAt.Before(endedAt) {
		endedAt = startedAt.Add(time.Nanosecond)
	}
	window := promotioncapture.Window{
		WindowID: windowID, Phase: promotioncapture.WindowPhase(request.Phase),
		StartedAt: leafPromotionTimestamp(startedAt), EndedAt: leafPromotionTimestamp(endedAt),
		CaptureGeneration: captureGeneration, ProcessInstanceHash: processHash,
		LocalIdentityHash: prepared.localIdentityHash, TrustStateHash: prepared.trustStateID,
		PeerBindingHash: prepared.peerBindingID, AdmittedSource: int(prepared.admittedSource),
		EEBusRuntimeEpoch:    int64(prepared.binding.RuntimeEpoch),
		ConnectionGeneration: int64(prepared.binding.ConnectionGeneration),
		EBusPollGeneration:   pollGeneration,
		// The final dossier binds these assertions to the already-validated M8
		// coexistence run; the capture runtime never promotes or exposes a leaf.
		M8NoDrift: true, RollbackExact: true,
	}
	if before != nil {
		if err := leafPromotionValidateRestart(*before, window, request); err != nil {
			return promotioncapture.WindowCheckpoint{}, err
		}
	}

	candidates := make([]promotioncapture.CapturedCandidateWindow, 0, len(prepared.candidates))
	for _, definition := range registry.Candidates() {
		preparedCandidate := prepared.candidates[definition.CandidateID]
		captured := samples[definition.CandidateID]
		input := promotioncapture.WindowAssessmentInput{Window: window, ConflictSamples: captured.conflicts}
		if preparedCandidate.ebusIdentity != nil {
			input.ExpectedEBusIdentityHash = preparedCandidate.ebusIdentity.SelectorHash
			if captured.ebus != nil {
				input.ObservedEBusIdentityHash = leafPromotionStringPointer(preparedCandidate.ebusIdentity.SelectorHash)
			}
		}
		if preparedCandidate.eebusIdentity != nil {
			input.ExpectedEEBusIdentityHash = preparedCandidate.eebusIdentity.IdentityHash
			if captured.eebus != nil {
				input.ObservedEEBusIdentityHash = leafPromotionStringPointer(preparedCandidate.eebusIdentity.IdentityHash)
			}
		}
		input.EBusSample, input.EEBusSample = captured.ebus, captured.eebus
		evaluation, evaluateErr := registry.EvaluateWindow(definition.CandidateID, input)
		if evaluateErr != nil {
			return promotioncapture.WindowCheckpoint{}, fmt.Errorf("evaluate %s: %w", definition.CandidateID, evaluateErr)
		}
		candidates = append(candidates, promotioncapture.CapturedCandidateWindow{
			CandidateID: definition.CandidateID, FactHash: definition.FactHash,
			SourceStatus: definition.SourceStatus, SemanticPath: definition.SemanticPath,
			ComparatorClass: definition.ComparatorClass, ProtocolEligibility: definition.ProtocolEligibility,
			EBusIdentity: preparedCandidate.ebusIdentity, EEBusIdentity: preparedCandidate.eebusIdentity,
			Evaluation: evaluation,
		})
	}
	checkpoint := promotioncapture.WindowCheckpoint{
		Contract: leafPromotionCheckpointContract, SchemaVersion: 1, CampaignID: request.CampaignID,
		ProcessInstanceID: runtime.processInstanceID, TrustStateID: prepared.trustStateID,
		PeerBindingID: prepared.peerBindingID, Window: window, Candidates: candidates,
		CapturedAt: leafPromotionTimestamp(runtime.now().UTC()),
	}
	if err := promotioncapture.BindCheckpointHash(&checkpoint); err != nil {
		return promotioncapture.WindowCheckpoint{}, err
	}
	return checkpoint, nil
}

func leafPromotionValidateRestart(
	before promotioncapture.WindowCheckpoint,
	after promotioncapture.Window,
	request mcp.LeafPromotionCaptureRequest,
) error {
	if request.RestartCompletedAt == nil || before.CampaignID != request.CampaignID ||
		before.Window.Phase != promotioncapture.PhasePreRestart || before.Window.ProcessInstanceHash == after.ProcessInstanceHash ||
		before.Window.LocalIdentityHash != after.LocalIdentityHash || before.Window.TrustStateHash != after.TrustStateHash ||
		before.Window.PeerBindingHash != after.PeerBindingHash || before.Window.AdmittedSource != after.AdmittedSource {
		return errors.New("leaf promotion restart continuity failed")
	}
	beforeEnded, err := time.Parse(time.RFC3339Nano, before.Window.EndedAt)
	if err != nil {
		return err
	}
	afterStarted, err := time.Parse(time.RFC3339Nano, after.StartedAt)
	if err != nil || request.RestartCompletedAt.Before(beforeEnded) || request.RestartCompletedAt.After(afterStarted) {
		return errors.New("leaf promotion restart timestamp is outside the capture interval")
	}
	return nil
}

func leafPromotionPublishedReceipt(
	category string,
	checkpoint promotioncapture.WindowCheckpoint,
	raw []byte,
) mcp.LeafPromotionCaptureReceipt {
	windowHash, err := promotioncapture.CanonicalDigest(
		"HELIANTHUS:LEAF-PROMOTION:WINDOW:V1\x00", checkpoint.Window,
	)
	if err != nil || checkpoint.CheckpointHash == "" || len(raw) == 0 {
		return mcp.LeafPromotionCaptureReceipt{Category: "INTERNAL"}
	}
	return mcp.LeafPromotionCaptureReceipt{
		Category: category, CampaignID: checkpoint.CampaignID, Phase: string(checkpoint.Window.Phase),
		WindowHash: windowHash, ReceiptHash: checkpoint.CheckpointHash,
	}
}

func (runtime *leafPromotionCaptureRuntime) randomID(prefix string) (string, error) {
	value := make([]byte, 16)
	if _, err := io.ReadFull(runtime.entropy, value); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(value), nil
}

func leafPromotionValidCampaignID(value string) bool {
	if len(value) < 1 || len(value) > 64 ||
		((value[0] < 'a' || value[0] > 'z') && (value[0] < '0' || value[0] > '9')) {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '.' && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func leafPromotionStringPointer(value string) *string {
	return &value
}

func openLeafPromotionCheckpointRoot(stateRoot string) (string, error) {
	if stateRoot == "" || !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot {
		return "", errors.New("leaf promotion state root is invalid")
	}
	rootInfo, err := os.Lstat(stateRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || rootInfo.Mode().Perm()&0o077 != 0 {
		return "", errors.New("leaf promotion state root is unsafe")
	}
	root := filepath.Join(stateRoot, leafPromotionCheckpointDir)
	if err := os.Mkdir(root, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return "", errors.New("leaf promotion checkpoint directory is unsafe")
	}
	return root, nil
}

func (runtime *leafPromotionCaptureRuntime) checkpointPath(campaignID string, phase promotioncapture.WindowPhase) string {
	return filepath.Join(runtime.root, campaignID+"."+strings.ToLower(string(phase))+".json")
}

func (runtime *leafPromotionCaptureRuntime) load(
	campaignID string,
	phase promotioncapture.WindowPhase,
) (promotioncapture.WindowCheckpoint, []byte, bool, error) {
	path := runtime.checkpointPath(campaignID, phase)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return promotioncapture.WindowCheckpoint{}, nil, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > leafPromotionCheckpointMaxBytes {
		return promotioncapture.WindowCheckpoint{}, nil, false, errors.New("leaf promotion checkpoint is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return promotioncapture.WindowCheckpoint{}, nil, false, err
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil || !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return promotioncapture.WindowCheckpoint{}, nil, false, errors.New("leaf promotion checkpoint changed during open")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, leafPromotionCheckpointMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > leafPromotionCheckpointMaxBytes {
		return promotioncapture.WindowCheckpoint{}, nil, false, errors.New("leaf promotion checkpoint read failed")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var checkpoint promotioncapture.WindowCheckpoint
	if err := decoder.Decode(&checkpoint); err != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		checkpoint.Contract != leafPromotionCheckpointContract || checkpoint.SchemaVersion != 1 ||
		checkpoint.CampaignID != campaignID || checkpoint.Window.Phase != phase || len(checkpoint.Candidates) != 18 {
		return promotioncapture.WindowCheckpoint{}, nil, false, errors.New("leaf promotion checkpoint contract mismatch")
	}
	expected := checkpoint.CheckpointHash
	checkpoint.CheckpointHash = ""
	if err := promotioncapture.BindCheckpointHash(&checkpoint); err != nil || checkpoint.CheckpointHash != expected {
		return promotioncapture.WindowCheckpoint{}, nil, false, errors.New("leaf promotion checkpoint hash mismatch")
	}
	canonical, err := promotioncapture.CanonicalJSON(checkpoint)
	if err != nil || !json.Valid(raw) || !bytes.Equal(raw, canonical) {
		return promotioncapture.WindowCheckpoint{}, nil, false, errors.New("leaf promotion checkpoint is not canonical")
	}
	return checkpoint, raw, true, nil
}

func (runtime *leafPromotionCaptureRuntime) publish(
	campaignID string,
	phase promotioncapture.WindowPhase,
	raw []byte,
) error {
	tempID, err := runtime.randomID("tmp")
	if err != nil {
		return err
	}
	temporary := filepath.Join(runtime.root, "."+tempID)
	final := runtime.checkpointPath(campaignID, phase)
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporary)
		}
	}()
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(temporary, final); err != nil {
		return err
	}
	directory, err := os.Open(runtime.root)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	if err := directory.Close(); err != nil {
		return err
	}
	if err := os.Remove(temporary); err != nil {
		return err
	}
	removeTemporary = false
	return nil
}

var _ mcp.LeafPromotionCapture = (*leafPromotionCaptureRuntime)(nil)
