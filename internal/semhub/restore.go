package semhub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// RecoveryHooks are unused service persistence seams. BeforeControl may refuse
// submission; Accepted reports a committed result and cannot undo acceptance.
// Both receive owned copies outside hub, source and asset locks. Callbacks
// must not reenter publication/control mutations; pending work remains reserved.
type RecoveryHooks struct {
	BeforeControl func(ControlAttempt) error
	Accepted      func(AcceptedBatch)
}

// ControlAttempt contains the exact staged control bytes before submission.
type ControlAttempt struct {
	Control   ControlID
	Asset     semreg.AssetID
	Kind      ControlKind
	Attempt   Attempt
	Monotonic semreg.MonotonicPoint
}

// AcceptedBatch is a complete replay step and its exact accepted result.
// Delivery is at-least-once, keyed by asset, publication key, sequence and
// digest; duplicate deliveries retain the original incarnation and HubSeq.
// Batch is the JSON envelope; Canonical is the kernel's canonical snapshot.
type AcceptedBatch struct {
	Incarnation string
	Batch       []byte
	Monotonic   semreg.MonotonicPoint
	SnapshotID  semreg.SnapshotID
	Canonical   []byte
	Revisions   semreg.RevisionVector
	Cursor      semreg.PublicationCursor
	HubSeq      uint64
}

// RecoveryControl retains the last gate and its attempts as historical data.
// Settlement is never restored as a current acknowledgement.
type RecoveryControl struct {
	Monotonic  semreg.MonotonicPoint
	Abandoned  bool
	Status     ControlStatus
	Kind       ControlKind
	Epoch      semreg.SourceEpochID
	Generation uint64
	Attempts   []Attempt
}

// RecoveryAsset names the declaration preconditions and ordered accepted log.
type RecoveryAsset struct {
	SnapshotID  semreg.SnapshotID
	Revisions   semreg.RevisionVector
	HubSeq      uint64
	CommitCount int
	Asset       semreg.AssetID
	Source      semreg.SourceID
	Driver      string
	Packs       []semreg.PackRef
	Accepted    []AcceptedBatch
	Control     *RecoveryControl
	Failed      bool
}

// RecoveryRecord is a complete in-memory checkpoint. The service owns storage,
// WAL gap resolution, retention and compaction at verified restore points.
type RecoveryRecord struct {
	Version int
	Assets  []RecoveryAsset
	Epochs  []EpochRecord
	// Regimes retains the identity regime of every historically admitted source.
	// An empty family explicitly identifies a hub-native source.
	Regimes map[semreg.SourceID]LegacyFamily
	// Unfixed lists, in ascending order, every source with epoch history whose
	// identity regime is not fixed because each of its admissions was withdrawn.
	// Every source with epoch history appears in exactly one of Regimes and
	// Unfixed, so an omitted regime is detectable.
	Unfixed []semreg.SourceID
	// Drivers names the driver that owns every source in Regimes or Unfixed,
	// so restoring an assetless source keeps its identifier claimed.
	Drivers map[semreg.SourceID]string
}

// ErrRestore reports a failed restore precondition or rejected recovery asset.
var ErrRestore = errors.New("semhub: recovery refused")

// ErrRestoreInProgress refuses a declaration or admission while a restore is
// installing state: restore and admission never interleave.
var ErrRestoreInProgress = errors.New("semhub: restore in progress")

func copyAttempt(in Attempt) Attempt {
	in.Batch = append([]byte(nil), in.Batch...)
	return in
}

func copyAccepted(in AcceptedBatch) AcceptedBatch {
	in.Batch = append([]byte(nil), in.Batch...)
	in.Canonical = append([]byte(nil), in.Canonical...)
	return in
}

func acceptedBatch(raw []byte, mono semreg.MonotonicPoint, snapshot semreg.Snapshot, canonical []byte) AcceptedBatch {
	var batch semreg.PublicationBatch
	// All callers supply an already validated, encoded batch.
	_ = json.Unmarshal(raw, &batch)
	cursor, _ := cursorFor(snapshot, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration)
	return AcceptedBatch{Batch: append([]byte(nil), raw...), Monotonic: mono, SnapshotID: snapshot.SnapshotID,
		Canonical: append([]byte(nil), canonical...), Revisions: snapshot.Revisions, Cursor: cursor}
}

// Export returns detached replay material for the applied publication before
// Commit. It is not proof of acceptance; rejected transactions are never logged.
func (tx *Tx) Export() (AcceptedBatch, error) {
	if tx == nil || tx.done || tx.applied == nil {
		return AcceptedBatch{}, ErrTransaction
	}
	raw, err := json.Marshal(tx.applied)
	if err != nil {
		return AcceptedBatch{}, err
	}
	record := acceptedBatch(raw, tx.monotonic, tx.snapshot, tx.canonical)
	record.Incarnation = tx.hub.incarnation
	return record, nil
}

// replayedAcceptanceLocked returns the stored acceptance of an exact replayed
// batch; a batch without a stored acceptance is not delivered again. h.mu must
// be held.
func replayedAcceptanceLocked(target *asset, draft AcceptedBatch) AcceptedBatch {
	for i := len(target.records) - 1; i >= 0 && len(draft.Batch) > 0; i-- {
		if bytes.Equal(target.records[i].Batch, draft.Batch) {
			return target.records[i]
		}
	}
	return AcceptedBatch{}
}

func (h *Hub) exportAccepted(record AcceptedBatch) {
	if h.recovery != nil && h.recovery.Accepted != nil {
		h.recovery.Accepted(copyAccepted(record))
	}
}

// ExportRecovery copies a coherent checkpoint under bounded synchronization,
// then copies buffers outside the hub lock. It performs no persistence or I/O.
func (h *Hub) ExportRecovery() RecoveryRecord {
	h.mu.RLock()
	record := RecoveryRecord{Version: 1, Regimes: map[semreg.SourceID]LegacyFamily{}}
	// A hub that is still installing a restore has no coherent checkpoint:
	// the zero record (version 0) is returned, which no Restore accepts.
	if h.recovery == nil || h.restoring {
		h.mu.RUnlock()
		return RecoveryRecord{}
	}
	for id, family := range h.restoredRegimes {
		record.Regimes[id] = family
	}
	for id, src := range h.sources {
		if src.admissions > 0 {
			record.Regimes[id] = src.legacy
		}
	}
	for _, target := range h.assets {
		entry := RecoveryAsset{Asset: target.id, Source: target.source, Driver: target.driver,
			Packs: append([]semreg.PackRef(nil), target.packs...), Accepted: append([]AcceptedBatch(nil), target.records...), Failed: target.restoreFailed}
		entry.CommitCount = len(entry.Accepted)
		if len(entry.Accepted) > 0 {
			head := entry.Accepted[len(entry.Accepted)-1]
			entry.SnapshotID, entry.Revisions, entry.HubSeq = head.SnapshotID, head.Revisions, head.HubSeq
		}
		if ca := target.gate; ca != nil && ca.op.kind != "" {
			entry.Control = &RecoveryControl{Monotonic: ca.op.monotonic, Abandoned: ca.op.abandoned, Status: controlStatusLocked(ca), Kind: ca.op.kind, Epoch: ca.op.epoch,
				Generation: ca.op.generation, Attempts: append([]Attempt(nil), ca.attempts...)}
		}
		record.Assets = append(record.Assets, entry)
	}
	record.Epochs = append([]EpochRecord(nil), h.epochHistory...)
	h.mu.RUnlock()
	unfixed := map[semreg.SourceID]bool{}
	for _, epoch := range record.Epochs {
		if _, ok := record.Regimes[epoch.Source]; !ok && !unfixed[epoch.Source] {
			unfixed[epoch.Source] = true
			record.Unfixed = append(record.Unfixed, epoch.Source)
		}
	}
	sort.Slice(record.Unfixed, func(i, j int) bool { return record.Unfixed[i] < record.Unfixed[j] })
	record.Drivers = map[semreg.SourceID]string{}
	h.mu.RLock()
	for id := range record.Regimes {
		record.Drivers[id] = h.ids[string(id)].driver
	}
	for _, id := range record.Unfixed {
		record.Drivers[id] = h.ids[string(id)].driver
	}
	h.mu.RUnlock()
	for i := range record.Assets {
		entry := &record.Assets[i]
		for j := range entry.Accepted {
			entry.Accepted[j] = copyAccepted(entry.Accepted[j])
		}
		if entry.Control != nil {
			for j := range entry.Control.Attempts {
				entry.Control.Attempts[j] = copyAttempt(entry.Control.Attempts[j])
			}
		}
	}
	for i := range record.Epochs {
		record.Epochs[i] = cloneRecord(record.Epochs[i])
	}
	sort.Slice(record.Assets, func(i, j int) bool { return record.Assets[i].Asset < record.Assets[j].Asset })
	return record
}

// Restore replays complete accepted logs on detached empty kernels before any
// admission. Asset failures install no partial snapshot and remain closed;
// independent valid assets can restore but require fresh Resettle callbacks.
// Historical admissions, revocations and activations never recreate handles.
func (h *Hub) Restore(input RecoveryRecord) error {
	if h.recovery == nil {
		return fmt.Errorf("%w: recovery not enabled", ErrRestore)
	}
	// Detach caller memory before retaining anything or invoking validators.
	raw, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("%w: encode record: %v", ErrRestore, err)
	}
	var record RecoveryRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("%w: decode record: %v", ErrRestore, err)
	}
	if record.Version != 1 || validateRecoveryHistory(record) != nil {
		h.mu.RLock()
		closure := h.allRecoveryAssetsLocked(record)
		h.mu.RUnlock()
		h.failRestoreAssets(closure)
		return fmt.Errorf("%w: record version or history", ErrRestore)
	}
	sort.Slice(record.Assets, func(i, j int) bool { return record.Assets[i].Asset < record.Assets[j].Asset })
	// From the coverage scan to the end of installation, no declaration or
	// admission may land: every interleaving would otherwise have to be
	// rechecked individually under the install lock.
	h.lock()
	if h.restoring {
		h.unlock()
		return fmt.Errorf("%w: another restore is in progress", ErrRestore)
	}
	h.restoring = true
	h.unlock()
	defer func() {
		h.lock()
		h.restoring = false
		h.unlock()
	}()
	h.mu.RLock()
	targets := make([]*asset, len(record.Assets))
	sources := map[semreg.SourceID]*source{}
	for i, entry := range record.Assets {
		target := h.assets[entry.Asset]
		if target == nil || i > 0 && entry.Asset == record.Assets[i-1].Asset {
			// An invalid asset ID must not bypass closure of its source's
			// declared empty assets when source coverage cannot be checked.
			closure := h.allRecoveryAssetsLocked(record)
			h.mu.RUnlock()
			h.failRestoreAssets(closure)
			return fmt.Errorf("%w: missing or duplicate asset", ErrRestore)
		}
		targets[i] = target
		sources[target.source] = h.sources[target.source]
	}
	for id := range record.Regimes {
		if src := h.sources[id]; src != nil {
			sources[id] = src
		}
	}
	for _, id := range record.Unfixed {
		if src := h.sources[id]; src != nil {
			sources[id] = src
		}
	}
	// A complete checkpoint covers every declared recovery asset. An
	// omitted entry cannot silently become an empty, readable asset.
	named := make(map[semreg.AssetID]bool, len(record.Assets))
	for _, entry := range record.Assets {
		named[entry.Asset] = true
	}
	missing := false
	for id := range h.assets {
		if !named[id] {
			missing = true
			record.Assets = append(record.Assets, RecoveryAsset{Asset: id})
		}
	}
	h.mu.RUnlock()
	if missing {
		h.failRestoreAssets(record.Assets)
		return fmt.Errorf("%w: incomplete source asset coverage", ErrRestore)
	}
	sourceIDs := make([]semreg.SourceID, 0, len(sources))
	for id := range sources {
		sourceIDs = append(sourceIDs, id)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	for _, id := range sourceIDs {
		sources[id].control.Lock()
	}
	defer func() {
		for i := len(sourceIDs) - 1; i >= 0; i-- {
			sources[sourceIDs[i]].control.Unlock()
		}
	}()
	for _, target := range targets {
		target.commit.Lock()
	}
	defer func() {
		for i := len(targets) - 1; i >= 0; i-- {
			targets[i].commit.Unlock()
		}
	}()
	h.mu.RLock()
	for _, src := range sources {
		if src.admission != 0 {
			h.mu.RUnlock()
			return fmt.Errorf("%w: prior source admission", ErrRestore)
		}
	}
	for _, target := range targets {
		if target.hubSeq != 0 || len(target.records) != 0 || target.restored || target.restoreFailed || sources[target.source].admission != 0 {
			h.mu.RUnlock()
			return fmt.Errorf("%w: populated asset or prior admission", ErrRestore)
		}
	}
	for id, family := range record.Regimes {
		if prior, ok := h.restoredRegimes[id]; ok && prior != family {
			h.mu.RUnlock()
			return fmt.Errorf("%w: conflicting restored identity regime", ErrRestore)
		}
	}
	h.mu.RUnlock()
	h.fault("restore-install", "")
	h.lock()
	// An assetless source holds no control lock, so its claim, its admissions
	// and any earlier restore are checked under the same lock that installs
	// the restored state.
	for id := range record.Drivers {
		_, regime := h.restoredRegimes[id]
		if len(h.restoredEpochs[id]) > 0 || regime {
			h.unlock()
			return fmt.Errorf("%w: source already restored", ErrRestore)
		}
		if src := h.sources[id]; src != nil && src.admission != 0 {
			h.unlock()
			return fmt.Errorf("%w: prior source admission", ErrRestore)
		}
	}
	for id, driver := range record.Drivers {
		if err := h.claimAvailableLocked(string(id), KindSource, driver); err != nil {
			h.unlock()
			// The checkpoint contradicts the declared ownership, so its empty
			// declared assets close as for any other rejected checkpoint.
			h.failRestoreAssets(record.Assets)
			return fmt.Errorf("%w: source owned by another driver: %w", ErrRestore, err)
		}
	}
	if h.restoredRegimes == nil {
		h.restoredRegimes = map[semreg.SourceID]LegacyFamily{}
	}
	for id, family := range record.Regimes {
		h.restoredRegimes[id] = family
	}
	// Restored history keeps its source identifier claimed by the owning
	// driver, even when no declared asset supplies that claim.
	for id, driver := range record.Drivers {
		h.ids[string(id)] = idOwner{driver: driver, kind: KindSource}
	}
	// Historical epochs are reserved in the same critical section, so no
	// admission can reuse one between ownership and reservation. Epoch history
	// is inert and is never appended to the active EpochJournal.
	for _, epoch := range record.Epochs {
		h.epochHistory = append(h.epochHistory, cloneRecord(epoch))
		h.reserveRestoredEpochLocked(epoch.Source, epoch.Epoch)
		if src := h.sources[epoch.Source]; src != nil {
			src.epochs[epoch.Epoch] = true
		}
	}
	h.unlock()
	h.fault("restore-replay", "")
	var failures []error
	for i, entry := range record.Assets {
		if err := h.restoreAsset(targets[i], entry); err != nil {
			failures = append(failures, fmt.Errorf("%w: asset %s: %w", ErrRestore, entry.Asset, err))
		}
	}
	return errors.Join(failures...)
}

func (h *Hub) restoreAsset(target *asset, entry RecoveryAsset) (failure error) {
	defer func() {
		if failure != nil {
			h.lock()
			target.restoreFailed = true
			h.unlock()
		}
	}()
	if entry.Failed || entry.Source != target.source || entry.Driver != target.driver || !samePacks(entry.Packs, target.packs) {
		return errors.New("declaration or failed checkpoint")
	}
	kernel, err := target.kernel.Fork()
	if err != nil {
		return err
	}
	claims := map[string]Kind{}
	var snapshot semreg.Snapshot
	var canonical []byte
	var lastSeq uint64
	positions := map[string]uint64{}
	for _, accepted := range entry.Accepted {
		var batch semreg.PublicationBatch
		if err := json.Unmarshal(accepted.Batch, &batch); err != nil {
			return err
		}
		if batch.AssetID != target.id || batch.SourceID != target.source || accepted.Incarnation == "" || accepted.HubSeq == 0 || accepted.HubSeq <= positions[accepted.Incarnation] {
			return errors.New("batch ownership or commit order")
		}
		// Accepted logs contain mutations once, never rejected attempts or replays.
		if prior, _, ok := kernel.Current(); ok {
			if cursor, found := cursorFor(prior, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration); found && compareDecimal(batch.Sequence, cursor.LastSequence) <= 0 {
				return errors.New("duplicate or regressed accepted step")
			}
		}
		snapshot, canonical, err = kernel.Apply(batch, accepted.Monotonic)
		if err != nil {
			return err
		}
		cursor, found := cursorFor(snapshot, batch.SourceID, batch.SourceEpochID, batch.DriverGeneration)
		if !found || snapshot.SnapshotID != accepted.SnapshotID || !reflect.DeepEqual(snapshot.Revisions, accepted.Revisions) ||
			!reflect.DeepEqual(cursor, accepted.Cursor) || !bytes.Equal(canonical, accepted.Canonical) {
			return errors.New("accepted result changed")
		}
		h.mu.RLock()
		nested, claimErr := h.nestedClaimsLocked(&Tx{asset: target, applied: &batch})
		h.mu.RUnlock()
		if claimErr != nil {
			return claimErr
		}
		for _, claim := range nested {
			if kind, exists := claims[claim.id]; exists && kind != claim.kind {
				return ErrInvalidIdentity
			}
			claims[claim.id] = claim.kind
		}
		lastSeq = accepted.HubSeq
		positions[accepted.Incarnation] = accepted.HubSeq
	}
	if entry.CommitCount != len(entry.Accepted) || entry.SnapshotID != snapshot.SnapshotID || entry.Revisions != snapshot.Revisions || entry.HubSeq != lastSeq {
		return errors.New("incomplete accepted log or terminal checkpoint")
	}
	// An attempt the accepted log already holds was accepted, so a checkpoint
	// that reports it as still outstanding or refused contradicts the log.
	if entry.Control != nil && !semanticSuccess(entry.Control.Status.Semantic) && len(entry.Control.Attempts) > 0 {
		last := entry.Control.Attempts[len(entry.Control.Attempts)-1]
		for _, accepted := range entry.Accepted {
			if bytes.Equal(last.Batch, accepted.Batch) {
				return errors.New("control status contradicts the accepted log")
			}
		}
	}
	if entry.Control != nil && semanticSuccess(entry.Control.Status.Semantic) {
		if len(entry.Control.Attempts) == 0 {
			return errors.New("accepted control lacks an attempt")
		}
		attempt := entry.Control.Attempts[len(entry.Control.Attempts)-1]
		matched := false
		for _, accepted := range entry.Accepted {
			if bytes.Equal(attempt.Batch, accepted.Batch) {
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("control acceptance lacks an accepted replay step")
		}
	}
	gate, err := restoredGate(target, entry.Control, snapshot, canonical, lastSeq)
	if err != nil {
		return err
	}
	h.lock()
	defer h.unlock()
	// Recheck cross-asset identity claims at the install point.
	for id, kind := range claims {
		if err := h.claimAvailableLocked(id, kind, target.driver); err != nil {
			return err
		}
	}
	for id, kind := range claims {
		h.ids[id] = idOwner{driver: target.driver, kind: kind}
	}
	for _, cursor := range snapshot.Cursors {
		h.sources[target.source].epochs[cursor.SourceEpochID] = true
		h.reserveRestoredEpochLocked(target.source, cursor.SourceEpochID)
	}
	target.kernel, target.hubSeq, target.records = kernel, lastSeq, entry.Accepted
	target.gate, target.restored = gate, true
	return nil
}

// reserveRestoredEpochLocked records one restored epoch for a source, whether or
// not this hub has a record of the source yet. h.mu must be held.
func (h *Hub) reserveRestoredEpochLocked(source semreg.SourceID, epoch semreg.SourceEpochID) {
	if h.restoredEpochs == nil {
		h.restoredEpochs = map[semreg.SourceID]map[semreg.SourceEpochID]bool{}
	}
	if h.restoredEpochs[source] == nil {
		h.restoredEpochs[source] = map[semreg.SourceEpochID]bool{}
	}
	h.restoredEpochs[source][epoch] = true
}

func restoredGate(target *asset, control *RecoveryControl, snapshot semreg.Snapshot, canonical []byte, seq uint64) (*controlAsset, error) {
	op := &controlOp{src: newSource(target.source, target.driver)}
	ca := &controlAsset{op: op, asset: target, semantic: SemanticAccepted, reprojection: ReprojectionUnsettled,
		snapshot: snapshot, canonical: canonical, hubSeq: seq}
	if control == nil {
		return ca, nil
	}
	if control.Status.ID == nil || control.Status.ID.Incarnation == "" || control.Status.ID.N == 0 || control.Status.ID.Source != target.source || ValidateOpaque(string(control.Epoch)) != nil || (control.Kind != ControlFence && control.Kind != ControlRetire) {
		return nil, errors.New("invalid control identity")
	}
	switch control.Status.Semantic {
	// A revoked-unpublished asset never becomes a gate, so no exported
	// checkpoint carries that outcome.
	case SemanticAccepted, SemanticReplayed, SemanticUnbuilt, SemanticPending, SemanticRejected, SemanticUncertain:
	default:
		return nil, errors.New("invalid semantic outcome")
	}
	if control.Status.Attempts != len(control.Attempts) {
		return nil, errors.New("incomplete control attempts")
	}
	if len(control.Attempts) > 0 && control.Monotonic.Validate() != nil {
		return nil, errors.New("invalid control monotonic point")
	}
	for _, attempt := range control.Attempts {
		var batch semreg.PublicationBatch
		if err := json.Unmarshal(attempt.Batch, &batch); err != nil {
			return nil, err
		}
		digest, err := batch.ComputedDigest()
		if err != nil || digest != attempt.Digest || batch.BatchDigest != digest || batch.Sequence != attempt.Sequence || batch.AssetID != target.id || batch.SourceID != target.source || batch.SourceEpochID != control.Epoch {
			return nil, errors.New("corrupt control attempt")
		}
		if control.Kind == ControlFence && (batch.DriverGeneration != semreg.Uint64(strconv.FormatUint(control.Generation, 10)) || len(batch.GenerationFences) == 0) {
			return nil, errors.New("control fence metadata changed")
		}
		if control.Kind == ControlRetire && len(batch.SourceRetirements) == 0 {
			return nil, errors.New("control retirement metadata changed")
		}
	}
	op.monotonic, op.abandoned = control.Monotonic, control.Abandoned
	op.id, op.kind, op.epoch, op.generation = *control.Status.ID, control.Kind, control.Epoch, control.Generation
	ca.semantic, ca.errID, ca.attempts = control.Status.Semantic, control.Status.ErrorID, control.Attempts
	return ca, nil
}

// outsideControlLocks releases the caller's source/asset serialization for an
// export callback. The gate and exporting flag reserve the exact attempt, so a
// competing retry cannot submit it before the write-ahead callback completes.
func (h *Hub) outsideControlLocks(ca *controlAsset, callback func()) {
	h.lock()
	ca.exporting = true
	h.unlock()
	ca.asset.commit.Unlock()
	ca.op.src.control.Unlock()
	defer func() {
		ca.op.src.control.Lock()
		ca.asset.commit.Lock()
		h.lock()
		ca.exporting = false
		h.unlock()
	}()
	callback()
}

// allRecoveryAssetsLocked closes every declared asset on checkpoint rejection.
// h.mu must be held for reading.
func (h *Hub) allRecoveryAssetsLocked(record RecoveryRecord) []RecoveryAsset {
	closure := append([]RecoveryAsset(nil), record.Assets...)
	for id := range h.assets {
		closure = append(closure, RecoveryAsset{Asset: id})
	}
	return closure
}

// validateRecoveryHistory rejects malformed history before replay or reservation.
func validateRecoveryHistory(record RecoveryRecord) error {
	for id, family := range record.Regimes {
		if ValidateOpaque(string(id)) != nil || family != "" && !family.valid() {
			return errors.New("invalid source identity regime")
		}
	}
	unfixed := map[semreg.SourceID]bool{}
	for i, id := range record.Unfixed {
		if _, fixed := record.Regimes[id]; ValidateOpaque(string(id)) != nil || fixed || i > 0 && id <= record.Unfixed[i-1] {
			return errors.New("invalid unfixed source identity regime")
		}
		unfixed[id] = true
	}
	for id, driver := range record.Drivers {
		if _, fixed := record.Regimes[id]; driver == "" || !fixed && !unfixed[id] {
			return errors.New("invalid source driver ownership")
		}
	}
	for id := range record.Regimes {
		if record.Drivers[id] == "" {
			return errors.New("missing source driver ownership")
		}
	}
	for id := range unfixed {
		if record.Drivers[id] == "" {
			return errors.New("missing source driver ownership")
		}
	}
	history := map[semreg.SourceID]bool{}
	assets := map[semreg.AssetID]semreg.SourceID{}
	for _, entry := range record.Assets {
		assets[entry.Asset] = entry.Source
	}
	for _, epoch := range record.Epochs {
		if ValidateOpaque(epoch.Incarnation) != nil || ValidateOpaque(string(epoch.Source)) != nil || ValidateOpaque(string(epoch.Epoch)) != nil || epoch.Generation == 0 {
			return errors.New("invalid epoch identity")
		}
		// A withdrawn first admission leaves history behind and returns the
		// source to an unfixed regime, which the checkpoint must say explicitly.
		// Published entries (below) require the regime that produced them.
		if _, fixed := record.Regimes[epoch.Source]; !fixed && !unfixed[epoch.Source] {
			return errors.New("missing source identity regime")
		}
		history[epoch.Source] = true
		switch epoch.Kind {
		case EventEpochAdmitted, EventGenerationActivated:
			if len(epoch.Assets) != 0 {
				return errors.New("unexpected epoch assets")
			}
		case EventGenerationFenced, EventGenerationRevoked, EventSourceRetired:
		default:
			return errors.New("invalid epoch event kind")
		}
		for i, id := range epoch.Assets {
			if ValidateOpaque(string(id)) != nil || assets[id] != epoch.Source || i > 0 && id <= epoch.Assets[i-1] {
				return errors.New("invalid epoch assets")
			}
		}
	}
	if err := validateRecoverySuccessions(record); err != nil {
		return err
	}
	// Withdrawal retires the admitted epoch, so an unfixed source has no
	// admitted epoch left unretired; a current admission keeps its regime.
	admitted := map[semreg.SourceID]map[semreg.SourceEpochID]bool{}
	retired := map[semreg.SourceID]map[semreg.SourceEpochID]bool{}
	// History is in journal order: a retirement only retires an epoch that an
	// earlier record admitted.
	for _, epoch := range record.Epochs {
		switch epoch.Kind {
		case EventEpochAdmitted:
			if admitted[epoch.Source] == nil {
				admitted[epoch.Source] = map[semreg.SourceEpochID]bool{}
			}
			admitted[epoch.Source][epoch.Epoch] = true
			// A withdrawn epoch may be admitted again; the later admission is
			// the current one and supersedes the earlier retirement.
			delete(retired[epoch.Source], epoch.Epoch)
		case EventSourceRetired:
			if !admitted[epoch.Source][epoch.Epoch] {
				continue
			}
			if retired[epoch.Source] == nil {
				retired[epoch.Source] = map[semreg.SourceEpochID]bool{}
			}
			retired[epoch.Source][epoch.Epoch] = true
		}
	}
	published := map[semreg.SourceID]bool{}
	for _, entry := range record.Assets {
		if len(entry.Accepted) > 0 {
			published[entry.Source] = true
		}
	}
	// A regime is fixed by an admission: its history, or the accepted
	// publications that only an admitted source can produce.
	for id := range record.Regimes {
		if len(admitted[id]) == 0 && !published[id] {
			return errors.New("source identity regime without an admission")
		}
	}
	for _, id := range record.Unfixed {
		if !history[id] || len(admitted[id]) == 0 {
			return errors.New("unfixed source without an admission")
		}
		for epoch := range admitted[id] {
			if !retired[id][epoch] {
				return errors.New("unfixed source with a current admission")
			}
		}
	}
	for _, entry := range record.Assets {
		if len(entry.Accepted) > 0 {
			if _, ok := record.Regimes[entry.Source]; !ok {
				return errors.New("missing source identity regime")
			}
		}
	}
	return nil
}

// Succession pairs are adjacent because the journal appends them atomically.
// Counters belong to a source within an incarnation, not to restored authority.
func validateRecoverySuccessions(record RecoveryRecord) error {
	type sourceIncarnation struct {
		source      semreg.SourceID
		incarnation string
	}
	ordinals := map[sourceIncarnation]uint64{}
	current := map[sourceIncarnation]semreg.SourceEpochID{}
	retired := map[sourceIncarnation]EpochRecord{}
	for i, epoch := range record.Epochs {
		key := sourceIncarnation{epoch.Source, epoch.Incarnation}
		if epoch.Ordinal != 0 {
			if family, fixed := record.Regimes[epoch.Source]; !fixed || family != "" || len(epoch.Assets) != 0 {
				return errors.New("invalid succession regime or assets")
			}
			switch epoch.Kind {
			case EventSourceRetired:
				if i+1 == len(record.Epochs) {
					return errors.New("unpaired succession retirement")
				}
				next := record.Epochs[i+1]
				prior := retired[key]
				if next.Kind != EventEpochAdmitted || next.Source != epoch.Source || next.Incarnation != epoch.Incarnation || next.Ordinal != epoch.Ordinal || next.Generation != 1 ||
					next.Epoch != successorEpoch(epoch.Incarnation, epoch.Source, epoch.Ordinal) || next.Epoch == epoch.Epoch ||
					current[key] != epoch.Epoch || prior.Epoch != epoch.Epoch || prior.Generation != epoch.Generation ||
					ordinals[key] == ^uint64(0) || epoch.Ordinal != ordinals[key]+1 {
					return errors.New("invalid succession pair or ordinal")
				}
				ordinals[key] = epoch.Ordinal
			case EventEpochAdmitted:
				if i == 0 || record.Epochs[i-1].Kind != EventSourceRetired || record.Epochs[i-1].Ordinal != epoch.Ordinal {
					return errors.New("unpaired succession admission")
				}
			default:
				return errors.New("unexpected epoch ordinal")
			}
		}
		switch epoch.Kind {
		case EventEpochAdmitted:
			current[key] = epoch.Epoch
			delete(retired, key)
		case EventSourceRetired:
			retired[key] = epoch
		}
	}
	return nil
}

func (h *Hub) failRestoreAssets(entries []RecoveryAsset) {
	h.lock()
	defer h.unlock()
	for _, entry := range entries {
		if target := h.assets[entry.Asset]; target != nil && target.hubSeq == 0 && len(target.records) == 0 && h.sources[target.source].admission == 0 {
			target.restoreFailed = true
		}
	}
}
