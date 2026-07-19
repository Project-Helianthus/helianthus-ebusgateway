package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"sort"
	"sync"
	"time"
)

const (
	eebusV1ActiveTTL        = 5 * time.Minute
	eebusV1MaxActive        = 32
	eebusV1TombstoneTTL     = 5 * time.Minute
	eebusV1MaxTombstones    = 256
	eebusV1ReferenceVersion = 1
)

type eebusV1EvidenceRefsV1 struct {
	RuntimeStatusRef string `json:"runtime_status_ref"`
	ServicesListRef  string `json:"services_list_ref"`
	ServicesGetRef   string `json:"services_get_ref"`
	SessionsListRef  string `json:"sessions_list_ref"`
	SessionsGetRef   string `json:"sessions_get_ref"`
	TopologyRef      string `json:"topology_ref"`
	PairingStatusRef string `json:"pairing_status_ref"`
}

type eebusV1CapturedRootV1 struct {
	SnapshotRef         string                `json:"snapshot_ref"`
	ExpiresAt           string                `json:"expires_at"`
	SnapshotContentHash string                `json:"snapshot_content_hash"`
	EvidenceRefs        eebusV1EvidenceRefsV1 `json:"evidence_refs"`
	Snapshot            eebusV1SnapshotDataV1 `json:"snapshot"`
}

type eebusV1DropResultV1 struct {
	Status string `json:"status"`
}

type eebusV1ReferenceBinding struct {
	Version    int
	RuntimeKey string
	Contract   eebusV1ContractV1
	Tool       string
	Scope      string
	MaskTier   string
	AuthScope  string
}

func (binding eebusV1ReferenceBinding) matches(tool, scope string) bool {
	return binding.Version == eebusV1ReferenceVersion &&
		binding.Contract == eebusV1Contract &&
		binding.Tool == tool &&
		binding.Scope == scope &&
		binding.MaskTier == eebusV1MaskTier &&
		binding.AuthScope == eebusV1AuthScope &&
		binding.RuntimeKey != ""
}

type eebusV1StoredReference struct {
	RootToken string
	Root      bool
	Binding   eebusV1ReferenceBinding
}

type eebusV1ActiveRoot struct {
	RootToken  string
	RootBytes  [sha256.Size]byte
	ExpiresAt  time.Time
	Projection eebusV1Projection
	Captured   eebusV1CapturedRootV1
	References map[string]eebusV1StoredReference
}

type eebusV1TombstoneRoot struct {
	RootToken  string
	RootBytes  [sha256.Size]byte
	TerminalAt time.Time
	Runtime    eebusV1RuntimeStatusDataV1
	Timestamp  string
	References map[string]eebusV1StoredReference
}

type eebusV1SnapshotStore struct {
	mu      sync.Mutex
	now     func() time.Time
	entropy io.Reader

	activeRoots     map[string]*eebusV1ActiveRoot
	activeTokens    map[string]eebusV1StoredReference
	tombstoneRoots  map[string]*eebusV1TombstoneRoot
	tombstoneTokens map[string]eebusV1StoredReference
}

type eebusV1LookupResult struct {
	Projection *eebusV1Projection
	Runtime    eebusV1RuntimeStatusDataV1
	Timestamp  string
	ErrorCode  string
}

type eebusV1ReferenceSpec struct {
	Key   string
	Tool  string
	Scope string
	Root  bool
}

var eebusV1CaptureReferenceSpecs = []eebusV1ReferenceSpec{
	{Key: "snapshot_ref", Tool: eebusV1SnapshotCaptureTool, Scope: "whole-root", Root: true},
	{Key: "runtime_status_ref", Tool: eebusV1RuntimeStatusTool, Scope: "runtime-status"},
	{Key: "services_list_ref", Tool: eebusV1ServicesListTool, Scope: "services"},
	{Key: "services_get_ref", Tool: eebusV1ServicesGetTool, Scope: "service"},
	{Key: "sessions_list_ref", Tool: eebusV1SessionsListTool, Scope: "sessions"},
	{Key: "sessions_get_ref", Tool: eebusV1SessionsGetTool, Scope: "session"},
	{Key: "topology_ref", Tool: eebusV1TopologyGetTool, Scope: "topology"},
	{Key: "pairing_status_ref", Tool: eebusV1PairingStatusTool, Scope: "pairing-status"},
}

func newEEBusV1SnapshotStore(now func() time.Time, entropy io.Reader) *eebusV1SnapshotStore {
	return &eebusV1SnapshotStore{
		now:             now,
		entropy:         entropy,
		activeRoots:     make(map[string]*eebusV1ActiveRoot),
		activeTokens:    make(map[string]eebusV1StoredReference),
		tombstoneRoots:  make(map[string]*eebusV1TombstoneRoot),
		tombstoneTokens: make(map[string]eebusV1StoredReference),
	}
}

func (store *eebusV1SnapshotStore) capture(projection eebusV1Projection) (eebusV1CapturedRootV1, string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now()
	store.purgeLocked(now)
	if len(store.activeRoots) >= eebusV1MaxActive {
		return eebusV1CapturedRootV1{}, "quota_exceeded"
	}

	tokens := make(map[string]string, len(eebusV1CaptureReferenceSpecs))
	decoded := make(map[string][sha256.Size]byte, len(eebusV1CaptureReferenceSpecs))
	reserved := make(map[string]struct{}, len(eebusV1CaptureReferenceSpecs))
	for _, spec := range eebusV1CaptureReferenceSpecs {
		token, raw, err := store.mintTokenLocked(reserved)
		if err != nil {
			return eebusV1CapturedRootV1{}, "contract_violation"
		}
		tokens[spec.Key] = token
		decoded[spec.Key] = raw
		reserved[token] = struct{}{}
	}

	expiresAt := now.Add(eebusV1ActiveTTL)
	captured := eebusV1CapturedRootV1{
		SnapshotRef:         tokens["snapshot_ref"],
		ExpiresAt:           eebusV1Timestamp(expiresAt),
		SnapshotContentHash: projection.Snapshot.Meta.DataHash,
		EvidenceRefs: eebusV1EvidenceRefsV1{
			RuntimeStatusRef: tokens["runtime_status_ref"],
			ServicesListRef:  tokens["services_list_ref"],
			ServicesGetRef:   tokens["services_get_ref"],
			SessionsListRef:  tokens["sessions_list_ref"],
			SessionsGetRef:   tokens["sessions_get_ref"],
			TopologyRef:      tokens["topology_ref"],
			PairingStatusRef: tokens["pairing_status_ref"],
		},
		Snapshot: projection.Snapshot,
	}
	root := &eebusV1ActiveRoot{
		RootToken:  captured.SnapshotRef,
		RootBytes:  decoded["snapshot_ref"],
		ExpiresAt:  expiresAt,
		Projection: projection,
		Captured:   captured,
		References: make(map[string]eebusV1StoredReference, len(tokens)),
	}
	for _, spec := range eebusV1CaptureReferenceSpecs {
		token := tokens[spec.Key]
		reference := eebusV1StoredReference{
			RootToken: root.RootToken,
			Root:      spec.Root,
			Binding: eebusV1ReferenceBinding{
				Version: eebusV1ReferenceVersion, RuntimeKey: projection.RuntimeKey,
				Contract: eebusV1Contract, Tool: spec.Tool, Scope: spec.Scope,
				MaskTier: eebusV1MaskTier, AuthScope: eebusV1AuthScope,
			},
		}
		root.References[token] = reference
	}
	store.activeRoots[root.RootToken] = root
	for token, reference := range root.References {
		store.activeTokens[token] = reference
	}
	return captured, ""
}

func (store *eebusV1SnapshotStore) lookup(token, tool, scope string) eebusV1LookupResult {
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now()
	store.purgeLocked(now)

	if reference, exists := store.activeTokens[token]; exists {
		root := store.activeRoots[reference.RootToken]
		if root == nil {
			return eebusV1LookupResult{ErrorCode: "contract_violation"}
		}
		if !reference.Binding.matches(tool, scope) {
			return eebusV1LookupResult{Runtime: root.Projection.Runtime, Timestamp: root.Projection.DataTimestamp, ErrorCode: "permission_denied"}
		}
		projection := root.Projection
		return eebusV1LookupResult{Projection: &projection, Runtime: projection.Runtime, Timestamp: projection.DataTimestamp}
	}
	if reference, exists := store.tombstoneTokens[token]; exists {
		root := store.tombstoneRoots[reference.RootToken]
		if root == nil {
			return eebusV1LookupResult{ErrorCode: "contract_violation"}
		}
		if !reference.Binding.matches(tool, scope) {
			return eebusV1LookupResult{Runtime: root.Runtime, Timestamp: root.Timestamp, ErrorCode: "permission_denied"}
		}
		return eebusV1LookupResult{Runtime: root.Runtime, Timestamp: root.Timestamp, ErrorCode: "snapshot_gone"}
	}
	return eebusV1LookupResult{ErrorCode: "not_found"}
}

func (store *eebusV1SnapshotStore) drop(token string) eebusV1DropResultV1 {
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now()
	store.purgeLocked(now)

	reference, exists := store.activeTokens[token]
	if !exists || !reference.Root {
		return eebusV1DropResultV1{Status: "already_gone"}
	}
	root := store.activeRoots[reference.RootToken]
	if root == nil || root.RootToken != token {
		return eebusV1DropResultV1{Status: "already_gone"}
	}
	store.terminalizeLocked(root, now)
	store.enforceTombstoneBoundLocked()
	return eebusV1DropResultV1{Status: "dropped"}
}

func (store *eebusV1SnapshotStore) mintTokenLocked(reserved map[string]struct{}) (string, [sha256.Size]byte, error) {
	var raw [sha256.Size]byte
	for attempts := 0; attempts < 1024; attempts++ {
		if _, err := io.ReadFull(store.entropy, raw[:]); err != nil {
			return "", [sha256.Size]byte{}, err
		}
		token := base64.RawURLEncoding.EncodeToString(raw[:])
		if _, exists := store.activeTokens[token]; exists {
			continue
		}
		if _, exists := store.tombstoneTokens[token]; exists {
			continue
		}
		if _, exists := reserved[token]; exists {
			continue
		}
		return token, raw, nil
	}
	return "", [sha256.Size]byte{}, errors.New("reference entropy repeated")
}

func (store *eebusV1SnapshotStore) purgeLocked(now time.Time) {
	for rootToken, tombstone := range store.tombstoneRoots {
		if !now.Before(tombstone.TerminalAt.Add(eebusV1TombstoneTTL)) {
			store.removeTombstoneLocked(rootToken)
		}
	}

	expired := make([]*eebusV1ActiveRoot, 0)
	for _, root := range store.activeRoots {
		if !now.Before(root.ExpiresAt) {
			expired = append(expired, root)
		}
	}
	sort.Slice(expired, func(i, j int) bool {
		return bytes.Compare(expired[i].RootBytes[:], expired[j].RootBytes[:]) < 0
	})
	for _, root := range expired {
		store.terminalizeLocked(root, now)
	}
	store.enforceTombstoneBoundLocked()
}

func (store *eebusV1SnapshotStore) terminalizeLocked(root *eebusV1ActiveRoot, terminalAt time.Time) {
	if root == nil {
		return
	}
	delete(store.activeRoots, root.RootToken)
	for token := range root.References {
		delete(store.activeTokens, token)
	}
	tombstone := &eebusV1TombstoneRoot{
		RootToken:  root.RootToken,
		RootBytes:  root.RootBytes,
		TerminalAt: terminalAt,
		Runtime:    root.Projection.Runtime,
		Timestamp:  root.Projection.DataTimestamp,
		References: root.References,
	}
	store.tombstoneRoots[tombstone.RootToken] = tombstone
	for token, reference := range tombstone.References {
		store.tombstoneTokens[token] = reference
	}
}

func (store *eebusV1SnapshotStore) enforceTombstoneBoundLocked() {
	for len(store.tombstoneRoots) > eebusV1MaxTombstones {
		var oldest *eebusV1TombstoneRoot
		for _, candidate := range store.tombstoneRoots {
			if oldest == nil || candidate.TerminalAt.Before(oldest.TerminalAt) ||
				(candidate.TerminalAt.Equal(oldest.TerminalAt) && bytes.Compare(candidate.RootBytes[:], oldest.RootBytes[:]) < 0) {
				oldest = candidate
			}
		}
		if oldest == nil {
			return
		}
		store.removeTombstoneLocked(oldest.RootToken)
	}
}

func (store *eebusV1SnapshotStore) removeTombstoneLocked(rootToken string) {
	root := store.tombstoneRoots[rootToken]
	if root == nil {
		return
	}
	delete(store.tombstoneRoots, rootToken)
	for token := range root.References {
		delete(store.tombstoneTokens, token)
	}
}

func eebusV1ParseCanonicalToken(value any) (string, bool) {
	token, ok := value.(string)
	if !ok || len(token) != 43 {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != sha256.Size {
		return "", false
	}
	if base64.RawURLEncoding.EncodeToString(decoded) != token {
		return "", false
	}
	return token, true
}
