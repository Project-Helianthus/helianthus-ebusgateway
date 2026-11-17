package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// semanticResourceAllocator is the one resource allocator of a gateway
// process. Every publisher obtains its resource identifiers from it, so one
// anchor names one resource whichever publisher resolves it. The allocator is
// bound to the process's publication hub: a hub is the process boundary of
// every semantic identity, and two hubs never share an allocator.
type semanticResourceAllocator struct {
	allocator *semgraph.Allocator
	store     semgraph.Store

	mu sync.Mutex
	// ordinals caches, per operator slot base, the ordinal of the unit that
	// currently occupies the slot. It is recovered from the store on first use.
	ordinals map[string]uint64
}

// errSemanticResourceAllocatorBound refuses a second composition for a hub.
var errSemanticResourceAllocatorBound = errors.New("semantic resource allocator already bound")

// maxSemanticSlotOrdinal bounds how many units may succeed one another in one
// operator slot.
const maxSemanticSlotOrdinal = 4096

var semanticResourceAllocators = struct {
	mu    sync.Mutex
	byHub map[*semhub.Hub]*semanticResourceAllocator
}{byHub: map[*semhub.Hub]*semanticResourceAllocator{}}

// openSemanticResourceAllocator opens the allocator whose anchor table is kept
// in dir, or an in-memory table when dir is empty.
func openSemanticResourceAllocator(dir string) (*semanticResourceAllocator, error) {
	store, err := semgraph.OpenStore(dir)
	if err != nil {
		return nil, err
	}
	allocator, err := semgraph.NewAllocator(store)
	if err != nil {
		return nil, err
	}
	return &semanticResourceAllocator{allocator: allocator, store: store, ordinals: map[string]uint64{}}, nil
}

// composeSemanticResourceAllocator binds the allocator kept beside the
// runtime state file to hub. An empty runtime state path configures no
// directory, and the table is then kept in memory. A hub is bound once.
func composeSemanticResourceAllocator(hub *semhub.Hub, runtimeStatePath string) (*semanticResourceAllocator, error) {
	if hub == nil {
		return nil, errors.New("semantic resource allocator without a hub")
	}
	dir := ""
	if runtimeStatePath != "" {
		dir = filepath.Dir(runtimeStatePath)
	}
	semanticResourceAllocators.mu.Lock()
	defer semanticResourceAllocators.mu.Unlock()
	if _, bound := semanticResourceAllocators.byHub[hub]; bound {
		return nil, errSemanticResourceAllocatorBound
	}
	resources, err := openSemanticResourceAllocator(dir)
	if err != nil {
		return nil, fmt.Errorf("semantic resource allocator: %w", err)
	}
	semanticResourceAllocators.byHub[hub] = resources
	return resources, nil
}

// semanticResourceAllocatorFor returns the allocator bound to hub, binding an
// in-memory one when the composition configured none.
func semanticResourceAllocatorFor(hub *semhub.Hub) (*semanticResourceAllocator, error) {
	if hub == nil {
		return nil, errors.New("semantic resource allocator without a hub")
	}
	semanticResourceAllocators.mu.Lock()
	defer semanticResourceAllocators.mu.Unlock()
	if resources, bound := semanticResourceAllocators.byHub[hub]; bound {
		return resources, nil
	}
	resources, err := openSemanticResourceAllocator("")
	if err != nil {
		return nil, err
	}
	semanticResourceAllocators.byHub[hub] = resources
	return resources, nil
}

// resolve returns the identifier and anchor record of a resource of kind with
// anchor, and records asset as its one home.
func (r *semanticResourceAllocator) resolve(kind semreg.DefinitionID, anchor semgraph.Anchor, home semreg.AssetID) (semreg.ResourceID, semreg.ResourceAnchor, error) {
	id, record, err := r.allocator.Resolve(kind, anchor)
	if err != nil {
		return "", semreg.ResourceAnchor{}, err
	}
	if _, err := r.allocator.Home(id, home); err != nil {
		return "", semreg.ResourceAnchor{}, err
	}
	return id, record, nil
}

// slotOrdinal returns the ordinal of the unit that occupies the operator slot
// base of kind: the highest ordinal the anchor table holds for it, or 1 when
// none is held. advance moves the slot to the next unit; the new ordinal is
// held once the caller resolves it.
func (r *semanticResourceAllocator) slotOrdinal(kind semreg.DefinitionID, class semreg.AnchorClass, base string, advance bool) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ordinal, cached := r.ordinals[string(kind)+"\x00"+base]
	if !cached {
		ordinal = 1
		for next := uint64(1); next <= maxSemanticSlotOrdinal; next++ {
			probe := semgraph.Anchor{Class: class, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: semanticOrdinalSlot(base, next), Evidence: []semreg.EvidenceRef{semanticAnchorProbeEvidence}}
			key, err := probe.Key(kind)
			if err != nil {
				return 0, err
			}
			_, held, err := r.store.Lookup(key)
			if err != nil {
				return 0, err
			}
			if !held {
				break
			}
			ordinal = next
		}
	}
	if advance {
		if ordinal >= maxSemanticSlotOrdinal {
			return 0, errors.New("semantic resource slot ordinal exhausted")
		}
		ordinal++
	}
	return ordinal, nil
}

// keepSlotOrdinal records the ordinal a committed publication used.
func (r *semanticResourceAllocator) keepSlotOrdinal(kind semreg.DefinitionID, base string, ordinal uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ordinals[string(kind)+"\x00"+base] = ordinal
}

// semanticOrdinalSlot is the operator slot of the ordinal-th unit in base.
func semanticOrdinalSlot(base string, ordinal uint64) string {
	return base + "#" + strconv.FormatUint(ordinal, 10)
}

// semanticAnchorProbeEvidence completes a probe anchor; a probe is only keyed,
// never resolved or published.
var semanticAnchorProbeEvidence = func() semreg.EvidenceRef {
	sum := sha256.Sum256([]byte("helianthus.gateway.anchor.probe"))
	return semreg.EvidenceRef{Owner: "helianthus.gateway", Kind: "gateway.anchor.probe", Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:])), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}()
