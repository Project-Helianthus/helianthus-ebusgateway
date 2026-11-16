package semhub

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// OperatorEvidenceOwner owns the audit evidence of an operator declaration.
// An identity link whose basis holds a reference of this owner is an operator
// identity declaration.
const OperatorEvidenceOwner semreg.DefinitionID = "helianthus.gateway.configuration"

// ErrOperatorLink refuses, at Commit, a batch whose operator identity links
// differ from the links pending for its asset and source.
var ErrOperatorLink = errors.New("semhub: operator identity links differ from the pending declarations")

// OperatorLink is one operator identity declaration: the binding of the
// declaring source that observes the native resource Native observes
// Resource. Only that binding's source may publish the link, so the link
// travels in the batch of the driver whose binding it names. A declaration
// names the native resource, not a binding identifier, so it survives a
// generation fence: the successor's binding of the same native resource
// carries it. Basis holds the declaration's audit evidence, at least one
// reference of OperatorEvidenceOwner; Key is the optional identity-object
// digest of the unit's native token seen through the binding.
type OperatorLink struct {
	Native   semreg.EvidenceRef
	Resource semreg.ResourceID
	Key      *semreg.Digest
	Basis    []semreg.EvidenceRef
}

// operatorBasis reports whether basis holds operator declaration evidence.
func operatorBasis(basis []semreg.EvidenceRef) bool {
	for _, ref := range basis {
		if ref.Owner == OperatorEvidenceOwner {
			return true
		}
	}
	return false
}

// validOperatorLinks checks a declared set and returns a detached copy of it.
func validOperatorLinks(id semreg.AssetID, links []OperatorLink) ([]OperatorLink, error) {
	out := make([]OperatorLink, 0, len(links))
	seen := make(map[semreg.EvidenceRef]bool, len(links))
	for _, link := range links {
		trial := semreg.IdentityLink{AssetID: id, BindingID: "binding:operator-link-check", State: semreg.LinkQualified, Basis: link.Basis, Revision: "1"}
		if link.Native.Validate() != nil || link.Resource.Validate() != nil || trial.Validate() != nil || !operatorBasis(link.Basis) || seen[link.Native] {
			return nil, ErrInvalidIdentity
		}
		if link.Key != nil && link.Key.Validate() != nil {
			return nil, ErrInvalidIdentity
		}
		seen[link.Native] = true
		copied := OperatorLink{Native: link.Native, Resource: link.Resource, Basis: append([]semreg.EvidenceRef{}, link.Basis...)}
		if link.Key != nil {
			key := *link.Key
			copied.Key = &key
		}
		out = append(out, copied)
	}
	return out, nil
}

// SetOperatorLinks replaces the operator identity declarations of src on the
// asset. src is the asset's owner source or one of its declared contributors.
// Nothing is published here: every later transaction of src on the asset
// exposes the links still to be published through Tx.OperatorLinks, and its
// Commit refuses operator links that differ from them. The declarations stay
// pending while the source's generation is fenced. An empty set withdraws
// every published operator link of src on the asset.
func (h *Hub) SetOperatorLinks(id semreg.AssetID, src semreg.SourceID, links []OperatorLink) error {
	declared, err := validOperatorLinks(id, links)
	if err != nil {
		return err
	}
	h.lock()
	defer h.unlock()
	target := h.assets[id]
	if target == nil {
		return ErrUnknownAsset
	}
	if !target.publishes(src) {
		return fmt.Errorf("%w: the source does not publish on the asset", ErrUnknownSource)
	}
	if target.operatorLinks == nil {
		target.operatorLinks = make(map[semreg.SourceID][]OperatorLink)
	}
	target.operatorLinks[src] = declared
	return nil
}

// OperatorLinks returns the operator identity links the transaction's batch
// must carry to publish the declarations of its source: one link per binding
// of the batch's source, epoch and generation that a declaration names, or
// whose published operator link is no longer declared (a withdrawal). The
// bindings are the batch's binding upserts; the bindings the asset already
// holds current for the transaction's generation are added. A publisher
// replaces its own link of each returned binding with the returned link. A
// binding whose link is already withdrawn takes no link again; its
// successor binding does. Links are ordered by binding.
func (tx *Tx) OperatorLinks(bindings []semreg.NativeBinding) []semreg.IdentityLink {
	if tx == nil || tx.done {
		return []semreg.IdentityLink{}
	}
	h := tx.hub
	h.mu.RLock()
	declared := tx.asset.operatorLinks[tx.handle.source]
	snapshot, _, ok := tx.asset.kernel.Current()
	pending := pendingOperatorLinks(declared, snapshot, ok, tx.asset.id, tx.handle.source, tx.epoch, tx.generationID(), bindings)
	h.mu.RUnlock()
	return pending
}

// pendingOperatorLinks computes the operator links still to be published by
// (asset, src, epoch, generation) from the declared set, the asset's current
// snapshot and the batch's binding upserts.
func pendingOperatorLinks(declared []OperatorLink, snapshot semreg.Snapshot, present bool, id semreg.AssetID, src semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, upserts []semreg.NativeBinding) []semreg.IdentityLink {
	current := make(map[semreg.NativeBindingID]semreg.NativeBinding)
	links := make(map[semreg.NativeBindingID]semreg.IdentityLink)
	own := func(binding semreg.NativeBinding) bool {
		return binding.AssetID == id && binding.SourceID == src && binding.SourceEpochID == epoch && binding.DriverGeneration == generation && binding.State == semreg.BindingCurrent
	}
	if present {
		for _, binding := range snapshot.Bindings {
			if own(binding) {
				current[binding.BindingID] = binding
			}
		}
		for _, link := range snapshot.IdentityLinks {
			links[link.BindingID] = link
		}
	}
	for _, binding := range upserts {
		if own(binding) {
			current[binding.BindingID] = binding
		}
	}
	byNative := make(map[semreg.EvidenceRef]OperatorLink, len(declared))
	for _, link := range declared {
		byNative[link.Native] = link
	}
	out := []semreg.IdentityLink{}
	for bindingID, binding := range current {
		existing, linked := links[bindingID]
		if linked && existing.State == semreg.LinkWithdrawn {
			continue
		}
		declaration, ok := byNative[binding.NativeResource]
		if !ok {
			if linked && operatorBasis(existing.Basis) {
				existing.State, existing.Revision = semreg.LinkWithdrawn, incrementUint(existing.Revision)
				out = append(out, existing)
			}
			continue
		}
		resource := declaration.Resource
		desired := semreg.IdentityLink{AssetID: id, BindingID: bindingID, State: semreg.LinkQualified, Basis: append([]semreg.EvidenceRef{}, declaration.Basis...), Revision: "1", Resource: &resource}
		if declaration.Key != nil {
			key := *declaration.Key
			desired.Key = &key
		}
		if linked {
			desired.Revision = existing.Revision
			if reflect.DeepEqual(existing, desired) {
				continue
			}
			desired.Revision = incrementUint(existing.Revision)
		}
		out = append(out, desired)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BindingID < out[j].BindingID })
	return out
}

// checkOperatorLinksLocked refuses an applied batch whose operator identity
// links differ from the links pending for its source. A link of the batch is
// an operator link when its basis holds operator evidence or when it
// replaces a published operator link that is not withdrawn. A batch that
// carries no operator link is accepted and leaves the pending links pending;
// one that carries any must carry exactly the pending set. The pending set is
// computed from the committed snapshot, which the asset commit lock holds
// still. h.mu must be held.
func (h *Hub) checkOperatorLinksLocked(tx *Tx) error {
	snapshot, _, ok := tx.asset.kernel.Current()
	published := make(map[semreg.NativeBindingID]bool)
	if ok {
		for _, link := range snapshot.IdentityLinks {
			published[link.BindingID] = link.State != semreg.LinkWithdrawn && operatorBasis(link.Basis)
		}
	}
	carried := []semreg.IdentityLink{}
	for _, link := range tx.applied.IdentityLinkUpserts {
		if operatorBasis(link.Basis) || published[link.BindingID] {
			carried = append(carried, link)
		}
	}
	if len(carried) == 0 {
		return nil
	}
	sort.Slice(carried, func(i, j int) bool { return carried[i].BindingID < carried[j].BindingID })
	pending := pendingOperatorLinks(tx.asset.operatorLinks[tx.applied.SourceID], snapshot, ok, tx.asset.id, tx.applied.SourceID, tx.applied.SourceEpochID, tx.applied.DriverGeneration, tx.applied.BindingUpserts)
	if !reflect.DeepEqual(carried, pending) {
		return ErrOperatorLink
	}
	return nil
}
