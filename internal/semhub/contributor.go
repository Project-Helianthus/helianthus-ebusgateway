package semhub

import (
	"fmt"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// ContributorSpec names one contributor source of an asset. The asset's owner
// lists its contributors when it declares the asset. A contributor publishes
// its own bindings, identity links, observed facts, services and capabilities
// on the asset; the owner keeps the asset claim and fixes its packs.
type ContributorSpec struct {
	SourceID semreg.SourceID
	Driver   string
	// Validators name the contributor's packs. They must be a subset of the
	// asset's packs, because the asset's kernel registry is fixed when the
	// asset is declared.
	Validators []semreg.PackValidator
	// OnControl reprojects the contributor family's cache after a control
	// commit on the asset.
	OnControl func(ControlEvent) error
}

// contributor is one declared contributor of an asset.
type contributor struct {
	source    semreg.SourceID
	driver    string
	packs     []semreg.PackRef
	onControl func(ControlEvent) error
}

// errSharedAssetsDisabled refuses a contributor list on a production hub.
var errSharedAssetsDisabled = fmt.Errorf("%w: several sources per asset are not enabled yet", ErrDeclarationConflict)

// contributorsOf validates the contributor list of spec against the owner's
// source, asset and packs, and returns it ordered by source. It takes no lock.
func contributorsOf(spec AssetSpec, packs []semreg.PackRef) ([]contributor, error) {
	if len(spec.Contributors) == 0 {
		return nil, nil
	}
	held := make(map[semreg.PackRef]bool, len(packs))
	for _, pack := range packs {
		held[pack] = true
	}
	out := make([]contributor, 0, len(spec.Contributors))
	seen := make(map[semreg.SourceID]bool, len(spec.Contributors))
	subset := true
	for _, item := range spec.Contributors {
		if item.Driver == "" || len(item.Validators) == 0 || ValidateOpaque(string(item.SourceID)) != nil || string(item.SourceID) == string(spec.AssetID) {
			return nil, ErrInvalidIdentity
		}
		own := make([]semreg.PackRef, 0, len(item.Validators))
		for _, validator := range item.Validators {
			if validator == nil {
				return nil, ErrInvalidIdentity
			}
			pack := validator.Pack()
			subset = subset && held[pack]
			own = append(own, pack)
		}
		if item.SourceID == spec.SourceID || seen[item.SourceID] {
			return nil, fmt.Errorf("%w: a contributor repeats the owner source or another contributor", ErrDeclarationConflict)
		}
		seen[item.SourceID] = true
		out = append(out, contributor{source: item.SourceID, driver: item.Driver, packs: own, onControl: item.OnControl})
	}
	if !subset {
		return nil, fmt.Errorf("%w: a contributor's packs are not a subset of the asset's packs", ErrDeclarationConflict)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].source < out[j].source })
	return out, nil
}

// sameContributors compares two ordered contributor lists by source, driver
// and packs.
func sameContributors(left, right []contributor) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].source != right[index].source || left[index].driver != right[index].driver || !samePacks(left[index].packs, right[index].packs) {
			return false
		}
	}
	return true
}

// contributorsAvailableLocked reports a collision when another driver, or
// another kind, already holds a contributor source. h.mu must be held.
func (h *Hub) contributorsAvailableLocked(contributors []contributor) error {
	for _, item := range contributors {
		if err := h.claimAvailableLocked(string(item.source), KindSource, item.driver); err != nil {
			return err
		}
	}
	return nil
}

// attachContributorsLocked claims each contributor source for its driver and
// adds the asset to each contributor source's asset set. The claims were
// checked by contributorsAvailableLocked under the same lock. h.mu must be
// held.
func (h *Hub) attachContributorsLocked(target *asset) error {
	for _, item := range target.contributors {
		src, err := h.sourceLocked(item.source, item.driver)
		if err != nil {
			return err
		}
		src.assets[target.id] = struct{}{}
	}
	return nil
}

// publishes reports whether src may publish on the asset: its owner source or
// one of its declared contributors.
func (a *asset) publishes(src semreg.SourceID) bool {
	if a.source == src {
		return true
	}
	for _, item := range a.contributors {
		if item.source == src {
			return true
		}
	}
	return false
}

// publishingDriverLocked returns the driver of the transaction's publishing
// source; without a handle (a restored batch) it is the asset's driver.
// h.mu must be held.
func (h *Hub) publishingDriverLocked(tx *Tx) string {
	if tx.handle != nil {
		if src := h.sources[tx.handle.source]; src != nil {
			return src.driver
		}
	}
	return tx.asset.driver
}

// inferredClaimsLocked enforces, on an asset with contributors, that an
// inferred or operator-origin candidate (one without a binding) belongs to
// the driver that first upserted it: another driver's upsert or withdrawal of
// that candidate identifier is a CollisionError naming both drivers. It
// returns the candidate identifiers the batch creates. Nothing is recorded
// for a single-source asset. h.mu must be held.
func (h *Hub) inferredClaimsLocked(tx *Tx, driver string) ([]semreg.CandidateID, error) {
	if len(tx.asset.contributors) == 0 {
		return nil, nil
	}
	created := []semreg.CandidateID{}
	check := func(id semreg.CandidateID) error {
		if creator, ok := tx.asset.inferred[id]; ok && creator != driver {
			return &CollisionError{Drivers: [2]string{driver, creator}}
		}
		return nil
	}
	for _, candidate := range tx.applied.FactUpserts {
		if err := check(candidate.CandidateID); err != nil {
			return nil, err
		}
		if _, ok := tx.asset.inferred[candidate.CandidateID]; !ok && candidate.BindingID == nil {
			created = append(created, candidate.CandidateID)
		}
	}
	for _, id := range tx.applied.FactWithdrawals {
		if err := check(id); err != nil {
			return nil, err
		}
	}
	return created, nil
}
