// Graph cuts of the hub.
//
// A graph cut reads the per-asset snapshots of every declared asset together:
// one capture under the hub read lock, the same lock and admission rules as
// Capture, so no commit lands between two of its reads and no asset is read
// twice. The cut is then assembled outside the lock by SemReg's pure
// AssembleCut with the element catalog compiled into the process. There is no
// global revision: the cut's identity is the vector of its member snapshot
// identifiers, and a reader that needs to know whether anything changed
// compares cut identifiers. A declared asset the capture does not admit is
// not a member; the cut lists it with its reason and is partial. The public
// read surfaces still read per-asset captures; they adopt cuts with the
// retrieval work.

package semhub

import (
	"context"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

// CutNotIncludedReason tells why a declared asset is not an included member
// of a graph cut.
type CutNotIncludedReason string

// CutNotIncludedReason values.
const (
	// CutSourceUnavailable is an asset none of whose publishing sources is
	// admitted, live, unfenced and of an unretired epoch.
	CutSourceUnavailable CutNotIncludedReason = "source_unavailable"
	// CutPublicationWithheld is an asset whose source is available but which
	// has no publication the hub exposes: its read is not admitted, it has
	// published nothing, its current generation has not published, or its
	// snapshot holds private evidence.
	CutPublicationWithheld CutNotIncludedReason = "publication_withheld"
	// CutExcluded is an asset that was read but that the cut excluded for a
	// duplicate identity finding.
	CutExcluded CutNotIncludedReason = "excluded"
)

// CutNotIncluded is one declared asset a graph cut does not include, with its
// one reason.
type CutNotIncluded struct {
	Asset  semreg.AssetID       `json:"asset"`
	Reason CutNotIncludedReason `json:"reason"`
}

// GraphCut is one graph cut of the hub: the cut, its canonical bytes and every
// declared asset it does not include, in bytewise asset order. The list is
// empty, never nil, when the cut includes every declared asset.
type GraphCut struct {
	Cut         *graph.Cut
	Canonical   []byte
	NotIncluded []CutNotIncluded
}

// CaptureCut reads every declared asset in one coherent capture and assembles
// one graph cut from the snapshots the capture admits. The selection is the
// set of declared assets at the instant of the read, taken under the same
// read lock. Completeness is asserted complete only when every declared asset
// was admitted; the assembly makes the cut partial when it excludes a member.
// The snapshots are not evaluated, so the cut reads no clock. More declared
// assets than one cut reads is bounds_exceeded.
func (h *Hub) CaptureCut(ctx context.Context) (GraphCut, error) {
	if h == nil {
		return GraphCut{}, ErrCaptureUnavailable
	}
	if err := ctx.Err(); err != nil {
		return GraphCut{}, err
	}
	catalog, err := graph.Embedded()
	if err != nil {
		return GraphCut{}, err
	}
	capture, err := h.captureDeclared(ctx)
	if err != nil {
		return GraphCut{}, err
	}
	snapshots := make([]semreg.Snapshot, 0, len(capture.Assets))
	reasons := map[semreg.AssetID]CutNotIncludedReason{}
	for _, item := range capture.Assets {
		switch {
		case item.Snapshot != nil:
			snapshots = append(snapshots, *item.Snapshot)
		case !item.SourceAdmitted:
			reasons[item.AssetID] = CutSourceUnavailable
		default:
			reasons[item.AssetID] = CutPublicationWithheld
		}
	}
	completeness := graph.CutComplete
	if len(reasons) != 0 {
		completeness = graph.CutPartial
	}
	cut, err := graph.AssembleCut(snapshots, catalog, completeness)
	if err != nil {
		return GraphCut{}, err
	}
	canonical, err := cutCanonical(cut)
	if err != nil {
		return GraphCut{}, err
	}
	for _, id := range cut.Excluded() {
		reasons[id] = CutExcluded
	}
	notIncluded := make([]CutNotIncluded, 0, len(reasons))
	for id, reason := range reasons {
		notIncluded = append(notIncluded, CutNotIncluded{Asset: id, Reason: reason})
	}
	sort.Slice(notIncluded, func(i, j int) bool { return notIncluded[i].Asset < notIncluded[j].Asset })
	return GraphCut{Cut: cut, Canonical: canonical, NotIncluded: notIncluded}, nil
}

// cutCanonical is the canonical encoding of the cut's result. The result's
// detached accessors return nil for an empty collection, which the result's
// own validation refuses, so every collection is an empty list before it is
// encoded; a cut whose collections are all non-empty encodes to the bytes of
// Cut.CanonicalJSON.
func cutCanonical(cut *graph.Cut) ([]byte, error) {
	result := cut.Result()
	if result.Members == nil {
		result.Members = []graph.CutMember{}
	}
	if result.Excluded == nil {
		result.Excluded = []semreg.AssetID{}
	}
	if result.Findings == nil {
		result.Findings = []graph.CutFinding{}
	}
	if result.Relations == nil {
		result.Relations = []graph.CutRelation{}
	}
	return semreg.CanonicalJSON(result)
}

// captureDeclared selects every declared asset and captures it under one hub
// read lock, so no declaration or commit lands between the selection and the
// reads.
func (h *Hub) captureDeclared(ctx context.Context) (CoherentCapture, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.assets) > graph.MaxCutMembers {
		return CoherentCapture{}, &semreg.Error{ID: semreg.BoundsExceeded, Detail: "more declared assets than one graph cut reads"}
	}
	ids := make([]semreg.AssetID, 0, len(h.assets))
	for id := range h.assets {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return h.captureLocked(ctx, ids, func() (semreg.TimePoint, captureContext, error) {
		return semreg.TimePoint{}, nil, nil
	})
}
