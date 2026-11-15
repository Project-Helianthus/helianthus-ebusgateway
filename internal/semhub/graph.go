package semhub

import (
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

// GraphEnforcement tells whether an asset's kernel was built with the element
// catalog.
type GraphEnforcement string

// GraphEnforcement values.
const (
	// GraphEnforced is an asset whose kernel was built with the embedded,
	// digest-checked element catalog: it publishes graph records under the
	// catalog's rules.
	GraphEnforced GraphEnforcement = "enforced"
	// GraphNotEnforced is an asset whose kernel was built without the catalog:
	// it publishes as before and refuses every graph member.
	GraphNotEnforced GraphEnforcement = "not_enforced"
)

// AssetGraph is the graph enforcement of one declared asset.
type AssetGraph struct {
	Asset       semreg.AssetID   `json:"asset"`
	Driver      string           `json:"driver"`
	Enforcement GraphEnforcement `json:"enforcement"`
}

// GraphState is the graph enforcement of every declared asset, in bytewise
// asset order, with the digest of the element catalog compiled into the
// process, reported once for the release bill of materials. Every
// graph-enforced kernel was built with that catalog.
type GraphState struct {
	CatalogDigest semreg.Digest `json:"catalog_digest"`
	Assets        []AssetGraph  `json:"assets"`
}

// newAssetKernel builds the declared asset's kernel: with the embedded
// element catalog when the declaration asks for graph enforcement, without it
// otherwise. A graph-enforced kernel whose packs the catalog does not cover
// is refused with the kernel's own error.
func newAssetKernel(spec AssetSpec) (*semreg.PublicationKernel, error) {
	if !spec.Graph {
		return semreg.NewPublicationKernel(spec.AssetID, spec.Validators...)
	}
	catalog, err := graph.Embedded()
	if err != nil {
		return nil, err
	}
	return semreg.NewGraphPublicationKernel(spec.AssetID, catalog, spec.Validators...)
}

// GraphState returns the graph enforcement of every declared asset and the
// element catalog digest. It fails only when the embedded catalog does not
// load.
func (h *Hub) GraphState() (GraphState, error) {
	catalog, err := graph.Embedded()
	if err != nil {
		return GraphState{}, err
	}
	h.mu.RLock()
	assets := make([]AssetGraph, 0, len(h.assets))
	for _, target := range h.assets {
		enforcement := GraphNotEnforced
		if target.graph {
			enforcement = GraphEnforced
		}
		assets = append(assets, AssetGraph{Asset: target.id, Driver: target.driver, Enforcement: enforcement})
	}
	h.mu.RUnlock()
	sort.Slice(assets, func(i, j int) bool { return assets[i].Asset < assets[j].Asset })
	return GraphState{CatalogDigest: catalog.Digest(), Assets: assets}, nil
}
