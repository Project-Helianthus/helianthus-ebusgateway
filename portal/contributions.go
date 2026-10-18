package portal

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portalgraphql"
)

// ContributionOptions binds the existing admission registry to a host-owned
// read adapter. Capture consumes only the shared detached semantic view; it
// must preserve complete source records, unavailable siblings and loss data.
// It performs no acquisition, decoding or invocation.
type ContributionOptions struct {
	Registry *contributionv1.Registry
	Index    contributionv1.SemanticIndex
	Capture  func(semanticview.View) ([]catalogv1.Resource, []catalogv1.Field, error)
}

// RegisterContributions rejects duplicate full identities before the registry's
// atomic, generation-fenced admission. The registry remains the sole owner.
func RegisterContributions(registry *contributionv1.Registry, owner contributionv1.DriverGeneration, manifests []contributionv1.Manifest) error {
	seen := make(map[contributionv1.DescriptorKey]bool, len(manifests))
	for _, manifest := range manifests {
		key := contributionv1.DescriptorKey{DriverID: manifest.Contributor.DriverID, ManifestID: manifest.ManifestID, ManifestVersion: manifest.ManifestVersion}
		if seen[key] {
			return errors.New("duplicate contribution identity")
		}
		seen[key] = true
	}
	return registry.ReplaceGeneration(owner, manifests)
}

func (h *handler) contributionAvailable() bool {
	return h.opts.Contributions != nil && h.opts.Contributions.Registry != nil && h.opts.Contributions.Index != nil
}

type contributionCapture struct {
	ctx      context.Context
	opts     Options
	revision uint64
}

func (c contributionCapture) Fence() string {
	return strconv.FormatUint(c.opts.Contributions.Registry.Snapshot().Revision, 10)
}

func (c contributionCapture) Capture(time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	registry := c.opts.Contributions.Registry
	if registry.Snapshot().Revision != c.revision {
		return nil, nil, catalogv1.ErrCatalogChanged
	}
	if c.opts.SharedSemanticView == nil || c.opts.Contributions.Capture == nil {
		return nil, nil, nil
	}
	view, err := c.opts.SharedSemanticView(c.ctx)
	if err != nil {
		return nil, nil, err
	}
	resources, fields, err := c.opts.Contributions.Capture(view)
	if err != nil {
		return nil, nil, err
	}
	if registry.Snapshot().Revision != c.revision {
		return nil, nil, catalogv1.ErrCatalogChanged
	}
	return resources, fields, nil
}

func (h *handler) serveContributionCatalog(w http.ResponseWriter, r *http.Request) {
	if !h.contributionAvailable() {
		http.Error(w, "portal catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	// This seam is read-only even if the existing catalog transport also supports
	// action invocation elsewhere. No authorizer, invoker or revalidator is wired.
	snapshot := h.opts.Contributions.Registry.Snapshot()
	composer := catalogv1.New(h.opts.Contributions.Index, contributionCapture{ctx: r.Context(), opts: h.opts, revision: snapshot.Revision}, nil, nil)
	if err := composer.InstallDetachedRegistrySnapshot(snapshot.Accepted, snapshot.Quarantined); err != nil {
		http.Error(w, "portal catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	portalgraphql.Handler{Catalog: composer}.ServeHTTP(w, r)
}

type portalReadBootstrap struct {
	BasePath              string   `json:"base_path"`
	SnapshotOperation     string   `json:"snapshot_operation"`
	StreamEndpoint        *string  `json:"stream_endpoint"`
	UnavailableOperations []string `json:"unavailable_operations"`
}

func (h *handler) portalRead() portalReadBootstrap {
	out := portalReadBootstrap{BasePath: h.opts.BasePath, SnapshotOperation: "semanticSnapshot", UnavailableOperations: []string{}}
	if !h.opts.PortalCatalogAvailable && !h.contributionAvailable() {
		out.UnavailableOperations = append(out.UnavailableOperations, "PortalCatalogV1")
	}
	if h.opts.SharedSemanticView == nil || !h.opts.SemanticSnapshotAvailable {
		out.UnavailableOperations = append(out.UnavailableOperations, "semanticSnapshot")
	}
	return out
}
