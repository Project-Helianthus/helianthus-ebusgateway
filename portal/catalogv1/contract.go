// Package catalogv1 owns the immutable, detached Portal catalog contract.
// It has no transport, driver, or native acquisition dependency.
package catalogv1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
)

const Contract = "helianthus.gateway.portal-catalog/v1"

var ErrCatalogChanged = errors.New("portal catalog changed during detached capture")
var ErrUnauthorized = errors.New("portal action is not authorized")
var ErrStale = errors.New("portal action claims are stale")

// FivePacks is structural input, never a product-specific source switch.
var FivePacks = []contributionv1.PackRef{
	{ID: "helianthus.pack.thermal", Version: "1.0.0"},
	{ID: "helianthus.pack.pv", Version: "1.0.0"},
	{ID: "helianthus.pack.storage", Version: "1.1.0"},
	{ID: "helianthus.pack.evse", Version: "1.0.0"},
	{ID: "helianthus.pack.infrastructure", Version: "1.0.0"},
}

type Identity struct {
	DriverID        string `json:"driver_id"`
	ManifestID      string `json:"manifest_id"`
	ManifestVersion string `json:"manifest_version"`
	Digest          string `json:"digest"`
}
type Domain struct {
	Pack        contributionv1.PackRef `json:"pack"`
	SourceState string                 `json:"source_state"`
	Reason      string                 `json:"reason,omitempty"`
}
type Source struct {
	AssetID          string `json:"asset_id"`
	SnapshotID       string `json:"snapshot_id"`
	Revision         string `json:"revision"`
	EvaluationDigest string `json:"evaluation_digest"`
	BindingID        string `json:"binding_id"`
	SourceEpoch      string `json:"source_epoch"`
	DriverGeneration uint64 `json:"driver_generation"`
	// These detached records retain the evidence-bearing SemReg tuple.  The
	// scalar fence members above are for admission claims; they never replace
	// provenance, disposition, loss, quality, binding, or revision data.
	Snapshot   json.RawMessage `json:"snapshot"`
	Evaluation json.RawMessage `json:"evaluation"`
	Selections json.RawMessage `json:"selections"`
	Projection json.RawMessage `json:"projection"`
}
type Resource struct {
	ID                          string `json:"id"`
	Domain                      string `json:"domain"`
	ServiceID                   string `json:"service_id"`
	CapabilityID                string `json:"capability_id"`
	ContributionDriverID        string `json:"contribution_driver_id"`
	ContributionManifestID      string `json:"contribution_manifest_id"`
	ContributionManifestVersion string `json:"contribution_manifest_version"`
	Source                      Source `json:"source"`
	State                       string `json:"state"`
}
type Field struct {
	ID                          string          `json:"id"`
	ResourceID                  string          `json:"resource_id"`
	DefinitionID                string          `json:"definition_id"`
	UnitID                      string          `json:"unit_id"`
	ContributionDriverID        string          `json:"contribution_driver_id"`
	ContributionManifestID      string          `json:"contribution_manifest_id"`
	ContributionManifestVersion string          `json:"contribution_manifest_version"`
	Value                       json.RawMessage `json:"value"`
	Quality                     string          `json:"quality"`
	Projection                  string          `json:"projection"`
}
type Action struct {
	ID                          string `json:"id"`
	ResourceID                  string `json:"resource_id"`
	ServiceID                   string `json:"service_id"`
	CapabilityID                string `json:"capability_id"`
	OperationID                 string `json:"operation_id"`
	ContributionDriverID        string `json:"contribution_driver_id"`
	ContributionManifestID      string `json:"contribution_manifest_id"`
	ContributionManifestVersion string `json:"contribution_manifest_version"`
	Source                      Source `json:"source"`
	Discoverable                bool   `json:"discoverable"`
	Enabled                     bool   `json:"enabled"`
	Reason                      string `json:"reason,omitempty"`
}
type Quarantine struct {
	DriverID        string `json:"driver_id"`
	ManifestID      string `json:"manifest_id"`
	ManifestVersion string `json:"manifest_version"`
	Reason          string `json:"reason"`
}
type Catalog struct {
	Contract           string       `json:"contract"`
	CatalogRevision    string       `json:"catalog_revision"`
	CatalogDigest      string       `json:"catalog_digest"`
	EvaluationInstant  time.Time    `json:"evaluation_instant"`
	AuthorizationScope string       `json:"authorization_scope"`
	Domains            []Domain     `json:"domains"`
	Contributions      []Identity   `json:"contributions"`
	Resources          []Resource   `json:"resources"`
	Fields             []Field      `json:"fields"`
	Actions            []Action     `json:"actions"`
	Quarantines        []Quarantine `json:"quarantines"`
}

// Descriptor is a validated contribution plus its host-computed digest.
type Descriptor struct {
	Manifest contributionv1.Manifest
	Digest   string
}
type SourceCapture interface {
	Capture(at time.Time) ([]Resource, []Field, error)
}

// FencedSourceCapture exposes a stable, detached source/lifecycle vector. The
// composer compares it before and after capture and never publishes a mixed view.
type FencedSourceCapture interface {
	SourceCapture
	Fence() string
}
type Authorizer interface {
	Scope(caller any) (string, error)
	Discover(caller any, action Action) bool
	Invoke(caller any, action Action) bool
}
type Invoker interface {
	Invoke(action Action, claim Claims) (json.RawMessage, error)
}
type Revalidator interface {
	Revalidate(action Action, claim Claims) error
}
type Claims struct {
	CatalogRevision  string    `json:"catalog_revision"`
	Digest           string    `json:"digest"`
	DriverID         string    `json:"driver_id"`
	ManifestID       string    `json:"manifest_id"`
	ManifestVersion  string    `json:"manifest_version"`
	ActionID         string    `json:"action_id"`
	ResourceID       string    `json:"resource_id"`
	CapabilityID     string    `json:"capability_id"`
	SnapshotID       string    `json:"snapshot_id"`
	Revision         string    `json:"revision"`
	BindingID        string    `json:"binding_id"`
	SourceEpoch      string    `json:"source_epoch"`
	IdempotencyKey   string    `json:"idempotency_key"`
	DriverGeneration uint64    `json:"driver_generation"`
	Deadline         time.Time `json:"deadline"`
}

type Composer struct {
	mu          sync.RWMutex
	index       contributionv1.SemanticIndex
	descriptors map[identity]Descriptor
	quarantined map[identity]bool
	revision    uint64
	source      SourceCapture
	auth        Authorizer
	now         func() time.Time
	invoker     Invoker
	revalidator Revalidator
}

func (c *Composer) SetRevalidator(r Revalidator) { c.mu.Lock(); defer c.mu.Unlock(); c.revalidator = r }

type identity struct{ driver, manifest, version string }

func New(index contributionv1.SemanticIndex, source SourceCapture, auth Authorizer, invoker Invoker) *Composer {
	return &Composer{index: index, descriptors: make(map[identity]Descriptor), quarantined: make(map[identity]bool), source: source, auth: auth, invoker: invoker, now: func() time.Time { return time.Now().UTC() }}
}

// Publish validates and binds the digest itself. A same identity with divergent
// content remains quarantined until that manifest identity/version changes.
func (c *Composer) Publish(m contributionv1.Manifest) error {
	digest, err := contributionv1.CanonicalDigest(m, c.index)
	if err != nil {
		return err
	}
	k := identity{m.Contributor.DriverID, m.ManifestID, m.ManifestVersion}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.descriptors[k]; ok && old.Digest != digest {
		delete(c.descriptors, k)
		c.quarantined[k] = true
		c.revision++
		return nil
	}
	if c.quarantined[k] {
		return errors.New("contribution identity quarantined")
	}
	c.descriptors[k] = Descriptor{Manifest: m, Digest: digest}
	c.revision++
	return nil
}
func (c *Composer) Withdraw(driver, manifest, version string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := identity{driver, manifest, version}
	delete(c.descriptors, k)
	delete(c.quarantined, k)
	c.revision++
}

func canonical(v any) ([]byte, error) { return json.Marshal(v) }
func hash(v any) (string, error) {
	b, err := canonical(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func clone[T any](v T) T { b, _ := json.Marshal(v); var out T; _ = json.Unmarshal(b, &out); return out }
func sortCatalog(c *Catalog) {
	sort.Slice(c.Domains, func(i, j int) bool {
		return less(c.Domains[i].Pack.ID, c.Domains[i].Pack.Version, c.Domains[j].Pack.ID, c.Domains[j].Pack.Version)
	})
	sort.Slice(c.Contributions, func(i, j int) bool {
		return less(c.Contributions[i].DriverID, c.Contributions[i].ManifestID, c.Contributions[i].ManifestVersion, c.Contributions[j].DriverID, c.Contributions[j].ManifestID, c.Contributions[j].ManifestVersion)
	})
	sort.Slice(c.Resources, func(i, j int) bool {
		a, b := c.Resources[i], c.Resources[j]
		return less(a.ContributionDriverID, a.ContributionManifestID, a.ContributionManifestVersion, a.ID, a.ServiceID, a.CapabilityID, b.ContributionDriverID, b.ContributionManifestID, b.ContributionManifestVersion, b.ID, b.ServiceID, b.CapabilityID)
	})
	sort.Slice(c.Fields, func(i, j int) bool {
		a, b := c.Fields[i], c.Fields[j]
		return less(a.ContributionDriverID, a.ContributionManifestID, a.ContributionManifestVersion, a.ResourceID, a.ID, a.DefinitionID, a.UnitID, b.ContributionDriverID, b.ContributionManifestID, b.ContributionManifestVersion, b.ResourceID, b.ID, b.DefinitionID, b.UnitID)
	})
	sort.Slice(c.Actions, func(i, j int) bool {
		a, b := c.Actions[i], c.Actions[j]
		return less(a.ContributionDriverID, a.ContributionManifestID, a.ContributionManifestVersion, a.ResourceID, a.ID, a.ServiceID, a.CapabilityID, a.OperationID, b.ContributionDriverID, b.ContributionManifestID, b.ContributionManifestVersion, b.ResourceID, b.ID, b.ServiceID, b.CapabilityID, b.OperationID)
	})
	sort.Slice(c.Quarantines, func(i, j int) bool {
		return less(c.Quarantines[i].DriverID, c.Quarantines[i].ManifestID, c.Quarantines[i].ManifestVersion, c.Quarantines[j].DriverID, c.Quarantines[j].ManifestID, c.Quarantines[j].ManifestVersion)
	})
}
func less(a ...string) bool {
	n := len(a) / 2
	for i := 0; i < n; i++ {
		if a[i] == a[n+i] {
			continue
		}
		return a[i] < a[n+i]
	}
	return false
}
func canonicalDomain(id string) string { return strings.TrimSpace(id) }
