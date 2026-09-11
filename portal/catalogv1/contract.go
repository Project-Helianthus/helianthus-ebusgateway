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
	{ID: "helianthus.thermal-hvac", Version: "1.0.0"},
	{ID: "helianthus.pv", Version: "1.0.0"},
	{ID: "helianthus.storage-bms", Version: "1.1.0"},
	{ID: "helianthus.evse", Version: "1.0.0"},
	{ID: "helianthus.infrastructure", Version: "1.0.0"},
}

type Identity struct{ DriverID, ManifestID, ManifestVersion, Digest string }
type Domain struct {
	Pack        contributionv1.PackRef `json:"pack"`
	SourceState string                 `json:"source_state"`
	Reason      string                 `json:"reason,omitempty"`
}
type Source struct {
	AssetID, SnapshotID, Revision, EvaluationDigest, BindingID, SourceEpoch string
	DriverGeneration                                                        uint64
}
type Resource struct {
	ID, Domain, ServiceID, CapabilityID string
	Source                              Source
	State                               string `json:"state"`
}
type Field struct {
	ID, ResourceID, DefinitionID, UnitID string
	Value                                json.RawMessage
	Quality, Projection                  string
}
type Action struct {
	ID, ResourceID, ServiceID, CapabilityID, OperationID string
	Discoverable, Enabled                                bool
	Reason                                               string `json:"reason,omitempty"`
}
type Quarantine struct{ DriverID, ManifestID, ManifestVersion, Reason string }
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
	CatalogRevision, Digest, ActionID, ResourceID, CapabilityID, SnapshotID, Revision, BindingID, SourceEpoch, IdempotencyKey string
	DriverGeneration                                                                                                          uint64
	Deadline                                                                                                                  time.Time
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
func hash(v any) string               { b, _ := canonical(v); h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func clone[T any](v T) T              { b, _ := json.Marshal(v); var out T; _ = json.Unmarshal(b, &out); return out }
func sortCatalog(c *Catalog) {
	sort.Slice(c.Domains, func(i, j int) bool { return c.Domains[i].Pack.ID < c.Domains[j].Pack.ID })
	sort.Slice(c.Contributions, func(i, j int) bool {
		return c.Contributions[i].DriverID+c.Contributions[i].ManifestID+c.Contributions[i].ManifestVersion < c.Contributions[j].DriverID+c.Contributions[j].ManifestID+c.Contributions[j].ManifestVersion
	})
	sort.Slice(c.Resources, func(i, j int) bool { return c.Resources[i].ID < c.Resources[j].ID })
	sort.Slice(c.Fields, func(i, j int) bool { return c.Fields[i].ID < c.Fields[j].ID })
	sort.Slice(c.Actions, func(i, j int) bool { return c.Actions[i].ID < c.Actions[j].ID })
	sort.Slice(c.Quarantines, func(i, j int) bool {
		return c.Quarantines[i].DriverID+c.Quarantines[i].ManifestID < c.Quarantines[j].DriverID+c.Quarantines[j].ManifestID
	})
}
func canonicalDomain(id string) string { return strings.TrimSpace(id) }
