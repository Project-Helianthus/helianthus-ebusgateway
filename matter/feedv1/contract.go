// Package feedv1 implements the public, detached Matter binding feed.
//
// It deliberately has no Matter SDK, endpoint, fabric, transport, or native
// route dependency. A Gateway-owned Provider supplies atomic captures and is
// the sole authority that can revalidate and dispatch an admitted operation.
package feedv1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/matter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const Contract = "helianthus.gateway.matter-binding-feed/v1"

var (
	ErrInvalidSnapshot = errors.New("matter binding feed snapshot is invalid")
	ErrInvalidIntent   = errors.New("matter binding feed intent is invalid")
)

// Cursor is monotonically increasing only within Instance. A new process must
// use a new Instance value, so a stale client cursor can only resynchronize.
type Cursor struct {
	Instance string `json:"instance"`
	Sequence uint64 `json:"sequence"`
}

// Target pins a feed to the accepted pure Matter mapping, rather than to a
// Matter node, endpoint allocation, fabric, or SDK implementation.
type Target struct {
	ProjectionContract string `json:"projection_contract"`
	TargetID           string `json:"target_id"`
	TargetVersion      string `json:"target_version"`
	MappingRevision    string `json:"mapping_revision"`
}

// Resource is one stable resource identity and its lifecycle/source fence.
// Projected is absent after withdrawal, quarantine, or source unavailability;
// those states must never retain an old projection or admitted operation.
type Resource struct {
	AssetID          string                `json:"asset_id"`
	ResourceID       string                `json:"resource_id"`
	SnapshotID       string                `json:"snapshot_id"`
	Revisions        semreg.RevisionVector `json:"revisions"`
	BindingID        string                `json:"binding_id"`
	SourceEpoch      string                `json:"source_epoch"`
	DriverGeneration uint64                `json:"driver_generation"`
	State            string                `json:"state"`
	Projected        *matter.Result        `json:"projected,omitempty"`
	Causal           *semreg.CausalContext `json:"causal,omitempty"`
	LastFence        *Fence                `json:"last_fence,omitempty"`
}

// Fence is an optional explicit historical reference for a non-current row.
// It is never a projection and cannot make withdrawn capabilities actionable.
type Fence struct {
	SnapshotID       string                `json:"snapshot_id"`
	Revisions        semreg.RevisionVector `json:"revisions"`
	BindingID        string                `json:"binding_id"`
	SourceEpoch      string                `json:"source_epoch"`
	DriverGeneration uint64                `json:"driver_generation"`
}

const (
	ResourceCurrent     = "CURRENT"
	ResourceWithdrawn   = "WITHDRAWN"
	ResourceQuarantined = "QUARANTINED"
	ResourceUnavailable = "UNAVAILABLE"
)

// AdmittedOperation is a capability operation that the provider has made
// discoverable and enabled at capture time. It contains no native route.
type AdmittedOperation struct {
	AssetID string           `json:"asset_id"`
	Action  catalogv1.Action `json:"action"`
	Claim   OperationClaim   `json:"claim"`
}

// OperationClaim is the immutable admission fence captured with an operation.
// Intent-specific deadline and idempotency data deliberately do not appear here.
type OperationClaim struct {
	CatalogRevision  string `json:"catalog_revision"`
	Digest           string `json:"digest"`
	DriverID         string `json:"driver_id"`
	ManifestID       string `json:"manifest_id"`
	ManifestVersion  string `json:"manifest_version"`
	ActionID         string `json:"action_id"`
	ResourceID       string `json:"resource_id"`
	CapabilityID     string `json:"capability_id"`
	OperationID      string `json:"operation_id"`
	SnapshotID       string `json:"snapshot_id"`
	Revision         string `json:"revision"`
	BindingID        string `json:"binding_id"`
	SourceEpoch      string `json:"source_epoch"`
	DriverGeneration uint64 `json:"driver_generation"`
}

// Snapshot is an atomic feed capture. Ledger carries the complete five-pack
// accounting report once; resources carry their own projection and provenance.
type Snapshot struct {
	Contract   string              `json:"contract"`
	Target     Target              `json:"target"`
	Ledger     Ledger              `json:"ledger"`
	Resources  []Resource          `json:"resources"`
	Operations []AdmittedOperation `json:"operations"`
}

// Ledger accounts for every requested item even when no resource is current.
// It intentionally omits per-resource snapshot identifiers and provenance.
type Ledger struct {
	Manifest     projection.ProjectionManifest      `json:"manifest"`
	Requested    []projection.RequestedItem         `json:"requested"`
	Dispositions []projection.ProjectionDisposition `json:"dispositions"`
}

// Intent is selected only from an observed AdmittedOperation. Arguments stay
// opaque to the feed; route selection and native dispatch remain provider-only.
type Intent struct {
	AssetID        string               `json:"asset_id"`
	Claim          OperationClaim       `json:"claim"`
	IdempotencyKey string               `json:"idempotency_key"`
	Deadline       time.Time            `json:"deadline"`
	Arguments      json.RawMessage      `json:"arguments"`
	Causal         semreg.CausalContext `json:"causal"`
}

// Execution separates acknowledgement, readback, and final outcome. The feed
// does not infer one state from another and does not retry an indeterminate I/O.
type Execution struct {
	Contract string          `json:"contract"`
	ACK      string          `json:"ack"`
	Readback json.RawMessage `json:"readback"`
	Outcome  json.RawMessage `json:"outcome"`
}

// Provider must return one self-consistent detached snapshot. Invoke must
// revalidate the claim, causal context, lifecycle, deadline and native owner
// immediately before it does any native I/O.
type Provider interface {
	Capture(context.Context) (Snapshot, error)
	Invoke(context.Context, Invocation) (Execution, error)
}

// Invocation is assembled by the server only after its current detached feed
// contains the exact operation claim. The provider must revalidate before I/O.
type Invocation struct {
	Operation AdmittedOperation `json:"operation"`
	Intent    Intent            `json:"intent"`
}

func expectedTarget() Target {
	return Target{ProjectionContract: matter.Contract, TargetID: string(matter.TargetID), TargetVersion: string(matter.TargetVersion), MappingRevision: string(matter.MappingRevision)}
}

// CanonicalJSON validates and returns detached, byte-stable JSON. The standard
// encoder has stable struct-field order; all public collections are additionally
// required to be sorted so callers cannot depend on provider map iteration.
func CanonicalJSON(snapshot Snapshot) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	copy, err := detach(snapshot)
	if err != nil {
		return nil, err
	}
	sortSnapshot(&copy)
	return json.Marshal(copy)
}

func (s Snapshot) Validate() error {
	if s.Contract != Contract || s.Target != expectedTarget() || s.Resources == nil || s.Operations == nil {
		return ErrInvalidSnapshot
	}
	if len(s.Resources) > 256 || len(s.Operations) > 256 {
		return ErrInvalidSnapshot
	}
	if err := validateLedger(s.Ledger); err != nil {
		return ErrInvalidSnapshot
	}
	resources := make(map[string]Resource, len(s.Resources))
	for _, resource := range s.Resources {
		if err := resource.validate(); err != nil {
			return ErrInvalidSnapshot
		}
		key := resource.AssetID + "\x00" + resource.ResourceID
		if _, exists := resources[key]; exists {
			return ErrInvalidSnapshot
		}
		resources[key] = resource
	}
	for _, operation := range s.Operations {
		if err := operation.validate(resources); err != nil {
			return ErrInvalidSnapshot
		}
	}
	return nil
}

func validateLedger(ledger Ledger) error {
	if !reflect.DeepEqual(ledger.Manifest, matter.Manifest()) || ledger.Requested == nil || ledger.Dispositions == nil {
		return ErrInvalidSnapshot
	}
	want, _ := matter.Ledger()
	_, wantDispositions := matter.Ledger()
	if !reflect.DeepEqual(ledger.Requested, want) || !reflect.DeepEqual(ledger.Dispositions, wantDispositions) {
		return ErrInvalidSnapshot
	}
	return nil
}

func (r Resource) validate() error {
	if !publicID(r.AssetID) || !publicID(r.ResourceID) {
		return ErrInvalidSnapshot
	}
	switch r.State {
	case ResourceCurrent:
		if r.LastFence != nil || r.SnapshotID == "" || r.BindingID == "" || r.SourceEpoch == "" || r.DriverGeneration == 0 || r.Revisions.Validate() != nil || r.Projected == nil || r.Projected.Document.Contract != matter.Contract || r.Projected.Document.TargetID != matter.TargetID || r.Projected.Document.TargetVersion != matter.TargetVersion || r.Projected.Document.MappingRevision != matter.MappingRevision || r.Projected.Report.SnapshotID != semreg.SnapshotID(r.SnapshotID) || !reflect.DeepEqual(r.Projected.Report.Revisions, r.Revisions) {
			return ErrInvalidSnapshot
		}
		if _, err := matter.CanonicalJSON(*r.Projected); err != nil {
			return ErrInvalidSnapshot
		}
	case ResourceWithdrawn, ResourceQuarantined, ResourceUnavailable:
		if r.Projected != nil {
			return ErrInvalidSnapshot
		}
		if r.SnapshotID != "" || r.BindingID != "" || r.SourceEpoch != "" || r.DriverGeneration != 0 || !reflect.DeepEqual(r.Revisions, semreg.RevisionVector{}) {
			return ErrInvalidSnapshot
		}
		if r.LastFence != nil && (r.LastFence.SnapshotID == "" || r.LastFence.BindingID == "" || r.LastFence.SourceEpoch == "" || r.LastFence.DriverGeneration == 0 || r.LastFence.Revisions.Validate() != nil) {
			return ErrInvalidSnapshot
		}
	default:
		return ErrInvalidSnapshot
	}
	if r.Causal != nil && r.Causal.Validate() != nil {
		return ErrInvalidSnapshot
	}
	return nil
}

func (a AdmittedOperation) validate(resources map[string]Resource) error {
	claim, action := a.Claim, a.Action
	if !publicID(a.AssetID) || !action.Discoverable || !action.Enabled || !publicID(action.ResourceID) || !publicID(action.CapabilityID) || !publicID(action.OperationID) {
		return ErrInvalidSnapshot
	}
	resource, ok := resources[a.AssetID+"\x00"+claim.ResourceID]
	if !ok || resource.State != ResourceCurrent || action.ResourceID != resource.ResourceID || claim.ResourceID != resource.ResourceID || claim.CapabilityID != action.CapabilityID || claim.OperationID != action.OperationID || claim.ActionID != action.ID || claim.SnapshotID != resource.SnapshotID || claim.Revision != revisionsText(resource.Revisions) || claim.BindingID != resource.BindingID || claim.SourceEpoch != resource.SourceEpoch || claim.DriverGeneration != resource.DriverGeneration || claim.DriverID != action.ContributionDriverID || claim.ManifestID != action.ContributionManifestID || claim.ManifestVersion != action.ContributionManifestVersion || claim.Digest == "" {
		return ErrInvalidSnapshot
	}
	return nil
}

func revisionsText(revisions semreg.RevisionVector) string {
	raw, err := json.Marshal(revisions)
	if err != nil {
		return ""
	}
	return string(raw)
}

func publicID(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value
}

func (i Intent) Validate() error {
	if !publicID(i.AssetID) || i.Deadline.IsZero() || !publicID(i.IdempotencyKey) || len(i.Arguments) == 0 || len(i.Arguments) > 64*1024 || i.Causal.Validate() != nil {
		return ErrInvalidIntent
	}
	decoder := json.NewDecoder(bytes.NewReader(i.Arguments))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return ErrInvalidIntent
	}
	return nil
}

func (e Execution) Validate() error {
	if e.Contract != Contract || e.ACK == "" || !validJSON(e.Readback) || !validJSON(e.Outcome) {
		return ErrInvalidIntent
	}
	return nil
}

func validJSON(raw json.RawMessage) bool {
	if len(raw) == 0 || len(raw) > 64*1024 {
		return false
	}
	var value any
	return json.Unmarshal(raw, &value) == nil
}

func detach[T any](value T) (T, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		var zero T
		return zero, err
	}
	var copy T
	if err := json.Unmarshal(raw, &copy); err != nil {
		var zero T
		return zero, err
	}
	return copy, nil
}

func sortSnapshot(snapshot *Snapshot) {
	sort.Slice(snapshot.Resources, func(i, j int) bool {
		return snapshot.Resources[i].AssetID+"\x00"+snapshot.Resources[i].ResourceID < snapshot.Resources[j].AssetID+"\x00"+snapshot.Resources[j].ResourceID
	})
	sort.Slice(snapshot.Operations, func(i, j int) bool {
		a, b := snapshot.Operations[i].Claim, snapshot.Operations[j].Claim
		return fmt.Sprint(a.DriverID, "\x00", a.ManifestID, "\x00", a.ActionID, "\x00", a.ResourceID) < fmt.Sprint(b.DriverID, "\x00", b.ManifestID, "\x00", b.ActionID, "\x00", b.ResourceID)
	})
}
