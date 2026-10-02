package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/matter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/feedv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

var errMatterBindingOperationUnavailable = errors.New("matter binding operation is not admitted")

// gatewayMatterBindingFeedProvider projects only the detached, contribution-
// fenced Portal source. Portal is reused as a capture primitive; it does not
// become the owner of the Matter wire contract or target semantics.
type gatewayMatterBindingFeedProvider struct {
	source        catalogv1.SourceCapture
	contributions *gatewayPortalCatalogContributions
	allowedAssets map[string]struct{}
}

type matterBindingStaticCapture struct {
	resources []catalogv1.Resource
	fields    []catalogv1.Field
}

func (s matterBindingStaticCapture) Capture(time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	return append([]catalogv1.Resource(nil), s.resources...), append([]catalogv1.Field(nil), s.fields...), nil
}

func newGatewayMatterBindingFeedProvider(source catalogv1.SourceCapture, allowedAssets []string) (*gatewayMatterBindingFeedProvider, error) {
	owner, ok := source.(gatewayPortalCatalogContributionOwner)
	if !ok || owner.PortalCatalogContributions() == nil {
		return nil, errors.New("matter binding feed contribution registry unavailable")
	}
	allowed := make(map[string]struct{}, len(allowedAssets))
	for _, asset := range allowedAssets {
		if asset == "" {
			return nil, errors.New("matter binding feed allowed asset is invalid")
		}
		allowed[asset] = struct{}{}
	}
	return &gatewayMatterBindingFeedProvider{source: source, contributions: owner.PortalCatalogContributions(), allowedAssets: allowed}, nil
}

func (p *gatewayMatterBindingFeedProvider) Capture(ctx context.Context) (feedv1.Snapshot, error) {
	if p == nil || p.source == nil || p.contributions == nil {
		return feedv1.Snapshot{}, errors.New("matter binding feed provider unavailable")
	}
	if err := ctx.Err(); err != nil {
		return feedv1.Snapshot{}, err
	}
	registry := p.contributions.snapshot()
	bound := &gatewayPortalCatalogBoundCapture{source: p.source, contributions: p.contributions, revision: registry.Revision}
	rawResources, rawFields, err := bound.Capture(time.Now().UTC())
	if err != nil {
		return feedv1.Snapshot{}, fmt.Errorf("matter binding feed source capture: %w", err)
	}
	composer := catalogv1.New(p.contributions.index, matterBindingStaticCapture{resources: rawResources, fields: rawFields}, nil, nil)
	if err := composer.InstallDetachedRegistrySnapshot(registry.Accepted, registry.Quarantined); err != nil {
		return feedv1.Snapshot{}, fmt.Errorf("matter binding feed catalog composition: %w", err)
	}
	catalog, err := composer.Catalog(nil)
	if err != nil {
		return feedv1.Snapshot{}, fmt.Errorf("matter binding feed catalog capture: %w", err)
	}
	if p.contributions.revision() != registry.Revision {
		return feedv1.Snapshot{}, catalogv1.ErrCatalogChanged
	}
	rawByIdentity := make(map[string]catalogv1.Resource, len(rawResources))
	for _, resource := range rawResources {
		key := matterBindingResourceKey(resource)
		if _, duplicate := rawByIdentity[key]; duplicate {
			return feedv1.Snapshot{}, errors.New("matter binding feed raw resource identity is ambiguous")
		}
		rawByIdentity[key] = resource
	}
	requested, dispositions := matter.Ledger()
	out := feedv1.Snapshot{
		Contract: feedv1.Contract,
		Target: feedv1.Target{
			ProjectionContract: matter.Contract,
			TargetID:           string(matter.TargetID),
			TargetVersion:      string(matter.TargetVersion),
			MappingRevision:    string(matter.MappingRevision),
		},
		Ledger: feedv1.Ledger{
			Manifest:     matter.Manifest(),
			Requested:    requested,
			Dispositions: dispositions,
		},
		Resources:  make([]feedv1.Resource, 0, len(catalog.Resources)),
		Operations: make([]feedv1.AdmittedOperation, 0, len(catalog.Actions)),
	}
	resourceByID := make(map[string]feedv1.Resource, len(catalog.Resources))
	for _, resource := range catalog.Resources {
		if _, allowed := p.allowedAssets[resource.Source.AssetID]; !allowed {
			continue
		}
		raw, ok := rawByIdentity[matterBindingResourceKey(resource)]
		if !ok {
			return feedv1.Snapshot{}, errors.New("matter binding feed catalog resource has no raw source")
		}
		projected, converted, err := projectMatterFeedResource(raw)
		if err != nil {
			return feedv1.Snapshot{}, fmt.Errorf("matter binding feed resource projection: %w", err)
		}
		key := converted.AssetID + "\x00" + converted.ResourceID
		if _, duplicate := resourceByID[key]; duplicate {
			return feedv1.Snapshot{}, errors.New("matter binding feed resource identity is ambiguous")
		}
		converted.Projected = &projected
		resourceByID[key] = converted
		out.Resources = append(out.Resources, converted)
	}
	digests := make(map[string]string, len(catalog.Contributions))
	for _, contribution := range catalog.Contributions {
		key := contribution.DriverID + "\x00" + contribution.ManifestID + "\x00" + contribution.ManifestVersion
		if contribution.Digest == "" || digests[key] != "" {
			return feedv1.Snapshot{}, errors.New("matter binding feed contribution identity is invalid")
		}
		digests[key] = contribution.Digest
	}
	for _, action := range catalog.Actions {
		if !action.Discoverable || !action.Enabled {
			continue
		}
		if _, allowed := p.allowedAssets[action.Source.AssetID]; !allowed {
			continue
		}
		key := action.Source.AssetID + "\x00" + action.ResourceID
		resource, ok := resourceByID[key]
		if !ok {
			return feedv1.Snapshot{}, errors.New("matter binding feed action has no current resource")
		}
		digest := digests[action.ContributionDriverID+"\x00"+action.ContributionManifestID+"\x00"+action.ContributionManifestVersion]
		if digest == "" {
			return feedv1.Snapshot{}, errors.New("matter binding feed action has no contribution digest")
		}
		out.Operations = append(out.Operations, feedv1.AdmittedOperation{
			AssetID: resource.AssetID,
			Action:  action,
			Claim: feedv1.OperationClaim{
				CatalogRevision:  catalog.CatalogRevision,
				Digest:           digest,
				DriverID:         action.ContributionDriverID,
				ManifestID:       action.ContributionManifestID,
				ManifestVersion:  action.ContributionManifestVersion,
				ActionID:         action.ID,
				ResourceID:       action.ResourceID,
				CapabilityID:     action.CapabilityID,
				OperationID:      action.OperationID,
				SnapshotID:       resource.SnapshotID,
				Revision:         action.Source.Revision,
				BindingID:        resource.BindingID,
				SourceEpoch:      resource.SourceEpoch,
				DriverGeneration: resource.DriverGeneration,
			},
		})
	}
	if err := ctx.Err(); err != nil {
		return feedv1.Snapshot{}, err
	}
	if err := out.Validate(); err != nil {
		return feedv1.Snapshot{}, fmt.Errorf("matter binding feed capture: %w", err)
	}
	return out, nil
}

func matterBindingResourceKey(resource catalogv1.Resource) string {
	return resource.ContributionDriverID + "\x00" + resource.ContributionManifestID + "\x00" + resource.ContributionManifestVersion + "\x00" + resource.ID + "\x00" + resource.ServiceID + "\x00" + resource.CapabilityID
}

func projectMatterFeedResource(resource catalogv1.Resource) (matter.Result, feedv1.Resource, error) {
	if resource.State != feedv1.ResourceCurrent {
		return matter.Result{}, feedv1.Resource{}, errors.New("matter binding feed source is not current")
	}
	var snapshot semreg.Snapshot
	if err := json.Unmarshal(resource.Source.Snapshot, &snapshot); err != nil {
		return matter.Result{}, feedv1.Resource{}, errors.New("matter binding feed snapshot is invalid")
	}
	var evaluation semreg.EvaluationView
	if err := json.Unmarshal(resource.Source.Evaluation, &evaluation); err != nil {
		return matter.Result{}, feedv1.Resource{}, errors.New("matter binding feed evaluation is invalid")
	}
	if string(snapshot.AssetID) != resource.Source.AssetID || string(snapshot.SnapshotID) != resource.Source.SnapshotID {
		return matter.Result{}, feedv1.Resource{}, errors.New("matter binding feed source identity mismatch")
	}
	revisionBytes, err := json.Marshal(snapshot.Revisions)
	if err != nil || string(revisionBytes) != resource.Source.Revision {
		return matter.Result{}, feedv1.Resource{}, errors.New("matter binding feed source revision mismatch")
	}
	projected, err := matter.Project(snapshot, evaluation)
	if err != nil {
		return matter.Result{}, feedv1.Resource{}, err
	}
	converted := feedv1.Resource{
		AssetID:          resource.Source.AssetID,
		ResourceID:       resource.ID,
		SnapshotID:       resource.Source.SnapshotID,
		Revisions:        snapshot.Revisions,
		BindingID:        resource.Source.BindingID,
		SourceEpoch:      resource.Source.SourceEpoch,
		DriverGeneration: resource.Source.DriverGeneration,
		State:            feedv1.ResourceCurrent,
	}
	return projected, converted, nil
}

// No production Matter operation is admitted yet. The feed publishes an empty
// operation collection, and this final fail-closed boundary prevents a future
// descriptor from becoming executable until Gateway supplies an accepted
// authorizer, revalidator and exact native-owner bridge.
func (*gatewayMatterBindingFeedProvider) Invoke(context.Context, feedv1.Invocation) (feedv1.Execution, error) {
	return feedv1.Execution{}, errMatterBindingOperationUnavailable
}
