package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The configuration provider is one hub-native source of the gateway. It
// publishes operator records: records whose origin is an operator
// declaration rather than a native observation. A record whose starting end
// is a driver's resource is published on the driver's asset, where the
// provider is a declared contributor; a resource that only configuration
// knows is homed in an asset the provider owns alone.
const (
	operatorRecordSource semreg.SourceID = "source:gateway-configuration"
	operatorRecordDriver string          = "gateway-configuration"
	// operatorRecordOwner owns the provider's evidence references.
	operatorRecordOwner semreg.DefinitionID = "helianthus.gateway.configuration"
)

// Reasons of an operator-record refusal.
const (
	// operatorReasonStartingEndUnresolved refuses a relation whose starting
	// end is not a current resource of the asset it is published on.
	operatorReasonStartingEndUnresolved semreg.DefinitionID = "operator.record.starting_end_unresolved"
	// operatorReasonAuditMissing refuses a record set without the digest of
	// the operator declaration that carries it.
	operatorReasonAuditMissing semreg.DefinitionID = "operator.record.audit_missing"
	// operatorReasonDuplicate refuses a record set that names one record key
	// twice.
	operatorReasonDuplicate semreg.DefinitionID = "operator.record.duplicate"
	// operatorReasonNativeUnobserved defers an identity link whose native
	// resource no binding of the asset observes yet. The rest of that asset's
	// set is not submitted. The hub keeps a link pending for the binding's
	// source, and that source is not known before the binding is published.
	operatorReasonNativeUnobserved semreg.DefinitionID = "operator.record.native_unobserved"
)

// operatorRecordRefusal is a refused operator record set. Nothing of the set
// was submitted.
type operatorRecordRefusal struct {
	Asset    semreg.AssetID
	Resource semreg.ResourceID
	Reason   semreg.DefinitionID
}

func (e *operatorRecordRefusal) Error() string {
	return fmt.Sprintf("operator records of %s refused: %s (%s)", e.Asset, e.Reason, e.Resource)
}

// operatorAudit is the audit evidence of one operator declaration: the
// digest of the configuration entry that declares the records. Every record
// of the set carries it as its origin evidence; a relation also carries it
// as its basis and a resource as its anchor evidence.
type operatorAudit struct {
	Declaration semreg.Digest
}

// evidence returns the audit evidence reference of the declaration.
func (a operatorAudit) evidence() semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: operatorRecordOwner, Kind: "configuration.operator_declaration", Digest: a.Declaration, Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

// operatorResource is one resource that only configuration knows. Its anchor
// is an operator slot: Slot within Parent, or within the installation when
// Parent is empty.
type operatorResource struct {
	ID     semreg.ResourceID
	Kind   semreg.DefinitionID
	Class  semreg.AnchorClass
	Parent semreg.ResourceID
	Slot   string
}

// operatorRelation is one configured relation instance, keyed by its
// definition and both ends. From is a resource of the asset the relation is
// published on; To may be homed in another asset.
type operatorRelation struct {
	Definition semreg.DefinitionRef
	From       semreg.ResourceID
	To         semreg.ResourceID
}

// operatorIdentityLink is one configured identity link. The provider does not
// publish it: only the binding's source can publish an identity link. The
// provider declares it to the hub as pending for the source whose binding
// observes Native, and that source's next transaction carries it.
type operatorIdentityLink struct {
	Native     semreg.EvidenceRef
	Resource   semreg.ResourceID
	BasisClass string
}

// operatorRecords is the complete desired operator-record set of one asset.
type operatorRecords struct {
	Resources []operatorResource
	Relations []operatorRelation
	Links     []operatorIdentityLink
}

// operatorAssetRecords is the configured record set of one asset.
// ConfigurationOnly is set when the home holds a resource that only
// configuration knows. A relation or an identity link does not make its home
// configuration-only. Startup does not declare such a home: the section does
// not say whether a driver owns the identifier, and a declaration would claim
// it for good.
type operatorAssetRecords struct {
	Asset             semreg.AssetID
	Records           operatorRecords
	Audit             operatorAudit
	ConfigurationOnly bool
}

// operatorDesiredResource and operatorDesiredRelation are one desired
// record with the provider's own revision of it.
type operatorDesiredResource struct {
	Record   operatorResource
	Revision uint64
}

type operatorDesiredRelation struct {
	Record   operatorRelation
	Revision uint64
}

// operatorDesired is the provider's desired record set of one asset, in key
// order.
type operatorDesired struct {
	Resources []operatorDesiredResource
	Relations []operatorDesiredRelation
}

// operatorAssetState holds the desired records of one asset. revisions keep
// the latest revision of every record key the provider ever published on the
// asset, so a withdrawn and re-added record continues its revisions.
type operatorAssetState struct {
	resources map[semreg.ResourceID]operatorDesiredResource
	relations map[string]operatorDesiredRelation
	revisions map[string]uint64
}

// operatorRecordOptions configures the provider. Zero values select the
// gateway's configuration source and the process clocks.
type operatorRecordOptions struct {
	Now func() (semreg.TimePoint, semreg.MonotonicPoint)
}

// operatorRecordPublisher is the hub-native publisher of the configuration
// provider. Set never runs inside a hub commit callback: it takes the
// asset's commit lock.
type operatorRecordPublisher struct {
	hub     *semhub.Hub
	now     func() (semreg.TimePoint, semreg.MonotonicPoint)
	started semreg.TimePoint

	mu      sync.Mutex
	handle  *semhub.Handle
	desired map[semreg.AssetID]*operatorAssetState
	pending map[semreg.AssetID]operatorAssetRecords
	failed  map[semreg.AssetID]error
	// links holds the identity links declared to the hub, by asset and by
	// the source whose binding observes their native resource.
	links map[semreg.AssetID]map[semreg.SourceID][]semhub.OperatorLink
}

// operatorPublishers keeps the one provider of each hub. Startup composition
// and the portal runtime share it, so a source admitted by the first
// publication is the source the later runtime uses.
var operatorPublishers sync.Map

// operatorPublisherFor returns the provider already retained for the hub, or
// retains a new one. The new provider admits its source only when it first
// publishes.
func operatorPublisherFor(hub *semhub.Hub) (*operatorRecordPublisher, error) {
	if hub == nil {
		return nil, errors.New("operator records: no semantic publication hub")
	}
	if value, ok := operatorPublishers.Load(hub); ok {
		if publisher, _ := value.(*operatorRecordPublisher); publisher != nil {
			return publisher, nil
		}
	}
	publisher, err := newOperatorRecordPublisher(hub, operatorRecordOptions{})
	if err != nil {
		return nil, err
	}
	actual, loaded := operatorPublishers.LoadOrStore(hub, publisher)
	if loaded {
		if publisher, _ := actual.(*operatorRecordPublisher); publisher != nil {
			return publisher, nil
		}
	}
	return publisher, nil
}

// newOperatorRecordPublisher returns the provider. It admits its source only
// when it first publishes, so a provider without records leaves the hub
// untouched.
func newOperatorRecordPublisher(hub *semhub.Hub, options operatorRecordOptions) (*operatorRecordPublisher, error) {
	if hub == nil {
		return nil, errors.New("operator records: no semantic publication hub")
	}
	now := options.Now
	if now == nil {
		start := time.Now()
		clock := semreg.ClockEpochID("clock:gateway-configuration:" + strconv.FormatInt(start.UnixNano(), 10))
		now = func() (semreg.TimePoint, semreg.MonotonicPoint) {
			return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(time.Now().UnixNano(), 10)), ClockID: "clock.utc", UncertaintyNS: "0"},
				semreg.MonotonicPoint{ClockEpochID: clock, Nanoseconds: semreg.Uint64(strconv.FormatInt(time.Since(start).Nanoseconds(), 10))}
		}
	}
	started, _ := now()
	return &operatorRecordPublisher{hub: hub, now: now, started: started, desired: make(map[semreg.AssetID]*operatorAssetState)}, nil
}

// Contributor returns the contributor declaration an asset's owner names so
// the provider may publish operator records on that asset. The validators
// name the packs of the records' definitions and must be a subset of the
// asset's packs. The provider keeps no cache of the asset, so it needs no
// reprojection.
func (p *operatorRecordPublisher) Contributor(validators ...semreg.PackValidator) semhub.ContributorSpec {
	return semhub.ContributorSpec{SourceID: operatorRecordSource, Driver: operatorRecordDriver, Validators: validators}
}

// DeclareHome declares an asset whose single source is the configuration
// provider, to home resources that only configuration knows. It is
// graph-enforced.
func (p *operatorRecordPublisher) DeclareHome(id semreg.AssetID, validators ...semreg.PackValidator) error {
	_, err := p.hub.Declare(semhub.AssetSpec{AssetID: id, SourceID: operatorRecordSource, Driver: operatorRecordDriver, Validators: validators, Graph: true})
	return err
}

// Compose applies every configured record set in order. A set whose starting
// end does not resolve yet, or whose asset is not declared yet, because its
// owner has not published, stays pending and is applied again after each
// later publication of the hub; any other refusal stops the composition.
func (p *operatorRecordPublisher) Compose(sets []operatorAssetRecords) error {
	if len(sets) == 0 {
		return nil
	}
	// Subscribing before the first attempt leaves no publication unseen.
	subscription := p.hub.Subscribe()
	for _, set := range sets {
		err := p.Set(set.Asset, set.Records, set.Audit)
		if err == nil {
			continue
		}
		if !operatorRecordDeferred(err) {
			subscription.Close()
			return err
		}
		p.mu.Lock()
		if p.pending == nil {
			p.pending = make(map[semreg.AssetID]operatorAssetRecords)
		}
		p.pending[set.Asset] = set
		p.mu.Unlock()
	}
	if len(p.Pending()) == 0 {
		subscription.Close()
		return nil
	}
	go p.watch(subscription)
	return nil
}

// operatorRecordDeferred reports a refusal that a later publication of the
// asset's owner can resolve.
func operatorRecordDeferred(err error) bool {
	var refusal *operatorRecordRefusal
	if errors.As(err, &refusal) {
		return refusal.Reason == operatorReasonStartingEndUnresolved || refusal.Reason == operatorReasonNativeUnobserved
	}
	return errors.Is(err, semhub.ErrUnknownAsset)
}

// Pending returns the assets whose configured records wait for their owner's
// resources, in order.
func (p *operatorRecordPublisher) Pending() []semreg.AssetID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]semreg.AssetID, 0, len(p.pending))
	for id := range p.pending {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Failures returns the deferred record sets that were refused for a reason a
// later publication cannot resolve, by asset. Those records are not published.
func (p *operatorRecordPublisher) Failures() map[semreg.AssetID]error {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[semreg.AssetID]error, len(p.failed))
	for id, err := range p.failed {
		out[id] = err
	}
	return out
}

// Retry applies the pending record sets again and keeps those that still
// wait. A set refused for another reason is dropped from the pending sets,
// recorded for Failures and returned: every refusal of one retry is joined
// into the returned error, in asset order, so none is lost.
func (p *operatorRecordPublisher) Retry() error {
	var refused []error
	for _, id := range p.Pending() {
		p.mu.Lock()
		set, ok := p.pending[id]
		if !ok {
			p.mu.Unlock()
			continue
		}
		// Hold the publisher lock across the attempt so a Portal replacement
		// cannot clear this set and then lose a publication of it.
		err := p.setLocked(set.Asset, set.Records, set.Audit)
		if err == nil || !operatorRecordDeferred(err) {
			if current, still := p.pending[id]; !still || reflect.DeepEqual(current, set) {
				delete(p.pending, id)
			}
			if err != nil {
				if p.failed == nil {
					p.failed = make(map[semreg.AssetID]error)
				}
				p.failed[id] = err
			}
		}
		p.mu.Unlock()
		if err != nil && !operatorRecordDeferred(err) {
			refused = append(refused, err)
		}
	}
	return errors.Join(refused...)
}

// operatorRecordRefusals splits an error returned by Retry into its
// refusals, one per record set.
func operatorRecordRefusals(err error) []error {
	if err == nil {
		return nil
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return joined.Unwrap()
	}
	return []error{err}
}

// watch retries the pending sets after every hub event until none is left.
func (p *operatorRecordPublisher) watch(subscription *semhub.Subscription) {
	defer subscription.Close()
	for {
		// Retry first: an event queued before the goroutine started is covered.
		for _, err := range operatorRecordRefusals(p.Retry()) {
			log.Printf("operator records: a deferred record set was refused and is not published: %v", err)
		}
		if len(p.Pending()) == 0 {
			return
		}
		if _, err := subscription.Next(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
			return
		}
	}
}

// Desired returns the provider's desired records of one asset with their
// revisions, in key order.
func (p *operatorRecordPublisher) Desired(id semreg.AssetID) operatorDesired {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.desired[id]
	out := operatorDesired{Resources: []operatorDesiredResource{}, Relations: []operatorDesiredRelation{}}
	if state == nil {
		return out
	}
	for _, resource := range state.resources {
		out.Resources = append(out.Resources, resource)
	}
	for _, relation := range state.relations {
		out.Relations = append(out.Relations, relation)
	}
	sort.Slice(out.Resources, func(i, j int) bool { return out.Resources[i].Record.ID < out.Resources[j].Record.ID })
	sort.Slice(out.Relations, func(i, j int) bool {
		return operatorRelationKey(out.Relations[i].Record) < operatorRelationKey(out.Relations[j].Record)
	})
	return out
}

// supersedePending replaces the startup-pending sets with the records a
// Portal replacement still wants. A home the replacement omits is dropped,
// so the startup watcher cannot publish a relation the operator deleted.
// Homes that were not pending are not added; the Portal runtime retries those.
func (p *operatorRecordPublisher) supersedePending(sets []operatorAssetRecords) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pending) == 0 {
		return
	}
	keep := make(map[semreg.AssetID]operatorAssetRecords, len(sets))
	for _, set := range sets {
		if len(set.Records.Resources) == 0 && len(set.Records.Relations) == 0 && len(set.Records.Links) == 0 {
			continue
		}
		keep[set.Asset] = set
	}
	next := make(map[semreg.AssetID]operatorAssetRecords)
	for id := range p.pending {
		if set, ok := keep[id]; ok {
			next[id] = set
		}
	}
	p.pending = next
}

// finishPendingLocked records the effect of Set on the startup-pending entry
// of one asset. A refusal a later publication can resolve keeps the records
// just set, so a replacement is what the watcher retries. Any other result
// drops the entry: the set was published, withdrawn or refused for good.
// p.mu must be held.
func (p *operatorRecordPublisher) finishPendingLocked(id semreg.AssetID, records operatorRecords, audit operatorAudit, err error) {
	if err != nil && operatorRecordDeferred(err) {
		// A deferred Set is a deliberate attempt (composition or a Portal
		// replacement). The startup watcher reads this entry and must not
		// keep a set the replacement has moved on from. A missing entry stays
		// missing: composition installs it after Set returns, and a
		// replacement that dropped the home must not be undone.
		if _, had := p.pending[id]; !had {
			return
		}
		p.pending[id] = operatorAssetRecords{Asset: id, Records: records, Audit: audit}
		return
	}
	delete(p.pending, id)
}

// Set replaces the desired operator records of one asset and publishes the
// difference to the asset's current snapshot as one batch of the provider's
// own source, epoch and generation: new and changed records are upserted
// with their next revision, records no longer desired are withdrawn. A set
// whose records do not resolve is refused with an operatorRecordRefusal
// before anything is submitted, and the desired records stay as they were.
// The call also supersedes any startup-pending set of the same asset.
func (p *operatorRecordPublisher) Set(id semreg.AssetID, records operatorRecords, audit operatorAudit) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.setLocked(id, records, audit)
	p.finishPendingLocked(id, records, audit, err)
	return err
}

// setLocked is Set without the publisher lock. It resolves the identity links
// before it publishes the resources and relations, then declares the links.
// A link that does not resolve refuses the whole set before anything is
// submitted, so a replacement cannot withdraw records that were already
// published. p.mu must be held.
func (p *operatorRecordPublisher) setLocked(id semreg.AssetID, records operatorRecords, audit operatorAudit) error {
	plan, err := p.prepareLinksLocked(id, records.Links, audit)
	if err != nil {
		return err
	}
	if err := p.publishLocked(id, records, audit); err != nil {
		return err
	}
	return p.applyLinksLocked(id, plan)
}

// operatorLinkPlan is the identity-link declarations of one asset after they
// have resolved. apply is false when the asset has no links to declare and
// none held, so the caller submits nothing for links.
type operatorLinkPlan struct {
	next    map[semreg.SourceID][]semhub.OperatorLink
	changed []semreg.SourceID
	apply   bool
}

// prepareLinksLocked resolves the configured identity links of one asset to
// the sources whose bindings observe each native resource. It does not
// declare them. A link whose native resource no binding observes yet is
// deferred with operatorReasonNativeUnobserved, and a duplicate native
// resource or a missing audit digest is refused. Either result is returned
// before the rest of the set is submitted. p.mu must be held.
func (p *operatorRecordPublisher) prepareLinksLocked(id semreg.AssetID, links []operatorIdentityLink, audit operatorAudit) (operatorLinkPlan, error) {
	held := p.links[id]
	if len(links) == 0 && len(held) == 0 {
		return operatorLinkPlan{}, nil
	}
	if len(links) > 0 && audit.evidence().Validate() != nil {
		return operatorLinkPlan{}, &operatorRecordRefusal{Asset: id, Reason: operatorReasonAuditMissing}
	}
	view, err := p.hub.Inspect(id)
	if err != nil {
		return operatorLinkPlan{}, err
	}
	next := make(map[semreg.SourceID][]semhub.OperatorLink)
	seen := make(map[semreg.EvidenceRef]bool, len(links))
	for _, link := range links {
		if seen[link.Native] {
			return operatorLinkPlan{}, &operatorRecordRefusal{Asset: id, Resource: link.Resource, Reason: operatorReasonDuplicate}
		}
		seen[link.Native] = true
		sources := map[semreg.SourceID]bool{}
		for _, binding := range view.Snapshot.Bindings {
			if binding.NativeResource == link.Native && binding.SourceID != operatorRecordSource {
				sources[binding.SourceID] = true
			}
		}
		if len(sources) == 0 {
			return operatorLinkPlan{}, &operatorRecordRefusal{Asset: id, Resource: link.Resource, Reason: operatorReasonNativeUnobserved}
		}
		for src := range sources {
			next[src] = append(next[src], semhub.OperatorLink{Native: link.Native, Resource: link.Resource, Basis: []semreg.EvidenceRef{audit.evidence()}})
		}
	}
	changed := make([]semreg.SourceID, 0, len(next)+len(held))
	for src := range held {
		if _, kept := next[src]; !kept {
			changed = append(changed, src)
		}
	}
	for src, declared := range next {
		if !reflect.DeepEqual(held[src], declared) {
			changed = append(changed, src)
		}
	}
	sort.Slice(changed, func(i, j int) bool { return changed[i] < changed[j] })
	return operatorLinkPlan{next: next, changed: changed, apply: true}, nil
}

// applyLinksLocked declares a resolved link plan to the hub and withdraws the
// declarations no longer configured. Nothing is published: the binding's
// source carries the links in its next transaction. p.mu must be held.
func (p *operatorRecordPublisher) applyLinksLocked(id semreg.AssetID, plan operatorLinkPlan) error {
	if !plan.apply {
		return nil
	}
	for _, src := range plan.changed {
		if err := p.hub.SetOperatorLinks(id, src, plan.next[src]); err != nil {
			return err
		}
		if p.links == nil {
			p.links = make(map[semreg.AssetID]map[semreg.SourceID][]semhub.OperatorLink)
		}
		if p.links[id] == nil {
			p.links[id] = make(map[semreg.SourceID][]semhub.OperatorLink)
		}
		if len(plan.next[src]) == 0 {
			delete(p.links[id], src)
		} else {
			p.links[id][src] = plan.next[src]
		}
	}
	if len(p.links[id]) == 0 {
		delete(p.links, id)
	}
	return nil
}

// publishLocked publishes the resources and relations of one asset. p.mu
// must be held.
func (p *operatorRecordPublisher) publishLocked(id semreg.AssetID, records operatorRecords, audit operatorAudit) error {
	state := p.desired[id]
	if state == nil && len(records.Resources) == 0 && len(records.Relations) == 0 {
		return nil
	}
	if err := validateOperatorRecords(id, records, audit); err != nil {
		return err
	}
	// The starting ends are resolved against the published snapshot before
	// the source is admitted, so a refused set leaves the hub untouched, and
	// again under the asset commit lock below.
	view, err := p.hub.Inspect(id)
	if err != nil {
		return err
	}
	if err := resolveOperatorStartingEnds(id, records, view.Snapshot); err != nil {
		return err
	}
	handle, admitted, err := p.handleLocked()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		// An admission whose first publication did not commit is withdrawn,
		// so a refused set leaves no source behind.
		if admitted && !committed {
			if p.hub.Withdraw(handle) == nil {
				p.handle = nil
			}
		}
	}()
	tx, err := p.hub.Begin(handle, id)
	if err != nil {
		return err
	}
	defer tx.Abort()
	current, _ := tx.Current()
	if err := resolveOperatorStartingEnds(id, records, current); err != nil {
		return err
	}
	next, batch, changed, err := p.buildLocked(tx, state, records, audit, current)
	if err != nil {
		return err
	}
	if !changed {
		committed = !admitted
		p.desired[id] = next
		return nil
	}
	_, mono := p.now()
	if _, _, err := tx.Apply(batch, mono); err != nil {
		return err
	}
	if _, err := tx.Commit(nil); err != nil {
		return err
	}
	committed = true
	p.desired[id] = next
	return nil
}

// handleLocked returns the provider's publication handle, admitting the
// source on first use. p.mu must be held.
func (p *operatorRecordPublisher) handleLocked() (*semhub.Handle, bool, error) {
	if p.handle != nil {
		return p.handle, false, nil
	}
	handle, err := p.hub.AdmitSource(semhub.Admission{Source: operatorRecordSource, Driver: operatorRecordDriver})
	if err != nil {
		return nil, false, err
	}
	p.handle = handle
	return handle, true, nil
}

// validateOperatorRecords applies the checks that need no snapshot: the
// audit digest is present and no record key repeats.
func validateOperatorRecords(id semreg.AssetID, records operatorRecords, audit operatorAudit) error {
	if audit.evidence().Validate() != nil {
		return &operatorRecordRefusal{Asset: id, Reason: operatorReasonAuditMissing}
	}
	resources := make(map[semreg.ResourceID]bool, len(records.Resources))
	for _, resource := range records.Resources {
		if resources[resource.ID] {
			return &operatorRecordRefusal{Asset: id, Resource: resource.ID, Reason: operatorReasonDuplicate}
		}
		resources[resource.ID] = true
	}
	relations := make(map[string]bool, len(records.Relations))
	for _, relation := range records.Relations {
		key := operatorRelationKey(relation)
		if relations[key] {
			return &operatorRecordRefusal{Asset: id, Resource: relation.From, Reason: operatorReasonDuplicate}
		}
		relations[key] = true
	}
	return nil
}

// resolveOperatorStartingEnds refuses a relation whose starting end is
// neither a current resource of the asset's snapshot nor a resource of the
// same record set.
func resolveOperatorStartingEnds(id semreg.AssetID, records operatorRecords, current semreg.Snapshot) error {
	homed := make(map[semreg.ResourceID]bool, len(current.Resources)+len(records.Resources))
	for _, resource := range current.Resources {
		if resource.State == semreg.ResourceCurrent {
			homed[resource.ResourceID] = true
		}
	}
	for _, resource := range records.Resources {
		homed[resource.ID] = true
	}
	for _, relation := range records.Relations {
		if !homed[relation.From] {
			return &operatorRecordRefusal{Asset: id, Resource: relation.From, Reason: operatorReasonStartingEndUnresolved}
		}
	}
	return nil
}

// buildLocked builds the provider's next batch from the transaction's
// current snapshot, under the asset commit lock. It returns the next desired
// state and whether the batch changes anything. p.mu must be held.
func (p *operatorRecordPublisher) buildLocked(tx *semhub.Tx, state *operatorAssetState, records operatorRecords, audit operatorAudit, current semreg.Snapshot) (*operatorAssetState, semreg.PublicationBatch, bool, error) {
	inc := tx.Incarnation()
	next := &operatorAssetState{resources: map[semreg.ResourceID]operatorDesiredResource{}, relations: map[string]operatorDesiredRelation{}, revisions: map[string]uint64{}}
	if state != nil {
		for key, revision := range state.revisions {
			next.revisions[key] = revision
		}
	}
	heldResources := make(map[semreg.ResourceID]semreg.Resource, len(current.Resources))
	for _, resource := range current.Resources {
		heldResources[resource.ResourceID] = resource
	}
	heldRelations := make(map[string]semreg.RelationInstance, len(current.Relations))
	for _, relation := range current.Relations {
		heldRelations[operatorRelationKey(operatorRelation{Definition: relation.Definition, From: relation.From, To: relation.To})] = relation
	}
	evidence := []semreg.EvidenceRef{audit.evidence()}
	// revise returns the record's next revision: above the provider's own
	// latest revision of the key and above the snapshot's.
	revise := func(key string, held semreg.Uint64, present bool) uint64 {
		revision := next.revisions[key] + 1
		if present {
			if value, err := strconv.ParseUint(string(held), 10, 64); err == nil && value >= revision {
				revision = value + 1
			}
		}
		next.revisions[key] = revision
		return revision
	}

	var resourceUpserts []semreg.Resource
	for _, desired := range records.Resources {
		key := "resource\x00" + string(desired.ID)
		record := semreg.Resource{ResourceID: desired.ID, Kind: desired.Kind, Anchor: operatorAnchor(desired, evidence), State: semreg.ResourceCurrent, Origin: operatorOrigin(inc.Asset, key, evidence)}
		held, present := heldResources[desired.ID]
		if present {
			compare := held
			compare.Revision = ""
			if reflect.DeepEqual(compare, record) {
				revision, _ := strconv.ParseUint(string(held.Revision), 10, 64)
				next.revisions[key] = max(next.revisions[key], revision)
				next.resources[desired.ID] = operatorDesiredResource{Record: desired, Revision: revision}
				continue
			}
		}
		revision := revise(key, held.Revision, present)
		record.Revision = semreg.Uint64(strconv.FormatUint(revision, 10))
		resourceUpserts = append(resourceUpserts, record)
		next.resources[desired.ID] = operatorDesiredResource{Record: desired, Revision: revision}
	}
	if state != nil {
		for resourceID := range state.resources {
			if _, kept := next.resources[resourceID]; kept {
				continue
			}
			held, present := heldResources[resourceID]
			if !present || held.State != semreg.ResourceCurrent {
				continue
			}
			key := "resource\x00" + string(resourceID)
			withdrawn := held
			withdrawn.State = semreg.ResourceWithdrawn
			withdrawn.Revision = semreg.Uint64(strconv.FormatUint(revise(key, held.Revision, true), 10))
			resourceUpserts = append(resourceUpserts, withdrawn)
		}
	}

	var relationUpserts []semreg.RelationInstance
	for _, desired := range records.Relations {
		key := operatorRelationKey(desired)
		record := semreg.RelationInstance{Definition: desired.Definition, From: desired.From, To: desired.To, State: semreg.LinkQualified, BasisClass: semreg.RelationConfiguration, Basis: evidence, Origin: operatorOrigin(inc.Asset, "relation\x00"+key, evidence)}
		held, present := heldRelations[key]
		if present {
			compare := held
			compare.Revision = ""
			if compare.State == semreg.LinkConflict {
				compare.State = semreg.LinkQualified
			}
			if reflect.DeepEqual(compare, record) {
				revision, _ := strconv.ParseUint(string(held.Revision), 10, 64)
				next.revisions["relation\x00"+key] = max(next.revisions["relation\x00"+key], revision)
				next.relations[key] = operatorDesiredRelation{Record: desired, Revision: revision}
				continue
			}
		}
		revision := revise("relation\x00"+key, held.Revision, present)
		record.Revision = semreg.Uint64(strconv.FormatUint(revision, 10))
		relationUpserts = append(relationUpserts, record)
		next.relations[key] = operatorDesiredRelation{Record: desired, Revision: revision}
	}
	var relationWithdrawals []semreg.RelationKey
	if state != nil {
		for key, desired := range state.relations {
			if _, kept := next.relations[key]; kept {
				continue
			}
			if _, present := heldRelations[key]; present {
				relationWithdrawals = append(relationWithdrawals, semreg.RelationKey{Definition: desired.Record.Definition, From: desired.Record.From, To: desired.Record.To})
			}
		}
	}

	if len(resourceUpserts) == 0 && len(relationUpserts) == 0 && len(relationWithdrawals) == 0 {
		return next, semreg.PublicationBatch{}, false, nil
	}
	sort.Slice(resourceUpserts, func(i, j int) bool { return resourceUpserts[i].ResourceID < resourceUpserts[j].ResourceID })
	sort.Slice(relationUpserts, func(i, j int) bool {
		return operatorRelationKey(operatorRelation{Definition: relationUpserts[i].Definition, From: relationUpserts[i].From, To: relationUpserts[i].To}) <
			operatorRelationKey(operatorRelation{Definition: relationUpserts[j].Definition, From: relationUpserts[j].From, To: relationUpserts[j].To})
	})
	sort.Slice(relationWithdrawals, func(i, j int) bool {
		return operatorRelationKey(operatorRelation(relationWithdrawals[i])) < operatorRelationKey(operatorRelation(relationWithdrawals[j]))
	})

	observed, _ := p.now()
	generation := semreg.Uint64(strconv.FormatUint(inc.Generation, 10))
	sequence := tx.NextSequence()
	expected := semreg.Uint64("0")
	if current.SnapshotID != "" {
		expected = current.Revisions.Semantic
	}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1,
		BatchID: semreg.BatchID("batch:gateway-configuration:" + operatorHash("batch", string(inc.Asset), string(inc.Epoch), string(generation), string(sequence))),
		AssetID: inc.Asset, SourceID: inc.Source, SourceEpochID: inc.Epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: observed,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{},
		ResourceUpserts: resourceUpserts, RelationUpserts: relationUpserts, RelationWithdrawals: relationWithdrawals,
		FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !operatorSourceDescribed(current, inc.Source, inc.Epoch) {
		registry := semreg.EvidenceRef{Owner: operatorRecordOwner, Kind: "configuration.provider", Digest: semreg.Digest("sha256:" + operatorHash("provider")), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: inc.Source, SourceEpochID: inc.Epoch, ProtocolID: "gateway.configuration", ProfileID: "gateway.configuration.operator_records", ProfileVersion: "1", RegistryEvidence: registry, StartedAt: p.started, State: semreg.SourceCurrent, Revision: "1"}}
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		return nil, semreg.PublicationBatch{}, false, err
	}
	batch.BatchDigest = digest
	return next, batch, true, nil
}

// operatorAnchor is the operator slot anchor of a configuration-only
// resource, with the declaration's audit evidence.
func operatorAnchor(resource operatorResource, evidence []semreg.EvidenceRef) semreg.ResourceAnchor {
	slot := resource.Slot
	anchor := semreg.ResourceAnchor{Class: resource.Class, Basis: semreg.AnchorOperatorSlot, Slot: &slot, Evidence: evidence}
	if resource.Parent != "" {
		parent := resource.Parent
		anchor.Parent = &parent
	}
	return anchor
}

// operatorOrigin is the operator origin of one record: no native path, the
// declaration's audit evidence, and an origin identifier derived from the
// asset and the record key.
func operatorOrigin(id semreg.AssetID, key string, evidence []semreg.EvidenceRef) semreg.OriginRef {
	return semreg.OriginRef{OriginID: semreg.OriginID("origin:gateway-configuration:" + operatorHash("origin", string(id), key)), Kind: semreg.OriginOperator, Evidence: evidence}
}

// operatorSourceDescribed reports whether the snapshot holds the provider's
// current source descriptor of its epoch.
func operatorSourceDescribed(snapshot semreg.Snapshot, src semreg.SourceID, epoch semreg.SourceEpochID) bool {
	for _, descriptor := range snapshot.Sources {
		if descriptor.SourceID == src && descriptor.SourceEpochID == epoch && descriptor.State == semreg.SourceCurrent {
			return true
		}
	}
	return false
}

// operatorRelationKey orders relation keys as the kernel does: by the
// canonical bytes of the definition, then the starting and the far end.
func operatorRelationKey(relation operatorRelation) string {
	definition, err := semreg.CanonicalJSON(relation.Definition)
	if err != nil {
		definition = []byte(fmt.Sprintf("%v", relation.Definition))
	}
	return string(definition) + "\x00" + string(relation.From) + "\x00" + string(relation.To)
}

// operatorHash is the provider's domain-separated identifier hash.
func operatorHash(parts ...string) string {
	sum := sha256.Sum256([]byte("helianthus.gateway.configuration.v1\x00" + strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
