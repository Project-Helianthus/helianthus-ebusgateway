package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// portalConfigDriverIDs is the editable driver set of the Portal record:
// ebus.primary and the three managed sidecars, in bytewise order. It is the
// compiled set, independent of registration outcomes, so a refused
// registration never invalidates an accepted record. The record requires
// exactly this set, so changing it needs a documented, supported record migration.
var portalConfigDriverIDs = []runtimecontrol.DriverID{primaryEBusDriverID, managedEEBusID, managedGrowattID, managedSunSpecID}

// portalConfigRecordFile is the store's record inside the host-owned root.
const portalConfigRecordFile = "portal-config.json"

var errPortalConfigWithheld = errors.New("portal configuration did not load; record-controlled driver withheld until local repair")

// gatewayPortalConfigRuntime loads the bounded Portal record once, before any
// driver is registered, and applies only its driver selections, as configured
// eligibility. The record has no passive observation or cache option, and its
// sharing, proxy and output selections remain unapplied.
type gatewayPortalConfigRuntime struct {
	store *portalconfig.Store
	// loadErr is the cold-load error; nil when a valid record loaded.
	loadErr error
	// failClosed is set when a record may exist but did not load.
	failClosed bool
	// adopted is set when the loaded record carries an accepted local edit.
	// Revision 0 is the host-supplied initial record.
	adopted  bool
	selected map[runtimecontrol.DriverID]bool

	mu      sync.Mutex
	applied map[runtimecontrol.DriverID]portalConfigApplication
	// operators feeds the operator-record section to the configuration
	// provider; nil until the runtime is bound to the publication hub.
	operators *portalOperatorRecords
}

// portalConfigApplication is what startup installed for one driver.
type portalConfigApplication struct {
	host     bool // host-configured eligibility before the record
	enabled  bool // configured eligibility installed at startup
	withheld bool // host-enabled, but withheld because the record did not load
}

// openGatewayPortalConfigRuntime opens the host-owned store with the compiled
// driver set and their host-configured enablement as the initial record.
func openGatewayPortalConfigRuntime(cfg ebusgateway.Config) *gatewayPortalConfigRuntime {
	seed := make([]portalconfig.Driver, 0, len(portalConfigDriverIDs))
	for _, id := range portalConfigDriverIDs {
		seed = append(seed, portalconfig.Driver{ID: string(id), Enabled: portalConfigHostEnabled(cfg, id)})
	}
	runtime := &gatewayPortalConfigRuntime{applied: map[runtimecontrol.DriverID]portalConfigApplication{}}
	runtime.store, runtime.loadErr = openGatewayPortalConfigDrivers(cfg, seed)
	if runtime.loadErr == nil {
		record, err := runtime.store.Read()
		if err == nil {
			runtime.adopted = record.Revision != "0"
			runtime.selected = make(map[runtimecontrol.DriverID]bool, len(record.Drivers))
			for _, driver := range record.Drivers {
				runtime.selected[runtimecontrol.DriverID(driver.ID)] = driver.Enabled
			}
			return runtime
		}
		runtime.loadErr = err
	}
	runtime.failClosed = portalConfigRecordMayExist(cfg.PortalConfigRoot)
	if !runtime.failClosed {
		log.Printf("portal configuration: no record file (%s); host driver configuration applies", runtime.errorCode())
	}
	return runtime
}

// portalConfigHostEnabled is the host-configured disposition that the managed
// composition gives each compiled driver. The eeBUS disposition comes from the
// private eeBUS descriptor seam itself, which constructs nothing.
func portalConfigHostEnabled(cfg ebusgateway.Config, id runtimecontrol.DriverID) bool {
	switch id {
	case primaryEBusDriverID:
		return true
	case managedEEBusID:
		return managedEEBusDescriptor(cfg, runtimecontrol.SourceID(managedEEBusID)).Disposition == runtimecontrol.DispositionEnabled
	case managedGrowattID:
		return cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled
	case managedSunSpecID:
		return modbusTCPV1SlotConfigured(cfg.ModbusTCPConfig)
	}
	return false
}

// portalConfigRecordMayExist separates an absent record (nothing was ever
// adopted, for example under an unprovisioned root) from a record that
// exists but did not load. Anything but a definite absence fails closed.
func portalConfigRecordMayExist(root string) bool {
	_, err := os.Lstat(filepath.Join(root, portalConfigRecordFile))
	return !errors.Is(err, fs.ErrNotExist)
}

// application is the eligibility the record gives one driver, without
// recording it.
func (r *gatewayPortalConfigRuntime) application(id runtimecontrol.DriverID, host bool) portalConfigApplication {
	application := portalConfigApplication{host: host, enabled: host}
	if host && r != nil {
		if r.failClosed {
			application.withheld = true
		} else if selected, ok := r.selected[id]; ok && r.adopted {
			application.enabled = selected
		}
	}
	return application
}

// configure applies the startup record to one driver's host-configured
// eligibility and records the result for readback. The record only narrows
// host configuration, and only after a local edit was accepted. A withheld
// driver keeps its configured eligibility, so the service presents it as
// failed while it stays unconstructed and unopened. A nil runtime leaves
// host configuration unchanged.
func (r *gatewayPortalConfigRuntime) configure(id runtimecontrol.DriverID, host bool) (bool, error) {
	if r == nil {
		return host, nil
	}
	application := r.application(id, host)
	r.mu.Lock()
	r.applied[id] = application
	r.mu.Unlock()
	if application.withheld {
		log.Printf("portal configuration: record did not load (%s); %s withheld until local repair", r.errorCode(), id)
		return true, ebusControlFailure(drivermanager.ReasonConfigInvalid, errPortalConfigWithheld)
	}
	return application.enabled, nil
}

// callbacks binds the Portal configuration API to this store and to the
// runtime control service, which is the owner of driver readback, and binds
// the operator-record section to the control's publication hub.
func (r *gatewayPortalConfigRuntime) callbacks(cfg ebusgateway.Config, control *gatewayRuntimeControl) *portal.ConfigCallbacks {
	callbacks := gatewayPortalConfigCallbacks(r.store, cfg)
	if control != nil {
		r.bindOperatorRecords(control.hub)
	}
	callbacks.OperatorRecords = r.operatorRecordCallbacks()
	callbacks.Drivers = func(_ context.Context, record *portalconfig.Record) []portal.ConfigDriverState {
		return r.driverStates(record, func(id runtimecontrol.DriverID) (runtimecontrol.DriverStatus, bool) {
			service := control.Service()
			if service == nil {
				return runtimecontrol.DriverStatus{}, false
			}
			status, err := service.Get(id)
			return status, err == nil
		})
	}
	return callbacks
}

// driverStates reports each driver's desired selection, what startup applied
// and the owner readback. Effective requires a current RUNNING readback; an
// unstarted, failed or quarantined driver is never reported effective.
func (r *gatewayPortalConfigRuntime) driverStates(record *portalconfig.Record, readback func(runtimecontrol.DriverID) (runtimecontrol.DriverStatus, bool)) []portal.ConfigDriverState {
	ids := portalConfigDriverIDs
	if record != nil {
		ids = make([]runtimecontrol.DriverID, 0, len(record.Drivers))
		for _, driver := range record.Drivers {
			ids = append(ids, runtimecontrol.DriverID(driver.ID))
		}
	}
	states := make([]portal.ConfigDriverState, 0, len(ids))
	for i, id := range ids {
		status, observed := readback(id)
		state := portal.ConfigDriverState{
			DriverID:  string(id),
			Effective: observed && status.Enabled && status.ObservedState == drivermanager.ObservedRunning && !status.SafetyQuarantined,
		}
		if observed {
			state.Recovery = portalConfigRecovery(status)
		}
		if record == nil {
			state.Status, state.Reason = "error", r.errorCode()
		} else {
			desired := record.Drivers[i].Enabled
			state.Desired = &desired
			state.Status, state.Reason = r.selectionStatus(id, desired, record.Revision != "0", status, observed)
		}
		states = append(states, state)
	}
	return states
}

func (r *gatewayPortalConfigRuntime) selectionStatus(id runtimecontrol.DriverID, desired, adopted bool, status runtimecontrol.DriverStatus, observed bool) (string, string) {
	r.mu.Lock()
	application, applied := r.applied[id]
	r.mu.Unlock()
	if !applied {
		// This driver was not composed through the startup application.
		return "withheld", "evidence_unavailable"
	}
	configured := application.host
	if adopted {
		configured = application.host && desired
	}
	switch {
	case application.withheld:
		return "error", r.errorCode()
	case configured != application.enabled:
		// Enablement is configured eligibility, applied only at startup;
		// never START, STOP or RESTART here.
		return "restart_required", "restart_required"
	case desired && !application.host:
		return "withheld", "unsupported_selection"
	case !adopted && desired != application.host:
		return "withheld", "record_not_adopted"
	case !observed:
		return "withheld", "evidence_unavailable"
	case !application.enabled:
		if !status.Enabled {
			return "disabled", "not_selected"
		}
		return "withheld", "evidence_unavailable"
	}
	return portalConfigReadbackStatus(status)
}

// portalConfigRecovery is the recovery schedule of a driver the owner reports
// in backoff, and nil in every other state.
func portalConfigRecovery(status runtimecontrol.DriverStatus) *portal.ConfigDriverRecovery {
	recovery, ok := status.Recovery()
	if !ok {
		return nil
	}
	return &portal.ConfigDriverRecovery{
		Attempt:          recovery.Attempt,
		NextAttemptAt:    recovery.NextAttemptAt,
		LastFailureClass: string(recovery.FailureClass),
		LastFailureCause: recovery.FailureCause,
	}
}

// portalConfigReadbackStatus maps the service view of a configured-enabled
// driver to the closed Portal status vocabulary.
func portalConfigReadbackStatus(status runtimecontrol.DriverStatus) (string, string) {
	switch {
	case !status.Enabled:
		return "withheld", "evidence_unavailable"
	case status.SafetyQuarantined:
		return "error", "safety_quarantined"
	case status.ObservedState == drivermanager.ObservedRunning:
		return "effective", "running"
	case status.ObservedState == drivermanager.ObservedFailed:
		if status.Reason == "" || status.Reason == drivermanager.ReasonNone {
			return "error", "failed"
		}
		return "error", strings.ToLower(string(status.Reason))
	case status.DesiredState == runtimecontrol.DesiredStopped:
		return "withheld", "lifecycle_stopped"
	}
	return "pending", strings.ToLower(string(status.ObservedState))
}

// errorCode is the closed code of the cold-load failure.
func (r *gatewayPortalConfigRuntime) errorCode() string {
	var typed *portalconfig.Error
	if r != nil && errors.As(r.loadErr, &typed) {
		switch typed.Code {
		case "invalid_record", "unsupported_format", "unsupported_selection", "persistence_failed":
			return typed.Code
		}
	}
	return "persistence_failed"
}

// Close stops the operator-record retries and releases the store's
// exclusive writer. Repeated calls are harmless.
func (r *gatewayPortalConfigRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	operators := r.operators
	r.mu.Unlock()
	if operators != nil {
		operators.close()
	}
	if r.store == nil {
		return nil
	}
	return r.store.Close()
}

// errPortalOperatorRecordsUnavailable is returned when no store is mounted.
var errPortalOperatorRecordsUnavailable = errors.New("portal configuration: operator-record section unavailable")

// Waiting reasons of an operator record. A record whose home asset is
// published by the provider's own key waits until the asset is declared, the
// provider is admitted to it and the starting end is published by its owner.
const (
	portalOperatorPublisherUnavailable = "publisher_unavailable"
	portalOperatorNotApplied           = "not_applied"
	portalOperatorHomeNotDeclared      = "home_not_declared"
	portalOperatorHomeNotAdmitted      = "home_not_admitted"
	portalOperatorPublicationFailed    = "publication_failed"
)

// portalOperatorOutcome is the result of the last publication attempt of one
// home asset's records. retry marks a wait that a later publication of
// another source can end.
type portalOperatorOutcome struct {
	status string
	reason string
	retry  bool
}

// portalOperatorRecords feeds the Portal's operator-record section to the
// configuration provider: one desired record set per home asset, the outcome
// of its last attempt, and a retry of each home whose wait a later event of
// another source can end.
type portalOperatorRecords struct {
	publisher *operatorRecordPublisher
	stopOnce  sync.Once
	stop      func()

	mu       sync.Mutex
	desired  map[semreg.AssetID]operatorAssetRecords
	outcomes map[semreg.AssetID]portalOperatorOutcome
	applied  uint64
}

// operatorRecordCallbacks serves the section from the store; a replacement
// is applied to the provider once it is accepted.
func (r *gatewayPortalConfigRuntime) operatorRecordCallbacks() *portal.ConfigOperatorRecords {
	return &portal.ConfigOperatorRecords{
		Read: func(context.Context) (portalconfig.OperatorRecordSet, error) {
			if r.store == nil {
				return portalconfig.OperatorRecordSet{}, errPortalOperatorRecordsUnavailable
			}
			return r.store.OperatorRecords()
		},
		Replace: func(_ context.Context, set portalconfig.OperatorRecordSet) (portalconfig.OperatorRecordSet, error) {
			if r.store == nil {
				return portalconfig.OperatorRecordSet{}, errPortalOperatorRecordsUnavailable
			}
			saved, err := r.store.ReplaceOperatorRecords(set)
			if err == nil {
				r.mu.Lock()
				operators := r.operators
				r.mu.Unlock()
				if operators != nil {
					operators.apply(saved)
				}
			}
			return saved, err
		},
		States: func(_ context.Context, set portalconfig.OperatorRecordSet) []portalconfig.OperatorRecordState {
			return r.operatorRecordStates(set)
		},
	}
}

// bindOperatorRecords starts the configuration provider on the hub once,
// applies the accepted section and retries waiting homes after every later
// hub event. A section that does not load publishes nothing.
func (r *gatewayPortalConfigRuntime) bindOperatorRecords(hub *semhub.Hub) {
	if r == nil || hub == nil || r.store == nil {
		return
	}
	r.mu.Lock()
	if r.operators != nil {
		r.mu.Unlock()
		return
	}
	publisher, err := operatorPublisherFor(hub)
	if err != nil {
		r.mu.Unlock()
		log.Printf("portal configuration: operator records not published: %v", err)
		return
	}
	operators := &portalOperatorRecords{publisher: publisher, desired: map[semreg.AssetID]operatorAssetRecords{}, outcomes: map[semreg.AssetID]portalOperatorOutcome{}}
	// Subscribing before the first application leaves no publication unseen.
	subscription := hub.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	operators.stop = func() {
		// Closing the subscription drops queued events, so a busy hub
		// cannot hold the watcher past the cancellation.
		cancel()
		subscription.Close()
		<-done
	}
	r.operators = operators
	r.mu.Unlock()
	if set, err := r.store.OperatorRecords(); err == nil {
		operators.apply(set)
	} else {
		log.Printf("portal configuration: operator-record section did not load (%s); nothing published", portalconfig.SafeOperatorRecordError(err).Code)
	}
	go operators.watch(ctx, subscription, done)
}

func (o *portalOperatorRecords) close() {
	o.stopOnce.Do(o.stop)
}

// watch retries the waiting homes after every hub event of another source
// until closed. The provider's own admissions, withdrawals and publications
// end no wait, and retrying on them could admit and withdraw the provider
// without end.
func (o *portalOperatorRecords) watch(ctx context.Context, subscription *semhub.Subscription, done chan struct{}) {
	defer close(done)
	defer subscription.Close()
	for {
		event, err := subscription.Next(ctx)
		if err != nil || ctx.Err() != nil {
			return
		}
		if event.Source == operatorRecordSource {
			continue
		}
		o.retry()
	}
}

// apply replaces the desired record sets by those of the section. A home
// that no longer holds a record is given the empty set, so the provider
// withdraws what it published there.
func (o *portalOperatorRecords) apply(set portalconfig.OperatorRecordSet) {
	sets := portalOperatorAssetRecords(set)
	revision, _ := strconv.ParseUint(set.Revision, 10, 64)
	o.mu.Lock()
	defer o.mu.Unlock()
	// Overlapping replacements may reach here out of order; an older
	// revision never replaces a newer one that was applied.
	if revision < o.applied {
		return
	}
	o.applied = revision
	// The startup watcher shares this publisher. Drop or replace its pending
	// sets before publishing, so a relation this section no longer holds cannot
	// be published when the driver asset appears later.
	o.publisher.supersedePending(sets)
	next := make(map[semreg.AssetID]operatorAssetRecords, len(sets))
	for _, assetSet := range sets {
		next[assetSet.Asset] = assetSet
	}
	for home := range o.desired {
		if _, kept := next[home]; !kept {
			next[home] = operatorAssetRecords{Asset: home, Audit: portalOperatorAudit(home, nil)}
		}
	}
	o.desired = next
	o.outcomes = make(map[semreg.AssetID]portalOperatorOutcome, len(next))
	for _, home := range portalOperatorHomes(next) {
		o.setLocked(home)
	}
}

// retry applies again the homes whose wait a publication can end.
func (o *portalOperatorRecords) retry() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, home := range portalOperatorHomes(o.desired) {
		if outcome := o.outcomes[home]; outcome.status == portalconfig.OperatorRecordWaiting && outcome.retry {
			o.setLocked(home)
		}
	}
}

// setLocked publishes one home's desired set and records the outcome. A
// withdrawal that completed is forgotten. o.mu must be held.
func (o *portalOperatorRecords) setLocked(home semreg.AssetID) {
	desired := o.desired[home]
	outcome := portalOperatorClassify(o.publisher.Set(desired.Asset, desired.Records, desired.Audit))
	if outcome.status == portalconfig.OperatorRecordRefused {
		log.Printf("portal configuration: operator records of %s refused: %s", home, outcome.reason)
	}
	if len(desired.Records.Resources) == 0 && len(desired.Records.Relations) == 0 && outcome.status != portalconfig.OperatorRecordWaiting {
		delete(o.desired, home)
		delete(o.outcomes, home)
		return
	}
	o.outcomes[home] = outcome
}

// portalOperatorClassify maps one publication attempt to a record status.
// A home asset that is not declared, not open to the provider, or whose
// starting end its owner has not published yet waits; any other refusal is
// shown with its stable error. A home declared without the provider stays
// so until restart, because declarations are static, so its wait is not
// retried on hub events; the next accepted replacement applies it again.
func portalOperatorClassify(err error) portalOperatorOutcome {
	if err == nil {
		return portalOperatorOutcome{status: portalconfig.OperatorRecordPublished}
	}
	waiting := func(reason string, retry bool) portalOperatorOutcome {
		return portalOperatorOutcome{status: portalconfig.OperatorRecordWaiting, reason: reason, retry: retry}
	}
	var refusal *operatorRecordRefusal
	if errors.As(err, &refusal) {
		if refusal.Reason == operatorReasonStartingEndUnresolved {
			return waiting("starting_end_unresolved", true)
		}
		return portalOperatorOutcome{status: portalconfig.OperatorRecordRefused, reason: string(refusal.Reason)}
	}
	switch {
	case errors.Is(err, semhub.ErrUnknownAsset):
		return waiting(portalOperatorHomeNotDeclared, true)
	case errors.Is(err, semhub.ErrControlPending), errors.Is(err, semhub.ErrReprojectionPending):
		return waiting(portalOperatorHomeNotAdmitted, true)
	case errors.Is(err, semhub.ErrTransaction):
		return waiting(portalOperatorHomeNotAdmitted, false)
	}
	var stable *semreg.Error
	if errors.As(err, &stable) {
		return portalOperatorOutcome{status: portalconfig.OperatorRecordRefused, reason: string(stable.ID)}
	}
	return portalOperatorOutcome{status: portalconfig.OperatorRecordRefused, reason: portalOperatorPublicationFailed}
}

// operatorRecordStates reports each record of a section by the outcomes of
// its home assets: refused before waiting before published. Before the
// runtime is bound every record waits for the publisher.
func (r *gatewayPortalConfigRuntime) operatorRecordStates(set portalconfig.OperatorRecordSet) []portalconfig.OperatorRecordState {
	r.mu.Lock()
	operators := r.operators
	r.mu.Unlock()
	var outcomes map[semreg.AssetID]portalOperatorOutcome
	if operators != nil {
		operators.mu.Lock()
		outcomes = make(map[semreg.AssetID]portalOperatorOutcome, len(operators.outcomes))
		for home, outcome := range operators.outcomes {
			outcomes[home] = outcome
		}
		operators.mu.Unlock()
	}
	rank := map[string]int{portalconfig.OperatorRecordPublished: 0, portalconfig.OperatorRecordWaiting: 1, portalconfig.OperatorRecordRefused: 2}
	states := make([]portalconfig.OperatorRecordState, 0, len(set.Records))
	for i, record := range set.Records {
		state := portalconfig.OperatorRecordState{Index: i, Form: record.Form, Status: portalconfig.OperatorRecordWaiting, Reason: portalOperatorPublisherUnavailable}
		if operators != nil {
			var worst *portalOperatorOutcome
			for _, home := range record.Homes() {
				outcome, ok := outcomes[semreg.AssetID(home)]
				if !ok {
					outcome = portalOperatorOutcome{status: portalconfig.OperatorRecordWaiting, reason: portalOperatorNotApplied}
				}
				if worst == nil || rank[outcome.status] > rank[worst.status] {
					worst = &outcome
				}
			}
			if worst != nil {
				state.Status, state.Reason = worst.status, worst.reason
			}
		}
		states = append(states, state)
	}
	return states
}

// portalOperatorAssetRecords groups the section's records by home asset, in
// bytewise asset order: an anchor and each view of a pairing become an
// operator-slot resource of the catalog class of their kind, a relation a
// configured relation instance, and an identity link a link the provider
// does not publish. An anchor or a pairing view marks its home
// configuration-only. Each home carries the audit digest of the records
// declared for it.
func portalOperatorAssetRecords(set portalconfig.OperatorRecordSet) []operatorAssetRecords {
	byHome := map[semreg.AssetID]*operatorAssetRecords{}
	declared := map[semreg.AssetID][]portalconfig.OperatorRecord{}
	home := func(id string, record portalconfig.OperatorRecord) *operatorAssetRecords {
		asset := semreg.AssetID(id)
		if byHome[asset] == nil {
			byHome[asset] = &operatorAssetRecords{Asset: asset}
		}
		declared[asset] = append(declared[asset], record)
		return byHome[asset]
	}
	resource := func(id string, record portalconfig.OperatorRecord) operatorResource {
		class, _ := portalconfig.OperatorResourceClass(record.Kind)
		return operatorResource{ID: semreg.ResourceID(id), Kind: semreg.DefinitionID(record.Kind), Class: class, Parent: semreg.ResourceID(record.Parent), Slot: record.Slot}
	}
	for _, record := range set.Records {
		switch record.Form {
		case portalconfig.OperatorFormAnchor:
			target := home(record.Home, record)
			target.ConfigurationOnly = true
			target.Records.Resources = append(target.Records.Resources, resource(record.ResourceID, record))
		case portalconfig.OperatorFormRelation:
			if record.Definition == nil {
				continue
			}
			target := home(record.Home, record)
			target.Records.Relations = append(target.Records.Relations, operatorRelation{Definition: *record.Definition, From: semreg.ResourceID(record.From), To: semreg.ResourceID(record.To)})
		case portalconfig.OperatorFormPairing:
			for _, view := range record.Views {
				target := home(view.Home, record)
				target.ConfigurationOnly = true
				target.Records.Resources = append(target.Records.Resources, resource(view.ResourceID, record))
			}
		case portalconfig.OperatorFormIdentityLink:
			if record.Native == nil {
				continue
			}
			target := home(record.Home, record)
			target.Records.Links = append(target.Records.Links, operatorIdentityLink{Native: *record.Native, Resource: semreg.ResourceID(record.ResourceID), BasisClass: record.BasisClass})
		}
	}
	out := make([]operatorAssetRecords, 0, len(byHome))
	for _, asset := range portalOperatorHomes(byHome) {
		target := *byHome[asset]
		target.Audit = portalOperatorAudit(asset, declared[asset])
		out = append(out, target)
	}
	return out
}

// portalOperatorAudit is the audit of the declarations of one home: the
// domain-separated digest of the home and its records as stored.
func portalOperatorAudit(home semreg.AssetID, records []portalconfig.OperatorRecord) operatorAudit {
	if records == nil {
		records = []portalconfig.OperatorRecord{}
	}
	encoded, _ := json.Marshal(records)
	sum := sha256.Sum256(append([]byte("helianthus.portal-config.operator-records.v1\x00"+string(home)+"\x00"), encoded...))
	return operatorAudit{Declaration: semreg.Digest("sha256:" + hex.EncodeToString(sum[:]))}
}

// portalOperatorHomes returns the keys of a home map in bytewise order.
func portalOperatorHomes[T any](homes map[semreg.AssetID]T) []semreg.AssetID {
	out := make([]semreg.AssetID, 0, len(homes))
	for home := range homes {
		out = append(out, home)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
