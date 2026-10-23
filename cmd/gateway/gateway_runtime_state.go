package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sort"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func initRuntimeStateManager(ctx context.Context, cfg ebusgateway.Config, buildInfo gatewayBuildInfo) (*runtimestate.Manager, *runtimestate.State) {
	mgr := runtimestate.New(runtimestate.Options{
		Path:         cfg.RuntimeStatePath,
		GatewayBuild: gatewayBuildString(buildInfo),
		AddonVersion: buildInfo.ReleaseVersion,
	})
	state, err := mgr.Load(ctx)
	if err != nil {
		log.Printf("runtime_state: load returned error (continuing with empty state): %v", err)
		state = &runtimestate.State{}
	}

	// AD08 eager persist: when -instance-guid is supplied, durably write
	// meta.{schema_version, instance_guid, written_at} within ~1 s so the
	// crash-before-first-periodic-persist window is closed. Provenance per
	// AD27.
	if cfg.InstanceGUID != "" {
		source := identitySourceFromCfg(cfg.InstanceGUIDSource)
		if perr := mgr.EagerPersistInstanceGUID(ctx, cfg.InstanceGUID, source); perr != nil {
			log.Printf("runtime_state: eager-persist instance_guid failed (continuing): %v", perr)
		}
	} else if perr := mgr.Flush(ctx); perr != nil {
		log.Printf("runtime_state: startup metadata persist failed (continuing): %v", perr)
	}

	if serr := mgr.Start(ctx); serr != nil {
		log.Printf("runtime_state: start failed (continuing without periodic persister): %v", serr)
	}

	return mgr, state
}

func projectGatewayReadiness(ebusProxyReadiness func() string, snapshot eebusRuntimeLifecycleSnapshot) portal.RuntimeReadiness {
	eebusReadiness := eebusReadinessForLifecycle(snapshot)
	proxyReadiness := ebusProxyReadinessDisabled
	if ebusProxyReadiness != nil {
		proxyReadiness = ebusProxyReadiness()
	}
	return portal.RuntimeReadiness{
		ProcessReadiness:    string(eebusReadiness.ProcessReadiness),
		HTTPReadiness:       "READY",
		ProxyReadiness:      proxyReadiness,
		EEBusReadiness:      string(eebusReadiness.EEBusReadiness),
		EEBusDegradedReason: string(eebusReadiness.EEBusDegradedReason),
	}
}

// identitySourceFromCfg maps the CLI -instance-guid-source flag value to the
// runtimestate.IdentitySource enum, with AD27 deprecation log when the flag
// is absent. The well-formed value set is enforced here; unknown values fall
// back to "cli-override" with a warning rather than failing startup.
func identitySourceFromCfg(raw string) runtimestate.IdentitySource {
	switch raw {
	case string(runtimestate.IdentitySourceRuntimeState):
		return runtimestate.IdentitySourceRuntimeState
	case string(runtimestate.IdentitySourceLegacyMigrated):
		return runtimestate.IdentitySourceLegacyMigrated
	case string(runtimestate.IdentitySourceGenerated):
		return runtimestate.IdentitySourceGenerated
	case string(runtimestate.IdentitySourceCLIOverride):
		return runtimestate.IdentitySourceCLIOverride
	case "":
		log.Print("runtime_state: -instance-guid-source absent; defaulting to cli-override (deprecated; pass -instance-guid-source explicitly)")
		return runtimestate.IdentitySourceCLIOverride
	default:
		log.Printf("runtime_state: -instance-guid-source=%q is not a recognised AD27 value; treating as cli-override", raw)
		return runtimestate.IdentitySourceCLIOverride
	}
}

// --- runtime control composition: durable store bridge, D5 adapter and
// ebus.primary identity ---
//
// internal/runtimecontrol may import neither internal/runtimestate nor the
// semantic hub, so the production bridge from the J09 runtime_control
// namespace to runtimecontrol.Store lives in this composition layer.

// runtimeControlStore adapts *runtimestate.Manager to runtimecontrol.Store.
// Every call goes straight to the store; the bridge keeps no state.
type runtimeControlStore struct{ m *runtimestate.Manager }

var _ runtimecontrol.Store = (*runtimeControlStore)(nil)

func controlTransitionToStore(t *runtimecontrol.Transition) *runtimestate.ControlTransition {
	if t == nil {
		return nil
	}
	return &runtimestate.ControlTransition{
		Kind: runtimestate.TransitionKind(t.Kind), DesiredRevision: uint64(t.DesiredRevision),
		Sequence: uint64(t.Sequence), Phase: runtimestate.TransitionPhase(t.Phase), Epoch: string(t.Epoch),
		Generation: uint64(t.Generation), NativeClosePending: t.NativeClosePending,
		SemanticCleanupPending: t.SemanticCleanupPending, AttemptsUsed: t.AttemptsUsed, DeadlineUTC: t.DeadlineUTC,
	}
}

func controlTransitionFromStore(t *runtimestate.ControlTransition) *runtimecontrol.Transition {
	if t == nil {
		return nil
	}
	return &runtimecontrol.Transition{
		Kind: runtimecontrol.TransitionKind(t.Kind), DesiredRevision: runtimecontrol.DesiredRevision(t.DesiredRevision),
		Sequence: runtimecontrol.LifecycleSequence(t.Sequence), Phase: runtimecontrol.TransitionPhase(t.Phase),
		Epoch: runtimecontrol.SourceEpoch(t.Epoch), Generation: runtimecontrol.DriverGeneration(t.Generation),
		NativeClosePending: t.NativeClosePending, SemanticCleanupPending: t.SemanticCleanupPending,
		AttemptsUsed: t.AttemptsUsed, DeadlineUTC: t.DeadlineUTC,
	}
}

// controlStoreError converts a store refusal into the service's refusal with
// the same code, row and view; every other error keeps its identity.
func controlStoreError(err error) error {
	var refusal *runtimestate.ControlRefusal
	if errors.As(err, &refusal) {
		out := &runtimecontrol.Refusal{Code: runtimecontrol.RefusalCode(refusal.Code), Row: refusal.Row, Reason: refusal.Reason}
		if refusal.View != nil {
			out.View = &runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredState(refusal.View.DesiredState),
				DesiredRevision: runtimecontrol.DesiredRevision(refusal.View.DesiredRevision)}
		}
		return out
	}
	return err
}

func controlRecordFromStore(v runtimestate.ControlDriverView) runtimecontrol.ControlRecord {
	return runtimecontrol.ControlRecord{
		DriverID: runtimecontrol.DriverID(v.DriverID), Enabled: v.Enabled, Failed: v.Failed, ReadOnly: v.ReadOnly,
		HasRecord: v.HasRecord, DesiredState: runtimecontrol.DesiredState(v.DesiredState),
		DesiredRevision: runtimecontrol.DesiredRevision(v.DesiredRevision),
		Current:         controlTransitionFromStore(v.Current), Reserved: controlTransitionFromStore(v.Reserved),
	}
}

func (s *runtimeControlStore) Decide(ctx context.Context, method runtimecontrol.Method, req runtimecontrol.Request, planner runtimecontrol.Planner) (runtimecontrol.DesiredView, error) {
	view, err := s.m.DecideControl(ctx, runtimestate.ControlMethod(method),
		runtimestate.ControlRequest{DriverID: string(req.DriverID), ExpectedRevision: uint64(req.ExpectedRevision)},
		func(in runtimestate.ControlPlanInput) (runtimestate.ControlPlan, *runtimestate.ControlRefusal) {
			plan, refusal := planner(runtimecontrol.PlanInput{Method: runtimecontrol.Method(in.Method), DriverID: runtimecontrol.DriverID(in.DriverID),
				HasRecord: in.HasRecord, Current: controlTransitionFromStore(in.Record.Current), Reserved: controlTransitionFromStore(in.Record.Reserved)})
			if refusal != nil {
				return runtimestate.ControlPlan{}, &runtimestate.ControlRefusal{Code: runtimestate.RefusalCode(refusal.Code), Row: refusal.Row, Reason: refusal.Reason}
			}
			return runtimestate.ControlPlan{Current: controlTransitionToStore(plan.Current), Reserved: controlTransitionToStore(plan.Reserved)}, nil
		})
	if err != nil {
		return runtimecontrol.DesiredView{}, controlStoreError(err)
	}
	return runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredState(view.DesiredState), DesiredRevision: runtimecontrol.DesiredRevision(view.DesiredRevision)}, nil
}

func (s *runtimeControlStore) CommitWork(ctx context.Context, id runtimecontrol.DriverID, current, reserved *runtimecontrol.Transition) error {
	return controlStoreError(s.m.CommitControlWork(ctx, string(id), controlTransitionToStore(current), controlTransitionToStore(reserved)))
}

func (s *runtimeControlStore) AdvanceCatalog(ctx context.Context) (runtimecontrol.CatalogRevision, error) {
	v, err := s.m.AdvanceControlCatalog(ctx)
	return runtimecontrol.CatalogRevision(v), controlStoreError(err)
}

func (s *runtimeControlStore) CommitRecovery(ctx context.Context, material *runtimecontrol.RecoveryMaterial) error {
	if material == nil {
		return controlStoreError(s.m.CommitControlRecovery(ctx, nil))
	}
	ids := make([]string, 0, len(material.DriverIDs))
	for _, id := range material.DriverIDs {
		ids = append(ids, string(id))
	}
	return controlStoreError(s.m.CommitControlRecovery(ctx, &runtimestate.ControlRecovery{DriverIDs: ids,
		MaxDesiredRevision: uint64(material.MaxDesiredRevision), Payload: append([]byte(nil), material.Payload...)}))
}

func (s *runtimeControlStore) Recovery() *runtimecontrol.RecoveryMaterial {
	stored := s.m.ControlRecoveryPayload()
	if stored == nil {
		return nil
	}
	out := &runtimecontrol.RecoveryMaterial{MaxDesiredRevision: runtimecontrol.DesiredRevision(stored.MaxDesiredRevision), Payload: stored.Payload}
	for _, id := range stored.DriverIDs {
		out.DriverIDs = append(out.DriverIDs, runtimecontrol.DriverID(id))
	}
	return out
}

func (s *runtimeControlStore) Catalog() (runtimecontrol.ControlCatalog, error) {
	v, err := s.m.ControlSnapshot()
	if err != nil {
		return runtimecontrol.ControlCatalog{}, controlStoreError(err)
	}
	out := runtimecontrol.ControlCatalog{ReadOnly: v.ReadOnly, IssuedMark: runtimecontrol.DesiredRevision(v.IssuedRevisionMark),
		CatalogRevision: runtimecontrol.CatalogRevision(v.CatalogRevision)}
	for _, d := range v.Drivers {
		out.Drivers = append(out.Drivers, controlRecordFromStore(d))
	}
	return out, nil
}

func (s *runtimeControlStore) Driver(id runtimecontrol.DriverID) (runtimecontrol.ControlRecord, error) {
	v, err := s.m.ControlDriver(string(id))
	if err != nil {
		return runtimecontrol.ControlRecord{}, controlStoreError(err)
	}
	return controlRecordFromStore(v), nil
}

func (s *runtimeControlStore) AdmissionOpen() bool { return s.m.ControlAdmissionOpen() }

func (s *runtimeControlStore) ResolveReadback(ctx context.Context) error {
	return controlStoreError(s.m.ResolveControlReadback(ctx))
}

// errLifecycleOnlyRecoveryMaterial reports committed D5 material that no
// configured managed writer owns. The lifecycle-only composition never
// writes such material, so its presence fails the named drivers closed
// instead of being ignored or discarded.
var errLifecycleOnlyRecoveryMaterial = errors.New("runtime control: committed recovery material has no managed owner in this composition")

// controlRecovery is the D5 RecoveryAdapter of the gateway composition. It
// runs inside the service's recovery step, before any reconciler or executor
// call, and orders the two pre-admission duties of every driver:
//
//  1. restore committed D5 material first: through the managed adapter
//     (inner, for example a Recorder over managed semantic assets) when one
//     is configured; with no managed adapter, committed material has no
//     owner and fails exactly the drivers it names closed, unchanged;
//  2. then prepare each driver that restoration did not withhold: for the
//     asset-free ebus.primary source this is the inert hub admission with an
//     empty, complete epoch barrier, also when desired state is STOPPED.
//
// A failed restoration or preparation is reported as a scoped restore
// failure, so the service withholds only the named drivers (FAILED until an
// explicit repair) and every other driver keeps running. Secure delegates to
// the managed adapter; lifecycle-only transitions retain nothing new.
type controlRecovery struct {
	inner   runtimecontrol.RecoveryAdapter
	drivers []runtimecontrol.DriverID
	prepare map[runtimecontrol.DriverID]func(context.Context) error
	// withheld names drivers the Portal record withholds. They are failed
	// closed here, whatever their durable intent, so a retained STOPPED
	// intent cannot present them as merely stopped.
	withheld map[runtimecontrol.DriverID]error
}

var (
	_ runtimecontrol.RecoveryAdapter = (*controlRecovery)(nil)
	_ runtimecontrol.EmptyRestorer   = (*controlRecovery)(nil)
	_ runtimecontrol.ReadbackBinder  = (*controlRecovery)(nil)
)

func (r *controlRecovery) Restore(ctx context.Context, material *runtimecontrol.RecoveryMaterial) error {
	var restoreErr error
	blocked := map[runtimecontrol.DriverID]bool{}
	switch {
	case r.inner != nil:
		restoreErr = r.inner.Restore(ctx, material)
		r.block(blocked, restoreErr, nil)
	case material != nil:
		restoreErr = errLifecycleOnlyRecoveryMaterial
		r.block(blocked, restoreErr, material.DriverIDs)
	}
	return r.finish(ctx, blocked, restoreErr)
}

func (r *controlRecovery) RestoreEmpty(ctx context.Context) error {
	var restoreErr error
	blocked := map[runtimecontrol.DriverID]bool{}
	if empty, ok := r.inner.(runtimecontrol.EmptyRestorer); ok {
		restoreErr = empty.RestoreEmpty(ctx)
		r.block(blocked, restoreErr, nil)
	}
	return r.finish(ctx, blocked, restoreErr)
}

// block marks the drivers a restoration failure withholds: the drivers a
// scoped failure names, else the named fallback, else every driver.
func (r *controlRecovery) block(blocked map[runtimecontrol.DriverID]bool, err error, named []runtimecontrol.DriverID) {
	if err == nil {
		return
	}
	var scoped *runtimecontrol.RestoreError
	switch {
	case errors.As(err, &scoped) && len(scoped.Drivers) > 0:
		named = scoped.Drivers
	case len(named) == 0:
		named = r.drivers
	}
	for _, id := range named {
		blocked[id] = true
	}
}

func (r *controlRecovery) finish(ctx context.Context, blocked map[runtimecontrol.DriverID]bool, restoreErr error) error {
	var prepErrs []error
	for _, id := range r.drivers {
		prepare := r.prepare[id]
		if blocked[id] || prepare == nil {
			continue
		}
		if err := prepare(ctx); err != nil {
			blocked[id] = true
			prepErrs = append(prepErrs, fmt.Errorf("%s: %w", id, err))
		}
	}
	for id, err := range r.withheld {
		if !blocked[id] {
			blocked[id] = true
			prepErrs = append(prepErrs, fmt.Errorf("%s: %w", id, err))
		}
	}
	if len(blocked) == 0 {
		return nil
	}
	failed := make([]runtimecontrol.DriverID, 0, len(blocked))
	for id := range blocked {
		failed = append(failed, id)
	}
	sort.Slice(failed, func(i, j int) bool { return failed[i] < failed[j] })
	return &runtimecontrol.RestoreError{Drivers: failed, Err: errors.Join(append([]error{restoreErr}, prepErrs...)...)}
}

func (r *controlRecovery) Secure(ctx context.Context, id runtimecontrol.DriverID, terminal runtimecontrol.Transition) (*runtimecontrol.RecoveryMaterial, error) {
	if r.inner == nil {
		return nil, nil
	}
	return r.inner.Secure(ctx, id, terminal)
}

// BindReadback and ReadbackResolved forward the service's bounded readback
// resolver to a managed adapter that writes its own D5 envelope.
func (r *controlRecovery) BindReadback(arm func()) {
	if binder, ok := r.inner.(runtimecontrol.ReadbackBinder); ok {
		binder.BindReadback(arm)
	}
}

func (r *controlRecovery) ReadbackResolved() {
	if binder, ok := r.inner.(runtimecontrol.ReadbackBinder); ok {
		binder.ReadbackResolved()
	}
}

// errEBusPrimaryIdentityUnavailable reports that the configured owner
// identity of ebus.primary (the persisted instance GUID) is missing, so no
// source identity can be derived and the driver is withheld.
var errEBusPrimaryIdentityUnavailable = errors.New("ebus.primary: no configured instance GUID; source identity unavailable")

// ebusPrimarySourceID derives the stable configured source of ebus.primary
// from its DriverID and the configured owner identity (the persisted
// instance GUID). It never uses an endpoint, address, connection handle or
// discovery result, so reconnecting or changing the endpoint cannot change
// source ownership.
func ebusPrimarySourceID(instanceGUID string) (runtimecontrol.SourceID, error) {
	guid, err := normalizeInstanceGUID(instanceGUID)
	if err != nil || guid == "" {
		return "", errEBusPrimaryIdentityUnavailable
	}
	id, err := semhub.NativeID(semhub.KindSource, "ebus", primaryEBusDriverID, guid)
	if err != nil {
		return "", fmt.Errorf("ebus.primary source identity: %w", err)
	}
	return runtimecontrol.SourceID(id), nil
}

// Native owner of the ebus.primary lifecycle: the public eBUS transport
// module and its DriverRuntime API.
const (
	ebusPrimaryNativeOwner    = "github.com/Project-Helianthus/helianthus-ebusgo"
	ebusPrimaryNativeContract = "transport.DriverRuntime"
)

// ebusPrimaryNativeDevelVersion marks a build whose exact linked native
// module version is not known: a workspace or development build reports a
// devel version, and a Replace directive substitutes the module. The
// reference then names the module path with this marker, registration is
// admitted, and status presents the reference verbatim.
const ebusPrimaryNativeDevelVersion = "devel"

// readEBusPrimaryNativeContract names the native module version linked into
// this binary: the exact module version when the build records one, the
// devel marker otherwise.
var readEBusPrimaryNativeContract = func() (runtimecontrol.NativeContractRef, error) {
	return ebusPrimaryNativeContractFrom(debug.ReadBuildInfo())
}

func ebusPrimaryNativeContractFrom(info *debug.BuildInfo, ok bool) (runtimecontrol.NativeContractRef, error) {
	ref := runtimecontrol.NativeContractRef{Owner: ebusPrimaryNativeOwner, Contract: ebusPrimaryNativeContract, Version: ebusPrimaryNativeDevelVersion}
	if !ok || info == nil {
		return ref, nil
	}
	for _, dep := range info.Deps {
		if dep.Path != ebusPrimaryNativeOwner {
			continue
		}
		if dep.Replace == nil && exactModuleVersion(dep.Version) {
			ref.Version = dep.Version
		}
		return ref, nil
	}
	return ref, nil
}

// exactModuleVersion accepts a released or pseudo module version ("v" then
// a numeric major) and refuses the development placeholder and empty values.
func exactModuleVersion(version string) bool {
	if len(version) < 2 || version[0] != 'v' || version[1] < '0' || version[1] > '9' {
		return false
	}
	for _, r := range version {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '+':
		default:
			return false
		}
	}
	return true
}
