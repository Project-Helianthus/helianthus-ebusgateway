package main

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
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
// runtime control service, which is the owner of driver readback.
func (r *gatewayPortalConfigRuntime) callbacks(cfg ebusgateway.Config, control *gatewayRuntimeControl) *portal.ConfigCallbacks {
	callbacks := gatewayPortalConfigCallbacks(r.store, cfg)
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

// Close releases the store's exclusive writer. Repeated calls are harmless.
func (r *gatewayPortalConfigRuntime) Close() error {
	if r == nil || r.store == nil {
		return nil
	}
	return r.store.Close()
}
