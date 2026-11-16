package portal

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
)

// ConfigCallbacks are host-owned persistence callbacks. They do not apply runtime changes.
type ConfigCallbacks struct {
	Read   func(context.Context) (portalconfig.Record, error)
	Update func(context.Context, portalconfig.Record) (portalconfig.Record, error)
	// Bindings snapshots host provisioned listeners; nil withholds listener plans.
	Bindings func(context.Context) portalconfig.ListenerAvailability
	// Drivers reports host-owned driver application and owner readback for the
	// shown record, or for nil when no valid record is available. Nil keeps
	// every driver withheld with evidence_unavailable.
	Drivers func(context.Context, *portalconfig.Record) []ConfigDriverState
	// OperatorRecords serves the operator-record section; nil withholds it.
	OperatorRecords *ConfigOperatorRecords
}

// ConfigOperatorRecords are the host callbacks of the operator-record
// section: read and replace the accepted section, and report the
// publication status of each record of a section. A nil States keeps every
// record waiting with evidence_unavailable, so the Portal never claims a
// publication the host did not report.
type ConfigOperatorRecords struct {
	Read    func(context.Context) (portalconfig.OperatorRecordSet, error)
	Replace func(context.Context, portalconfig.OperatorRecordSet) (portalconfig.OperatorRecordSet, error)
	States  func(context.Context, portalconfig.OperatorRecordSet) []portalconfig.OperatorRecordState
}

// operatorRecordsSection is the query value that selects the section.
const operatorRecordsSection = "operator_records"

func (o *ConfigOperatorRecords) states(ctx context.Context, set portalconfig.OperatorRecordSet) []portalconfig.OperatorRecordState {
	if o != nil && o.States != nil {
		if states := o.States(ctx, set); states != nil {
			return states
		}
		return []portalconfig.OperatorRecordState{}
	}
	states := make([]portalconfig.OperatorRecordState, 0, len(set.Records))
	for i, record := range set.Records {
		states = append(states, portalconfig.OperatorRecordState{Index: i, Form: record.Form, Status: portalconfig.OperatorRecordWaiting, Reason: "evidence_unavailable"})
	}
	return states
}

func operatorRecordsView(ctx context.Context, o *ConfigOperatorRecords, set portalconfig.OperatorRecordSet) map[string]any {
	return map[string]any{"desired": set, "state": map[string]any{"records": o.states(ctx, set)}}
}

// operatorRecordsSummary is the section as the configuration view carries
// it: omitted when the host serves no section or the section is empty.
func operatorRecordsSummary(ctx context.Context, o *ConfigOperatorRecords) map[string]any {
	if o == nil || o.Read == nil {
		return nil
	}
	set, err := o.Read(ctx)
	if err != nil {
		return map[string]any{"desired": nil, "error": portalconfig.SafeOperatorRecordError(err)}
	}
	if len(set.Records) == 0 {
		return nil
	}
	return operatorRecordsView(ctx, o, set)
}

// handleOperatorRecords reads (GET) or replaces (PUT) the whole
// operator-record section. A refusal keeps the accepted section visible as
// historical and names the field that caused it.
func (h *handler) handleOperatorRecords(w http.ResponseWriter, r *http.Request, selected []string) {
	var o *ConfigOperatorRecords
	if h.opts.Config != nil {
		o = h.opts.Config.OperatorRecords
	}
	fail := func(err error, historical *portalconfig.OperatorRecordSet) {
		safe := portalconfig.SafeOperatorRecordError(err)
		status := http.StatusBadRequest
		switch safe.Code {
		case "persistence_failed", "catalog_unavailable":
			status = http.StatusServiceUnavailable
		case "revision_conflict", "revision_exhausted":
			status = http.StatusConflict
		}
		view := map[string]any{"desired": nil, "error": safe, "state": map[string]any{"records": []portalconfig.OperatorRecordState{}}}
		if historical != nil && historical.Format == portalconfig.OperatorRecordsFormat {
			view = operatorRecordsView(r.Context(), o, *historical)
			view["historical"] = true
			view["error"] = safe
		}
		writeJSON(w, status, view)
	}
	if len(selected) != 1 || selected[0] != operatorRecordsSection {
		fail(&portalconfig.Error{Code: "invalid_record", Field: "section"}, nil)
		return
	}
	if o == nil || o.Read == nil || (r.Method == http.MethodPut && o.Replace == nil) {
		fail(errors.New("unavailable"), nil)
		return
	}
	if r.Method == http.MethodGet {
		set, err := o.Read(r.Context())
		if err != nil {
			fail(err, nil)
			return
		}
		writeJSON(w, http.StatusOK, operatorRecordsView(r.Context(), o, set))
		return
	}
	var set portalconfig.OperatorRecordSet
	data, err := io.ReadAll(io.LimitReader(r.Body, portalconfig.MaxBytes+1))
	if err != nil {
		err = &portalconfig.Error{Code: "invalid_record", Field: "operator_records"}
	} else {
		set, err = portalconfig.DecodeOperatorRecords(data)
	}
	if err == nil {
		set, err = o.Replace(r.Context(), set)
	}
	if err != nil {
		// Read can retain the accepted section even when the write failed.
		accepted, readErr := o.Read(r.Context())
		if readErr != nil {
			fail(err, nil)
			return
		}
		fail(err, &accepted)
		return
	}
	writeJSON(w, http.StatusOK, operatorRecordsView(r.Context(), o, set))
}

// ConfigDriverState is one driver's desired selection (nil when no valid
// record is available), owner-confirmed effective state and closed status.
// Recovery is present only while the owner reports the driver in backoff with a
// schedule; it is absent, never null, in every other state.
type ConfigDriverState struct {
	DriverID  string                `json:"driver_id"`
	Desired   *bool                 `json:"desired"`
	Effective bool                  `json:"effective"`
	Status    string                `json:"status"`
	Reason    string                `json:"reason"`
	Recovery  *ConfigDriverRecovery `json:"recovery,omitempty"`
}

// ConfigDriverRecovery is the recovery schedule of a driver in backoff, in the
// shape of the MCP runtime tools' recovery member: the one-based number of the
// attempt scheduled for NextAttemptAt, its due time (RFC 3339 UTC, Z
// designator), the failure class (DIAL_FAILED, TIMEOUT, TRANSPORT_CLOSED or
// OTHER) and the address-free cause text of at most 160 printable ASCII
// characters.
type ConfigDriverRecovery struct {
	Attempt          int    `json:"attempt"`
	NextAttemptAt    string `json:"next_attempt_at"`
	LastFailureClass string `json:"last_failure_class"`
	LastFailureCause string `json:"last_failure_cause"`
}

// driverStates never lets a missing host callback claim application.
func (c *ConfigCallbacks) driverStates(ctx context.Context, r *portalconfig.Record) []ConfigDriverState {
	if c != nil && c.Drivers != nil {
		if states := c.Drivers(ctx, r); states != nil {
			return states
		}
		return []ConfigDriverState{}
	}
	states := []ConfigDriverState{}
	if r == nil {
		return states
	}
	for _, d := range r.Drivers {
		desired := d.Enabled
		states = append(states, ConfigDriverState{DriverID: d.ID, Desired: &desired, Status: "withheld", Reason: "evidence_unavailable"})
	}
	return states
}

func configView(r portalconfig.Record, drivers []ConfigDriverState, availability ...portalconfig.ListenerAvailability) map[string]any {
	var bindings portalconfig.ListenerAvailability
	if len(availability) == 1 {
		bindings = availability[0]
	}
	plan := portalconfig.DeriveListenerPlan(r, bindings)
	return map[string]any{"desired": r, "listener_plan": plan, "listener_application": "pending", "state": map[string]any{"drivers": drivers, "sharing": map[string]any{"graphql": plan.Surfaces[1], "mcp": plan.Surfaces[2]}, "adapter_proxy": plan.Surfaces[3], "output_bindings": map[string]any{"matter": plan.Surfaces[4]}}}
}

func configError(w http.ResponseWriter, err error, drivers func(*portalconfig.Record) []ConfigDriverState, historical ...portalconfig.Record) {
	safe := &portalconfig.Error{Code: "persistence_failed", Field: "record", NextStep: "check local storage"}
	var typed *portalconfig.Error
	if errors.As(err, &typed) {
		steps := map[string]string{"invalid_record": "repair structure", "unsupported_format": "use supported format", "unsupported_selection": "choose a provisioned option", "revision_conflict": "reload", "revision_exhausted": "use a future supported format migration", "listener_conflict": "choose non-conflicting host bindings", "invalid_binding": "repair host listener configuration"}
		if step, ok := steps[typed.Code]; ok {
			safe = &portalconfig.Error{Code: typed.Code, Field: "record", NextStep: step}
		}
	}
	status := http.StatusBadRequest
	switch safe.Code {
	case "persistence_failed":
		status = http.StatusServiceUnavailable
	case "revision_conflict", "revision_exhausted":
		status = http.StatusConflict
	}
	unavailable := map[string]any{"desired": nil, "effective": false, "status": "error", "reason": safe.Code}
	view := map[string]any{"desired": nil, "error": safe, "state": map[string]any{
		"drivers":         drivers(nil),
		"sharing":         map[string]any{"graphql": unavailable, "mcp": unavailable},
		"adapter_proxy":   unavailable,
		"output_bindings": map[string]any{"matter": unavailable},
	}}
	if len(historical) == 1 && historical[0].Format == portalconfig.Format {
		view = configView(historical[0], drivers(&historical[0]))
		view["historical"] = true
		view["error"] = safe
	}
	writeJSON(w, status, view)
}

func (h *handler) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	c := h.opts.Config
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		w.Header().Set("Allow", "GET, PUT")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if selected, ok := r.URL.Query()["section"]; ok {
		h.handleOperatorRecords(w, r, selected)
		return
	}
	drivers := func(record *portalconfig.Record) []ConfigDriverState { return c.driverStates(r.Context(), record) }
	if c == nil || c.Read == nil || (r.Method == http.MethodPut && c.Update == nil) {
		configError(w, errors.New("unavailable"), drivers)
		return
	}
	var availability portalconfig.ListenerAvailability
	if c.Bindings != nil {
		availability = c.Bindings(r.Context())
	}
	var record portalconfig.Record
	var err error
	if r.Method == http.MethodGet {
		record, err = c.Read(r.Context())
	} else {
		data, e := io.ReadAll(io.LimitReader(r.Body, portalconfig.MaxBytes+1))
		if e != nil {
			err = &portalconfig.Error{Code: "invalid_record"}
		} else {
			record, err = portalconfig.Decode(data)
		}
		if err != nil {
			// Read can retain accepted history even when storage is unavailable.
			record, _ = c.Read(r.Context())
		} else {
			plan := portalconfig.DeriveListenerPlan(record, availability)
			if len(plan.Conflicts) > 0 {
				err = &portalconfig.Error{Code: "listener_conflict"}
			}
			for _, surface := range plan.Surfaces {
				if err == nil && surface.Reason == "invalid_binding" {
					err = &portalconfig.Error{Code: "invalid_binding"}
				}
			}
			if err != nil {
				record, _ = c.Read(r.Context())
			} else {
				record, err = c.Update(r.Context(), record)
			}
		}
	}
	if err != nil {
		configError(w, err, drivers, record)
		return
	}
	view := configView(record, drivers(&record), availability)
	if section := operatorRecordsSummary(r.Context(), c.OperatorRecords); section != nil {
		view["operator_records"] = section
	}
	writeJSON(w, http.StatusOK, view)
}
