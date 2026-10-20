// Package runtimestate — durable runtime_control namespace (G5 storage seam).
//
// This file mirrors the D3/D2 durable control contract of the docs-ebus
// runtime driver lifecycle page (sections "S1/D3: minimal durable control
// decisions" and "S3/D2: bootstrap, load failures and explicit repair") as a
// storage-level seam: the independently versioned namespace, its load
// dispositions, the namespace-wide serialization point, the atomic durable
// control transaction and the one-time bootstrap seed. It performs no live
// reconciliation and no endpoint I/O: callers receive a startup plan and
// decision results, never a started driver.
//
// The only durable control data are the versioned desired state and desired
// revision per driver, the namespace-wide issued-revision mark, the catalog
// view revision, a bounded typed in-flight transition record per driver and
// a typed, writer-opaque D5 recovery payload. There is no receipt, idempotency
// key, principal, request digest or completed-operation store.

package runtimestate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ControlNamespaceName is the top-level runtime-state key of the namespace.
const ControlNamespaceName = "runtime_control"

// ControlSchemaVersion is the only runtime_control format version this
// package decodes. Any other version is preserved verbatim and fails control
// closed; it is never dropped or read as missing.
const ControlSchemaVersion = 1

// ControlRecoveryFormat tags the typed D5 recovery payload envelope.
const ControlRecoveryFormat = "helianthus.runtime-control.d5-recovery/v1"

// Version 1 bounds. The record limit counts driver records; the issued
// revision mark is the one additional namespace record.
const (
	MaxControlDriverRecords    = 128
	MaxControlSemanticAttempts = 8
	maxControlEpochBytes       = 256
)

// maxControlNamespaceBytes bounds the encoded namespace (64 MiB). It is a
// variable only so tests can exercise the byte limit without allocating it.
var maxControlNamespaceBytes = 64 << 20

// DefaultControlWaitBound is the default bounded wait at the namespace-wide
// serialization point. The effective wait never outlasts the caller deadline.
const DefaultControlWaitBound = 2 * time.Second

// Quarantine copy name infixes. The runtime-state corrupt rename and the
// offline reset copy both count as control quarantine copies by existence.
const (
	corruptQuarantineInfix = ".corrupt-"
	controlQuarantineInfix = ".runtime-control-quarantine-"
)

// DesiredState is durable driver intent.
type DesiredState string

const (
	DesiredRunning DesiredState = "RUNNING"
	DesiredStopped DesiredState = "STOPPED"
)

// ControlMethod is the lifecycle method selecting a decision.
type ControlMethod string

const (
	ControlStart   ControlMethod = "START"
	ControlStop    ControlMethod = "STOP"
	ControlRestart ControlMethod = "RESTART"
)

// TransitionKind is the exact stored kind of one typed work slot.
// CANCEL_ATTEMPT is the synthesized cancellation of an untyped attempt.
type TransitionKind string

const (
	TransitionStart         TransitionKind = "START"
	TransitionStop          TransitionKind = "STOP"
	TransitionRestart       TransitionKind = "RESTART"
	TransitionCancelAttempt TransitionKind = "CANCEL_ATTEMPT"
)

// TransitionPhase is the persisted phase of one typed work slot.
type TransitionPhase string

const (
	PhasePending TransitionPhase = "PENDING"
	PhaseActive  TransitionPhase = "ACTIVE"
	PhaseCleanup TransitionPhase = "CLEANUP"
)

// ControlTransition is one bounded typed in-flight transition/cleanup record.
// It is internal recovery bookkeeping, never a client operation handle.
type ControlTransition struct {
	Kind                   TransitionKind
	DesiredRevision        uint64
	Sequence               uint64
	Phase                  TransitionPhase
	Epoch                  string
	Generation             uint64
	NativeClosePending     bool
	SemanticCleanupPending bool
	AttemptsUsed           int
	DeadlineUTC            time.Time
}

// ControlDriverRecord is the durable record of one decided driver.
type ControlDriverRecord struct {
	DesiredState      DesiredState
	DesiredRevision   uint64
	LifecycleSequence uint64
	Current           *ControlTransition
	Reserved          *ControlTransition
}

// ControlRecovery is the typed D5 recovery envelope. The payload is opaque to
// this writer: it is stored and returned byte-exactly and verified by digest.
// DriverIDs names the drivers whose recovery material the payload holds, and
// MaxDesiredRevision bounds every desired revision recorded in it.
type ControlRecovery struct {
	DriverIDs          []string
	MaxDesiredRevision uint64
	Payload            []byte
}

// ControlLoadDisposition distinguishes the runtime_control load outcomes.
// Missing, corrupt, unreadable and unsupported never collapse.
type ControlLoadDisposition string

const (
	// ControlLoadMissing: readable state with no control material at all; the
	// only disposition eligible for the one-time bootstrap seed.
	ControlLoadMissing ControlLoadDisposition = "MISSING"
	// ControlLoadLoaded: a valid version-1 namespace.
	ControlLoadLoaded ControlLoadDisposition = "LOADED"
	// ControlLoadMissingWithMaterial: no namespace although a quarantine
	// copy exists; a load failure that requires repair.
	ControlLoadMissingWithMaterial ControlLoadDisposition = "MISSING_WITH_MATERIAL"
	// ControlLoadCorrupt: whole-file corruption (quarantined) or a namespace
	// whose structure, mark, catalog revision or recovery cannot be validated.
	ControlLoadCorrupt ControlLoadDisposition = "CORRUPT"
	// ControlLoadUnreadable: the state file could not be read.
	ControlLoadUnreadable ControlLoadDisposition = "UNREADABLE"
	// ControlLoadUnsupported: a runtime_control schema version other than 1.
	ControlLoadUnsupported ControlLoadDisposition = "UNSUPPORTED"
	// ControlLoadLocked: another writer holds the exclusive store lock.
	ControlLoadLocked ControlLoadDisposition = "LOCKED"
)

// ControlLoadStatus is the load result exposed to the shell.
type ControlLoadStatus struct {
	Disposition ControlLoadDisposition
	Detail      string
}

// Failed reports whether control failed closed for the whole namespace.
func (s ControlLoadStatus) Failed() bool {
	return s.Disposition != ControlLoadMissing && s.Disposition != ControlLoadLoaded
}

// RefusalCode is one closed control refusal code.
type RefusalCode string

const (
	RefusalInvalidArgument    RefusalCode = "INVALID_ARGUMENT"
	RefusalNotFound           RefusalCode = "NOT_FOUND"
	RefusalFailedPrecondition RefusalCode = "FAILED_PRECONDITION"
	RefusalUnavailable        RefusalCode = "UNAVAILABLE"
	RefusalDeadlineExceeded   RefusalCode = "DEADLINE_EXCEEDED"
	RefusalRevisionConflict   RefusalCode = "REVISION_CONFLICT"
	RefusalTransitionBusy     RefusalCode = "TRANSITION_BUSY"
	RefusalResourceExhausted  RefusalCode = "RESOURCE_EXHAUSTED"
	RefusalCounterExhausted   RefusalCode = "COUNTER_EXHAUSTED"
)

// ControlRefusal is a refused control request. Row is the refusal-table
// order, or 0 for the pre-commit write failure outside the ordered checks.
// View is set only for REVISION_CONFLICT. Every refusal leaves durable state,
// the mark and both revisions unchanged.
type ControlRefusal struct {
	Code   RefusalCode
	Row    int
	View   *ControlView
	Reason string
}

func (r *ControlRefusal) Error() string {
	return fmt.Sprintf("runtime_control: %s (row %d): %s", r.Code, r.Row, r.Reason)
}

// Retryable reports the refusal table's retryable column.
func (r *ControlRefusal) Retryable() bool {
	switch r.Code {
	case RefusalUnavailable, RefusalDeadlineExceeded, RefusalRevisionConflict,
		RefusalTransitionBusy, RefusalResourceExhausted:
		return true
	}
	return false
}

func refuse(code RefusalCode, row int, reason string) *ControlRefusal {
	return &ControlRefusal{Code: code, Row: row, Reason: reason}
}

// ControlView is the closed decision result: desired state and desired
// revision only.
type ControlView struct {
	DesiredState    DesiredState
	DesiredRevision uint64
}

// MarshalJSON encodes {desired_state, desired_revision} with the revision as
// a canonical decimal string.
func (v ControlView) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		DesiredState    DesiredState `json:"desired_state"`
		DesiredRevision string       `json:"desired_revision"`
	}{v.DesiredState, FormatControlRevision(v.DesiredRevision)})
}

// ControlRequest is the closed two-field lifecycle request.
type ControlRequest struct {
	DriverID         string
	ExpectedRevision uint64
}

// ControlDriverConfig is one configured driver and its eligibility.
type ControlDriverConfig struct {
	DriverID string
	Enabled  bool
}

// ControlStartupAction is the startup reconciliation derived from durable
// desired state. It is a plan only; this package opens no endpoint.
type ControlStartupAction string

const (
	StartupStarting ControlStartupAction = "STARTING"
	StartupStopped  ControlStartupAction = "STOPPED"
	StartupDisabled ControlStartupAction = "DISABLED"
	StartupFailed   ControlStartupAction = "FAILED"
)

// ControlStartup is the derived startup entry of one configured driver.
type ControlStartup struct {
	DriverID        string
	Action          ControlStartupAction
	DesiredState    DesiredState
	DesiredRevision uint64
	Current         *ControlTransition
	Reserved        *ControlTransition
}

// ControlDriverView is a coherent read of one visible driver. ReadOnly marks
// the last-known-good durable view loaded while control is LOCKED: it is
// readable, but no decision, work, catalog or recovery write is admitted.
type ControlDriverView struct {
	DriverID        string
	Enabled         bool
	Failed          bool
	ReadOnly        bool
	HasRecord       bool
	DesiredState    DesiredState
	DesiredRevision uint64
	Current         *ControlTransition
	Reserved        *ControlTransition
}

// ControlCatalogView is one coherent read of the committed namespace.
// ReadOnly has the ControlDriverView meaning.
type ControlCatalogView struct {
	ReadOnly           bool
	IssuedRevisionMark uint64
	CatalogRevision    uint64
	Drivers            []ControlDriverView
}

// ControlPlanInput is what a planner sees under the serialization point.
type ControlPlanInput struct {
	Method    ControlMethod
	DriverID  string
	HasRecord bool
	Record    ControlDriverRecord
}

// ControlPlan is the complete replacement of a driver's two typed slots. A
// new transition carries Sequence 0 and DesiredRevision 0; the store assigns
// the accepted revision and the next lifecycle sequences (Current first).
// A retained transition must equal the stored one exactly.
type ControlPlan struct {
	Current  *ControlTransition
	Reserved *ControlTransition
}

// ControlPlanner decides slot placement for a changing request. It may refuse
// only with refusal-table rows 7 (FAILED_PRECONDITION) or 8 (TRANSITION_BUSY).
// It runs under the serialization point and must not block or perform I/O.
type ControlPlanner func(ControlPlanInput) (ControlPlan, *ControlRefusal)

// Errors returned by the control seam outside the refusal table.
var (
	ErrControlNotBootstrappable = errors.New("runtime_control: state is not eligible for the bootstrap seed")
	ErrControlBootstrapGUID     = errors.New("runtime_control: bootstrap requires a valid persisted instance GUID")
	ErrControlInvalidConfig     = errors.New("runtime_control: invalid driver configuration")
	ErrControlReadbackPending   = errors.New("runtime_control: ambiguous persistence awaiting readback")
	ErrControlInvalidRecovery   = errors.New("runtime_control: invalid recovery envelope")
	ErrControlInvalidWork       = errors.New("runtime_control: invalid transition record")
)

var (
	controlDriverIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)
	controlEpochPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]*$`)
	instanceGUIDPattern    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	controlSHA256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// CanonicalControlDriverID reports whether id is a canonical driver ID
// (the registry rule: lowercase dotted/dashed segments, at most 128 bytes).
func CanonicalControlDriverID(id string) bool {
	return len(id) > 0 && len(id) <= 128 && controlDriverIDPattern.MatchString(id)
}

// FormatControlRevision encodes a revision as a canonical decimal string.
func FormatControlRevision(v uint64) string { return strconv.FormatUint(v, 10) }

// ParseControlRevision decodes a canonical decimal string: ASCII digits only,
// no sign, no leading zero except "0", at most the unsigned 64-bit maximum.
func ParseControlRevision(s string, allowZero bool) (uint64, bool) {
	if len(s) == 0 || len(s) > 20 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	if len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || (v == 0 && !allowZero) {
		return 0, false
	}
	return v, true
}

// ParseControlRequest applies closed decoding (refusal-table row 1): exactly
// the members DriverID and ExpectedRevision, both strings, no duplicate or
// additional member, no trailing JSON, and ExpectedRevision a canonical
// decimal string of a nonzero unsigned 64-bit value. DriverID canonicality
// and registration are row 2 and are checked by DecideControl.
func ParseControlRequest(data []byte) (ControlRequest, error) {
	invalid := func(reason string) (ControlRequest, error) {
		return ControlRequest{}, refuse(RefusalInvalidArgument, 1, reason)
	}
	fields, err := strictObject(data)
	if err != nil {
		return invalid("request is not one closed JSON object")
	}
	if len(fields) != 2 {
		return invalid("request must contain exactly DriverID and ExpectedRevision")
	}
	rawID, okID := fields["DriverID"]
	rawRev, okRev := fields["ExpectedRevision"]
	if !okID || !okRev {
		return invalid("request must contain exactly DriverID and ExpectedRevision")
	}
	id, err := decodeJSONString(rawID)
	if err != nil {
		return invalid("DriverID must be a string")
	}
	revText, err := decodeJSONString(rawRev)
	if err != nil {
		return invalid("ExpectedRevision must be a string")
	}
	rev, ok := ParseControlRevision(revText, false)
	if !ok {
		return invalid("ExpectedRevision must be a canonical nonzero decimal string")
	}
	return ControlRequest{DriverID: id, ExpectedRevision: rev}, nil
}

// --- namespace model ---

type controlFailedRecord struct {
	raw    json.RawMessage
	reason string
}

// controlNamespace is the in-memory runtime_control namespace. When status is
// not Loaded, raw holds the namespace value verbatim and is re-emitted on
// every write so unsupported or malformed bytes round-trip.
type controlNamespace struct {
	status   ControlLoadDisposition
	detail   string
	raw      json.RawMessage
	mark     uint64
	catalog  uint64
	records  map[string]ControlDriverRecord
	failed   map[string]controlFailedRecord
	recovery *ControlRecovery
}

func newLoadedControlNamespace() *controlNamespace {
	return &controlNamespace{
		status:  ControlLoadLoaded,
		catalog: 1,
		records: map[string]ControlDriverRecord{},
		failed:  map[string]controlFailedRecord{},
	}
}

func cloneTransition(t *ControlTransition) *ControlTransition {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}

func cloneRecord(r ControlDriverRecord) ControlDriverRecord {
	r.Current = cloneTransition(r.Current)
	r.Reserved = cloneTransition(r.Reserved)
	return r
}

func cloneRecovery(r *ControlRecovery) *ControlRecovery {
	if r == nil {
		return nil
	}
	return &ControlRecovery{
		DriverIDs:          append([]string(nil), r.DriverIDs...),
		MaxDesiredRevision: r.MaxDesiredRevision,
		Payload:            append([]byte(nil), r.Payload...),
	}
}

func (ns *controlNamespace) clone() *controlNamespace {
	if ns == nil {
		return nil
	}
	cp := *ns
	cp.raw = append(json.RawMessage(nil), ns.raw...)
	cp.records = make(map[string]ControlDriverRecord, len(ns.records))
	for id, r := range ns.records {
		cp.records[id] = cloneRecord(r)
	}
	cp.failed = make(map[string]controlFailedRecord, len(ns.failed))
	for id, f := range ns.failed {
		cp.failed[id] = controlFailedRecord{raw: append(json.RawMessage(nil), f.raw...), reason: f.reason}
	}
	cp.recovery = cloneRecovery(ns.recovery)
	return &cp
}

func (ns *controlNamespace) recordCount() int { return len(ns.records) + len(ns.failed) }

// --- codec ---

type controlNamespaceDTO struct {
	SchemaVersion      int                        `json:"schema_version"`
	IssuedRevisionMark string                     `json:"issued_revision_mark"`
	CatalogRevision    string                     `json:"catalog_revision"`
	Drivers            map[string]json.RawMessage `json:"drivers"`
	Recovery           *controlRecoveryDTO        `json:"recovery,omitempty"`
}

type controlDriverDTO struct {
	DesiredState      string                `json:"desired_state"`
	DesiredRevision   string                `json:"desired_revision"`
	LifecycleSequence string                `json:"lifecycle_sequence"`
	Current           *controlTransitionDTO `json:"current"`
	Reserved          *controlTransitionDTO `json:"reserved"`
}

type controlTransitionDTO struct {
	Kind                   string `json:"kind"`
	DesiredRevision        string `json:"desired_revision"`
	Sequence               string `json:"sequence"`
	Phase                  string `json:"phase"`
	Epoch                  string `json:"epoch"`
	Generation             string `json:"generation"`
	NativeClosePending     bool   `json:"native_close_pending"`
	SemanticCleanupPending bool   `json:"semantic_cleanup_pending"`
	AttemptsUsed           int    `json:"attempts_used"`
	DeadlineUTC            string `json:"deadline_utc"`
}

type controlRecoveryDTO struct {
	Format             string   `json:"format"`
	DriverIDs          []string `json:"driver_ids"`
	MaxDesiredRevision string   `json:"max_desired_revision"`
	PayloadSHA256      string   `json:"payload_sha256"`
	Payload            string   `json:"payload"`
}

func transitionToDTO(t *ControlTransition) *controlTransitionDTO {
	if t == nil {
		return nil
	}
	dto := &controlTransitionDTO{
		Kind:                   string(t.Kind),
		DesiredRevision:        FormatControlRevision(t.DesiredRevision),
		Sequence:               FormatControlRevision(t.Sequence),
		Phase:                  string(t.Phase),
		Epoch:                  t.Epoch,
		Generation:             FormatControlRevision(t.Generation),
		NativeClosePending:     t.NativeClosePending,
		SemanticCleanupPending: t.SemanticCleanupPending,
		AttemptsUsed:           t.AttemptsUsed,
	}
	if !t.DeadlineUTC.IsZero() {
		dto.DeadlineUTC = t.DeadlineUTC.UTC().Format(time.RFC3339Nano)
	}
	return dto
}

func recoveryToDTO(r *ControlRecovery) *controlRecoveryDTO {
	if r == nil {
		return nil
	}
	sum := sha256.Sum256(r.Payload)
	ids := append([]string{}, r.DriverIDs...)
	return &controlRecoveryDTO{
		Format:             ControlRecoveryFormat,
		DriverIDs:          ids,
		MaxDesiredRevision: FormatControlRevision(r.MaxDesiredRevision),
		PayloadSHA256:      hex.EncodeToString(sum[:]),
		Payload:            base64.StdEncoding.EncodeToString(r.Payload),
	}
}

// encode returns the namespace value. A non-Loaded namespace is re-emitted
// verbatim. Driver records are keyed and therefore emitted in bytewise
// DriverID order; invalid records keep their original bytes.
func (ns *controlNamespace) encode() (json.RawMessage, error) {
	if ns.status != ControlLoadLoaded {
		return append(json.RawMessage(nil), ns.raw...), nil
	}
	dto := controlNamespaceDTO{
		SchemaVersion:      ControlSchemaVersion,
		IssuedRevisionMark: FormatControlRevision(ns.mark),
		CatalogRevision:    FormatControlRevision(ns.catalog),
		Drivers:            make(map[string]json.RawMessage, ns.recordCount()),
		Recovery:           recoveryToDTO(ns.recovery),
	}
	for id, r := range ns.records {
		raw, err := json.Marshal(controlDriverDTO{
			DesiredState:      string(r.DesiredState),
			DesiredRevision:   FormatControlRevision(r.DesiredRevision),
			LifecycleSequence: FormatControlRevision(r.LifecycleSequence),
			Current:           transitionToDTO(r.Current),
			Reserved:          transitionToDTO(r.Reserved),
		})
		if err != nil {
			return nil, err
		}
		dto.Drivers[id] = raw
	}
	for id, f := range ns.failed {
		dto.Drivers[id] = append(json.RawMessage(nil), f.raw...)
	}
	return json.Marshal(dto)
}

// decodeControlNamespace decodes the namespace value. It never returns nil and
// never reinterprets unsupported or malformed bytes as missing or empty.
func decodeControlNamespace(raw json.RawMessage) *controlNamespace {
	failed := func(status ControlLoadDisposition, detail string) *controlNamespace {
		return &controlNamespace{status: status, detail: detail, raw: append(json.RawMessage(nil), raw...)}
	}
	fields, err := strictObject(raw)
	if err != nil {
		return failed(ControlLoadCorrupt, "namespace is not a closed JSON object")
	}
	rawVersion, ok := fields["schema_version"]
	if !ok {
		return failed(ControlLoadCorrupt, "schema_version missing")
	}
	version, err := decodeJSONSmallInt(rawVersion)
	if err != nil {
		return failed(ControlLoadCorrupt, "schema_version is not an integer")
	}
	if version != ControlSchemaVersion {
		return failed(ControlLoadUnsupported, fmt.Sprintf("unsupported schema_version %d", version))
	}
	for key := range fields {
		switch key {
		case "schema_version", "issued_revision_mark", "catalog_revision", "drivers", "recovery":
		default:
			return failed(ControlLoadCorrupt, fmt.Sprintf("unknown member %q", key))
		}
	}
	ns := newLoadedControlNamespace()
	markText, err := decodeJSONString(fields["issued_revision_mark"])
	if err != nil {
		return failed(ControlLoadCorrupt, "issued_revision_mark missing or not a string")
	}
	if ns.mark, ok = ParseControlRevision(markText, true); !ok {
		return failed(ControlLoadCorrupt, "issued_revision_mark is not a canonical revision")
	}
	catalogText, err := decodeJSONString(fields["catalog_revision"])
	if err != nil {
		return failed(ControlLoadCorrupt, "catalog_revision missing or not a string")
	}
	if ns.catalog, ok = ParseControlRevision(catalogText, false); !ok {
		return failed(ControlLoadCorrupt, "catalog_revision is not a canonical nonzero revision")
	}
	rawDrivers, ok := fields["drivers"]
	if !ok {
		return failed(ControlLoadCorrupt, "drivers missing")
	}
	drivers, err := strictObject(rawDrivers)
	if err != nil {
		return failed(ControlLoadCorrupt, "drivers is not a closed JSON object")
	}
	if len(drivers) > MaxControlDriverRecords {
		return failed(ControlLoadCorrupt, "driver record limit exceeded")
	}
	for id, rawRecord := range drivers {
		record, reason := decodeControlDriverRecord(id, rawRecord, ns.mark)
		if reason != "" {
			ns.failed[id] = controlFailedRecord{raw: append(json.RawMessage(nil), rawRecord...), reason: reason}
			continue
		}
		ns.records[id] = record
	}
	if rawRecovery, present := fields["recovery"]; present {
		recovery, err := decodeControlRecovery(rawRecovery)
		if err != nil {
			return failed(ControlLoadCorrupt, err.Error())
		}
		if recovery.MaxDesiredRevision > ns.mark {
			return failed(ControlLoadCorrupt, "recovery records a desired revision above the issued-revision mark")
		}
		ns.recovery = recovery
	}
	return ns
}

func decodeControlDriverRecord(id string, raw json.RawMessage, mark uint64) (ControlDriverRecord, string) {
	if !CanonicalControlDriverID(id) {
		return ControlDriverRecord{}, "noncanonical driver id"
	}
	fields, err := strictObject(raw)
	if err != nil {
		return ControlDriverRecord{}, "record is not a closed JSON object"
	}
	if !exactKeys(fields, "desired_state", "desired_revision", "lifecycle_sequence", "current", "reserved") {
		return ControlDriverRecord{}, "record members are not the closed version-1 set"
	}
	var r ControlDriverRecord
	state, err := decodeJSONString(fields["desired_state"])
	if err != nil || (state != string(DesiredRunning) && state != string(DesiredStopped)) {
		return ControlDriverRecord{}, "desired_state invalid"
	}
	r.DesiredState = DesiredState(state)
	var ok bool
	text, err := decodeJSONString(fields["desired_revision"])
	if err != nil {
		return ControlDriverRecord{}, "desired_revision invalid"
	}
	if r.DesiredRevision, ok = ParseControlRevision(text, false); !ok || r.DesiredRevision > mark {
		return ControlDriverRecord{}, "desired_revision invalid or above the issued-revision mark"
	}
	text, err = decodeJSONString(fields["lifecycle_sequence"])
	if err != nil {
		return ControlDriverRecord{}, "lifecycle_sequence invalid"
	}
	if r.LifecycleSequence, ok = ParseControlRevision(text, true); !ok {
		return ControlDriverRecord{}, "lifecycle_sequence invalid"
	}
	if r.Current, err = decodeControlTransition(fields["current"]); err != nil {
		return ControlDriverRecord{}, "current: " + err.Error()
	}
	if r.Reserved, err = decodeControlTransition(fields["reserved"]); err != nil {
		return ControlDriverRecord{}, "reserved: " + err.Error()
	}
	if reason := validateRecordSlots(r); reason != "" {
		return ControlDriverRecord{}, reason
	}
	return r, ""
}

func validateRecordSlots(r ControlDriverRecord) string {
	if r.Reserved != nil && r.Current == nil {
		return "reserved slot without a current slot"
	}
	for _, t := range []*ControlTransition{r.Current, r.Reserved} {
		if t == nil {
			continue
		}
		if t.DesiredRevision > r.DesiredRevision {
			return "transition revision above the desired revision"
		}
		if t.Sequence > r.LifecycleSequence {
			return "transition sequence above the lifecycle sequence"
		}
	}
	if r.Current != nil && r.Reserved != nil {
		if r.Current.Sequence == r.Reserved.Sequence {
			return "duplicate transition sequence"
		}
		// The reserved slot is the newer transition; startup reconciles
		// current before reserved, so a reversed layout would replay the
		// transitions out of order.
		if r.Current.Sequence > r.Reserved.Sequence {
			return "current transition sequence does not precede the reserved sequence"
		}
	}
	return ""
}

func validateTransition(t *ControlTransition) error {
	switch t.Kind {
	case TransitionStart, TransitionStop, TransitionRestart, TransitionCancelAttempt:
	default:
		return errors.New("kind invalid")
	}
	switch t.Phase {
	case PhasePending, PhaseActive, PhaseCleanup:
	default:
		return errors.New("phase invalid")
	}
	if t.DesiredRevision == 0 || t.Sequence == 0 {
		return errors.New("revision and sequence must be nonzero")
	}
	if t.Epoch != "" && (len(t.Epoch) > maxControlEpochBytes || !controlEpochPattern.MatchString(t.Epoch)) {
		return errors.New("epoch invalid")
	}
	if t.AttemptsUsed < 0 || t.AttemptsUsed > MaxControlSemanticAttempts {
		return errors.New("attempts_used out of bounds")
	}
	if !t.DeadlineUTC.IsZero() && t.DeadlineUTC.Location() != time.UTC {
		return errors.New("deadline must be UTC")
	}
	return nil
}

func decodeControlTransition(raw json.RawMessage) (*ControlTransition, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	fields, err := strictObject(raw)
	if err != nil {
		return nil, errors.New("not a closed JSON object")
	}
	if !exactKeys(fields, "kind", "desired_revision", "sequence", "phase", "epoch", "generation",
		"native_close_pending", "semantic_cleanup_pending", "attempts_used", "deadline_utc") {
		return nil, errors.New("members are not the closed version-1 set")
	}
	var t ControlTransition
	var ok bool
	kind, err := decodeJSONString(fields["kind"])
	if err != nil {
		return nil, errors.New("kind invalid")
	}
	t.Kind = TransitionKind(kind)
	phase, err := decodeJSONString(fields["phase"])
	if err != nil {
		return nil, errors.New("phase invalid")
	}
	t.Phase = TransitionPhase(phase)
	for _, field := range []struct {
		name      string
		dst       *uint64
		allowZero bool
	}{{"desired_revision", &t.DesiredRevision, false}, {"sequence", &t.Sequence, false}, {"generation", &t.Generation, true}} {
		text, err := decodeJSONString(fields[field.name])
		if err != nil {
			return nil, fmt.Errorf("%s invalid", field.name)
		}
		if *field.dst, ok = ParseControlRevision(text, field.allowZero); !ok {
			return nil, fmt.Errorf("%s invalid", field.name)
		}
	}
	if t.Epoch, err = decodeJSONString(fields["epoch"]); err != nil {
		return nil, errors.New("epoch invalid")
	}
	if t.NativeClosePending, err = decodeJSONBool(fields["native_close_pending"]); err != nil {
		return nil, errors.New("native_close_pending invalid")
	}
	if t.SemanticCleanupPending, err = decodeJSONBool(fields["semantic_cleanup_pending"]); err != nil {
		return nil, errors.New("semantic_cleanup_pending invalid")
	}
	if t.AttemptsUsed, err = decodeJSONSmallInt(fields["attempts_used"]); err != nil {
		return nil, errors.New("attempts_used invalid")
	}
	deadline, err := decodeJSONString(fields["deadline_utc"])
	if err != nil {
		return nil, errors.New("deadline_utc invalid")
	}
	if deadline != "" {
		parsed, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil || !strings.HasSuffix(deadline, "Z") {
			return nil, errors.New("deadline_utc must be RFC 3339 UTC")
		}
		t.DeadlineUTC = parsed.UTC()
	}
	if err := validateTransition(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

func decodeControlRecovery(raw json.RawMessage) (*ControlRecovery, error) {
	fields, err := strictObject(raw)
	if err != nil || !exactKeys(fields, "format", "driver_ids", "max_desired_revision", "payload_sha256", "payload") {
		return nil, errors.New("recovery envelope is not the closed version-1 object")
	}
	format, err := decodeJSONString(fields["format"])
	if err != nil || format != ControlRecoveryFormat {
		return nil, errors.New("recovery format unsupported")
	}
	var ids []string
	if !bytes.HasPrefix(bytes.TrimSpace(fields["driver_ids"]), []byte("[")) || json.Unmarshal(fields["driver_ids"], &ids) != nil {
		return nil, errors.New("recovery driver_ids invalid")
	}
	maxText, err := decodeJSONString(fields["max_desired_revision"])
	if err != nil {
		return nil, errors.New("recovery max_desired_revision invalid")
	}
	maxRev, ok := ParseControlRevision(maxText, true)
	if !ok {
		return nil, errors.New("recovery max_desired_revision invalid")
	}
	digest, err := decodeJSONString(fields["payload_sha256"])
	if err != nil || !controlSHA256Pattern.MatchString(digest) {
		return nil, errors.New("recovery payload_sha256 invalid")
	}
	encoded, err := decodeJSONString(fields["payload"])
	if err != nil {
		return nil, errors.New("recovery payload invalid")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, errors.New("recovery payload is not canonical base64")
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != digest {
		return nil, errors.New("recovery payload digest mismatch")
	}
	rec := &ControlRecovery{DriverIDs: ids, MaxDesiredRevision: maxRev, Payload: payload}
	if err := validateRecoveryIDs(rec.DriverIDs); err != nil {
		return nil, err
	}
	return rec, nil
}

func validateRecoveryIDs(ids []string) error {
	for i, id := range ids {
		if !CanonicalControlDriverID(id) {
			return errors.New("recovery driver_ids contains a noncanonical id")
		}
		if i > 0 && ids[i-1] >= id {
			return errors.New("recovery driver_ids must be bytewise sorted and unique")
		}
	}
	return nil
}

// strictObject decodes one JSON object, rejecting duplicate members and
// trailing values, and returns each member's raw value.
func strictObject(raw []byte) (map[string]json.RawMessage, error) {
	return decodeTopLevelNamespaces(raw)
}

func exactKeys(fields map[string]json.RawMessage, keys ...string) bool {
	if len(fields) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := fields[key]; !ok {
			return false
		}
	}
	return true
}

func decodeJSONString(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", errors.New("not a JSON string")
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return "", err
	}
	return s, nil
}

func decodeJSONBool(raw json.RawMessage) (bool, error) {
	switch string(bytes.TrimSpace(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, errors.New("not a JSON boolean")
}

func decodeJSONSmallInt(raw json.RawMessage) (int, error) {
	text := string(bytes.TrimSpace(raw))
	if len(text) == 0 || len(text) > 9 {
		return 0, errors.New("not a small JSON integer")
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, errors.New("not a small JSON integer")
		}
	}
	if len(text) > 1 && text[0] == '0' {
		return 0, errors.New("not a canonical JSON integer")
	}
	return strconv.Atoi(text)
}

// --- quarantine material ---

// controlQuarantineCopies reports whether any quarantine copy of the state
// file exists beside it. Existence counts; content is never read.
func controlQuarantineCopies(path string) (bool, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, base+corruptQuarantineInfix) || strings.HasPrefix(name, base+controlQuarantineInfix) {
			return true, nil
		}
	}
	return false, nil
}

// --- Manager control seam ---

// ControlLoad returns the runtime_control load status for the shell.
func (m *Manager) ControlLoad() ControlLoadStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.controlStatus
}

// ControlAdmissionOpen reports whether decisions can be admitted: control is
// loaded, no ambiguous persistence awaits readback, the manager has not been
// stopped and persistence is not blocked.
func (m *Manager) ControlAdmissionOpen() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.controlStatus.Disposition == ControlLoadLoaded && m.controlAmbiguous == nil &&
		!m.stopped && m.persistBlocked == nil
}

// admissionClosedLocked refuses (UNAVAILABLE, row 3) every control decision
// and work commit once the manager is stopped or persistence is blocked,
// including requests that would otherwise take a no-op or comparison path.
// Callers hold m.mu.
func (m *Manager) admissionClosedLocked() *ControlRefusal {
	if m.stopped {
		return errControlStopped()
	}
	if m.persistBlocked != nil {
		return refuse(RefusalUnavailable, 3, "admission closed: persistence blocked: "+m.persistBlocked.Error())
	}
	return nil
}

func errControlStopped() *ControlRefusal {
	return refuse(RefusalUnavailable, 3, "admission closed: manager stopped")
}

func validateControlConfigs(configs []ControlDriverConfig) ([]ControlDriverConfig, error) {
	if len(configs) > MaxControlDriverRecords {
		return nil, fmt.Errorf("%w: more than %d drivers", ErrControlInvalidConfig, MaxControlDriverRecords)
	}
	sorted := append([]ControlDriverConfig(nil), configs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].DriverID < sorted[j].DriverID })
	for i, c := range sorted {
		if !CanonicalControlDriverID(c.DriverID) {
			return nil, fmt.Errorf("%w: noncanonical driver id", ErrControlInvalidConfig)
		}
		if i > 0 && sorted[i-1].DriverID == c.DriverID {
			return nil, fmt.Errorf("%w: duplicate driver id %s", ErrControlInvalidConfig, c.DriverID)
		}
	}
	return sorted, nil
}

func addCounter(v, n uint64) (uint64, bool) {
	if v > math.MaxUint64-n {
		return 0, false
	}
	return v + n, true
}

// BootstrapControl registers the configured drivers for this process
// incarnation and returns the startup plan derived from durable desired
// state. It never opens an endpoint.
//
//   - MISSING with a valid persisted GUID: exactly one atomic seed. The mark
//     starts at 0 and each configured driver, in bytewise DriverID order, is
//     one accepted decision at mark + 1: enabled RUNNING, disabled STOPPED.
//   - LOADED: a configured driver without a record becomes visible through a
//     mark-only write; a record whose driver is no longer configured and that
//     holds no in-flight work or D5 material is deleted with a mark-only
//     write. Both happen in one durable transaction, before any read.
//   - Any failed disposition: no write; every configured driver is FAILED.
//
// Disabled configuration always wins and never rewrites durable intent.
func (m *Manager) BootstrapControl(ctx context.Context, configs []ControlDriverConfig) ([]ControlStartup, error) {
	sorted, err := validateControlConfigs(configs)
	if err != nil {
		return nil, err
	}
	if err := m.acquireWriteCtx(ctx); err != nil {
		return nil, err
	}
	defer m.releaseWrite()

	// Admission is enforced at the serialization point: a registration queued
	// behind an in-flight writer after Stop began writes and publishes nothing.
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil, errControlStopped()
	}
	configured := make(map[string]ControlDriverConfig, len(sorted))
	for _, c := range sorted {
		configured[c.DriverID] = c
	}
	status := m.controlStatus.Disposition
	ambiguous := m.controlAmbiguous != nil
	var committed *controlNamespace
	var guid string
	priorRegistered := make([]string, 0, len(m.controlConfigs))
	for id := range m.controlConfigs {
		priorRegistered = append(priorRegistered, id)
	}
	if m.state != nil {
		committed = m.state.control.clone()
		guid = m.state.Meta.InstanceGUID
	}
	m.mu.Unlock()

	var commitErr error
	switch {
	case ambiguous:
		commitErr = ErrControlReadbackPending
	case status == ControlLoadMissing:
		if !instanceGUIDPattern.MatchString(guid) {
			commitErr = ErrControlBootstrapGUID
			break
		}
		stage := newLoadedControlNamespace()
		for _, c := range sorted {
			stage.mark++
			stage.catalog++
			desired := DesiredRunning
			if !c.Enabled {
				desired = DesiredStopped
			}
			stage.records[c.DriverID] = ControlDriverRecord{DesiredState: desired, DesiredRevision: stage.mark}
		}
		if refusal := checkControlBytes(stage); refusal != nil {
			commitErr = refusal
			break
		}
		commitErr = m.commitControl(stage, true)
	case status == ControlLoadLoaded && committed != nil:
		stage := committed.clone()
		recoveryIDs := map[string]bool{}
		if stage.recovery != nil {
			for _, id := range stage.recovery.DriverIDs {
				recoveryIDs[id] = true
			}
		}
		var changes uint64
		removed := map[string]bool{}
		for _, id := range sortedRecordIDs(stage.records) {
			r := stage.records[id]
			if _, ok := configured[id]; ok || r.Current != nil || r.Reserved != nil || recoveryIDs[id] {
				continue
			}
			delete(stage.records, id)
			removed[id] = true
			changes++
		}
		// A driver registered earlier in this process that is no longer
		// configured leaves the visible catalog even when it holds no stored
		// record (record-less) or its record is retained for in-flight work:
		// that removal is a mark-only write as well.
		sort.Strings(priorRegistered)
		priorSet := make(map[string]bool, len(priorRegistered))
		for _, id := range priorRegistered {
			priorSet[id] = true
			if _, ok := configured[id]; !ok && !removed[id] {
				changes++
			}
		}
		for _, c := range sorted {
			_, hasRecord := stage.records[c.DriverID]
			_, hasFailed := stage.failed[c.DriverID]
			wasRegistered := priorSet[c.DriverID]
			if !hasRecord && !hasFailed && !wasRegistered {
				changes++
			}
		}
		if changes > 0 {
			var ok1, ok2 bool
			stage.mark, ok1 = addCounter(stage.mark, changes)
			stage.catalog, ok2 = addCounter(stage.catalog, changes)
			if !ok1 || !ok2 {
				commitErr = refuse(RefusalCounterExhausted, 10, "registration mark or catalog increment would overflow")
				break
			}
			if refusal := checkControlBytes(stage); refusal != nil {
				commitErr = refusal
				break
			}
			commitErr = m.commitControl(stage, false)
		}
	}
	// Publish registration only after its durable fence. Failed load
	// dispositions still expose configured drivers as FAILED startup entries.
	if commitErr == nil || (status != ControlLoadLoaded && !ambiguous) {
		m.mu.Lock()
		m.controlConfigs = configured
		m.mu.Unlock()
	}
	return m.controlStartupPlan(), commitErr
}

func sortedRecordIDs(records map[string]ControlDriverRecord) []string {
	ids := make([]string, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// controlStartupPlan derives each configured driver's startup action from
// committed durable state.
func (m *Manager) controlStartupPlan() []ControlStartup {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.controlConfigs))
	for id := range m.controlConfigs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var ns *controlNamespace
	if m.state != nil {
		ns = m.state.control
	}
	loaded := m.controlStatus.Disposition == ControlLoadLoaded && ns != nil && ns.status == ControlLoadLoaded
	plan := make([]ControlStartup, 0, len(ids))
	for _, id := range ids {
		cfg := m.controlConfigs[id]
		entry := ControlStartup{DriverID: id, DesiredState: DesiredStopped}
		if !loaded {
			entry.Action = StartupFailed
			plan = append(plan, entry)
			continue
		}
		entry.DesiredRevision = ns.mark
		if _, bad := ns.failed[id]; bad {
			entry.Action = StartupFailed
			entry.DesiredRevision = 0
			plan = append(plan, entry)
			continue
		}
		if r, ok := ns.records[id]; ok {
			entry.DesiredState = r.DesiredState
			entry.DesiredRevision = r.DesiredRevision
			entry.Current = cloneTransition(r.Current)
			entry.Reserved = cloneTransition(r.Reserved)
		}
		switch {
		case !cfg.Enabled:
			entry.Action = StartupDisabled
		case entry.DesiredState == DesiredRunning:
			entry.Action = StartupStarting
		default:
			entry.Action = StartupStopped
		}
		plan = append(plan, entry)
	}
	return plan
}

// ControlSnapshot returns one coherent read of committed control state for
// every visible driver, sorted bytewise by DriverID.
func (m *Manager) ControlSnapshot() (ControlCatalogView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns, readOnly := m.readableControlLocked()
	if ns == nil {
		return ControlCatalogView{}, refuse(RefusalFailedPrecondition, 3, "runtime_control is not loaded: "+string(m.controlStatus.Disposition))
	}
	ids := make([]string, 0, len(m.controlConfigs))
	for id := range m.controlConfigs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	view := ControlCatalogView{ReadOnly: readOnly, IssuedRevisionMark: ns.mark, CatalogRevision: ns.catalog}
	for _, id := range ids {
		d := driverViewLocked(ns, id, m.controlConfigs[id])
		d.ReadOnly = readOnly
		view.Drivers = append(view.Drivers, d)
	}
	return view, nil
}

// ControlDriver returns a coherent read of one visible driver.
func (m *Manager) ControlDriver(id string) (ControlDriverView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg, ok := m.controlConfigs[id]
	if !CanonicalControlDriverID(id) || !ok {
		return ControlDriverView{}, refuse(RefusalNotFound, 2, "driver not registered")
	}
	ns, readOnly := m.readableControlLocked()
	if ns == nil {
		return ControlDriverView{DriverID: id, Enabled: cfg.Enabled, Failed: true}, nil
	}
	v := driverViewLocked(ns, id, cfg)
	v.ReadOnly = readOnly
	return v, nil
}

func driverViewLocked(ns *controlNamespace, id string, cfg ControlDriverConfig) ControlDriverView {
	v := ControlDriverView{DriverID: id, Enabled: cfg.Enabled, DesiredState: DesiredStopped, DesiredRevision: ns.mark}
	if _, bad := ns.failed[id]; bad {
		v.Failed = true
		v.DesiredRevision = 0
		return v
	}
	if r, ok := ns.records[id]; ok {
		v.HasRecord = true
		v.DesiredState = r.DesiredState
		v.DesiredRevision = r.DesiredRevision
		v.Current = cloneTransition(r.Current)
		v.Reserved = cloneTransition(r.Reserved)
	}
	return v
}

// readableControlLocked returns the committed namespace for reads. While
// control is LOCKED it returns the last-known-good namespace decoded from the
// readable file with readOnly set; it is never used for admission or writes.
func (m *Manager) readableControlLocked() (ns *controlNamespace, readOnly bool) {
	if ns := m.loadedControlLocked(); ns != nil {
		return ns, false
	}
	if m.controlStatus.Disposition == ControlLoadLocked && m.state != nil && m.state.control != nil && m.state.control.status == ControlLoadLoaded {
		return m.state.control, true
	}
	return nil, false
}

// loadedControlLocked returns the committed namespace only when control is
// LOADED; every admission and write path uses it.
func (m *Manager) loadedControlLocked() *controlNamespace {
	if m.controlStatus.Disposition != ControlLoadLoaded || m.state == nil || m.state.control == nil || m.state.control.status != ControlLoadLoaded {
		return nil
	}
	return m.state.control
}

// currentViewLocked returns the committed view of a driver (the mark for a
// driver without a record).
func currentViewLocked(ns *controlNamespace, id string) (ControlView, bool) {
	if r, ok := ns.records[id]; ok {
		return ControlView{DesiredState: r.DesiredState, DesiredRevision: r.DesiredRevision}, true
	}
	return ControlView{DesiredState: DesiredStopped, DesiredRevision: ns.mark}, false
}

// DefaultControlPlanner places a changing request in the current slot when it
// is empty, otherwise in the empty reserved slot unless an accepted RESTART is
// still executing; anything else is TRANSITION_BUSY.
func DefaultControlPlanner(in ControlPlanInput) (ControlPlan, *ControlRefusal) {
	next := &ControlTransition{Kind: TransitionKind(in.Method), Phase: PhasePending}
	cur, res := in.Record.Current, in.Record.Reserved
	switch {
	case cur == nil:
		return ControlPlan{Current: next}, nil
	case res == nil && cur.Kind != TransitionRestart:
		return ControlPlan{Current: cur, Reserved: next}, nil
	}
	return ControlPlan{}, refuse(RefusalTransitionBusy, 8, "typed transition slots are occupied")
}

// DecideControl validates, decides and durably commits one control request
// following the refusal table of the lifecycle contract. A nil planner uses
// DefaultControlPlanner. The comparison, mark read, allocation and durable
// write happen under the namespace-wide serialization point, which is the
// runtime-state sole-writer lock shared with cache, meta and ticker writes.
func (m *Manager) DecideControl(ctx context.Context, method ControlMethod, req ControlRequest, planner ControlPlanner) (ControlView, error) {
	if planner == nil {
		planner = DefaultControlPlanner
	}
	// Row 1: closed shape.
	switch method {
	case ControlStart, ControlStop, ControlRestart:
	default:
		return ControlView{}, refuse(RefusalInvalidArgument, 1, "unknown method")
	}
	if req.ExpectedRevision == 0 {
		return ControlView{}, refuse(RefusalInvalidArgument, 1, "ExpectedRevision must be nonzero")
	}
	// Rows 2-4 without the serialization point.
	if refusal := m.precheckControl(req.DriverID); refusal != nil {
		return ControlView{}, refusal
	}
	// Row 5: finite, unexpired deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		return ControlView{}, refuse(RefusalInvalidArgument, 5, "call context has no finite deadline")
	}
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		return ControlView{}, refuse(RefusalDeadlineExceeded, 5, "deadline expired at validation")
	}

	// Step 2: bounded wait for the namespace-wide serialization point.
	acquired, deadlineHit := m.acquireWriteBounded(ctx, deadline)
	if !acquired {
		if deadlineHit {
			return ControlView{}, refuse(RefusalDeadlineExceeded, 5, "deadline expired during the serialization wait")
		}
		return ControlView{}, m.waitTimeoutRefusal(req)
	}
	defer m.releaseWrite()

	// Admission confirmation, then the deadline recheck, before comparison.
	if refusal := m.precheckControl(req.DriverID); refusal != nil {
		return ControlView{}, refusal
	}
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		return ControlView{}, refuse(RefusalDeadlineExceeded, 5, "deadline expired after acquiring the serialization point")
	}

	m.mu.Lock()
	ns := m.loadedControlLocked().clone()
	m.mu.Unlock()
	if ns == nil {
		return ControlView{}, refuse(RefusalFailedPrecondition, 3, "runtime_control is not loaded")
	}
	current, hasRecord := currentViewLocked(ns, req.DriverID)
	// Row 6: optimistic comparison precedes the same-desired no-op test.
	if req.ExpectedRevision != current.DesiredRevision {
		r := refuse(RefusalRevisionConflict, 6, "expected revision differs from the current desired revision")
		view := current
		r.View = &view
		return ControlView{}, r
	}
	// Step 3: same-desired no-op writes nothing and allocates nothing.
	if (method == ControlStart && current.DesiredState == DesiredRunning) ||
		(method == ControlStop && current.DesiredState == DesiredStopped) {
		return current, nil
	}
	if method == ControlRestart && current.DesiredState != DesiredRunning {
		return ControlView{}, refuse(RefusalFailedPrecondition, 6, "RESTART requires desired RUNNING")
	}
	record := ControlDriverRecord{}
	if hasRecord {
		record = cloneRecord(ns.records[req.DriverID])
	}
	// Rows 7-8: planner (quarantine/unresolved recovery, transition slots).
	plan, refusal := planner(ControlPlanInput{Method: method, DriverID: req.DriverID, HasRecord: hasRecord, Record: cloneRecord(record)})
	if refusal != nil {
		if refusal.Code != RefusalFailedPrecondition && refusal.Code != RefusalTransitionBusy {
			return ControlView{}, refuse(RefusalTransitionBusy, 8, "planner returned an out-of-table refusal")
		}
		return ControlView{}, refusal
	}
	// Row 9: record limit.
	if !hasRecord && ns.recordCount()+1 > MaxControlDriverRecords {
		return ControlView{}, refuse(RefusalResourceExhausted, 9, "driver record limit reached")
	}
	// Row 10: counters.
	newRevision, okMark := addCounter(ns.mark, 1)
	newCatalog, okCatalog := addCounter(ns.catalog, 1)
	if !okMark || !okCatalog {
		return ControlView{}, refuse(RefusalCounterExhausted, 10, "mark or catalog increment would overflow")
	}
	updated, err := applyControlPlan(record, plan, newRevision, TransitionKind(method))
	if err != nil {
		var r *ControlRefusal
		if errors.As(err, &r) {
			return ControlView{}, r
		}
		return ControlView{}, refuse(RefusalTransitionBusy, 8, err.Error())
	}
	desired := DesiredRunning
	if method == ControlStop {
		desired = DesiredStopped
	}
	updated.DesiredState = desired
	updated.DesiredRevision = newRevision
	ns.records[req.DriverID] = updated
	ns.mark = newRevision
	ns.catalog = newCatalog
	if refusal := checkControlBytes(ns); refusal != nil {
		return ControlView{}, refusal
	}
	// Pre-commit deadline recheck.
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		return ControlView{}, refuse(RefusalDeadlineExceeded, 5, "deadline expired at the pre-commit recheck")
	}
	if err := m.commitControl(ns, false); err != nil {
		return ControlView{}, err
	}
	return ControlView{DesiredState: desired, DesiredRevision: newRevision}, nil
}

// applyControlPlan validates a plan and assigns revision and sequences to its
// new transitions.
func applyControlPlan(record ControlDriverRecord, plan ControlPlan, newRevision uint64, requestedKind TransitionKind) (ControlDriverRecord, error) {
	if plan.Reserved != nil && plan.Current == nil {
		return record, fmt.Errorf("%w: reserved slot without current slot", ErrControlInvalidWork)
	}
	existing := map[uint64]*ControlTransition{}
	for _, t := range []*ControlTransition{record.Current, record.Reserved} {
		if t != nil {
			existing[t.Sequence] = t
		}
	}
	out := cloneRecord(record)
	slots := []*ControlTransition{cloneTransition(plan.Current), cloneTransition(plan.Reserved)}
	requestedAllocated := false
	for _, t := range slots {
		if t == nil {
			continue
		}
		if t.Sequence == 0 {
			if t.Kind == requestedKind {
				requestedAllocated = true
			}
			next, ok := addCounter(out.LifecycleSequence, 1)
			if !ok {
				return record, refuse(RefusalCounterExhausted, 10, "lifecycle sequence increment would overflow")
			}
			out.LifecycleSequence = next
			t.Sequence = next
			t.DesiredRevision = newRevision
		} else if prior, ok := existing[t.Sequence]; !ok || *prior != *t {
			return record, fmt.Errorf("%w: retained transition does not match the stored slot", ErrControlInvalidWork)
		}
		if err := validateTransition(t); err != nil {
			return record, fmt.Errorf("%w: %v", ErrControlInvalidWork, err)
		}
	}
	if !requestedAllocated {
		return record, fmt.Errorf("%w: plan must allocate the requested transition kind", ErrControlInvalidWork)
	}
	out.Current, out.Reserved = slots[0], slots[1]
	if out.Current != nil && out.Reserved != nil && out.Current.Sequence >= out.Reserved.Sequence {
		return record, fmt.Errorf("%w: current transition must precede the reserved transition", ErrControlInvalidWork)
	}
	return out, nil
}

// checkControlBytes enforces the namespace byte limit on the staged namespace.
// Every control commit (seed, registration, decision, work, catalog and
// recovery) calls it before commitControl, so an oversize namespace is refused
// RESOURCE_EXHAUSTED and never persisted.
func checkControlBytes(ns *controlNamespace) *ControlRefusal {
	raw, err := ns.encode()
	if err != nil {
		return refuse(RefusalUnavailable, 0, "namespace marshal failed")
	}
	if len(raw) > maxControlNamespaceBytes {
		return refuse(RefusalResourceExhausted, 9, "namespace byte limit reached")
	}
	return nil
}

// precheckControl applies rows 2-4: registration, control state and
// configured eligibility.
func (m *Manager) precheckControl(id string) *ControlRefusal {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg, registered := m.controlConfigs[id]
	if !CanonicalControlDriverID(id) || !registered {
		return refuse(RefusalNotFound, 2, "driver id noncanonical or not registered")
	}
	ns := m.loadedControlLocked()
	if ns == nil {
		return refuse(RefusalFailedPrecondition, 3, "runtime_control failed to load: "+string(m.controlStatus.Disposition))
	}
	if refusal := m.admissionClosedLocked(); refusal != nil {
		return refusal
	}
	if _, bad := ns.failed[id]; bad {
		return refuse(RefusalFailedPrecondition, 3, "driver record malformed")
	}
	if m.controlAmbiguous != nil {
		return refuse(RefusalUnavailable, 3, "readback after ambiguous persistence still pending")
	}
	if !cfg.Enabled {
		return refuse(RefusalFailedPrecondition, 4, "driver disabled by configuration")
	}
	return nil
}

// waitTimeoutRefusal answers a request whose bounded wait ended without the
// serialization point: after admission confirmation, one read-only load of the
// driver's current desired revision decides REVISION_CONFLICT (conclusively
// stale) or TRANSITION_BUSY. No durable change is made.
func (m *Manager) waitTimeoutRefusal(req ControlRequest) *ControlRefusal {
	if refusal := m.precheckControl(req.DriverID); refusal != nil {
		return refusal
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ns := m.loadedControlLocked()
	if ns == nil {
		return refuse(RefusalFailedPrecondition, 3, "runtime_control is not loaded")
	}
	current, _ := currentViewLocked(ns, req.DriverID)
	if req.ExpectedRevision < current.DesiredRevision {
		r := refuse(RefusalRevisionConflict, 6, "expected revision is conclusively stale")
		r.View = &current
		return r
	}
	return refuse(RefusalTransitionBusy, 8, "serialization point held beyond the bounded wait")
}

// CommitControlWork durably replaces a driver's typed slots without changing
// desired state, its revision or the mark (phase updates, promotion of the
// reserved slot, removal after terminal obligations). Transitions must keep
// the kind, sequence and revision of a currently stored slot. A reserved slot
// may be retained or promoted, and must be promoted before clearing both slots.
// Clearing the final slot asserts terminal obligations have settled, which
// remains the caller's responsibility. Updates must preserve correlation,
// cleanup and budget progression.
func (m *Manager) CommitControlWork(ctx context.Context, id string, current, reserved *ControlTransition) error {
	if err := m.acquireWriteCtx(ctx); err != nil {
		return err
	}
	defer m.releaseWrite()
	m.mu.Lock()
	ns := m.loadedControlLocked().clone()
	ambiguous := m.controlAmbiguous != nil
	closed := m.admissionClosedLocked()
	m.mu.Unlock()
	if ns == nil {
		return refuse(RefusalFailedPrecondition, 3, "runtime_control is not loaded")
	}
	if closed != nil {
		return closed
	}
	if ambiguous {
		return refuse(RefusalUnavailable, 3, "readback after ambiguous persistence still pending")
	}
	r, ok := ns.records[id]
	if !ok {
		return refuse(RefusalNotFound, 2, "driver has no record")
	}
	sameIdentity := func(a, b *ControlTransition) bool {
		return a != nil && b != nil && a.Sequence == b.Sequence &&
			a.DesiredRevision == b.DesiredRevision && a.Kind == b.Kind
	}
	// A worker may update only an identity that still occupies a stored slot.
	// Bounds alone would let a completed transition replace newer work.
	for _, t := range []*ControlTransition{current, reserved} {
		if t == nil {
			continue
		}
		if !sameIdentity(t, r.Current) && !sameIdentity(t, r.Reserved) {
			return fmt.Errorf("%w: transition does not match a stored slot", ErrControlInvalidWork)
		}
	}
	if reserved != nil && !sameIdentity(reserved, r.Reserved) {
		return fmt.Errorf("%w: current transition cannot be demoted to reserved", ErrControlInvalidWork)
	}
	if r.Reserved != nil &&
		!sameIdentity(current, r.Reserved) && !sameIdentity(reserved, r.Reserved) {
		return fmt.Errorf("%w: work update would discard the reserved transition", ErrControlInvalidWork)
	}
	// Durable progress is monotonic: a stale update of the same identity can
	// neither move the phase backwards, lower the attempts used, nor extend or
	// remove an established deadline (that would renew the bounded budget).
	for _, t := range []*ControlTransition{current, reserved} {
		if t == nil {
			continue
		}
		stored := r.Current
		if !sameIdentity(t, stored) {
			stored = r.Reserved
		}
		if reason := controlRegression(stored, t); reason != "" {
			return fmt.Errorf("%w: %s", ErrControlInvalidWork, reason)
		}
	}
	r.Current, r.Reserved = cloneTransition(current), cloneTransition(reserved)
	for _, t := range []*ControlTransition{r.Current, r.Reserved} {
		if t == nil {
			continue
		}
		if err := validateTransition(t); err != nil {
			return fmt.Errorf("%w: %v", ErrControlInvalidWork, err)
		}
	}
	if reason := validateRecordSlots(r); reason != "" {
		return fmt.Errorf("%w: %s", ErrControlInvalidWork, reason)
	}
	ns.records[id] = r
	if refusal := checkControlBytes(ns); refusal != nil {
		return refusal
	}
	return m.commitControl(ns, false)
}

func controlPhaseRank(p TransitionPhase) int {
	switch p {
	case PhasePending:
		return 0
	case PhaseActive:
		return 1
	case PhaseCleanup:
		return 2
	}
	return -1
}

// controlRegression reports why next would regress the stored slot of the
// same identity, or "" when it only moves forward. Initial correlation
// assignment and PENDING-to-ACTIVE obligation assignment are allowed. A new
// epoch may incur new obligations only after the old correlation is settled;
// within an epoch generations may grow but cleared obligations stay cleared.
func controlRegression(stored, next *ControlTransition) string {
	if stored == nil || next == nil {
		return ""
	}
	switch {
	case controlPhaseRank(next.Phase) < controlPhaseRank(stored.Phase):
		return "work update moves the phase backwards"
	case next.AttemptsUsed < stored.AttemptsUsed:
		return "work update lowers attempts used"
	case !stored.DeadlineUTC.IsZero() && next.DeadlineUTC.IsZero():
		return "work update removes an established deadline"
	case !stored.DeadlineUTC.IsZero() && next.DeadlineUTC.After(stored.DeadlineUTC):
		return "work update extends an established deadline"
	}
	switch {
	case stored.Epoch == "":
		return ""
	case next.Epoch == "":
		return "work update removes an established correlation"
	case next.Epoch != stored.Epoch:
		if stored.NativeClosePending || stored.SemanticCleanupPending {
			return "work update replaces correlation before its obligations cleared"
		}
		return ""
	case next.Generation < stored.Generation:
		return "work update lowers generation"
	}
	incurring := stored.Phase == PhasePending && next.Phase == PhaseActive
	if !incurring && !stored.NativeClosePending && next.NativeClosePending {
		return "work update reactivates cleared native close"
	}
	if !incurring && !stored.SemanticCleanupPending && next.SemanticCleanupPending {
		return "work update reactivates cleared semantic cleanup"
	}
	return ""
}

// AdvanceControlCatalog durably advances the catalog view revision once for an
// externally visible observed-view transaction and returns the new value. The
// caller exposes the new view only after this returns successfully.
func (m *Manager) AdvanceControlCatalog(ctx context.Context) (uint64, error) {
	if err := m.acquireWriteCtx(ctx); err != nil {
		return 0, err
	}
	defer m.releaseWrite()
	m.mu.Lock()
	ns := m.loadedControlLocked().clone()
	ambiguous := m.controlAmbiguous != nil
	closed := m.admissionClosedLocked()
	m.mu.Unlock()
	if ns == nil {
		return 0, refuse(RefusalFailedPrecondition, 3, "runtime_control is not loaded")
	}
	if closed != nil {
		return 0, closed
	}
	if ambiguous {
		return 0, refuse(RefusalUnavailable, 3, "readback after ambiguous persistence still pending")
	}
	next, ok := addCounter(ns.catalog, 1)
	if !ok {
		return 0, refuse(RefusalCounterExhausted, 10, "catalog revision increment would overflow")
	}
	ns.catalog = next
	if refusal := checkControlBytes(ns); refusal != nil {
		return 0, refusal
	}
	if err := m.commitControl(ns, false); err != nil {
		return 0, err
	}
	return next, nil
}

// CommitControlRecovery durably stores the typed D5 recovery envelope written
// ahead by the control service. The payload is never interpreted. A nil
// envelope removes it (only after a verified restore point makes it
// unnecessary; that verification is the service's obligation).
func (m *Manager) CommitControlRecovery(ctx context.Context, recovery *ControlRecovery) error {
	if recovery != nil {
		if err := validateRecoveryIDs(recovery.DriverIDs); err != nil {
			return fmt.Errorf("%w: %v", ErrControlInvalidRecovery, err)
		}
	}
	if err := m.acquireWriteCtx(ctx); err != nil {
		return err
	}
	defer m.releaseWrite()
	m.mu.Lock()
	ns := m.loadedControlLocked().clone()
	ambiguous := m.controlAmbiguous != nil
	closed := m.admissionClosedLocked()
	m.mu.Unlock()
	if ns == nil {
		return refuse(RefusalFailedPrecondition, 3, "runtime_control is not loaded")
	}
	if closed != nil {
		return closed
	}
	if ambiguous {
		return refuse(RefusalUnavailable, 3, "readback after ambiguous persistence still pending")
	}
	if recovery != nil && recovery.MaxDesiredRevision > ns.mark {
		return fmt.Errorf("%w: recovery revision above the issued-revision mark", ErrControlInvalidRecovery)
	}
	ns.recovery = cloneRecovery(recovery)
	if refusal := checkControlBytes(ns); refusal != nil {
		return refusal
	}
	return m.commitControl(ns, false)
}

// ControlRecoveryPayload returns a detached copy of the committed D5 envelope.
func (m *Manager) ControlRecoveryPayload() *ControlRecovery {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns, _ := m.readableControlLocked()
	if ns == nil {
		return nil
	}
	return cloneRecovery(ns.recovery)
}

// ResolveControlReadback is the control service's readback after ambiguous
// persistence. Admission reopens only when the stored namespace agrees with
// either the staged or the previously committed namespace AND parent-
// directory durability is established by a successful directory fsync.
// Agreement alone never reopens admission. Disagreement or an unreadable or
// corrupt readback applies the load-failure rules and keeps control closed.
func (m *Manager) ResolveControlReadback(ctx context.Context) error {
	if err := m.acquireWriteCtx(ctx); err != nil {
		return err
	}
	defer m.releaseWrite()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return errControlStopped()
	}
	staged := m.controlAmbiguous
	var committed *controlNamespace
	if m.state != nil {
		committed = m.state.control
	}
	m.mu.Unlock()
	if staged == nil {
		return nil
	}
	data, err := os.ReadFile(m.opts.Path)
	if err != nil {
		m.failControl(ControlLoadUnreadable, "readback failed", nil, fmt.Errorf("%w: readback: %v", ErrStatePersistBlocked, err))
		return fmt.Errorf("runtime_control: readback: %w", err)
	}
	readState, _, err := unmarshalState(data)
	if err != nil {
		m.failControl(ControlLoadCorrupt, "readback is not a valid state file", nil, fmt.Errorf("%w: readback corrupt", ErrStatePersistBlocked))
		return fmt.Errorf("runtime_control: readback: %w", err)
	}
	stagedRaw, err := staged.encode()
	if err != nil {
		return err
	}
	var readRaw, committedRaw json.RawMessage
	if readState.control != nil {
		if readRaw, err = readState.control.encode(); err != nil {
			return err
		}
	}
	if committed != nil {
		if committedRaw, err = committed.encode(); err != nil {
			return err
		}
	}
	switch {
	case readRaw != nil && bytes.Equal(readRaw, stagedRaw):
		if err := m.hooks.FsyncDir(filepath.Dir(m.opts.Path)); err != nil {
			return fmt.Errorf("%w: directory durability not established: %v", ErrControlReadbackPending, err)
		}
		m.mu.Lock()
		m.state.control = staged
		m.controlStatus = ControlLoadStatus{Disposition: ControlLoadLoaded}
		m.controlAmbiguous = nil
		m.mu.Unlock()
		return nil
	case bytes.Equal(readRaw, committedRaw):
		if err := m.hooks.FsyncDir(filepath.Dir(m.opts.Path)); err != nil {
			return fmt.Errorf("%w: directory durability not established: %v", ErrControlReadbackPending, err)
		}
		m.mu.Lock()
		m.controlAmbiguous = nil
		m.mu.Unlock()
		return nil
	}
	m.failControl(ControlLoadCorrupt, "readback disagrees with the staged and committed namespaces", readState.control, nil)
	return fmt.Errorf("runtime_control: readback disagrees with every known namespace")
}

// failControl closes control for the whole namespace. When preserved is set,
// its bytes are retained verbatim so later writes never overwrite them.
func (m *Manager) failControl(status ControlLoadDisposition, detail string, preserved *controlNamespace, block error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.controlStatus = ControlLoadStatus{Disposition: status, Detail: detail}
	m.controlAmbiguous = nil
	// Without readback bytes to preserve, no later cache flush may write the
	// old committed namespace back over the failed readback.
	if preserved == nil && block == nil {
		block = fmt.Errorf("%w: control readback has no namespace", ErrStatePersistBlocked)
	}
	if block != nil {
		m.persistBlocked = block
	}
	if preserved != nil && m.state != nil {
		raw, err := preserved.encode()
		if err == nil {
			m.state.control = &controlNamespace{status: status, detail: detail, raw: raw}
		}
	}
}

// commitControl persists stage as the whole runtime-state file under the
// already-held serialization point and exposes it only after the commit is
// durable. A failure before the rename discards the staging (UNAVAILABLE, no
// retry on a later trigger). A failure after a successful rename is ambiguous
// persistence: nothing is exposed and admission stays closed until
// ResolveControlReadback establishes directory durability.
func (m *Manager) commitControl(stage *controlNamespace, seed bool) error {
	m.mu.Lock()
	if m.persistBlocked != nil {
		err := m.persistBlocked
		m.mu.Unlock()
		return &ControlRefusal{Code: RefusalUnavailable, Row: 3, Reason: "persistence blocked: " + err.Error()}
	}
	if m.state == nil {
		m.state = &State{SchemaVersion: SchemaVersion}
	}
	snap := cloneState(m.state)
	snap.control = stage
	snap.Meta.WrittenAt = m.now().UTC()
	gen := m.writeGen
	m.mu.Unlock()

	renamed, err := m.atomicWrite(snap, true)
	if err != nil {
		if !renamed {
			m.opts.Logger.Warn("runtime_state: control transaction discarded before commit", "error", err)
			return &ControlRefusal{Code: RefusalUnavailable, Reason: "pre-commit write failure: " + err.Error()}
		}
		m.mu.Lock()
		m.controlAmbiguous = stage
		m.mu.Unlock()
		m.opts.Logger.Warn("runtime_state: control transaction ambiguous; admission closed until readback", "error", err)
		return &ControlRefusal{Code: RefusalUnavailable, Row: 3, Reason: "ambiguous persistence: " + err.Error()}
	}
	m.mu.Lock()
	m.state.control = stage
	m.state.Meta.WrittenAt = snap.Meta.WrittenAt
	if seed || m.controlStatus.Disposition == ControlLoadMissing {
		m.controlStatus = ControlLoadStatus{Disposition: ControlLoadLoaded}
	}
	if m.writeGen == gen {
		m.dirty = false
	}
	m.mu.Unlock()
	return nil
}
