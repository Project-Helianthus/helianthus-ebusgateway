package eebusadmin

import (
	"io"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const ContractV1 = "helianthus.eebus.operator-admin.v1"

type Principal string

const PrincipalHostOperator Principal = "host_operator"

type ProcessReadiness string

const (
	ProcessReadinessReady    ProcessReadiness = "READY"
	ProcessReadinessNotReady ProcessReadiness = "NOT_READY"
)

type EEBusReadiness string

const (
	EEBusReadinessDisabled EEBusReadiness = "DISABLED"
	EEBusReadinessStarting EEBusReadiness = "STARTING"
	EEBusReadinessReady    EEBusReadiness = "READY"
	EEBusReadinessDegraded EEBusReadiness = "DEGRADED"
)

type EEBusDegradedReason string

const (
	EEBusDegradedReasonConfigurationInvalid      EEBusDegradedReason = "CONFIGURATION_INVALID"
	EEBusDegradedReasonLocalIdentityUnavailable  EEBusDegradedReason = "LOCAL_IDENTITY_UNAVAILABLE"
	EEBusDegradedReasonListenerUnavailable       EEBusDegradedReason = "LISTENER_UNAVAILABLE"
	EEBusDegradedReasonRuntimeFactoryUnavailable EEBusDegradedReason = "RUNTIME_FACTORY_UNAVAILABLE"
	EEBusDegradedReasonAdminBoundaryUnavailable  EEBusDegradedReason = "ADMIN_BOUNDARY_UNAVAILABLE"
	EEBusDegradedReasonUnknownStartupFailure     EEBusDegradedReason = "UNKNOWN_STARTUP_FAILURE"
)

type ReadinessV1 struct {
	ProcessReadiness    ProcessReadiness    `json:"process_readiness"`
	EEBusReadiness      EEBusReadiness      `json:"eebus_readiness"`
	EEBusDegradedReason EEBusDegradedReason `json:"eebus_degraded_reason,omitempty"`
}

type ReadinessProvider func() ReadinessV1

type Config struct {
	Admin     eebusruntime.AdminV1
	Raw       RawSnapshotProvider
	Audit     func(AuditEvent)
	Now       func() time.Time
	Random    io.Reader
	Readiness ReadinessProvider
}

// AuditEvent deliberately has no representation for operational identities,
// endpoints, request bodies, or transport/store internals.
type AuditEvent struct {
	Action              string    `json:"action"`
	Principal           Principal `json:"principal"`
	RequestID           string    `json:"request_id"`
	IdempotencyOutcome  string    `json:"idempotency_outcome"`
	PriorStateClass     string    `json:"prior_state_class"`
	ResultingStateClass string    `json:"resulting_state_class"`
	Timestamp           time.Time `json:"timestamp"`
	Reason              string    `json:"reason"`
}

type errorData struct {
	Code string `json:"code"`
}

type ownerEnvelope struct {
	Contract      string     `json:"contract"`
	RequestID     string     `json:"request_id,omitempty"`
	StateRevision uint64     `json:"state_revision"`
	Data          any        `json:"data"`
	Error         *errorData `json:"error"`
}

type ownerStatus struct {
	Readiness       ReadinessV1       `json:"readiness"`
	Status          string            `json:"status"`
	Window          string            `json:"pairing_window"`
	WindowDeadline  time.Time         `json:"pairing_window_deadline,omitempty"`
	Register        string            `json:"register"`
	Listener        string            `json:"listener"`
	Discovery       string            `json:"discovery"`
	TrustedCount    uint16            `json:"trusted_count"`
	ConnectedCount  uint16            `json:"connected_count"`
	DiscoveredCount uint16            `json:"discovered_count"`
	CandidateCount  uint16            `json:"candidate_count"`
	DegradedCode    string            `json:"degraded_code,omitempty"`
	ActiveAction    *activeActionData `json:"active_action,omitempty"`
}

// connectResultData is deliberately identity-free. The opaque action_id is
// sufficient to correlate the short-lived pairing outcome through /status.
type connectResultData struct {
	ActionID string `json:"action_id"`
	Outcome  string `json:"outcome"`
	Replayed bool   `json:"replayed"`
}

type activeActionData struct {
	ActionID  string    `json:"action_id"`
	Kind      string    `json:"kind"`
	State     string    `json:"state"`
	Outcome   string    `json:"outcome,omitempty"`
	Retryable bool      `json:"retryable"`
	ExpiresAt time.Time `json:"expiry"`
}

type partnersData struct {
	Partners []partnerRow `json:"partners"`
}

type partnerRow struct {
	PartnerID           string    `json:"partner_id,omitempty"`
	ObservationID       string    `json:"observation_id,omitempty"`
	View                string    `json:"view"`
	RemoteSKI           string    `json:"remote_ski,omitempty"`
	RemoteSHIPID        string    `json:"remote_ship_id,omitempty"`
	Brand               string    `json:"brand,omitempty"`
	DeviceType          string    `json:"device_type,omitempty"`
	Model               string    `json:"model,omitempty"`
	Endpoint            string    `json:"endpoint,omitempty"`
	TrustState          string    `json:"trust_state,omitempty"`
	ConnectionState     string    `json:"connection_state,omitempty"`
	LastSeen            time.Time `json:"last_seen,omitempty"`
	ObservationRevision uint64    `json:"observation_revision,omitempty"`
	CandidateState      string    `json:"candidate_state,omitempty"`
	CandidateExpiresAt  time.Time `json:"candidate_expires_at,omitempty"`
	DegradedReason      string    `json:"degraded_reason,omitempty"`
}
