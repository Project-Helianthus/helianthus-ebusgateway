package eebusadmin

import (
	"io"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const ContractV1 = "helianthus.eebus.operator-admin.v1"

type Principal string

const (
	PrincipalPortalOwner   Principal = "portal_owner"
	PrincipalHAIntegration Principal = "ha_integration"
)

type AuthConfig struct {
	OwnerUsername string
	OwnerSecret   []byte
	HASecret      []byte
	OwnerOrigin   string
	SessionTTL    time.Duration
	Now           func() time.Time
	Random        io.Reader
}

type Config struct {
	Admin eebusruntime.AdminV1
	Raw   RawSnapshotProvider
	Auth  AuthConfig
}

type errorData struct {
	Code string `json:"code"`
}

type ownerEnvelope struct {
	Contract      string     `json:"contract"`
	RequestID     string     `json:"request_id,omitempty"`
	StateRevision uint64     `json:"state_revision,omitempty"`
	Data          any        `json:"data"`
	Error         *errorData `json:"error"`
}

type haEnvelope struct {
	Contract           string     `json:"contract"`
	ProjectionRevision uint64     `json:"projection_revision,omitempty"`
	Data               any        `json:"data"`
	Error              *errorData `json:"error"`
}

type ownerStatus struct {
	Status          string    `json:"status"`
	Window          string    `json:"pairing_window"`
	WindowDeadline  time.Time `json:"pairing_window_deadline,omitempty"`
	Register        string    `json:"register"`
	Listener        string    `json:"listener"`
	Discovery       string    `json:"discovery"`
	TrustedCount    uint16    `json:"trusted_count"`
	ConnectedCount  uint16    `json:"connected_count"`
	DiscoveredCount uint16    `json:"discovered_count"`
	CandidateCount  uint16    `json:"candidate_count"`
	DegradedCode    string    `json:"degraded_code,omitempty"`
}

type haStatus struct {
	Listener        string `json:"listener"`
	Discovery       string `json:"discovery"`
	TrustedCount    uint16 `json:"trusted_count"`
	ConnectedCount  uint16 `json:"connected_count"`
	DiscoveredCount uint16 `json:"discovered_count"`
	DegradedCode    string `json:"degraded_code,omitempty"`
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
