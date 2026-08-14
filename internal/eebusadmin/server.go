package eebusadmin

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const maxRequestBodyBytes = 4096

type server struct {
	admin eebusruntime.AdminV1
	raw   RawSnapshotProvider
	auth  *authentication

	projectionMu       sync.Mutex
	projectionRevision uint64
	projectionHashes   map[string][32]byte

	capabilityMu       sync.Mutex
	capabilityRevision uint64
	capabilities       map[string]capabilityRecord
	capabilityByTarget map[string]string

	spineMu        sync.Mutex
	spineSnapshots map[string]*spineSnapshot
}

type capabilityKind string

const (
	capabilityPartner     capabilityKind = "partner"
	capabilityObservation capabilityKind = "observation"
	capabilitySelection   capabilityKind = "selection"
	capabilityCandidate   capabilityKind = "candidate"
	maxCapabilities                      = 512
)

type capabilityRecord struct {
	kind        capabilityKind
	sessionID   string
	revision    uint64
	expiresAt   time.Time
	ski         string
	partner     eebusruntime.PartnerHandleV1
	observation eebusruntime.ObservationHandleV1
	selection   eebusruntime.SelectionHandleV1
	candidate   eebusruntime.CandidateHandleV1
	trustAction bool
}

func NewServer(config Config) (http.Handler, error) {
	if config.Admin == nil {
		return nil, errors.New("admin capability is required")
	}
	auth, err := newAuthentication(config.Auth)
	if err != nil {
		return nil, err
	}
	return &server{
		admin: config.Admin, raw: config.Raw, auth: auth,
		capabilities: make(map[string]capabilityRecord), capabilityByTarget: make(map[string]string),
		projectionHashes: make(map[string][32]byte),
		spineSnapshots:   make(map[string]*spineSnapshot),
	}, nil
}

func (server *server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	identity, authFailure := server.auth.authenticate(w, request)
	if authFailure != "" {
		status := http.StatusUnauthorized
		if authFailure == "forbidden" {
			status = http.StatusForbidden
		}
		server.writeError(w, identity.principal, status, authFailure)
		return
	}
	if identity.principal == PrincipalPortalOwner && !server.auth.validateCSRF(request, identity.session) {
		server.writeError(w, identity.principal, http.StatusForbidden, "csrf_rejected")
		return
	}

	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/admin/eebus/v1/status":
		server.status(w, request, identity.principal)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/pairing-window:open":
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.openPairingWindow(w, request)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/pairing-window:close":
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.closePairingWindow(w, request)
	case request.Method == http.MethodGet && request.URL.Path == "/admin/eebus/v1/partners":
		server.partners(w, request, identity)
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/partners/") && strings.HasSuffix(request.URL.Path, "/spine"):
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.spinePage(w, request, identity.session)
	case request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/observations/") && strings.HasSuffix(request.URL.Path, ":select"):
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.selectObservation(w, request, identity.session)
	case request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/selections/") && strings.HasSuffix(request.URL.Path, ":connect"):
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.connectSelection(w, request, identity.session)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/candidate:confirm":
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.confirmCandidate(w, request, identity.session)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/candidate:cancel":
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.cancelCandidate(w, request, identity.session)
	case request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/partners/") && strings.HasSuffix(request.URL.Path, ":retry"):
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.mutateTrustedPartner(w, request, identity.session, true)
	case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/partners/") && strings.HasSuffix(request.URL.Path, "/trust"):
		if identity.principal != PrincipalPortalOwner {
			server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
			return
		}
		server.mutateTrustedPartner(w, request, identity.session, false)
	default:
		server.writeError(w, identity.principal, http.StatusNotFound, "invalid_request")
	}
}

func (server *server) partners(w http.ResponseWriter, request *http.Request, identity authenticatedRequest) {
	values := request.URL.Query()
	if len(values) != 1 || len(values["view"]) != 1 {
		server.writeError(w, identity.principal, http.StatusBadRequest, "invalid_request")
		return
	}
	view := eebusruntime.AdminViewV1(values.Get("view"))
	if view != eebusruntime.AdminViewV1Trusted && view != eebusruntime.AdminViewV1Connected &&
		view != eebusruntime.AdminViewV1Discovered && view != eebusruntime.AdminViewV1Candidate {
		server.writeError(w, identity.principal, http.StatusBadRequest, "invalid_request")
		return
	}
	if identity.principal == PrincipalHAIntegration && view == eebusruntime.AdminViewV1Candidate {
		server.writeError(w, identity.principal, http.StatusForbidden, "forbidden")
		return
	}
	snapshot, failure := server.admin.Snapshot(request.Context(), eebusruntime.AdminSnapshotRequestV1{View: view})
	if failure != nil {
		server.writeAdminFailure(w, identity.principal, failure)
		return
	}
	rows, err := server.projectPartners(identity, view, snapshot)
	if err != nil {
		server.writeError(w, identity.principal, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	if identity.principal == PrincipalHAIntegration {
		revision, projectionErr := server.acceptHAPartnerProjection(view, rows)
		if projectionErr != nil {
			server.writeError(w, identity.principal, http.StatusServiceUnavailable, "admin_boundary_unavailable")
			return
		}
		server.writeJSON(w, http.StatusOK, haEnvelope{Contract: ContractV1, ProjectionRevision: revision, Data: partnersData{Partners: rows}})
		return
	}
	server.writeJSON(w, http.StatusOK, ownerEnvelope{
		Contract: ContractV1, RequestID: server.requestID(), StateRevision: snapshot.StateRevision,
		Data: partnersData{Partners: rows},
	})
}

func (server *server) projectPartners(identity authenticatedRequest, view eebusruntime.AdminViewV1, snapshot eebusruntime.AdminSnapshotV1) ([]partnerRow, error) {
	rows := make([]partnerRow, 0)
	if identity.principal == PrincipalPortalOwner {
		server.resetCapabilities(snapshot.StateRevision)
	}
	switch view {
	case eebusruntime.AdminViewV1Trusted:
		for _, partner := range snapshot.Trusted {
			row := partnerRow{View: string(view), RemoteSKI: partner.SKI, RemoteSHIPID: partner.SHIPID, TrustState: partner.TrustState, LastSeen: partner.LastSeen}
			if identity.principal == PrincipalPortalOwner {
				id, err := server.issueCapability(identity.session.id, capabilityRecord{kind: capabilityPartner, revision: snapshot.StateRevision, ski: partner.SKI, partner: partner.Partner, trustAction: true}, "partner|"+partner.SKI)
				if err != nil {
					return nil, err
				}
				row.PartnerID = id
			} else {
				row = sanitizeHAPartner(row, server.haPseudonym(partner.SKI))
			}
			rows = append(rows, row)
		}
	case eebusruntime.AdminViewV1Connected:
		for _, partner := range snapshot.Connected {
			if identity.principal == PrincipalHAIntegration && partner.TrustState != "trusted" && partner.TrustState != "durably_trusted" {
				continue
			}
			row := partnerRow{View: string(view), RemoteSKI: partner.SKI, RemoteSHIPID: partner.SHIPID, Endpoint: partner.Endpoint, TrustState: partner.TrustState, ConnectionState: partner.ConnectionState, LastSeen: partner.LastSeen}
			if identity.principal == PrincipalPortalOwner {
				id, err := server.issueCapability(identity.session.id, capabilityRecord{kind: capabilityPartner, revision: snapshot.StateRevision, ski: partner.SKI}, "connected|"+partner.SKI)
				if err != nil {
					return nil, err
				}
				row.PartnerID = id
			} else {
				row = sanitizeHAPartner(row, server.haPseudonym(partner.SKI))
			}
			rows = append(rows, row)
		}
	case eebusruntime.AdminViewV1Discovered:
		for _, partner := range snapshot.Discovered {
			row := partnerRow{View: string(view), RemoteSKI: partner.SKI, Brand: partner.Brand, DeviceType: partner.Type, Model: partner.Model, Endpoint: partner.Endpoint, LastSeen: partner.LastSeen, ObservationRevision: partner.ObservationRevision}
			if identity.principal == PrincipalPortalOwner {
				id, err := server.issueCapability(identity.session.id, capabilityRecord{kind: capabilityObservation, revision: snapshot.StateRevision, ski: partner.SKI, observation: partner.Observation}, "observation|"+partner.SKI+"|"+partner.Endpoint+"|"+strconv.FormatUint(partner.ObservationRevision, 10))
				if err != nil {
					return nil, err
				}
				row.ObservationID = id
			} else {
				row = sanitizeHAPartner(row, server.haPseudonym(partner.SKI))
			}
			rows = append(rows, row)
		}
	case eebusruntime.AdminViewV1Candidate:
		if len(snapshot.Candidates) > 1 {
			return nil, errors.New("multiple current candidates")
		}
		for _, candidate := range snapshot.Candidates {
			_, err := server.issueCapability(identity.session.id, capabilityRecord{kind: capabilityCandidate, revision: snapshot.StateRevision, ski: candidate.SKI, candidate: candidate.Candidate}, "candidate|current")
			if err != nil {
				return nil, err
			}
			rows = append(rows, partnerRow{View: string(view), RemoteSKI: candidate.SKI, CandidateState: candidate.State, CandidateExpiresAt: candidate.ExpiresAt})
		}
	}
	return rows, nil
}

func sanitizeHAPartner(row partnerRow, id string) partnerRow {
	return partnerRow{PartnerID: id, View: row.View, Brand: row.Brand, DeviceType: row.DeviceType, Model: row.Model, TrustState: row.TrustState, ConnectionState: row.ConnectionState, LastSeen: row.LastSeen}
}

func (server *server) selectObservation(w http.ResponseWriter, request *http.Request, session ownerSession) {
	id, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/observations/", ":select")
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	var body skiMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if !validSKI(body.ExpectedSKI) || body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveCapability(id, session.id, capabilityObservation, body.StateRevision)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "observation_stale")
		return
	}
	result, failure := server.admin.Select(request.Context(), eebusruntime.SelectRequestV1{MutationPreconditionV1: eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: body.StateRevision}, Observation: record.observation, ExpectedSKI: body.ExpectedSKI})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, PrincipalPortalOwner, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	selectionID, err := server.issueCapability(session.id, capabilityRecord{kind: capabilitySelection, revision: result.StateRevision, ski: record.ski, selection: result.Selection}, "selection|current")
	if err != nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.writeMutationResult(w, result.AdminMutationResultV1, map[string]any{"selection_id": selectionID})
}

func (server *server) connectSelection(w http.ResponseWriter, request *http.Request, session ownerSession) {
	id, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/selections/", ":connect")
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveCapability(id, session.id, capabilitySelection, body.StateRevision)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "observation_stale")
		return
	}
	server.deleteCapability(id)
	result, failure := server.admin.Connect(request.Context(), eebusruntime.ConnectRequestV1{MutationPreconditionV1: eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: body.StateRevision}, Selection: record.selection})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, PrincipalPortalOwner, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	server.writeMutationResult(w, result, nil)
}

func (server *server) confirmCandidate(w http.ResponseWriter, request *http.Request, session ownerSession) {
	var body skiMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if !validSKI(body.ExpectedSKI) || body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveTargetCapability(session.id, "candidate|current", capabilityCandidate, body.StateRevision)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "candidate_expired")
		return
	}
	result, failure := server.admin.Confirm(request.Context(), eebusruntime.ConfirmRequestV1{MutationPreconditionV1: eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: body.StateRevision}, Candidate: record.candidate, ExpectedSKI: body.ExpectedSKI})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, PrincipalPortalOwner, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	server.writeMutationResult(w, result, nil)
}

func (server *server) closePairingWindow(w http.ResponseWriter, request *http.Request) {
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	result, failure := server.admin.ClosePairingWindow(request.Context(), eebusruntime.ClosePairingWindowRequestV1{MutationPreconditionV1: mutationPrecondition(request, body.StateRevision)})
	server.finishMutation(w, result, failure)
}

func (server *server) cancelCandidate(w http.ResponseWriter, request *http.Request, session ownerSession) {
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveTargetCapability(session.id, "candidate|current", capabilityCandidate, body.StateRevision)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "candidate_expired")
		return
	}
	result, failure := server.admin.Cancel(request.Context(), eebusruntime.CancelRequestV1{MutationPreconditionV1: mutationPrecondition(request, body.StateRevision), Candidate: record.candidate})
	server.finishMutation(w, result, failure)
}

func (server *server) mutateTrustedPartner(w http.ResponseWriter, request *http.Request, session ownerSession, retry bool) {
	suffix := "/trust"
	if retry {
		suffix = ":retry"
	}
	id, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/partners/", suffix)
	if !ok {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveCapability(id, session.id, capabilityPartner, body.StateRevision)
	if !ok || !record.trustAction {
		server.writeError(w, PrincipalPortalOwner, http.StatusConflict, "snapshot_expired")
		return
	}
	precondition := mutationPrecondition(request, body.StateRevision)
	var result eebusruntime.AdminMutationResultV1
	var failure *eebusruntime.AdminErrorV1
	if retry {
		result, failure = server.admin.RetryTrusted(request.Context(), eebusruntime.RetryTrustedRequestV1{MutationPreconditionV1: precondition, Partner: record.partner})
	} else {
		result, failure = server.admin.Untrust(request.Context(), eebusruntime.UntrustRequestV1{MutationPreconditionV1: precondition, Partner: record.partner})
	}
	server.finishMutation(w, result, failure)
}

func mutationPrecondition(request *http.Request, revision uint64) eebusruntime.MutationPreconditionV1 {
	return eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: revision}
}

func (server *server) finishMutation(w http.ResponseWriter, result eebusruntime.AdminMutationResultV1, failure *eebusruntime.AdminErrorV1) {
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, PrincipalPortalOwner, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	server.writeMutationResult(w, result, nil)
}

type revisionMutationBody struct {
	StateRevision uint64 `json:"state_revision"`
}
type skiMutationBody struct {
	StateRevision uint64 `json:"state_revision"`
	ExpectedSKI   string `json:"expected_ski"`
}

func (server *server) decodeMutation(w http.ResponseWriter, request *http.Request, target any) bool {
	if !strictJSONContentType(request.Header.Get("Content-Type")) || !validIdempotencyKey(request.Header.Get("Idempotency-Key")) || decodeStrictJSON(request.Body, target) != nil {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return false
	}
	return true
}

func (server *server) writeMutationResult(w http.ResponseWriter, result eebusruntime.AdminMutationResultV1, extra map[string]any) {
	data := map[string]any{"outcome": result.Outcome, "replayed": result.Replayed}
	for key, value := range extra {
		data[key] = value
	}
	server.writeJSON(w, http.StatusOK, ownerEnvelope{Contract: ContractV1, RequestID: server.requestID(), StateRevision: result.StateRevision, Data: data})
}

func pathIdentifier(value, prefix, suffix string) (string, bool) {
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, suffix) {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(value, prefix), suffix)
	return id, id != "" && !strings.Contains(id, "/") && path.Clean(id) == id
}

func (server *server) resetCapabilities(revision uint64) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	if server.capabilityRevision == revision {
		return
	}
	server.capabilityRevision = revision
	clear(server.capabilities)
	clear(server.capabilityByTarget)
}

func (server *server) invalidateCapabilities() {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	server.capabilityRevision = 0
	clear(server.capabilities)
	clear(server.capabilityByTarget)
}

func (server *server) issueCapability(sessionID string, record capabilityRecord, target string) (string, error) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	now := server.auth.now()
	for id, current := range server.capabilities {
		if !now.Before(current.expiresAt) {
			delete(server.capabilities, id)
		}
	}
	key := sessionID + "|" + string(record.kind) + "|" + target
	if id := server.capabilityByTarget[key]; id != "" {
		if current, ok := server.capabilities[id]; ok && now.Before(current.expiresAt) {
			return id, nil
		}
	}
	if len(server.capabilities) >= maxCapabilities {
		return "", errors.New("capability capacity exhausted")
	}
	kindCount := 0
	for _, current := range server.capabilities {
		if current.kind == record.kind {
			kindCount++
		}
	}
	if kindCount >= 128 {
		return "", errors.New("capability kind capacity exhausted")
	}
	id, err := randomToken(server.auth.random)
	if err != nil {
		return "", err
	}
	record.sessionID = sessionID
	record.expiresAt = now.Add(2 * time.Minute)
	server.capabilities[id] = record
	server.capabilityByTarget[key] = id
	return id, nil
}

func (server *server) resolveCapability(id, sessionID string, kind capabilityKind, revision uint64) (capabilityRecord, bool) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	record, ok := server.capabilities[id]
	return record, ok && record.sessionID == sessionID && record.kind == kind && record.revision == revision && server.capabilityRevision == revision && server.auth.now().Before(record.expiresAt)
}

func (server *server) resolveCurrentCapability(id, sessionID string, kind capabilityKind) (capabilityRecord, bool) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	record, ok := server.capabilities[id]
	return record, ok && record.sessionID == sessionID && record.kind == kind && record.revision == server.capabilityRevision && server.auth.now().Before(record.expiresAt)
}

func (server *server) resolveTargetCapability(sessionID, target string, kind capabilityKind, revision uint64) (capabilityRecord, bool) {
	server.capabilityMu.Lock()
	id := server.capabilityByTarget[sessionID+"|"+string(kind)+"|"+target]
	server.capabilityMu.Unlock()
	return server.resolveCapability(id, sessionID, kind, revision)
}

func (server *server) deleteCapability(id string) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	delete(server.capabilities, id)
}

func (server *server) haPseudonym(ski string) string {
	digest := hmac.New(sha256.New, server.auth.haSecret)
	_, _ = digest.Write([]byte("partner\x00" + ski))
	return "ha-" + hex.EncodeToString(digest.Sum(nil)[:16])
}

func (server *server) acceptHAPartnerProjection(view eebusruntime.AdminViewV1, rows []partnerRow) (uint64, error) {
	encoded, err := json.Marshal(struct {
		View     string       `json:"view"`
		Partners []partnerRow `json:"partners"`
	}{string(view), rows})
	if err != nil {
		return 0, err
	}
	hash := sha256.Sum256(encoded)
	return server.acceptHAProjectionHash("partners:"+string(view), hash)
}

func (server *server) status(w http.ResponseWriter, request *http.Request, principal Principal) {
	snapshot, failure := server.admin.Snapshot(request.Context(), eebusruntime.AdminSnapshotRequestV1{View: eebusruntime.AdminViewV1Trusted})
	if failure != nil {
		server.writeAdminFailure(w, principal, failure)
		return
	}
	if principal == PrincipalHAIntegration {
		connected, connectedFailure := server.admin.Snapshot(request.Context(), eebusruntime.AdminSnapshotRequestV1{View: eebusruntime.AdminViewV1Connected})
		if connectedFailure != nil {
			server.writeAdminFailure(w, principal, connectedFailure)
			return
		}
		if connected.StateRevision != snapshot.StateRevision {
			server.writeError(w, principal, http.StatusConflict, "state_conflict")
			return
		}
		trustedConnected := uint16(0)
		for _, partner := range connected.Connected {
			if partner.TrustState == "trusted" || partner.TrustState == "durably_trusted" {
				trustedConnected++
			}
		}
		data := haStatus{
			Listener: snapshot.Listener, Discovery: snapshot.Discovery,
			TrustedCount: snapshot.TrustedCount, ConnectedCount: trustedConnected,
			DiscoveredCount: snapshot.DiscoveredCount, DegradedCode: haDegradedCode(snapshot.DegradedCode),
		}
		revision, err := server.acceptHAProjection(data)
		if err != nil {
			server.writeError(w, principal, http.StatusServiceUnavailable, "admin_boundary_unavailable")
			return
		}
		server.writeJSON(w, http.StatusOK, haEnvelope{Contract: ContractV1, ProjectionRevision: revision, Data: data})
		return
	}
	data := ownerStatus{
		Status: snapshot.Status, Window: snapshot.Window, WindowDeadline: snapshot.WindowDeadline,
		Register: snapshot.Register, Listener: snapshot.Listener, Discovery: snapshot.Discovery,
		TrustedCount: snapshot.TrustedCount, ConnectedCount: snapshot.ConnectedCount,
		DiscoveredCount: snapshot.DiscoveredCount, CandidateCount: snapshot.CandidateCount,
		DegradedCode: string(snapshot.DegradedCode),
	}
	server.writeJSON(w, http.StatusOK, ownerEnvelope{
		Contract: ContractV1, RequestID: server.requestID(), StateRevision: snapshot.StateRevision, Data: data,
	})
}

type openPairingWindowBody struct {
	DurationSeconds uint64 `json:"duration_seconds"`
	StateRevision   uint64 `json:"state_revision"`
}

func (server *server) openPairingWindow(w http.ResponseWriter, request *http.Request) {
	if !strictJSONContentType(request.Header.Get("Content-Type")) {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(idempotencyKey) {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	var body openPairingWindowBody
	if err := decodeStrictJSON(request.Body, &body); err != nil || body.DurationSeconds == 0 || body.DurationSeconds > 300 || body.StateRevision == 0 {
		server.writeError(w, PrincipalPortalOwner, http.StatusBadRequest, "invalid_request")
		return
	}
	result, failure := server.admin.OpenPairingWindow(request.Context(), eebusruntime.OpenPairingWindowRequestV1{
		MutationPreconditionV1: eebusruntime.MutationPreconditionV1{
			IdempotencyKey: idempotencyKey, ExpectedStateRevision: body.StateRevision,
		},
		Duration: time.Duration(body.DurationSeconds) * time.Second,
	})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, PrincipalPortalOwner, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	server.writeJSON(w, http.StatusOK, ownerEnvelope{
		Contract: ContractV1, RequestID: server.requestID(), StateRevision: result.StateRevision,
		Data: map[string]any{"outcome": result.Outcome, "replayed": result.Replayed},
	})
}

func (server *server) acceptHAProjection(data haStatus) (uint64, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	hash := sha256.Sum256(encoded)
	return server.acceptHAProjectionHash("status", hash)
}

func (server *server) acceptHAProjectionHash(key string, hash [32]byte) (uint64, error) {
	server.projectionMu.Lock()
	defer server.projectionMu.Unlock()
	previous, known := server.projectionHashes[key]
	if server.projectionRevision == 0 {
		server.projectionRevision = 1
	} else if known && previous != hash {
		if server.projectionRevision == ^uint64(0) {
			return 0, errors.New("projection revision exhausted")
		}
		server.projectionRevision++
	}
	server.projectionHashes[key] = hash
	return server.projectionRevision, nil
}

func (server *server) writeAdminFailure(w http.ResponseWriter, principal Principal, failure *eebusruntime.AdminErrorV1) {
	code := sanitizedAdminErrorCode(failure)
	server.writeError(w, principal, adminFailureStatus(code), code)
}

func sanitizedAdminErrorCode(failure *eebusruntime.AdminErrorV1) string {
	if failure == nil {
		return "unknown_state"
	}
	switch failure.Code {
	case eebusruntime.AdminErrorCodeV1AdminBoundaryUnavailable,
		eebusruntime.AdminErrorCodeV1Unauthenticated,
		eebusruntime.AdminErrorCodeV1Forbidden,
		eebusruntime.AdminErrorCodeV1CSRFRejected,
		eebusruntime.AdminErrorCodeV1InvalidRequest,
		eebusruntime.AdminErrorCodeV1StateConflict,
		eebusruntime.AdminErrorCodeV1SnapshotExpired,
		eebusruntime.AdminErrorCodeV1IdempotencyConflict,
		eebusruntime.AdminErrorCodeV1PairingClosed,
		eebusruntime.AdminErrorCodeV1ObservationStale,
		eebusruntime.AdminErrorCodeV1IdentityMismatch,
		eebusruntime.AdminErrorCodeV1AssociationIncomplete,
		eebusruntime.AdminErrorCodeV1CandidateExpired,
		eebusruntime.AdminErrorCodeV1CandidateBusy,
		eebusruntime.AdminErrorCodeV1TrustDenied,
		eebusruntime.AdminErrorCodeV1ListenerUnavailable,
		eebusruntime.AdminErrorCodeV1DiscoveryUnavailable,
		eebusruntime.AdminErrorCodeV1AttemptTimeout,
		eebusruntime.AdminErrorCodeV1Disconnected,
		eebusruntime.AdminErrorCodeV1BackoffActive,
		eebusruntime.AdminErrorCodeV1TerminalQuarantine,
		eebusruntime.AdminErrorCodeV1PersistenceFailure,
		eebusruntime.AdminErrorCodeV1UnknownState:
		return string(failure.Code)
	default:
		return "unknown_state"
	}
}

func adminFailureStatus(code string) int {
	switch code {
	case "unauthenticated":
		return http.StatusUnauthorized
	case "forbidden", "csrf_rejected":
		return http.StatusForbidden
	case "invalid_request", "identity_mismatch":
		return http.StatusBadRequest
	case "state_conflict", "snapshot_expired", "idempotency_conflict", "observation_stale", "pairing_closed", "association_incomplete", "candidate_expired", "candidate_busy", "backoff_active":
		return http.StatusConflict
	case "admin_boundary_unavailable", "listener_unavailable", "discovery_unavailable", "terminal_quarantine", "persistence_failure":
		return http.StatusServiceUnavailable
	default:
		return http.StatusUnprocessableEntity
	}
}

func (server *server) writeError(w http.ResponseWriter, principal Principal, status int, code string) {
	if principal == PrincipalHAIntegration {
		server.writeJSON(w, status, haEnvelope{Contract: ContractV1, Error: &errorData{Code: code}})
		return
	}
	server.writeJSON(w, status, ownerEnvelope{Contract: ContractV1, Error: &errorData{Code: code}})
}

func (server *server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (server *server) requestID() string {
	buffer := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, buffer); err != nil {
		return "request-unavailable"
	}
	return hex.EncodeToString(buffer)
}

func strictJSONContentType(value string) bool {
	mediaType, parameters, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json" && len(parameters) == 0
}

func decodeStrictJSON(source io.Reader, target any) error {
	content, err := io.ReadAll(io.LimitReader(source, maxRequestBodyBytes+1))
	if err != nil || len(content) > maxRequestBodyBytes {
		return errors.New("request body exceeds limit")
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body has trailing data")
	}
	return nil
}

func haDegradedCode(code eebusruntime.AdminErrorCodeV1) string {
	switch code {
	case eebusruntime.AdminErrorCodeV1ListenerUnavailable, eebusruntime.AdminErrorCodeV1DiscoveryUnavailable:
		return string(code)
	default:
		return ""
	}
}

func validIdempotencyKey(value string) bool {
	return len(value) > 0 && len(value) <= 128 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

func validSKI(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
