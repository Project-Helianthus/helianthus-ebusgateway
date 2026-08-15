package eebusadmin

import (
	"bytes"
	"crypto/rand"
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

const maxHTTPMutationReplays = 128

type server struct {
	admin   eebusruntime.AdminV1
	raw     RawSnapshotProvider
	audit   func(AuditEvent)
	now     func() time.Time
	random  io.Reader
	scopeID string

	capabilityMu       sync.Mutex
	capabilityRevision uint64
	capabilities       map[string]capabilityRecord
	capabilityByTarget map[string]string

	spineMu        sync.Mutex
	spineSnapshots map[string]*spineSnapshot

	mutationMu      sync.Mutex
	mutationReplays map[string]httpMutationReplay
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
	scopeID     string
	revision    uint64
	expiresAt   time.Time
	ski         string
	partner     eebusruntime.PartnerHandleV1
	observation eebusruntime.ObservationHandleV1
	selection   eebusruntime.SelectionHandleV1
	candidate   eebusruntime.CandidateHandleV1
	trustAction bool
}

type httpMutationReplay struct {
	binding   string
	action    string
	status    int
	body      []byte
	expiresAt time.Time
}

type mutationResponseCapture struct {
	header             http.Header
	status             int
	body               bytes.Buffer
	invoked            bool
	locked             bool
	action             string
	idempotencyOutcome string
	cacheKey           string
	binding            string
	expiresAt          time.Time
}

func (capture *mutationResponseCapture) Header() http.Header { return capture.header }
func (capture *mutationResponseCapture) WriteHeader(status int) {
	if capture.status == 0 {
		capture.status = status
	}
}
func (capture *mutationResponseCapture) Write(content []byte) (int, error) {
	if capture.status == 0 {
		capture.status = http.StatusOK
	}
	return capture.body.Write(content)
}

func NewServer(config Config) (http.Handler, error) {
	if config.Admin == nil {
		return nil, errors.New("admin capability is required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	scopeID, err := randomToken(config.Random)
	if err != nil {
		return nil, errors.New("operator scope unavailable")
	}
	return &server{
		admin: config.Admin, raw: config.Raw, audit: config.Audit,
		now: config.Now, random: config.Random, scopeID: scopeID,
		capabilities: make(map[string]capabilityRecord), capabilityByTarget: make(map[string]string),
		spineSnapshots:  make(map[string]*spineSnapshot),
		mutationReplays: make(map[string]httpMutationReplay),
	}, nil
}

func (server *server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	destination := w
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		capture, handled := server.beginHTTPMutation(w, request)
		if capture != nil {
			w = capture
			if handled {
				server.finishHTTPMutation(destination, capture)
				return
			}
			defer server.finishHTTPMutation(destination, capture)
		}
		if handled {
			return
		}
	}

	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/admin/eebus/v1/status":
		server.status(w, request)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/pairing-window:open":
		server.openPairingWindow(w, request)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/pairing-window:close":
		server.closePairingWindow(w, request)
	case request.Method == http.MethodGet && request.URL.Path == "/admin/eebus/v1/partners":
		server.partners(w, request)
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/partners/") && strings.HasSuffix(request.URL.Path, "/spine"):
		server.spinePage(w, request)
	case request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/observations/") && strings.HasSuffix(request.URL.Path, ":select"):
		server.selectObservation(w, request)
	case request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/selections/") && strings.HasSuffix(request.URL.Path, ":connect"):
		server.connectSelection(w, request)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/candidate:confirm":
		server.confirmCandidate(w, request)
	case request.Method == http.MethodPost && request.URL.Path == "/admin/eebus/v1/candidate:cancel":
		server.cancelCandidate(w, request)
	case request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/partners/") && strings.HasSuffix(request.URL.Path, ":retry"):
		server.mutateTrustedPartner(w, request, true)
	case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/admin/eebus/v1/partners/") && strings.HasSuffix(request.URL.Path, "/trust"):
		server.mutateTrustedPartner(w, request, false)
	default:
		server.writeError(w, http.StatusNotFound, "invalid_request")
	}
}

func (server *server) beginHTTPMutation(w http.ResponseWriter, request *http.Request) (*mutationResponseCapture, bool) {
	capture := &mutationResponseCapture{
		header: make(http.Header), action: auditAction(request.Method, request.URL.Path),
		idempotencyOutcome: "rejected",
	}
	if request.URL.RawQuery != "" {
		server.writeError(capture, http.StatusBadRequest, "invalid_request")
		return capture, true
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(idempotencyKey) {
		return capture, false
	}
	content, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBodyBytes+1))
	request.Body = io.NopCloser(bytes.NewReader(content))
	if err != nil || len(content) > maxRequestBodyBytes {
		return capture, false
	}
	var body any
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	if decoder.Decode(&body) != nil {
		return capture, false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return capture, false
	}
	canonical, err := json.Marshal(body)
	if err != nil {
		return capture, false
	}
	binding := request.Method + "\x00" + request.URL.Path + "\x00" + string(canonical)
	cacheKey := server.scopeID + "\x00" + idempotencyKey

	server.mutationMu.Lock()
	now := server.now()
	for key, replay := range server.mutationReplays {
		if !now.Before(replay.expiresAt) {
			delete(server.mutationReplays, key)
		}
	}
	if replay, ok := server.mutationReplays[cacheKey]; ok {
		server.mutationMu.Unlock()
		if replay.binding != binding {
			capture.idempotencyOutcome = "conflict"
			server.writeError(capture, http.StatusConflict, "idempotency_conflict")
			return capture, true
		}
		capture.idempotencyOutcome = "replayed"
		writeHTTPMutationReplay(capture, replay)
		return capture, true
	}
	if len(server.mutationReplays) >= maxHTTPMutationReplays {
		server.mutationMu.Unlock()
		server.writeError(capture, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return capture, true
	}
	expiresAt := now.Add(2 * time.Minute)
	capture.locked = true
	capture.idempotencyOutcome = "executed"
	capture.cacheKey = cacheKey
	capture.binding = binding
	capture.expiresAt = expiresAt
	return capture, false
}

func (server *server) finishHTTPMutation(destination http.ResponseWriter, capture *mutationResponseCapture) {
	status := capture.status
	if status == 0 {
		status = http.StatusOK
	}
	body := append([]byte(nil), capture.body.Bytes()...)
	if capture.locked && capture.invoked && len(body) != 0 {
		server.mutationReplays[capture.cacheKey] = httpMutationReplay{binding: capture.binding, action: capture.action, status: status, body: append([]byte(nil), body...), expiresAt: capture.expiresAt}
	}
	if capture.locked {
		server.mutationMu.Unlock()
	}
	server.emitMutationAudit(capture.action, status, body, capture.idempotencyOutcome, capture.invoked)
	for key, values := range capture.header {
		destination.Header()[key] = append([]string(nil), values...)
	}
	destination.WriteHeader(status)
	_, _ = destination.Write(body)
}

func markHTTPMutationInvoked(w http.ResponseWriter) {
	if capture, ok := w.(*mutationResponseCapture); ok {
		capture.invoked = true
	}
}

func writeHTTPMutationReplay(w http.ResponseWriter, replay httpMutationReplay) {
	w.WriteHeader(replay.status)
	_, _ = w.Write(replay.body)
}

func auditAction(method, requestPath string) string {
	switch {
	case method == http.MethodPost && requestPath == "/admin/eebus/v1/pairing-window:open":
		return "open_pairing_window"
	case method == http.MethodPost && requestPath == "/admin/eebus/v1/pairing-window:close":
		return "close_pairing_window"
	case method == http.MethodPost && requestPath == "/admin/eebus/v1/candidate:confirm":
		return "confirm_candidate"
	case method == http.MethodPost && requestPath == "/admin/eebus/v1/candidate:cancel":
		return "cancel_candidate"
	case method == http.MethodPost && strings.HasPrefix(requestPath, "/admin/eebus/v1/observations/") && strings.HasSuffix(requestPath, ":select"):
		return "select_observation"
	case method == http.MethodPost && strings.HasPrefix(requestPath, "/admin/eebus/v1/selections/") && strings.HasSuffix(requestPath, ":connect"):
		return "connect_selection"
	case method == http.MethodPost && strings.HasPrefix(requestPath, "/admin/eebus/v1/partners/") && strings.HasSuffix(requestPath, ":retry"):
		return "retry_trusted_partner"
	case method == http.MethodDelete && strings.HasPrefix(requestPath, "/admin/eebus/v1/partners/") && strings.HasSuffix(requestPath, "/trust"):
		return "untrust_partner"
	default:
		return "unknown_mutation"
	}
}

func (server *server) emitMutationAudit(action string, status int, body []byte, idempotencyOutcome string, invoked bool) {
	if server.audit == nil {
		return
	}
	var envelope struct {
		RequestID string `json:"request_id"`
		Data      struct {
			Outcome string `json:"outcome"`
		} `json:"data"`
		Error *errorData `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return
	}
	reason := "unknown_state"
	resulting := "rejected"
	if envelope.Error != nil {
		reason = sanitizedAuditReason(envelope.Error.Code)
	} else if status >= http.StatusOK && status < http.StatusMultipleChoices {
		reason = sanitizedAuditReason(envelope.Data.Outcome)
		resulting = "changed"
	}
	prior := "host_operator"
	if invoked {
		prior = "precondition_accepted"
	}
	event := AuditEvent{
		Action: action, Principal: PrincipalHostOperator, RequestID: envelope.RequestID,
		IdempotencyOutcome: idempotencyOutcome, PriorStateClass: prior,
		ResultingStateClass: resulting, Timestamp: server.now(), Reason: reason,
	}
	func() {
		defer func() { _ = recover() }()
		server.audit(event)
	}()
}

func sanitizedAuditReason(value string) string {
	if value == "" || len(value) > 64 {
		return "unknown_state"
	}
	for index, character := range value {
		if (character < 'a' || character > 'z') && (index == 0 || character < '0' || character > '9') && character != '_' {
			return "unknown_state"
		}
	}
	return value
}

func (server *server) partners(w http.ResponseWriter, request *http.Request) {
	values := request.URL.Query()
	if len(values) != 1 || len(values["view"]) != 1 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	view := eebusruntime.AdminViewV1(values.Get("view"))
	if view != eebusruntime.AdminViewV1Trusted && view != eebusruntime.AdminViewV1Connected &&
		view != eebusruntime.AdminViewV1Discovered && view != eebusruntime.AdminViewV1Candidate {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	snapshot, failure := server.admin.Snapshot(request.Context(), eebusruntime.AdminSnapshotRequestV1{View: view})
	if failure != nil {
		server.writeAdminFailure(w, failure)
		return
	}
	rows, err := server.projectPartners(view, snapshot)
	if err != nil {
		server.writeError(w, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.writeJSON(w, http.StatusOK, ownerEnvelope{
		Contract: ContractV1, RequestID: server.requestID(), StateRevision: snapshot.StateRevision,
		Data: partnersData{Partners: rows},
	})
}

func (server *server) projectPartners(view eebusruntime.AdminViewV1, snapshot eebusruntime.AdminSnapshotV1) ([]partnerRow, error) {
	rows := make([]partnerRow, 0)
	server.resetCapabilities(snapshot.StateRevision)
	switch view {
	case eebusruntime.AdminViewV1Trusted:
		for _, partner := range snapshot.Trusted {
			row := partnerRow{View: string(view), RemoteSKI: partner.SKI, RemoteSHIPID: partner.SHIPID, TrustState: partner.TrustState, LastSeen: partner.LastSeen}
			id, err := server.issueCapability(capabilityRecord{kind: capabilityPartner, revision: snapshot.StateRevision, ski: partner.SKI, partner: partner.Partner, trustAction: true}, "partner|"+partner.SKI)
			if err != nil {
				return nil, err
			}
			row.PartnerID = id
			rows = append(rows, row)
		}
	case eebusruntime.AdminViewV1Connected:
		for _, partner := range snapshot.Connected {
			row := partnerRow{View: string(view), RemoteSKI: partner.SKI, RemoteSHIPID: partner.SHIPID, Endpoint: partner.Endpoint, TrustState: partner.TrustState, ConnectionState: partner.ConnectionState, LastSeen: partner.LastSeen}
			id, err := server.issueCapability(capabilityRecord{kind: capabilityPartner, revision: snapshot.StateRevision, ski: partner.SKI}, "connected|"+partner.SKI)
			if err != nil {
				return nil, err
			}
			row.PartnerID = id
			rows = append(rows, row)
		}
	case eebusruntime.AdminViewV1Discovered:
		for _, partner := range snapshot.Discovered {
			row := partnerRow{View: string(view), RemoteSKI: partner.SKI, Brand: partner.Brand, DeviceType: partner.Type, Model: partner.Model, Endpoint: partner.Endpoint, LastSeen: partner.LastSeen, ObservationRevision: partner.ObservationRevision}
			id, err := server.issueCapability(capabilityRecord{kind: capabilityObservation, revision: snapshot.StateRevision, ski: partner.SKI, observation: partner.Observation}, "observation|"+partner.SKI+"|"+partner.Endpoint+"|"+strconv.FormatUint(partner.ObservationRevision, 10))
			if err != nil {
				return nil, err
			}
			row.ObservationID = id
			rows = append(rows, row)
		}
	case eebusruntime.AdminViewV1Candidate:
		if len(snapshot.Candidates) > 1 {
			return nil, errors.New("multiple current candidates")
		}
		for _, candidate := range snapshot.Candidates {
			_, err := server.issueCapability(capabilityRecord{kind: capabilityCandidate, revision: snapshot.StateRevision, ski: candidate.SKI, candidate: candidate.Candidate}, "candidate|current")
			if err != nil {
				return nil, err
			}
			rows = append(rows, partnerRow{View: string(view), RemoteSKI: candidate.SKI, CandidateState: candidate.State, CandidateExpiresAt: candidate.ExpiresAt})
		}
	}
	return rows, nil
}

func (server *server) selectObservation(w http.ResponseWriter, request *http.Request) {
	id, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/observations/", ":select")
	if !ok {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var body skiMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if !validSKI(body.ExpectedSKI) || body.StateRevision == 0 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveCapability(id, capabilityObservation, body.StateRevision)
	if !ok {
		server.writeError(w, http.StatusConflict, "observation_stale")
		return
	}
	markHTTPMutationInvoked(w)
	result, failure := server.admin.Select(request.Context(), eebusruntime.SelectRequestV1{MutationPreconditionV1: eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: body.StateRevision}, Observation: record.observation, ExpectedSKI: body.ExpectedSKI})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	selectionID, err := server.issueCapability(capabilityRecord{kind: capabilitySelection, revision: result.StateRevision, ski: record.ski, selection: result.Selection}, "selection|current")
	if err != nil {
		server.writeError(w, http.StatusServiceUnavailable, "admin_boundary_unavailable")
		return
	}
	server.writeMutationResult(w, result.AdminMutationResultV1, map[string]any{"selection_id": selectionID})
}

func (server *server) connectSelection(w http.ResponseWriter, request *http.Request) {
	id, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/selections/", ":connect")
	if !ok {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveCapability(id, capabilitySelection, body.StateRevision)
	if !ok {
		server.writeError(w, http.StatusConflict, "observation_stale")
		return
	}
	server.deleteCapability(id)
	markHTTPMutationInvoked(w)
	result, failure := server.admin.Connect(request.Context(), eebusruntime.ConnectRequestV1{MutationPreconditionV1: eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: body.StateRevision}, Selection: record.selection})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	server.writeMutationResult(w, result, nil)
}

func (server *server) confirmCandidate(w http.ResponseWriter, request *http.Request) {
	var body skiMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if !validSKI(body.ExpectedSKI) || body.StateRevision == 0 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveTargetCapability("candidate|current", capabilityCandidate, body.StateRevision)
	if !ok {
		server.writeError(w, http.StatusConflict, "candidate_expired")
		return
	}
	markHTTPMutationInvoked(w)
	result, failure := server.admin.Confirm(request.Context(), eebusruntime.ConfirmRequestV1{MutationPreconditionV1: eebusruntime.MutationPreconditionV1{IdempotencyKey: request.Header.Get("Idempotency-Key"), ExpectedStateRevision: body.StateRevision}, Candidate: record.candidate, ExpectedSKI: body.ExpectedSKI})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, failure)
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
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	markHTTPMutationInvoked(w)
	result, failure := server.admin.ClosePairingWindow(request.Context(), eebusruntime.ClosePairingWindowRequestV1{MutationPreconditionV1: mutationPrecondition(request, body.StateRevision)})
	server.finishMutation(w, result, failure)
}

func (server *server) cancelCandidate(w http.ResponseWriter, request *http.Request) {
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveTargetCapability("candidate|current", capabilityCandidate, body.StateRevision)
	if !ok {
		server.writeError(w, http.StatusConflict, "candidate_expired")
		return
	}
	markHTTPMutationInvoked(w)
	result, failure := server.admin.Cancel(request.Context(), eebusruntime.CancelRequestV1{MutationPreconditionV1: mutationPrecondition(request, body.StateRevision), Candidate: record.candidate})
	server.finishMutation(w, result, failure)
}

func (server *server) mutateTrustedPartner(w http.ResponseWriter, request *http.Request, retry bool) {
	suffix := "/trust"
	if retry {
		suffix = ":retry"
	}
	id, ok := pathIdentifier(request.URL.Path, "/admin/eebus/v1/partners/", suffix)
	if !ok {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var body revisionMutationBody
	if !server.decodeMutation(w, request, &body) {
		return
	}
	if body.StateRevision == 0 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	record, ok := server.resolveCapability(id, capabilityPartner, body.StateRevision)
	if !ok || !record.trustAction {
		server.writeError(w, http.StatusConflict, "snapshot_expired")
		return
	}
	precondition := mutationPrecondition(request, body.StateRevision)
	var result eebusruntime.AdminMutationResultV1
	var failure *eebusruntime.AdminErrorV1
	markHTTPMutationInvoked(w)
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
		server.writeAdminFailure(w, failure)
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
		server.writeError(w, http.StatusBadRequest, "invalid_request")
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

func (server *server) issueCapability(record capabilityRecord, target string) (string, error) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	now := server.now()
	for id, current := range server.capabilities {
		if !now.Before(current.expiresAt) {
			delete(server.capabilities, id)
		}
	}
	key := server.scopeID + "|" + string(record.kind) + "|" + target
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
	id, err := randomToken(server.random)
	if err != nil {
		return "", err
	}
	record.scopeID = server.scopeID
	record.expiresAt = now.Add(2 * time.Minute)
	server.capabilities[id] = record
	server.capabilityByTarget[key] = id
	return id, nil
}

func (server *server) resolveCapability(id string, kind capabilityKind, revision uint64) (capabilityRecord, bool) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	record, ok := server.capabilities[id]
	return record, ok && record.scopeID == server.scopeID && record.kind == kind && record.revision == revision && server.capabilityRevision == revision && server.now().Before(record.expiresAt)
}

func (server *server) resolveCurrentCapability(id string, kind capabilityKind) (capabilityRecord, bool) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	record, ok := server.capabilities[id]
	return record, ok && record.scopeID == server.scopeID && record.kind == kind && record.revision == server.capabilityRevision && server.now().Before(record.expiresAt)
}

func (server *server) resolveTargetCapability(target string, kind capabilityKind, revision uint64) (capabilityRecord, bool) {
	server.capabilityMu.Lock()
	id := server.capabilityByTarget[server.scopeID+"|"+string(kind)+"|"+target]
	server.capabilityMu.Unlock()
	return server.resolveCapability(id, kind, revision)
}

func (server *server) deleteCapability(id string) {
	server.capabilityMu.Lock()
	defer server.capabilityMu.Unlock()
	delete(server.capabilities, id)
}

func (server *server) status(w http.ResponseWriter, request *http.Request) {
	snapshot, failure := server.admin.Snapshot(request.Context(), eebusruntime.AdminSnapshotRequestV1{View: eebusruntime.AdminViewV1Trusted})
	if failure != nil {
		server.writeAdminFailure(w, failure)
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
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(idempotencyKey) {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var body openPairingWindowBody
	if err := decodeStrictJSON(request.Body, &body); err != nil || body.DurationSeconds == 0 || body.DurationSeconds > 300 || body.StateRevision == 0 {
		server.writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	markHTTPMutationInvoked(w)
	result, failure := server.admin.OpenPairingWindow(request.Context(), eebusruntime.OpenPairingWindowRequestV1{
		MutationPreconditionV1: eebusruntime.MutationPreconditionV1{
			IdempotencyKey: idempotencyKey, ExpectedStateRevision: body.StateRevision,
		},
		Duration: time.Duration(body.DurationSeconds) * time.Second,
	})
	if failure != nil {
		server.invalidateCapabilities()
		server.writeAdminFailure(w, failure)
		return
	}
	server.resetCapabilities(result.StateRevision)
	server.writeJSON(w, http.StatusOK, ownerEnvelope{
		Contract: ContractV1, RequestID: server.requestID(), StateRevision: result.StateRevision,
		Data: map[string]any{"outcome": result.Outcome, "replayed": result.Replayed},
	})
}

func (server *server) writeAdminFailure(w http.ResponseWriter, failure *eebusruntime.AdminErrorV1) {
	code := sanitizedAdminErrorCode(failure)
	server.writeError(w, adminFailureStatus(code), code)
}

func sanitizedAdminErrorCode(failure *eebusruntime.AdminErrorV1) string {
	if failure == nil {
		return "unknown_state"
	}
	switch failure.Code {
	case eebusruntime.AdminErrorCodeV1AdminBoundaryUnavailable,
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

func (server *server) writeError(w http.ResponseWriter, status int, code string) {
	server.writeJSON(w, status, ownerEnvelope{Contract: ContractV1, RequestID: server.requestID(), Error: &errorData{Code: code}})
}

func (server *server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (server *server) requestID() string {
	buffer := make([]byte, 16)
	if _, err := io.ReadFull(server.random, buffer); err != nil {
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
