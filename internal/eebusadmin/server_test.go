package eebusadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type adminV1Stub struct {
	mu                sync.Mutex
	snapshot          eebusruntime.AdminSnapshotV1
	snapshots         map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1
	snapshotCalls     int
	openCalls         []eebusruntime.OpenPairingWindowRequestV1
	openRevision      uint64
	selectCalls       []eebusruntime.SelectRequestV1
	connectCalls      []eebusruntime.ConnectRequestV1
	confirmCalls      []eebusruntime.ConfirmRequestV1
	cancelCalls       []eebusruntime.CancelRequestV1
	closeCalls        []eebusruntime.ClosePairingWindowRequestV1
	retryCalls        []eebusruntime.RetryTrustedRequestV1
	untrustCalls      []eebusruntime.UntrustRequestV1
	connectStarted    chan struct{}
	connectRelease    chan struct{}
	connectStartOnce  sync.Once
	connectFailure    *eebusruntime.AdminErrorV1
	connectPanics     int
	connectPanicValue any
}

type issue848ObservedDoneContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
	done     chan struct{}
}

func (ctx *issue848ObservedDoneContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.done
}

func (stub *adminV1Stub) Snapshot(_ context.Context, request eebusruntime.AdminSnapshotRequestV1) (eebusruntime.AdminSnapshotV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.snapshotCalls++
	if snapshot, ok := stub.snapshots[request.View]; ok {
		return snapshot, nil
	}
	return stub.snapshot, nil
}

func (stub *adminV1Stub) OpenPairingWindow(_ context.Context, request eebusruntime.OpenPairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.openCalls = append(stub.openCalls, request)
	if stub.openRevision != 0 && request.ExpectedStateRevision != stub.openRevision {
		return eebusruntime.AdminMutationResultV1{}, &eebusruntime.AdminErrorV1{Code: eebusruntime.AdminErrorCodeV1StateConflict}
	}
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "pairing_window_opened"}, nil
}

func (stub *adminV1Stub) ClosePairingWindow(_ context.Context, request eebusruntime.ClosePairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.closeCalls = append(stub.closeCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "pairing_window_closed"}, nil
}

func (stub *adminV1Stub) Select(_ context.Context, request eebusruntime.SelectRequestV1) (eebusruntime.AdminSelectionResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.selectCalls = append(stub.selectCalls, request)
	return eebusruntime.AdminSelectionResultV1{AdminMutationResultV1: eebusruntime.AdminMutationResultV1{
		StateRevision: request.ExpectedStateRevision + 1,
		Outcome:       "selected",
	}}, nil
}

func (stub *adminV1Stub) Connect(ctx context.Context, request eebusruntime.ConnectRequestV1) (eebusruntime.ConnectResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	stub.connectCalls = append(stub.connectCalls, request)
	started, release, failure := stub.connectStarted, stub.connectRelease, stub.connectFailure
	shouldPanic := stub.connectPanics > 0
	if shouldPanic {
		stub.connectPanics--
	}
	stub.mu.Unlock()
	if started != nil {
		stub.connectStartOnce.Do(func() { close(started) })
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return eebusruntime.ConnectResultV1{}, &eebusruntime.AdminErrorV1{Code: eebusruntime.AdminErrorCodeV1AttemptTimeout}
		}
	}
	if failure != nil {
		return eebusruntime.ConnectResultV1{}, failure
	}
	if shouldPanic {
		if stub.connectPanicValue != nil {
			panic(stub.connectPanicValue)
		}
		panic("backend panic after launched effect")
	}
	return eebusruntime.ConnectResultV1{AdminMutationResultV1: eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "connection_started"}, ActionID: "action-1"}, nil
}

func TestIssue848ConnectAuditMatrixIsExactlyOnceAndSecretFree(t *testing.T) {
	const (
		ski         = "0123456789abcdef0123456789abcdef01234567"
		pin         = "A1b2C3d4"
		changedPIN  = "a1b2c3d4"
		idempotency = "connect-audit-848"
	)
	type auditCollector struct {
		mu     sync.Mutex
		events []AuditEvent
	}
	collect := func(target *auditCollector) func(AuditEvent) {
		return func(event AuditEvent) {
			target.mu.Lock()
			defer target.mu.Unlock()
			target.events = append(target.events, event)
		}
	}
	assertEvents := func(t *testing.T, target *auditCollector, wants []AuditEvent) {
		t.Helper()
		target.mu.Lock()
		defer target.mu.Unlock()
		if len(target.events) != len(wants) {
			t.Fatalf("audit events=%d, want %d: %#v", len(target.events), len(wants), target.events)
		}
		for index, want := range wants {
			got := target.events[index]
			if got.Action != "connect_selection" || got.Principal != PrincipalHostOperator || got.IdempotencyOutcome != want.IdempotencyOutcome || got.PriorStateClass != want.PriorStateClass || got.ResultingStateClass != want.ResultingStateClass || got.Reason != want.Reason || got.RequestID == "" || got.Timestamp.IsZero() {
				t.Fatalf("audit[%d]=%#v, want disposition=%q prior=%q resulting=%q reason=%q", index, got, want.IdempotencyOutcome, want.PriorStateClass, want.ResultingStateClass, want.Reason)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{ski, pin, changedPIN, idempotency, `"state_revision"`, `"pin"`, `"selection_id"`, "connect-binding", "hmac", "sha256"} {
				if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(forbidden)) {
					t.Fatalf("audit[%d] leaked request/PIN/HMAC/selection material %q: %s", index, forbidden, encoded)
				}
			}
		}
	}
	prepare := func(t *testing.T, admin *adminV1Stub, collector *auditCollector, revision uint64) (http.Handler, string) {
		t.Helper()
		snapshot := testAdminSnapshot()
		snapshot.StateRevision = revision
		snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 4}}
		admin.snapshot = snapshot
		admin.snapshots = map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}
		handler, err := NewServer(Config{Admin: admin, Audit: collect(collector)})
		if err != nil {
			t.Fatal(err)
		}
		listed := httptest.NewRecorder()
		handler.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil))
		observationID := issue817FirstOpaqueID(t, listed, "observation_id")
		selected := httptest.NewRecorder()
		handler.ServeHTTP(selected, issue817Mutation(http.MethodPost, "/admin/eebus/v1/observations/"+observationID+":select", "select-audit-848", `{"state_revision":`+strconv.FormatUint(revision, 10)+`,"expected_ski":"`+ski+`"}`))
		if selected.Code != http.StatusOK {
			t.Fatalf("select status=%d body=%s", selected.Code, selected.Body.String())
		}
		collector.mu.Lock()
		collector.events = nil // The matrix is scoped to the specialized Connect route.
		collector.mu.Unlock()
		return handler, "/admin/eebus/v1/selections/" + issue817DataString(t, selected, "selection_id") + ":connect"
	}
	request := func(path, key, body string) *http.Request {
		return issue817Mutation(http.MethodPost, path, key, body)
	}

	t.Run("launch replay and changed-binding conflict", func(t *testing.T) {
		collector := &auditCollector{}
		handler, route := prepare(t, &adminV1Stub{}, collector, 70)
		for _, body := range []string{
			`{"state_revision":71,"pin":"` + pin + `"}`,
			`{"state_revision":71,"pin":"` + pin + `"}`,
			`{"state_revision":71,"pin":"` + changedPIN + `"}`,
		} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request(route, idempotency, body))
		}
		assertEvents(t, collector, []AuditEvent{
			{IdempotencyOutcome: "executed", PriorStateClass: "precondition_accepted", ResultingStateClass: "changed", Reason: "connection_started"},
			{IdempotencyOutcome: "replayed", PriorStateClass: "host_operator", ResultingStateClass: "changed", Reason: "connection_started"},
			{IdempotencyOutcome: "conflict", PriorStateClass: "host_operator", ResultingStateClass: "rejected", Reason: "idempotency_conflict"},
		})
	})

	t.Run("backend rejection", func(t *testing.T) {
		collector := &auditCollector{}
		admin := &adminV1Stub{connectFailure: &eebusruntime.AdminErrorV1{Code: eebusruntime.AdminErrorCodeV1PairingClosed}}
		handler, route := prepare(t, admin, collector, 80)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request(route, "backend-failure-848", `{"state_revision":81,"pin":"`+pin+`"}`))
		if response.Code != http.StatusConflict {
			t.Fatalf("backend rejection status=%d body=%s", response.Code, response.Body.String())
		}
		assertEvents(t, collector, []AuditEvent{{IdempotencyOutcome: "executed", PriorStateClass: "precondition_accepted", ResultingStateClass: "rejected", Reason: "pairing_closed"}})
	})

	t.Run("canceled follower", func(t *testing.T) {
		collector := &auditCollector{}
		admin := &adminV1Stub{connectStarted: make(chan struct{}), connectRelease: make(chan struct{})}
		handler, route := prepare(t, admin, collector, 90)
		leaderDone := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request(route, "follower-cancel-848", `{"state_revision":91,"pin":"`+pin+`"}`))
			leaderDone <- response
		}()
		select {
		case <-admin.connectStarted:
		case <-time.After(time.Second):
			t.Fatal("leader did not reach backend")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		follower := httptest.NewRecorder()
		handler.ServeHTTP(follower, request(route, "follower-cancel-848", `{"state_revision":91,"pin":"`+pin+`"}`).WithContext(ctx))
		if follower.Code != http.StatusRequestTimeout {
			t.Fatalf("canceled follower status=%d body=%s", follower.Code, follower.Body.String())
		}
		close(admin.connectRelease)
		leader := <-leaderDone
		if leader.Code != http.StatusOK {
			t.Fatalf("leader status=%d body=%s", leader.Code, leader.Body.String())
		}
		assertEvents(t, collector, []AuditEvent{
			{IdempotencyOutcome: "rejected", PriorStateClass: "host_operator", ResultingStateClass: "rejected", Reason: "attempt_timeout"},
			{IdempotencyOutcome: "executed", PriorStateClass: "precondition_accepted", ResultingStateClass: "changed", Reason: "connection_started"},
		})
	})

	t.Run("panic after launched effect is a bounded replay tombstone", func(t *testing.T) {
		collector := &auditCollector{}
		admin := &adminV1Stub{connectStarted: make(chan struct{}), connectRelease: make(chan struct{}), connectPanics: 1}
		handler, route := prepare(t, admin, collector, 100)
		server := handler.(*server)
		const key = "panic-after-effect-848"
		body := `{"state_revision":101,"pin":"` + pin + `"}`

		leaderPanicked := make(chan bool, 1)
		go func() {
			panicked := false
			defer func() {
				if recover() != nil {
					panicked = true
				}
				leaderPanicked <- panicked
			}()
			handler.ServeHTTP(httptest.NewRecorder(), request(route, key, body))
		}()
		select {
		case <-admin.connectStarted:
		case <-time.After(time.Second):
			t.Fatal("leader did not launch backend effect")
		}

		followerObserved := make(chan struct{})
		followerContext := &issue848ObservedDoneContext{Context: context.Background(), observed: followerObserved, done: make(chan struct{})}
		followerDone := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request(route, key, body).WithContext(followerContext))
			followerDone <- response
		}()
		select {
		case <-followerObserved:
		case <-time.After(time.Second):
			t.Fatal("follower did not enter the shared reservation wait")
		}
		close(admin.connectRelease)
		if !<-leaderPanicked {
			t.Fatal("leader did not re-panic after the launched backend effect")
		}
		follower := <-followerDone
		if follower.Code != http.StatusServiceUnavailable {
			t.Fatalf("follower status=%d body=%s", follower.Code, follower.Body.String())
		}

		retry := httptest.NewRecorder()
		handler.ServeHTTP(retry, request(route, key, body))
		if retry.Code != http.StatusServiceUnavailable {
			t.Fatalf("identical retry status=%d body=%s", retry.Code, retry.Body.String())
		}
		conflict := httptest.NewRecorder()
		handler.ServeHTTP(conflict, request(route, key, `{"state_revision":101,"pin":"`+changedPIN+`"}`))
		if conflict.Code != http.StatusConflict {
			t.Fatalf("changed binding status=%d body=%s", conflict.Code, conflict.Body.String())
		}

		admin.mu.Lock()
		calls := len(admin.connectCalls)
		admin.mu.Unlock()
		if calls != 1 {
			t.Fatalf("backend Connect calls=%d, want one launched effect", calls)
		}
		server.connectMu.Lock()
		_, stillInFlight := server.connectInFlight[key]
		server.connectMu.Unlock()
		if stillInFlight {
			t.Fatal("panic left followers blocked on an in-flight reservation")
		}

		collector.mu.Lock()
		events := append([]AuditEvent(nil), collector.events...)
		collector.mu.Unlock()
		if len(events) != 4 {
			t.Fatalf("audit events=%d, want exactly one per request: %#v", len(events), events)
		}
		dispositions := map[string]int{}
		for _, event := range events {
			if event.Action != "connect_selection" || event.ResultingStateClass != "rejected" || (event.Reason != "admin_boundary_unavailable" && event.Reason != "idempotency_conflict") {
				t.Fatalf("unsanitized panic audit=%#v", event)
			}
			dispositions[event.IdempotencyOutcome]++
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{ski, pin, changedPIN, key, body, "backend panic", "launched effect", "hmac", "sha256", "binding"} {
				if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(forbidden)) {
					t.Fatalf("panic audit leaked %q: %s", forbidden, encoded)
				}
			}
		}
		if dispositions["executed"] != 1 || dispositions["replayed"] != 2 || dispositions["conflict"] != 1 {
			t.Fatalf("audit dispositions=%v, want executed=1 replayed=2 conflict=1", dispositions)
		}
	})

	t.Run("real HTTP server never logs a secret-bearing backend panic", func(t *testing.T) {
		const panicSecret = "panic-secret-A1b2C3d4-request-body"
		collector := &auditCollector{}
		admin := &adminV1Stub{connectPanics: 1, connectPanicValue: panicSecret}
		handler, route := prepare(t, admin, collector, 110)
		gatewayServer := handler.(*server)
		var errorLog bytes.Buffer
		httpServer := httptest.NewUnstartedServer(handler)
		httpServer.Config.ErrorLog = log.New(&errorLog, "", 0)
		httpServer.Start()
		defer httpServer.Close()

		const key = "real-http-panic-848"
		body := `{"state_revision":111,"pin":"` + pin + `"}`
		do := func(requestBody string) (*http.Response, error) {
			httpRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+route, strings.NewReader(requestBody))
			if err != nil {
				t.Fatal(err)
			}
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Idempotency-Key", key)
			return httpServer.Client().Do(httpRequest)
		}

		leader, leaderErr := do(body)
		var responseMaterial bytes.Buffer
		if leader != nil {
			content, _ := io.ReadAll(leader.Body)
			_ = leader.Body.Close()
			responseMaterial.Write(content)
		}
		if leaderErr == nil {
			t.Fatal("secret-bearing backend panic unexpectedly returned a normal HTTP response")
		}
		retry, err := do(body)
		if err != nil {
			t.Fatal(err)
		}
		retryBody, _ := io.ReadAll(retry.Body)
		_ = retry.Body.Close()
		responseMaterial.Write(retryBody)
		if retry.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("panic retry status=%d body=%s", retry.StatusCode, retryBody)
		}
		conflict, err := do(`{"state_revision":111,"pin":"` + changedPIN + `"}`)
		if err != nil {
			t.Fatal(err)
		}
		conflictBody, _ := io.ReadAll(conflict.Body)
		_ = conflict.Body.Close()
		responseMaterial.Write(conflictBody)
		if conflict.StatusCode != http.StatusConflict {
			t.Fatalf("panic changed-binding status=%d body=%s", conflict.StatusCode, conflictBody)
		}

		admin.mu.Lock()
		calls := len(admin.connectCalls)
		admin.mu.Unlock()
		if calls != 1 {
			t.Fatalf("real HTTP panic Connect calls=%d, want one", calls)
		}
		gatewayServer.connectMu.Lock()
		tombstone, retained := gatewayServer.connectReplays[key]
		gatewayServer.connectMu.Unlock()
		if !retained || tombstone.failure != eebusruntime.AdminErrorCodeV1AdminBoundaryUnavailable || tombstone.expiresAt.IsZero() {
			t.Fatalf("panic tombstone=%#v retained=%v", tombstone, retained)
		}
		if bytes.Contains(tombstone.binding[:], []byte(panicSecret)) {
			t.Fatal("panic tombstone retained the panic secret")
		}

		collector.mu.Lock()
		events := append([]AuditEvent(nil), collector.events...)
		collector.mu.Unlock()
		if len(events) != 3 {
			t.Fatalf("real HTTP panic audits=%d, want leader/retry/conflict: %#v", len(events), events)
		}
		auditJSON, err := json.Marshal(events)
		if err != nil {
			t.Fatal(err)
		}
		for _, surface := range []struct {
			name  string
			value string
		}{
			{"server error log", errorLog.String()},
			{"HTTP responses", responseMaterial.String()},
			{"audit", string(auditJSON)},
			{"replay result", tombstone.result.ActionID + tombstone.result.Outcome},
		} {
			for _, forbidden := range []string{panicSecret, pin, body, "request-body"} {
				if strings.Contains(strings.ToLower(surface.value), strings.ToLower(forbidden)) {
					t.Fatalf("%s leaked %q: %s", surface.name, forbidden, surface.value)
				}
			}
		}
	})
}

func (stub *adminV1Stub) Confirm(_ context.Context, request eebusruntime.ConfirmRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.confirmCalls = append(stub.confirmCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "confirmed"}, nil
}

func (stub *adminV1Stub) Cancel(_ context.Context, request eebusruntime.CancelRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.cancelCalls = append(stub.cancelCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "cancelled"}, nil
}

func (stub *adminV1Stub) RetryTrusted(_ context.Context, request eebusruntime.RetryTrustedRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.retryCalls = append(stub.retryCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "retry_started"}, nil
}

func (stub *adminV1Stub) Untrust(_ context.Context, request eebusruntime.UntrustRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.untrustCalls = append(stub.untrustCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "untrusted"}, nil
}

func TestIssue817UnavailableBoundaryStaysMountedAndSanitized(t *testing.T) {
	handler := NewUnavailableHandler()
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.Header.Set("Authorization", "Bearer must-not-be-reflected")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertIssue817ErrorEnvelope(t, response.Body.String(), "admin_boundary_unavailable")
	if strings.Contains(response.Body.String(), "must-not-be-reflected") {
		t.Fatalf("unavailable boundary reflected request material: %s", response.Body.String())
	}
}

func TestIssue846StatusExposesCanonicalReadinessWhenAvailableOrUnavailable(t *testing.T) {
	degraded := ReadinessV1{
		ProcessReadiness:    ProcessReadinessReady,
		EEBusReadiness:      EEBusReadinessDegraded,
		EEBusDegradedReason: EEBusDegradedReasonAdminBoundaryUnavailable,
	}

	unavailable := NewUnavailableHandlerWithReadiness(func() ReadinessV1 { return degraded })
	unavailableResponse := httptest.NewRecorder()
	unavailable.ServeHTTP(unavailableResponse, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil))
	assertIssue846Readiness(t, unavailableResponse, http.StatusServiceUnavailable, degraded)

	ready := ReadinessV1{ProcessReadiness: ProcessReadinessReady, EEBusReadiness: EEBusReadinessReady}
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 31
	available, err := NewServer(Config{
		Admin:     &adminV1Stub{snapshot: snapshot},
		Readiness: func() ReadinessV1 { return ready },
	})
	if err != nil {
		t.Fatal(err)
	}
	availableResponse := httptest.NewRecorder()
	available.ServeHTTP(availableResponse, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil))
	assertIssue846Readiness(t, availableResponse, http.StatusOK, ready)
}

func assertIssue846Readiness(t *testing.T, response *httptest.ResponseRecorder, wantStatus int, want ReadinessV1) {
	t.Helper()
	var envelope struct {
		Data struct {
			Readiness ReadinessV1 `json:"readiness"`
		} `json:"data"`
	}
	if response.Code != wantStatus || json.Unmarshal(response.Body.Bytes(), &envelope) != nil {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if envelope.Data.Readiness != want {
		t.Fatalf("readiness=%#v; want %#v; body=%s", envelope.Data.Readiness, want, response.Body.String())
	}
}

func TestIssue817CredentialFreeReadsUseOneStateRevisionEnvelope(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 21
	snapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: ski, SHIPID: "ship-1", TrustState: "durably_trusted"}}
	snapshot.Connected = []eebusruntime.ConnectedPartnerV1{{SKI: ski, SHIPID: "ship-1", ConnectionState: "connected"}}
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Identifier: "ship-1", Endpoint: "192.0.2.10:4712", ObservationRevision: 3}}
	snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: ski, State: "tls_bound", ExpiresAt: snapshot.CapturedAt.Add(time.Minute)}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{
		eebusruntime.AdminViewV1Trusted: snapshot, eebusruntime.AdminViewV1Connected: snapshot,
		eebusruntime.AdminViewV1Discovered: snapshot, eebusruntime.AdminViewV1Candidate: snapshot,
	}}
	handler := newIssue817Server(t, admin, nil, nil)

	paths := []string{"/admin/eebus/v1/status"}
	for _, view := range []string{"trusted", "connected", "discovered", "candidate"} {
		paths = append(paths, "/admin/eebus/v1/partners?view="+view)
	}
	for index, target := range paths {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		if index%2 == 1 {
			issue817AddIrrelevantAuthMaterial(request)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", target, response.Code, response.Body.String())
		}
		assertIssue817OperatorEnvelope(t, response, 21)
		assertIssue817NoAuthResponseHeaders(t, response)
		if strings.Contains(response.Body.String(), "projection_revision") {
			t.Fatalf("GET %s returned a split HA projection envelope: %s", target, response.Body.String())
		}
	}
}

func TestIssue817CapabilityCapacityFailsClosedWithoutPartialRows(t *testing.T) {
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 22
	for index := 0; index < 129; index++ {
		snapshot.Discovered = append(snapshot.Discovered, eebusruntime.DiscoveredPartnerV1{
			SKI:                 strings.Repeat(string(rune('a'+index%6)), 40),
			Endpoint:            "192.0.2.10:4712",
			ObservationRevision: uint64(index + 1),
			Identifier:          string(rune('A' + index)),
		})
	}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	handler := newIssue817Server(t, admin, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("capacity status=%d body=%s", response.Code, response.Body.String())
	}
	assertIssue817ErrorEnvelope(t, response.Body.String(), "admin_boundary_unavailable")
	if strings.Contains(response.Body.String(), "partners") {
		t.Fatalf("capacity failure emitted partial rows: %s", response.Body.String())
	}
}

func TestIssue817SelectionAndConnectShareProcessLocalScopeAcrossRequests(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 11
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 4}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	handler := newIssue817Server(t, admin, nil, nil)

	list := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil)
	listResponse := httptest.NewRecorder()
	handler.ServeHTTP(listResponse, list)
	observationID := issue817FirstOpaqueID(t, listResponse, "observation_id")

	selectRequest := issue817Mutation(http.MethodPost, "/admin/eebus/v1/observations/"+observationID+":select", "select-817", `{"state_revision":11,"expected_ski":"`+ski+`"}`)
	issue817AddIrrelevantAuthMaterial(selectRequest)
	selectResponse := httptest.NewRecorder()
	handler.ServeHTTP(selectResponse, selectRequest)
	if selectResponse.Code != http.StatusOK {
		t.Fatalf("select status=%d body=%s", selectResponse.Code, selectResponse.Body.String())
	}
	selectionID := issue817DataString(t, selectResponse, "selection_id")

	connectRequest := issue817Mutation(http.MethodPost, "/admin/eebus/v1/selections/"+selectionID+":connect", "connect-817", `{"state_revision":12}`)
	connectRequest.Header.Set("Authorization", "Basic another-client-value")
	connectResponse := httptest.NewRecorder()
	handler.ServeHTTP(connectResponse, connectRequest)
	if connectResponse.Code != http.StatusOK {
		t.Fatalf("connect status=%d body=%s", connectResponse.Code, connectResponse.Body.String())
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.selectCalls) != 1 || len(admin.connectCalls) != 1 || admin.selectCalls[0].ExpectedSKI != ski {
		t.Fatalf("select/connect calls=%#v/%#v", admin.selectCalls, admin.connectCalls)
	}
}

func TestIssue848ConnectPINIsSecretSafeAndReplayBound(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	const pin = "A1b2C3d4"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 40
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 4}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	var audits []AuditEvent
	handler, err := NewServer(Config{Admin: admin, Audit: func(event AuditEvent) { audits = append(audits, event) }})
	if err != nil {
		t.Fatal(err)
	}

	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil))
	observationID := issue817FirstOpaqueID(t, listed, "observation_id")
	selected := httptest.NewRecorder()
	handler.ServeHTTP(selected, issue817Mutation(http.MethodPost, "/admin/eebus/v1/observations/"+observationID+":select", "select-848", `{"state_revision":40,"expected_ski":"`+ski+`"}`))
	selectionID := issue817DataString(t, selected, "selection_id")

	path := "/admin/eebus/v1/selections/" + selectionID + ":connect"
	request := issue817Mutation(http.MethodPost, path, "connect-848", `{"state_revision":41,"pin":"`+pin+`"}`)
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request)
	if first.Code != http.StatusOK || strings.Contains(first.Body.String(), pin) {
		t.Fatalf("first connect status/body=%d/%s", first.Code, first.Body.String())
	}
	var firstEnvelope struct {
		Data connectResultData `json:"data"`
	}
	if json.Unmarshal(first.Body.Bytes(), &firstEnvelope) != nil || firstEnvelope.Data.ActionID != "action-1" || firstEnvelope.Data.Replayed {
		t.Fatalf("first ConnectResult=%s", first.Body.String())
	}

	replayed := httptest.NewRecorder()
	handler.ServeHTTP(replayed, issue817Mutation(http.MethodPost, path, "connect-848", `{"state_revision":41,"pin":"`+pin+`"}`))
	var replayEnvelope struct {
		Data connectResultData `json:"data"`
	}
	if replayed.Code != http.StatusOK || json.Unmarshal(replayed.Body.Bytes(), &replayEnvelope) != nil || replayEnvelope.Data.ActionID != "action-1" || !replayEnvelope.Data.Replayed {
		t.Fatalf("replayed ConnectResult=%d/%s", replayed.Code, replayed.Body.String())
	}
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, issue817Mutation(http.MethodPost, path, "connect-848", `{"state_revision":41,"pin":"a1b2c3d4"}`))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("changed PIN status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	admin.mu.Lock()
	calls := len(admin.connectCalls)
	admin.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Connect calls=%d, want one exact-bound launch", calls)
	}
	if got := first.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Fatalf("Cache-Control=%q", got)
	}
	for _, event := range audits {
		encoded, _ := json.Marshal(event)
		if strings.Contains(string(encoded), pin) {
			t.Fatalf("audit leaked PIN: %s", encoded)
		}
	}
}

func TestIssue848StatusPassesThroughIdentityFreeActiveAction(t *testing.T) {
	outcome := eebusruntime.AdminOutcomeV1("pin_required")
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 42
	snapshot.ActiveAction = &eebusruntime.ActiveActionV1{ActionID: "action-42", Kind: "connect", State: "terminal", Outcome: &outcome, Retryable: true, Expiry: snapshot.CapturedAt.Add(time.Minute)}
	handler := newIssue817Server(t, &adminV1Stub{snapshot: snapshot}, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "remote_ski") || strings.Contains(response.Body.String(), "expires_at") || !strings.Contains(response.Body.String(), `"action_id":"action-42"`) || !strings.Contains(response.Body.String(), `"outcome":"pin_required"`) || !strings.Contains(response.Body.String(), `"expiry"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestIssue848ConcurrentIdenticalConnectReservesOneBackendAction(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 50
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 4}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}, connectStarted: make(chan struct{}), connectRelease: make(chan struct{})}
	handler, err := NewServer(Config{Admin: admin})
	if err != nil {
		t.Fatal(err)
	}
	server := handler.(*server)
	hooks := make(chan struct{}, 2)
	releaseReservation := make(chan struct{})
	server.connectReservationHook = func() {
		hooks <- struct{}{}
		<-releaseReservation
	}

	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil))
	observationID := issue817FirstOpaqueID(t, listed, "observation_id")
	selected := httptest.NewRecorder()
	handler.ServeHTTP(selected, issue817Mutation(http.MethodPost, "/admin/eebus/v1/observations/"+observationID+":select", "select-848-race", `{"state_revision":50,"expected_ski":"`+ski+`"}`))
	selectionID := issue817DataString(t, selected, "selection_id")
	path := "/admin/eebus/v1/selections/" + selectionID + ":connect"

	responses := make(chan *httptest.ResponseRecorder, 2)
	launch := func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, issue817Mutation(http.MethodPost, path, "connect-848-race", `{"state_revision":51,"pin":"A1b2C3d4"}`))
		responses <- response
	}
	go launch()
	select {
	case <-hooks:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach reservation boundary")
	}
	go launch()
	time.Sleep(100 * time.Millisecond)
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, issue817Mutation(http.MethodPost, path, "connect-848-race", `{"state_revision":51,"pin":"a1b2c3d4"}`))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("changed in-flight PIN status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	close(releaseReservation)
	close(admin.connectRelease)
	first, second := <-responses, <-responses
	for _, response := range []*httptest.ResponseRecorder{first, second} {
		if response.Code != http.StatusOK {
			t.Fatalf("concurrent connect status=%d body=%s", response.Code, response.Body.String())
		}
	}
	admin.mu.Lock()
	calls := len(admin.connectCalls)
	admin.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Connect calls=%d, want one", calls)
	}
}

func TestIssue817ClosedMutationMatrixNeedsOnlyRevisionIdempotencyAndTypedArguments(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"

	t.Run("open and close", func(t *testing.T) {
		admin := &adminV1Stub{snapshot: testAdminSnapshot()}
		handler := newIssue817Server(t, admin, nil, nil)
		for _, mutation := range []struct{ path, key, body string }{
			{"/admin/eebus/v1/pairing-window:open", "open-817", `{"duration_seconds":60,"state_revision":7}`},
			{"/admin/eebus/v1/pairing-window:close", "close-817", `{"state_revision":7}`},
		} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue817Mutation(http.MethodPost, mutation.path, mutation.key, mutation.body))
			if response.Code != http.StatusOK {
				t.Fatalf("POST %s status=%d body=%s", mutation.path, response.Code, response.Body.String())
			}
			assertIssue817NoAuthResponseHeaders(t, response)
		}
		if len(admin.openCalls) != 1 || len(admin.closeCalls) != 1 {
			t.Fatalf("open/close calls=%d/%d", len(admin.openCalls), len(admin.closeCalls))
		}
	})

	for _, test := range []struct {
		name, route, key, body string
		confirm                bool
	}{
		{"confirm", "/admin/eebus/v1/candidate:confirm", "confirm-817", `{"state_revision":31,"expected_ski":"` + ski + `"}`, true},
		{"cancel", "/admin/eebus/v1/candidate:cancel", "cancel-817", `{"state_revision":31}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testAdminSnapshot()
			snapshot.StateRevision = 31
			snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: ski, State: "tls_bound", ExpiresAt: snapshot.CapturedAt.Add(time.Minute)}}
			admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Candidate: snapshot}}
			handler := newIssue817Server(t, admin, nil, nil)
			candidate := httptest.NewRecorder()
			handler.ServeHTTP(candidate, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=candidate", nil))
			if candidate.Code != http.StatusOK || !strings.Contains(candidate.Body.String(), ski) {
				t.Fatalf("candidate status=%d body=%s", candidate.Code, candidate.Body.String())
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue817Mutation(http.MethodPost, test.route, test.key, test.body))
			if response.Code != http.StatusOK {
				t.Fatalf("%s status=%d body=%s", test.name, response.Code, response.Body.String())
			}
			if test.confirm && (len(admin.confirmCalls) != 1 || admin.confirmCalls[0].ExpectedSKI != ski) {
				t.Fatalf("confirm calls=%#v", admin.confirmCalls)
			}
			if !test.confirm && len(admin.cancelCalls) != 1 {
				t.Fatalf("cancel calls=%#v", admin.cancelCalls)
			}
		})
	}

	for _, test := range []struct{ name, method, suffix, key string }{
		{"retry", http.MethodPost, ":retry", "retry-817"},
		{"untrust", http.MethodDelete, "/trust", "untrust-817"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testAdminSnapshot()
			snapshot.StateRevision = 41
			snapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: ski, TrustState: "durably_trusted"}}
			admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Trusted: snapshot}}
			handler := newIssue817Server(t, admin, nil, nil)
			trusted := httptest.NewRecorder()
			handler.ServeHTTP(trusted, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=trusted", nil))
			partnerID := issue817FirstOpaqueID(t, trusted, "partner_id")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue817Mutation(test.method, "/admin/eebus/v1/partners/"+partnerID+test.suffix, test.key, `{"state_revision":41}`))
			if response.Code != http.StatusOK {
				t.Fatalf("%s status=%d body=%s", test.name, response.Code, response.Body.String())
			}
			if test.name == "retry" && len(admin.retryCalls) != 1 {
				t.Fatalf("retry calls=%#v", admin.retryCalls)
			}
			if test.name == "untrust" && len(admin.untrustCalls) != 1 {
				t.Fatalf("untrust calls=%#v", admin.untrustCalls)
			}
		})
	}
}

func TestIssue817SuccessfulLostResponseReplaysLogicalResultAndOpaqueHandle(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 51
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.51:4712", ObservationRevision: 9}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	handler := newIssue817Server(t, admin, nil, nil)
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil))
	observationID := issue817FirstOpaqueID(t, list, "observation_id")
	route := "/admin/eebus/v1/observations/" + observationID + ":select"
	body := `{"state_revision":51,"expected_ski":"` + ski + `"}`

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, issue817Mutation(http.MethodPost, route, "lost-response-817", body))
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, issue817Mutation(http.MethodPost, route, "lost-response-817", body))
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, issue817Mutation(http.MethodPost, route, "lost-response-817", `{"state_revision":51,"expected_ski":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`))

	firstResult := issue817MutationResult(t, first)
	secondResult := issue817MutationResult(t, second)
	if first.Code != http.StatusOK || second.Code != http.StatusOK || conflict.Code != http.StatusConflict {
		t.Fatalf("replay statuses/bodies=%d/%d/%d\n%s\n%s\n%s", first.Code, second.Code, conflict.Code, first.Body.String(), second.Body.String(), conflict.Body.String())
	}
	if firstResult.Replayed || !secondResult.Replayed || firstResult.StateRevision != secondResult.StateRevision || firstResult.Outcome != secondResult.Outcome || firstResult.SelectionID == "" || firstResult.SelectionID != secondResult.SelectionID {
		t.Fatalf("lost-response replay changed logical result or handle: first=%#v second=%#v", firstResult, secondResult)
	}
	assertIssue817ErrorEnvelope(t, conflict.Body.String(), "idempotency_conflict")
	if len(admin.selectCalls) != 1 {
		t.Fatalf("lost-response retry invoked runtime %d times", len(admin.selectCalls))
	}
}

func TestIssue817UnseenStaleRevisionDoesNotReserveHTTPReplayKey(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot(), openRevision: 8}
	handler := newIssue817Server(t, admin, nil, nil)
	const route = "/admin/eebus/v1/pairing-window:open"
	const key = "stale-then-current-817"

	stale := httptest.NewRecorder()
	handler.ServeHTTP(stale, issue817Mutation(http.MethodPost, route, key, `{"duration_seconds":60,"state_revision":7}`))
	assertIssue817ErrorEnvelope(t, stale.Body.String(), "state_conflict")
	current := httptest.NewRecorder()
	handler.ServeHTTP(current, issue817Mutation(http.MethodPost, route, key, `{"duration_seconds":60,"state_revision":8}`))
	retry := httptest.NewRecorder()
	handler.ServeHTTP(retry, issue817Mutation(http.MethodPost, route, key, `{"duration_seconds":60,"state_revision":8}`))

	currentResult := issue817MutationResult(t, current)
	retryResult := issue817MutationResult(t, retry)
	if stale.Code != http.StatusConflict || current.Code != http.StatusOK || retry.Code != http.StatusOK {
		t.Fatalf("stale/current/retry statuses=%d/%d/%d\n%s\n%s\n%s", stale.Code, current.Code, retry.Code, stale.Body.String(), current.Body.String(), retry.Body.String())
	}
	if currentResult.Replayed || !retryResult.Replayed || currentResult.StateRevision != 9 || retryResult.StateRevision != currentResult.StateRevision || retryResult.Outcome != currentResult.Outcome {
		t.Fatalf("current terminal result was not replayed exactly: current=%#v retry=%#v", currentResult, retryResult)
	}
	if len(admin.openCalls) != 2 {
		t.Fatalf("stale/current/retry invoked runtime %d times, want 2", len(admin.openCalls))
	}
}

type issue817MutationResultEnvelope struct {
	StateRevision uint64 `json:"state_revision"`
	Data          struct {
		Outcome     string `json:"outcome"`
		Replayed    bool   `json:"replayed"`
		SelectionID string `json:"selection_id"`
	} `json:"data"`
}

func issue817MutationResult(t *testing.T, response *httptest.ResponseRecorder) struct {
	StateRevision uint64
	Outcome       string
	Replayed      bool
	SelectionID   string
} {
	t.Helper()
	var envelope issue817MutationResultEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode mutation result: %v body=%s", err, response.Body.String())
	}
	return struct {
		StateRevision uint64
		Outcome       string
		Replayed      bool
		SelectionID   string
	}{envelope.StateRevision, envelope.Data.Outcome, envelope.Data.Replayed, envelope.Data.SelectionID}
}

func TestIssue817AuditUsesOneOperatorClassificationAndExcludesRequestSecrets(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	var events []AuditEvent
	handler := newIssue817Server(t, admin, nil, func(event AuditEvent) { events = append(events, event) })
	request := issue817Mutation(http.MethodPost, "/admin/eebus/v1/pairing-window:open", "audit-key-817", `{"duration_seconds":60,"state_revision":7}`)
	issue817AddIrrelevantAuthMaterial(request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(events) != 1 {
		t.Fatalf("audit mutation status/events=%d/%d body=%s", response.Code, len(events), response.Body.String())
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	audit := string(encoded)
	for _, forbidden := range []string{"portal_owner", "ha_integration", "audit-key-817", "irrelevant-secret", "csrf", "cookie"} {
		if strings.Contains(strings.ToLower(audit), strings.ToLower(forbidden)) {
			t.Fatalf("audit retains split identity or request secret %q: %s", forbidden, audit)
		}
	}
}

func newIssue817Server(t *testing.T, admin eebusruntime.AdminV1, raw RawSnapshotProvider, audit func(AuditEvent)) http.Handler {
	t.Helper()
	handler, err := NewServer(Config{Admin: admin, Raw: raw, Audit: audit})
	if err != nil {
		t.Fatalf("credential-free NewServer: %v", err)
	}
	return handler
}

func issue817Mutation(method, path, idempotencyKey, body string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	return request
}

func issue817AddIrrelevantAuthMaterial(request *http.Request) {
	request.Header.Set("Authorization", "Bearer irrelevant-secret")
	request.Header.Set("Origin", "https://irrelevant.invalid")
	request.Header.Set("Referer", "https://irrelevant.invalid/portal")
	request.Header.Set("X-CSRF-Token", "irrelevant-csrf")
	request.AddCookie(&http.Cookie{Name: "irrelevant", Value: "irrelevant-cookie"})
}

func assertIssue817NoAuthResponseHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Set-Cookie") != "" || response.Header().Get("X-CSRF-Token") != "" {
		t.Fatalf("eeBUS operator response emitted auth/session headers: %v", response.Header())
	}
}

func assertIssue817OperatorEnvelope(t *testing.T, response *httptest.ResponseRecorder, revision uint64) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v body=%s", err, response.Body.String())
	}
	if envelope["contract"] != ContractV1 || envelope["state_revision"] != float64(revision) || envelope["request_id"] == "" || envelope["error"] != nil {
		t.Fatalf("operator envelope=%#v", envelope)
	}
}

func issue817FirstOpaqueID(t *testing.T, response *httptest.ResponseRecorder, field string) string {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data struct {
			Partners []map[string]any `json:"partners"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || len(envelope.Data.Partners) != 1 {
		t.Fatalf("decode partners: %v body=%s", err, response.Body.String())
	}
	value, _ := envelope.Data.Partners[0][field].(string)
	if value == "" {
		t.Fatalf("partner row lacks %s: %s", field, response.Body.String())
	}
	return value
}

func issue817DataString(t *testing.T, response *httptest.ResponseRecorder, field string) string {
	t.Helper()
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	value, _ := envelope.Data[field].(string)
	if value == "" {
		t.Fatalf("response lacks data.%s: %s", field, response.Body.String())
	}
	return value
}

func testAdminSnapshot() eebusruntime.AdminSnapshotV1 {
	return eebusruntime.AdminSnapshotV1{
		StateRevision: 7, CapturedAt: time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC),
		Status: "ready", Window: "open", WindowDeadline: time.Date(2026, 8, 14, 8, 5, 0, 0, time.UTC),
		Register: "true", Listener: "ready", Discovery: "ready",
		TrustedCount: 1, ConnectedCount: 1, DiscoveredCount: 1, CandidateCount: 1,
	}
}

func assertIssue817ErrorEnvelope(t *testing.T, body, code string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%q", err, body)
	}
	errorObject, ok := envelope["error"].(map[string]any)
	if envelope["contract"] != ContractV1 || !ok || errorObject["code"] != code {
		t.Fatalf("error envelope=%#v, want %q", envelope, code)
	}
}
