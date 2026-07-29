package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type issue749TypedRouter struct {
	mu sync.Mutex

	calls []eebusraw.ToolV1

	readAuth  eebusraw.ReadAuthorizationV1
	writeAuth eebusraw.WriteAuthorizationV1

	featuresGetData  eebusraw.FeaturesGetDataV1
	featuresGetError *eebusraw.ErrorV1
	dataGetData      eebusraw.FeatureDataGetDataV1
	dataGetError     *eebusraw.ErrorV1
	mutationData     eebusraw.MutationV1
	mutationError    *eebusraw.ErrorV1
}

func (router *issue749TypedRouter) record(tool eebusraw.ToolV1) {
	router.calls = append(router.calls, tool)
}

func (router *issue749TypedRouter) FeaturesGet(
	_ context.Context,
	auth eebusraw.ReadAuthorizationV1,
	_ eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.record(eebusraw.ToolV1FeaturesGet)
	router.readAuth = auth
	return router.featuresGetData, router.featuresGetError
}

func (router *issue749TypedRouter) FeaturesDataGet(
	_ context.Context,
	auth eebusraw.ReadAuthorizationV1,
	_ eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.record(eebusraw.ToolV1FeaturesDataGet)
	router.readAuth = auth
	return router.dataGetData, router.dataGetError
}

func (router *issue749TypedRouter) FeaturesDataSet(
	_ context.Context,
	auth eebusraw.WriteAuthorizationV1,
	_ eebusraw.FeatureDataSetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.record(eebusraw.ToolV1FeaturesDataSet)
	router.writeAuth = auth
	return issue749MutationOutcome(router.mutationData), router.mutationError
}

func (router *issue749TypedRouter) MutationsGet(
	_ context.Context,
	auth eebusraw.ReadAuthorizationV1,
	_ eebusraw.MutationGetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.record(eebusraw.ToolV1MutationsGet)
	router.readAuth = auth
	return issue749MutationOutcome(router.mutationData), router.mutationError
}

func (router *issue749TypedRouter) MutationsRollback(
	_ context.Context,
	auth eebusraw.WriteAuthorizationV1,
	_ eebusraw.MutationRollbackRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.record(eebusraw.ToolV1MutationsRollback)
	router.writeAuth = auth
	return issue749MutationOutcome(router.mutationData), router.mutationError
}

func issue749MutationOutcome(
	mutation eebusraw.MutationV1,
) eebusruntime.RawMutationOutcomeV1 {
	outcome := eebusruntime.RawMutationOutcomeV1{Mutation: mutation}
	if !reflect.ValueOf(mutation).IsZero() {
		runtime := mutation.Runtime
		outcome.Runtime = &runtime
	}
	return outcome
}

func (router *issue749TypedRouter) callCount() int {
	router.mu.Lock()
	defer router.mu.Unlock()
	return len(router.calls)
}

func TestIssue749CommandRouterRegistrationRejectsNilAndDuplicate(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)

	if err := server.RegisterEEBusV1CommandRouter(nil); err == nil {
		t.Fatal("nil command router registration succeeded")
	}
	var typedNil *issue749TypedRouter
	if err := server.RegisterEEBusV1CommandRouter(typedNil); err == nil {
		t.Fatal("typed-nil command router registration succeeded")
	}

	router := &issue749TypedRouter{}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatalf("register command router: %v", err)
	}
	if err := server.RegisterEEBusV1CommandRouter(&issue749TypedRouter{}); err == nil {
		t.Fatal("duplicate command router registration succeeded")
	}
}

func TestIssue749ProviderAndTypedNilRouterFailClosed(t *testing.T) {
	server, err := NewServer(
		&testRegistry{entries: make(map[byte]registry.DeviceEntry)},
		&testInvoker{},
	)
	if err != nil {
		t.Fatal(err)
	}
	var typedNilProvider *msp06Provider
	if err := server.registerEEBusV1Provider(typedNilProvider, eebusV1RegistrationOptions{}); err == nil {
		t.Fatal("typed-nil eeBUS provider registration succeeded")
	}

	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ = msp06TestServer(t, provider)
	var typedNilRouter *issue749TypedRouter
	server.eebusV1CommandRouter = typedNilRouter
	want := append([]string(nil), msp06ToolNames...)
	sort.Strings(want)
	if got := issue749ToolNames(issue749EEBusTools(t, issue743OperatorHandler(t, server))); !reflect.DeepEqual(got, want) {
		t.Fatalf("typed-nil router exposed command tools: got %v, want frozen public tools %v", got, want)
	}
}

func TestIssue749TypedSuccessErrorAndPartialResultEnvelopes(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	binding := eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3}
	cases := issue749CommandCases(t)
	featuresRequest := issue749DecodeArguments[eebusraw.FeaturesGetRequestV1](t, cases[0].arguments)
	router := &issue749TypedRouter{
		featuresGetData: issue749FeaturesGetData(t, featuresRequest, binding),
	}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	operator := issue743OperatorHandler(t, server)

	success := msp06Call(t, operator, cases[0].tool, cases[0].arguments)
	if success.isError || success.envelope["error"] != nil || success.envelope["data"] == nil {
		t.Fatalf("typed success envelope = %#v", success.envelope)
	}
	assertIssue749CommandMeta(t, success.envelope, cases[0].tool, cases[0].scope, binding)
	if router.readAuth != (eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner", Scope: eebusraw.AuthScopeV1RawRead,
		Tool: eebusraw.ToolV1FeaturesGet, MaskTier: eebusraw.MaskTierRaw,
	}) {
		t.Fatalf("server-owned read authorization = %#v", router.readAuth)
	}

	router.mu.Lock()
	setRequest := issue749DecodeArguments[eebusraw.FeatureDataSetRequestV1](t, cases[2].arguments)
	router.mutationData = issue749PreparedMutation(t, setRequest, binding)
	router.mutationError = eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1Internal, "fixed admitted failure", false, eebusraw.SourceLayerV1Runtime,
	)
	router.mu.Unlock()
	typedError := msp06Call(t, operator, cases[2].tool, cases[2].arguments)
	if !typedError.isError || typedError.envelope["data"] != nil {
		t.Fatalf("typed error envelope = %#v", typedError.envelope)
	}
	if code := issue749EnvelopeErrorCode(t, typedError.envelope); code != string(eebusraw.ErrorCodeV1Internal) {
		t.Fatalf("typed error code = %q", code)
	}
	assertIssue749CommandMeta(t, typedError.envelope, cases[2].tool, cases[2].scope, binding)
	if router.writeAuth != (eebusraw.WriteAuthorizationV1{
		PrincipalClass: "owner", Scope: eebusraw.AuthScopeV1RawWrite,
		Tool: eebusraw.ToolV1FeaturesDataSet, MaskTier: eebusraw.MaskTierRaw,
	}) {
		t.Fatalf("server-owned write authorization = %#v", router.writeAuth)
	}

	partialArguments := cloneIssue749Arguments(t, cases[1].arguments)
	targets := partialArguments["targets"].([]any)
	secondTarget := cloneIssue749Arguments(t, targets[0].(map[string]any))
	secondTarget["feature_address"] = float64(3)
	partialArguments["targets"] = append(targets, secondTarget)
	partialRequest := issue749DecodeArguments[eebusraw.FeatureDataGetRequestV1](t, partialArguments)
	partialData := issue749PartialData(t, partialRequest, binding)
	router.mu.Lock()
	router.dataGetData = partialData
	router.dataGetError = eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1PartialResult, "fixed partial result", true, eebusraw.SourceLayerV1Runtime,
	)
	router.mu.Unlock()
	partial := msp06Call(t, operator, cases[1].tool, partialArguments)
	if !partial.isError || partial.envelope["data"] == nil ||
		issue749EnvelopeErrorCode(t, partial.envelope) != string(eebusraw.ErrorCodeV1PartialResult) {
		t.Fatalf("partial-result envelope = %#v", partial.envelope)
	}
	if got := router.callCount(); got != 3 {
		t.Fatalf("router calls = %d, want exactly 3", got)
	}
}

func TestIssue749ShapeAndBoundaryErrorPrecedence(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	router := &issue749TypedRouter{}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	test := issue749CommandCases(t)[0]
	malformed := cloneIssue749Arguments(t, test.arguments)
	malformed["unknown"] = true

	beforeProvider := provider.callCount()
	for _, endpoint := range []struct {
		name    string
		handler http.Handler
	}{
		{name: "public", handler: server.Handler()},
		{name: "operator", handler: issue743OperatorHandler(t, server)},
	} {
		t.Run(endpoint.name, func(t *testing.T) {
			result := msp06Call(t, endpoint.handler, test.tool, malformed)
			if code := issue749EnvelopeErrorCode(t, result.envelope); code != string(eebusraw.ErrorCodeV1InvalidArgument) {
				t.Fatalf("malformed request error = %q, want invalid_argument", code)
			}
		})
	}
	if router.callCount() != 0 || provider.callCount() != beforeProvider {
		t.Fatalf("malformed calls contacted router/provider: router=%d provider=%d->%d",
			router.callCount(), beforeProvider, provider.callCount())
	}

	public := msp06Call(t, server.Handler(), test.tool, test.arguments)
	if code := issue749EnvelopeErrorCode(t, public.envelope); code != string(eebusraw.ErrorCodeV1PermissionDenied) {
		t.Fatalf("valid public request error = %q, want permission_denied", code)
	}
	publicAgain := msp06Call(t, server.Handler(), test.tool, test.arguments)
	publicMeta := msp06Map(t, public.envelope["meta"], "public command meta")
	publicAgainMeta := msp06Map(t, publicAgain.envelope["meta"], "repeated public command meta")
	if publicMeta["runtime"] != nil {
		t.Fatalf("zero-contact public denial fabricated runtime identity: %#v", publicMeta["runtime"])
	}
	if publicMeta["data_hash"] != publicAgainMeta["data_hash"] {
		t.Fatalf("zero-contact public denial hash is unstable: %v != %v",
			publicMeta["data_hash"], publicAgainMeta["data_hash"])
	}
	if router.callCount() != 0 || provider.callCount() != beforeProvider {
		t.Fatalf("public denial contacted router/provider: router=%d provider=%d->%d",
			router.callCount(), beforeProvider, provider.callCount())
	}
}

func TestIssue749DuplicateJSONKeysFailBeforeDispatch(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	router := &issue749TypedRouter{}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	operator := issue743OperatorHandler(t, server)
	test := issue749CommandCases(t)[0]
	encodedArguments, err := json.Marshal(test.arguments)
	if err != nil {
		t.Fatal(err)
	}
	remoteSKI := strings.Repeat("a", 40)
	duplicateArguments := strings.Replace(
		string(encodedArguments),
		`"remote_ski":`,
		`"remote_ski":"`+remoteSKI+`","remote_ski":`,
		1,
	)

	for _, rawParams := range []string{
		`{"name":` + mustJSON(test.tool) + `,"arguments":` + duplicateArguments + `}`,
		`{"name":` + mustJSON(test.tool) + `,"name":` + mustJSON(test.tool) +
			`,"arguments":` + string(encodedArguments) + `}`,
	} {
		result := issue749RawCommandCall(t, operator, json.RawMessage(rawParams))
		if code := issue749EnvelopeErrorCode(t, result.envelope); code != string(eebusraw.ErrorCodeV1InvalidArgument) {
			t.Fatalf("duplicate-key error = %q, want invalid_argument", code)
		}
		assertIssue749PreRuntimeFailure(t, result.envelope, true)
	}
	if router.callCount() != 0 {
		t.Fatalf("duplicate-key requests dispatched %d router calls", router.callCount())
	}
}

func TestIssue749SecretRequestFailsBeforeDispatchWithoutEcho(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	router := &issue749TypedRouter{}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	test := issue749CommandCases(t)[0]
	arguments := cloneIssue749Arguments(t, test.arguments)
	target := arguments["target"].(map[string]any)
	target["ship_id"] = "Bearer fixture-secret"

	result := msp06Call(t, issue743OperatorHandler(t, server), test.tool, arguments)
	if code := issue749EnvelopeErrorCode(t, result.envelope); code != string(eebusraw.ErrorCodeV1InvalidArgument) {
		t.Fatalf("secret request error = %q, want protected invalid_argument", code)
	}
	if strings.Contains(result.raw, "fixture-secret") {
		t.Fatalf("secret request was echoed in response: %s", result.raw)
	}
	assertIssue749PreRuntimeFailure(t, result.envelope, true)
	if router.callCount() != 0 {
		t.Fatalf("secret-bearing request dispatched %d router calls", router.callCount())
	}
}

func TestIssue749RouterOutputsAreValidatedBeforePublication(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	binding := eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3}
	test := issue749CommandCases(t)[0]
	request := issue749DecodeArguments[eebusraw.FeaturesGetRequestV1](t, test.arguments)
	invalid := issue749FeaturesGetData(t, request, binding)
	invalid.DataHash = eebusraw.HashV1("sha256:" + strings.Repeat("0", 64))
	router := &issue749TypedRouter{featuresGetData: invalid}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}

	result := msp06Call(t, issue743OperatorHandler(t, server), test.tool, test.arguments)
	if code := issue749EnvelopeErrorCode(t, result.envelope); code != string(eebusraw.ErrorCodeV1Internal) {
		t.Fatalf("invalid router output error = %q, want internal", code)
	}
	if result.envelope["data"] != nil {
		t.Fatalf("invalid router output was published: %#v", result.envelope["data"])
	}
	assertIssue749PreRuntimeFailure(t, result.envelope, false)
	if router.callCount() != 1 {
		t.Fatalf("router calls = %d, want one rejected output", router.callCount())
	}
}

func TestIssue749MutationGetNotFoundWithoutRuntimeIsPreserved(t *testing.T) {
	commandCases := issue749CommandCases(t)
	test := commandCases[3]
	notFound := eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1NotFound,
		"raw mutation was not found",
		false,
		eebusraw.SourceLayerV1Runtime,
	)
	malformed := notFound.Clone()
	malformed.Message = " "
	incoherent := issue749PreparedMutation(
		t,
		issue749DecodeArguments[eebusraw.FeatureDataSetRequestV1](t, commandCases[2].arguments),
		eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3},
	)
	incoherent.MutationRef = strings.Repeat("Q", 43)
	if terminal := eebusraw.ValidateMutationV1(incoherent); terminal != nil {
		t.Fatalf("incoherent fixture is not independently valid: %+v", terminal)
	}

	cases := []struct {
		name     string
		data     eebusraw.MutationV1
		terminal *eebusraw.ErrorV1
		wantCode eebusraw.ErrorCodeV1
	}{
		{name: "unknown reference", terminal: notFound, wantCode: eebusraw.ErrorCodeV1NotFound},
		{name: "malformed terminal", terminal: &malformed, wantCode: eebusraw.ErrorCodeV1Internal},
		{name: "incoherent data", data: incoherent, terminal: notFound, wantCode: eebusraw.ErrorCodeV1Internal},
	}
	for _, current := range cases {
		t.Run(current.name, func(t *testing.T) {
			provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
			server, _ := msp06TestServer(t, provider)
			router := &issue749TypedRouter{
				mutationData:  current.data,
				mutationError: current.terminal,
			}
			if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
				t.Fatal(err)
			}

			result := msp06Call(t, issue743OperatorHandler(t, server), test.tool, test.arguments)
			publicError := msp06Map(t, result.envelope["error"], "command error")
			if code := publicError["code"]; code != string(current.wantCode) {
				t.Fatalf("mutation get error = %q, want %q; envelope=%#v",
					code, current.wantCode, result.envelope)
			}
			if result.envelope["data"] != nil {
				t.Fatalf("mutation get error published data: %#v", result.envelope["data"])
			}
			assertIssue749PreRuntimeFailure(t, result.envelope, false)
			if current.wantCode == eebusraw.ErrorCodeV1NotFound &&
				(publicError["message"] != notFound.Message ||
					publicError["source_layer"] != string(eebusraw.SourceLayerV1Runtime)) {
				t.Fatalf("actionable not_found was not preserved: %#v", publicError)
			}
		})
	}
}

func TestIssue749ZeroDataRouterErrorValidatesOpaqueDetails(t *testing.T) {
	opaqueValue, err := eebusraw.NewTypedValueV1(map[string]any{"status": "opaque"})
	if err != nil {
		t.Fatal(err)
	}
	test := issue749CommandCases(t)[0]
	cases := []struct {
		name           string
		message        string
		classification string
		unknown        eebusraw.OpaqueObservationV1
		wantCode       eebusraw.ErrorCodeV1
	}{
		{
			name: "valid",
			unknown: eebusraw.OpaqueObservationV1{
				Path: "/remote/error", Source: "runtime", Value: opaqueValue,
			},
			wantCode: eebusraw.ErrorCodeV1InvalidArgument,
		},
		{
			name: "empty path",
			unknown: eebusraw.OpaqueObservationV1{
				Source: "runtime", Value: opaqueValue,
			},
			wantCode: eebusraw.ErrorCodeV1Internal,
		},
		{
			name: "blank source",
			unknown: eebusraw.OpaqueObservationV1{
				Path: "/remote/error", Source: "  ", Value: opaqueValue,
			},
			wantCode: eebusraw.ErrorCodeV1Internal,
		},
		{
			name:           "blank classification",
			classification: " ",
			unknown: eebusraw.OpaqueObservationV1{
				Path: "/remote/error", Source: "runtime", Value: opaqueValue,
			},
			wantCode: eebusraw.ErrorCodeV1Internal,
		},
		{
			name:    "invalid message utf8",
			message: string([]byte{0xff}),
			unknown: eebusraw.OpaqueObservationV1{
				Path: "/remote/error", Source: "runtime", Value: opaqueValue,
			},
			wantCode: eebusraw.ErrorCodeV1Internal,
		},
	}

	for _, current := range cases {
		t.Run(current.name, func(t *testing.T) {
			provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
			server, _ := msp06TestServer(t, provider)
			message := current.message
			if message == "" {
				message = "fixed router error"
			}
			terminal := eebusraw.NewErrorV1(
				eebusraw.ErrorCodeV1InvalidArgument,
				message,
				false,
				eebusraw.SourceLayerV1Runtime,
			)
			terminal.Details = &eebusraw.ErrorDetailsV1{
				Classification: current.classification,
				Unknown:        []eebusraw.OpaqueObservationV1{current.unknown},
			}
			router := &issue749TypedRouter{featuresGetError: terminal}
			if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
				t.Fatal(err)
			}

			result := msp06Call(
				t,
				issue743OperatorHandler(t, server),
				test.tool,
				test.arguments,
			)
			if code := issue749EnvelopeErrorCode(t, result.envelope); code != string(current.wantCode) {
				t.Fatalf("zero-data router error = %q, want %q; envelope=%#v",
					code, current.wantCode, result.envelope)
			}
			if result.envelope["data"] != nil {
				t.Fatalf("zero-data router error published data: %#v", result.envelope["data"])
			}
			assertIssue749PreRuntimeFailure(t, result.envelope, false)
		})
	}
}

func TestIssue749BatchRuntimeBindingRejectsMixedGenerations(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	test := issue749CommandCases(t)[1]
	arguments := cloneIssue749Arguments(t, test.arguments)
	targets := arguments["targets"].([]any)
	secondTarget := cloneIssue749Arguments(t, targets[0].(map[string]any))
	secondTarget["feature_address"] = float64(3)
	arguments["targets"] = append(targets, secondTarget)
	request := issue749DecodeArguments[eebusraw.FeatureDataGetRequestV1](t, arguments)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	router := &issue749TypedRouter{
		dataGetData: eebusraw.FeatureDataGetDataV1{
			Results: []eebusraw.ReadObservationV1{
				issue749Observation(t, request.Targets[0], eebusraw.RuntimeBindingV1{
					RuntimeEpoch: 7, ConnectionGeneration: 3,
				}, now),
				issue749Observation(t, request.Targets[1], eebusraw.RuntimeBindingV1{
					RuntimeEpoch: 7, ConnectionGeneration: 4,
				}, now),
			},
			Complete: true,
		},
	}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}

	result := msp06Call(t, issue743OperatorHandler(t, server), test.tool, arguments)
	if code := issue749EnvelopeErrorCode(t, result.envelope); code != string(eebusraw.ErrorCodeV1Internal) {
		t.Fatalf("mixed-runtime output error = %q, want internal", code)
	}
	if result.envelope["data"] != nil {
		t.Fatalf("mixed-runtime output was published: %#v", result.envelope["data"])
	}
	assertIssue749PreRuntimeFailure(t, result.envelope, false)
}

func TestIssue749CommandSchemasAreClosedBoundedAndSecretAware(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	if err := server.RegisterEEBusV1CommandRouter(&issue749TypedRouter{}); err != nil {
		t.Fatal(err)
	}
	tools := issue749EEBusTools(t, issue743OperatorHandler(t, server))
	byName := make(map[string]map[string]any, len(tools))
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		byName[name] = tool
	}

	for _, name := range issue749AdditiveToolNames {
		schema := msp06Map(t, byName[name]["inputSchema"], name+".inputSchema")
		if schema["additionalProperties"] != false ||
			schema["x-secret-classifier"] != "eebusraw.canonical.v1" {
			t.Fatalf("%s schema is not closed and secret-aware: %#v", name, schema)
		}
	}

	dataGetProperties := issue749SchemaProperties(t, byName[string(eebusraw.ToolV1FeaturesDataGet)])
	targets := msp06Map(t, dataGetProperties["targets"], "features.data.get.targets")
	if fmt.Sprint(targets["minItems"]) != "1" || fmt.Sprint(targets["maxItems"]) != "16" {
		t.Fatalf("features.data.get target bounds = %#v", targets)
	}
	readTarget := msp06Map(t, targets["items"], "features.data.get.target")
	issue749AssertTargetSchema(t, readTarget, eebusraw.OperationV1Read)

	dataSetProperties := issue749SchemaProperties(t, byName[string(eebusraw.ToolV1FeaturesDataSet)])
	writeTarget := msp06Map(t, dataSetProperties["target"], "features.data.set.target")
	issue749AssertTargetSchema(t, writeTarget, eebusraw.OperationV1Write)
	if msp06Map(t, dataSetProperties["read_token"], "features.data.set.read_token")["pattern"] != eebusV1TokenPattern {
		t.Fatal("features.data.set read_token schema does not enforce one canonical opaque reference")
	}
	idempotency := msp06Map(t, dataSetProperties["idempotency_key"], "features.data.set.idempotency_key")
	if fmt.Sprint(idempotency["minLength"]) != "16" || fmt.Sprint(idempotency["maxLength"]) != "128" {
		t.Fatalf("idempotency key bounds = %#v", idempotency)
	}
}

func TestIssue749TypedValueSchemaMatchesCanonicalDepthFour(t *testing.T) {
	depthFour := map[string]any{
		"one": []any{
			map[string]any{
				"two": []any{
					int64(4),
				},
			},
		},
	}
	depthFive := map[string]any{"zero": depthFour}
	if _, err := eebusraw.NewTypedValueV1(depthFour); err != nil {
		t.Fatalf("canonical depth-four value rejected: %v", err)
	}
	if _, err := eebusraw.NewTypedValueV1(depthFive); err == nil {
		t.Fatal("canonical validator accepted depth-five containers")
	}

	schema := eebusV1TypedValueSchema(1)
	for depth := 1; depth <= 4; depth++ {
		if !issue749TypedValueSchemaAllowsContainers(t, schema) {
			t.Fatalf("TypedValue schema rejects canonical container depth %d", depth)
		}
		schema = issue749TypedValueObjectChildSchema(t, schema)
	}
	if issue749TypedValueSchemaAllowsContainers(t, schema) {
		t.Fatal("TypedValue schema permits a fifth container depth")
	}
}

func TestIssue749OpaqueReferenceSchemaEnforcesCanonicalPadBits(t *testing.T) {
	canonical := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	nonCanonical := strings.Repeat("A", 42) + "B"
	if terminal := eebusraw.ValidateMutationGetRequestV1(
		eebusraw.MutationGetRequestV1{MutationRef: canonical},
	); terminal != nil {
		t.Fatalf("canonical reference rejected by eebusraw: %+v", terminal)
	}
	if terminal := eebusraw.ValidateMutationGetRequestV1(
		eebusraw.MutationGetRequestV1{MutationRef: nonCanonical},
	); terminal == nil {
		t.Fatal("non-canonical reference accepted by eebusraw")
	}

	pattern := regexp.MustCompile(eebusV1TokenPattern)
	if !pattern.MatchString(canonical) {
		t.Fatalf("schema pattern rejected canonical reference %q", canonical)
	}
	if pattern.MatchString(nonCanonical) {
		t.Fatalf("schema pattern accepted non-canonical pad bits %q", nonCanonical)
	}
	idPattern := regexp.MustCompile(eebusV1IDDigestPattern)
	if !idPattern.MatchString(canonical) ||
		!idPattern.MatchString("sha256:"+strings.Repeat("a", 64)) {
		t.Fatal("id_digest schema rejected a canonical opaque reference or SHA-256 digest")
	}
	if idPattern.MatchString(nonCanonical) {
		t.Fatalf("id_digest schema accepted non-canonical pad bits %q", nonCanonical)
	}

	for name, schema := range map[string]map[string]any{
		"read_token":        eebusV1FeaturesDataSetSchema(),
		"mutation_get":      eebusV1MutationsGetSchema(),
		"mutation_rollback": eebusV1MutationsRollbackSchema(),
	} {
		properties := msp06Map(t, schema["properties"], name+".properties")
		field := "mutation_ref"
		if name == "read_token" {
			field = "read_token"
		}
		if got := msp06Map(t, properties[field], name+"."+field)["pattern"]; got != eebusV1TokenPattern {
			t.Fatalf("%s pattern = %v, want %s", name, got, eebusV1TokenPattern)
		}
	}
}

func TestIssue749CommandEnvelopeJCSHashIsStable(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	router := &issue749TypedRouter{
		mutationData: eebusraw.MutationV1{
			MutationRef: "fixture", State: eebusraw.MutationStateV1Prepared,
			Runtime: eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3},
		},
	}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	operator := issue743OperatorHandler(t, server)
	test := issue749CommandCases(t)[2]
	first := cloneIssue749Arguments(t, test.arguments)
	first["value"] = map[string]any{"z": int64(2), "a": int64(1)}
	second := cloneIssue749Arguments(t, test.arguments)
	second["value"] = map[string]any{"a": int64(1), "z": int64(2)}

	firstResult := msp06Call(t, operator, test.tool, first)
	secondResult := msp06Call(t, operator, test.tool, second)
	firstMeta := msp06Map(t, firstResult.envelope["meta"], "first meta")
	secondMeta := msp06Map(t, secondResult.envelope["meta"], "second meta")
	if firstMeta["data_hash"] != secondMeta["data_hash"] {
		t.Fatalf("JCS data hashes differ: %v != %v", firstMeta["data_hash"], secondMeta["data_hash"])
	}
}

func TestIssue749CommandEnvelopeRejectsSecretBearingRouterOutput(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	router := &issue749TypedRouter{
		featuresGetError: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1Internal,
			"Bearer fixture-secret",
			false,
			eebusraw.SourceLayerV1Runtime,
		),
	}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	test := issue749CommandCases(t)[0]
	result := msp06Call(t, issue743OperatorHandler(t, server), test.tool, test.arguments)
	encoded, err := json.Marshal(result.envelope)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "fixture-secret") ||
		issue749EnvelopeErrorCode(t, result.envelope) != string(eebusraw.ErrorCodeV1SecretDetected) {
		t.Fatalf("secret-bearing router output was not failed closed: %s", encoded)
	}
	assertIssue749PreRuntimeFailure(t, result.envelope, true)
}

func assertIssue749CommandMeta(
	t *testing.T,
	envelope map[string]any,
	tool string,
	scope string,
	runtime eebusraw.RuntimeBindingV1,
) {
	t.Helper()
	meta := msp06Map(t, envelope["meta"], "command meta")
	if meta["contract"] != eebusV1RawFeatureContract ||
		meta["tool"] != tool ||
		meta["scope"] != scope ||
		meta["auth_scope"] != scope ||
		meta["mask_tier"] != string(eebusraw.MaskTierRaw) {
		t.Fatalf("command meta = %#v", meta)
	}
	gotRuntime := msp06Map(t, meta["runtime"], "command runtime")
	if fmt.Sprint(gotRuntime["runtime_epoch"]) != fmt.Sprint(runtime.RuntimeEpoch) ||
		fmt.Sprint(gotRuntime["connection_generation"]) != fmt.Sprint(runtime.ConnectionGeneration) {
		t.Fatalf("command runtime = %#v, want %#v", gotRuntime, runtime)
	}
}

func issue749EnvelopeErrorCode(t *testing.T, envelope map[string]any) string {
	t.Helper()
	publicError := msp06Map(t, envelope["error"], "command error")
	code, _ := publicError["code"].(string)
	return code
}

func issue749RawCommandCall(
	t *testing.T,
	handler http.Handler,
	params json.RawMessage,
) msp06CallResult {
	t.Helper()
	response := doRPC(t, handler, rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  params,
	})
	if response.Error != nil {
		t.Fatalf("raw command RPC error = %+v", response.Error)
	}
	result := msp06Map(t, response.Result, "raw command result")
	content := msp06Slice(t, result["content"], "raw command content")
	if len(content) != 1 {
		t.Fatalf("raw command content count = %d, want 1", len(content))
	}
	raw, _ := msp06Map(t, content[0], "raw command content[0]")["text"].(string)
	var envelope map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode raw command envelope: %v; text=%q", err, raw)
	}
	return msp06CallResult{envelope: envelope, raw: raw, isError: result["isError"] == true}
}

func assertIssue749PreRuntimeFailure(
	t *testing.T,
	envelope map[string]any,
	requestMustBeNil bool,
) {
	t.Helper()
	meta := msp06Map(t, envelope["meta"], "pre-runtime meta")
	if meta["runtime"] != nil {
		t.Fatalf("pre-runtime failure fabricated runtime: %#v", meta["runtime"])
	}
	if requestMustBeNil && envelope["request"] != nil {
		t.Fatalf("protected or undecodable request was retained: %#v", envelope["request"])
	}
}

func issue749SchemaProperties(t *testing.T, tool map[string]any) map[string]any {
	t.Helper()
	schema := msp06Map(t, tool["inputSchema"], "command inputSchema")
	return msp06Map(t, schema["properties"], "command inputSchema.properties")
}

func issue749AssertTargetSchema(
	t *testing.T,
	target map[string]any,
	operation eebusraw.OperationV1,
) {
	t.Helper()
	if target["additionalProperties"] != false ||
		target["x-secret-classifier"] != "eebusraw.canonical.v1" {
		t.Fatalf("target schema is not closed and secret-aware: %#v", target)
	}
	properties := msp06Map(t, target["properties"], "target.properties")
	remoteSKI := msp06Map(t, properties["remote_ski"], "target.remote_ski")
	if fmt.Sprint(remoteSKI["minLength"]) != "40" ||
		fmt.Sprint(remoteSKI["maxLength"]) != "40" ||
		remoteSKI["pattern"] != `^[0-9a-f]{40}$` {
		t.Fatalf("remote_ski schema = %#v", remoteSKI)
	}
	if msp06Map(t, properties["operation"], "target.operation")["const"] != string(operation) {
		t.Fatalf("target operation is not fixed to %s: %#v", operation, properties["operation"])
	}
}

func issue749TypedValueSchemaAllowsContainers(t *testing.T, schema map[string]any) bool {
	t.Helper()
	for _, raw := range msp06Slice(t, schema["oneOf"], "TypedValue.oneOf") {
		branch := msp06Map(t, raw, "TypedValue.oneOf branch")
		if branch["type"] == "array" || branch["type"] == "object" {
			return true
		}
	}
	return false
}

func issue749TypedValueObjectChildSchema(t *testing.T, schema map[string]any) map[string]any {
	t.Helper()
	for _, raw := range msp06Slice(t, schema["oneOf"], "TypedValue.oneOf") {
		branch := msp06Map(t, raw, "TypedValue.oneOf branch")
		if branch["type"] == "object" {
			return msp06Map(t, branch["additionalProperties"], "TypedValue object child")
		}
	}
	t.Fatal("TypedValue schema omitted object container branch")
	return nil
}

func cloneIssue749Arguments(t *testing.T, arguments map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func issue749DecodeArguments[T any](t *testing.T, arguments map[string]any) T {
	t.Helper()
	encoded, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	var request T
	if err := json.Unmarshal(encoded, &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func issue749FeaturesGetData(
	t *testing.T,
	request eebusraw.FeaturesGetRequestV1,
	binding eebusraw.RuntimeBindingV1,
) eebusraw.FeaturesGetDataV1 {
	t.Helper()
	data := eebusraw.FeaturesGetDataV1{
		Feature: request.Target, Description: "fixture feature",
		Runtime: binding, DataTimestamp: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		Source: eebusraw.ObservationSourceV1Live,
	}
	hash, err := data.ComputeDataHash()
	if err != nil {
		t.Fatal(err)
	}
	data.DataHash = hash
	return data
}

func issue749PartialData(
	t *testing.T,
	request eebusraw.FeatureDataGetRequestV1,
	binding eebusraw.RuntimeBindingV1,
) eebusraw.FeatureDataGetDataV1 {
	t.Helper()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	observation := issue749Observation(t, request.Targets[0], binding, now)
	return eebusraw.FeatureDataGetDataV1{
		Results: []eebusraw.ReadObservationV1{observation},
		Failures: []eebusraw.ReadFailureV1{{
			TargetIndex: 1,
			Target:      request.Targets[1],
			Error: *eebusraw.NewErrorV1(
				eebusraw.ErrorCodeV1Timeout,
				"fixture timeout",
				true,
				eebusraw.SourceLayerV1SpineRoundTrip,
			),
		}},
		Complete: false,
	}
}

func issue749Observation(
	t *testing.T,
	target eebusraw.FeatureTargetV1,
	binding eebusraw.RuntimeBindingV1,
	now time.Time,
) eebusraw.ReadObservationV1 {
	t.Helper()
	value, err := eebusraw.NewTypedValueV1(int64(18))
	if err != nil {
		t.Fatal(err)
	}
	observation := eebusraw.ReadObservationV1{
		Target: target, Runtime: binding,
		RawRequest: eebusraw.ProtocolMessageV1{
			Classifier: "READ", CorrelationKey: 1, Function: target.Function,
		},
		RawResponse: eebusraw.ProtocolMessageV1{
			Classifier: "REPLY", CorrelationKey: 1, Function: target.Function, Data: &value,
		},
		Value: value, RequestedAt: now, ReceivedAt: now.Add(time.Millisecond),
		DataTimestamp: now.Add(time.Millisecond), Source: eebusraw.ObservationSourceV1Live,
		ReadToken: eebusraw.ReadTokenV1{
			ReadToken: strings.Repeat("E", 43), ExpiresAt: now.Add(time.Minute),
			BindingHash: eebusraw.HashV1("sha256:" + strings.Repeat("1", 64)),
		},
	}
	hash, err := observation.ComputeDataHash()
	if err != nil {
		t.Fatal(err)
	}
	observation.DataHash = hash
	return observation
}

func issue749PreparedMutation(
	t *testing.T,
	request eebusraw.FeatureDataSetRequestV1,
	binding eebusraw.RuntimeBindingV1,
) eebusraw.MutationV1 {
	t.Helper()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	before, err := eebusraw.NewTypedValueV1(int64(18))
	if err != nil {
		t.Fatal(err)
	}
	transition := eebusraw.AuditTransitionV1{
		Sequence: 1, State: eebusraw.MutationStateV1Prepared, TransitionedAt: now,
	}
	transition.TransitionHash = issue749TransitionHash(t, transition)
	return eebusraw.MutationV1{
		MutationRef: strings.Repeat("M", 43),
		State:       eebusraw.MutationStateV1Prepared,
		Mode:        request.Mode,
		Target:      request.Target,
		Runtime:     binding,
		Before:      before,
		Requested:   request.Value,
		CreatedAt:   now,
		UpdatedAt:   now,
		Audit:       []eebusraw.AuditTransitionV1{transition},
	}
}

func issue749TransitionHash(
	t *testing.T,
	transition eebusraw.AuditTransitionV1,
) eebusraw.HashV1 {
	t.Helper()
	hash, err := eebusraw.CanonicalSHA256V1(struct {
		Sequence       uint64                   `json:"sequence"`
		State          eebusraw.MutationStateV1 `json:"state"`
		TransitionedAt time.Time                `json:"transitioned_at"`
		Classification string                   `json:"classification,omitempty"`
		PreviousHash   *eebusraw.HashV1         `json:"previous_hash"`
	}{
		Sequence: transition.Sequence, State: transition.State,
		TransitionedAt: transition.TransitionedAt,
		Classification: transition.Classification, PreviousHash: transition.PreviousHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
