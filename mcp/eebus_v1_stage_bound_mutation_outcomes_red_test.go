package mcp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type issue755StageBoundRouter struct {
	dataGetData     eebusraw.FeatureDataGetDataV1
	dataGetTerminal *eebusraw.ErrorV1
	outcome         eebusruntime.RawMutationOutcomeV1
	terminal        *eebusraw.ErrorV1
}

var _ EEBusV1CommandRouter = (*issue755StageBoundRouter)(nil)

func (*issue755StageBoundRouter) FeaturesGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeaturesGetDataV1{}, eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1Internal,
		"unexpected feature lookup",
		false,
		eebusraw.SourceLayerV1Runtime,
	)
}

func (router *issue755StageBoundRouter) FeaturesDataGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	return router.dataGetData.Clone(), issue755CloneTerminal(router.dataGetTerminal)
}

func (router *issue755StageBoundRouter) FeaturesDataSet(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.FeatureDataSetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	return router.outcome.Clone(), issue755CloneTerminal(router.terminal)
}

func (router *issue755StageBoundRouter) MutationsGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.MutationGetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	return router.outcome.Clone(), issue755CloneTerminal(router.terminal)
}

func (router *issue755StageBoundRouter) MutationsRollback(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.MutationRollbackRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	return router.outcome.Clone(), issue755CloneTerminal(router.terminal)
}

func issue755CloneTerminal(terminal *eebusraw.ErrorV1) *eebusraw.ErrorV1 {
	if terminal == nil {
		return nil
	}
	cloned := terminal.Clone()
	return &cloned
}

func TestIssue755LiveExpiredTokenKeepsCapturedRuntimeAndStableHash(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         2,
		ConnectionGeneration: 10,
	}
	router := &issue755StageBoundRouter{
		outcome: eebusruntime.RawMutationOutcomeV1{Runtime: &binding},
		terminal: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1StaleReadToken,
			"read token expired",
			false,
			eebusraw.SourceLayerV1("eebusreg-coordinator"),
		),
	}
	test := issue749CommandCases(t)[2]
	first := issue755CallCommand(t, router, test)
	second := issue755CallCommand(t, router, test)

	issue755AssertTerminalEnvelope(
		t,
		first.envelope,
		eebusraw.ErrorCodeV1StaleReadToken,
		eebusraw.SourceLayerV1("eebusreg-coordinator"),
		&binding,
	)
	firstMeta := msp06Map(t, first.envelope["meta"], "first meta")
	secondMeta := msp06Map(t, second.envelope["meta"], "second meta")
	if firstMeta["data_hash"] != secondMeta["data_hash"] {
		t.Fatalf("bound stale-token hash changed with data_timestamp: %v != %v",
			firstMeta["data_hash"], secondMeta["data_hash"])
	}
}

func TestIssue755SameStaleTokenCodeMayBeBoundOrUnbound(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         2,
		ConnectionGeneration: 10,
	}
	test := issue749CommandCases(t)[2]
	bound := issue755CallCommand(t, &issue755StageBoundRouter{
		outcome: eebusruntime.RawMutationOutcomeV1{Runtime: &binding},
		terminal: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1StaleReadToken,
			"authenticated read token expired",
			false,
			eebusraw.SourceLayerV1Runtime,
		),
	}, test)
	unbound := issue755CallCommand(t, &issue755StageBoundRouter{
		terminal: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1StaleReadToken,
			"unknown read token",
			false,
			eebusraw.SourceLayerV1Runtime,
		),
	}, test)

	issue755AssertTerminalEnvelope(
		t,
		bound.envelope,
		eebusraw.ErrorCodeV1StaleReadToken,
		eebusraw.SourceLayerV1Runtime,
		&binding,
	)
	issue755AssertTerminalEnvelope(
		t,
		unbound.envelope,
		eebusraw.ErrorCodeV1StaleReadToken,
		eebusraw.SourceLayerV1Runtime,
		nil,
	)
	boundHash := msp06Map(t, bound.envelope["meta"], "bound meta")["data_hash"]
	unboundHash := msp06Map(t, unbound.envelope["meta"], "unbound meta")["data_hash"]
	const (
		wantBoundHash   = "sha256:53f6b7f26272d6d955a3cbde49077d200703faca5f15384165e2d2b9c9dc265e"
		wantUnboundHash = "sha256:ac70761fdce6e5a750d811711189b82c09cd49e33451b09c00af6ecac72299ba"
	)
	if boundHash != wantBoundHash || unboundHash != wantUnboundHash {
		t.Fatalf(
			"terminal hash known-answer mismatch: bound=%v want=%s unbound=%v want=%s",
			boundHash,
			wantBoundHash,
			unboundHash,
			wantUnboundHash,
		)
	}
}

func TestIssue755MutationTerminalBindingAppliesToEveryMutationTool(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         7,
		ConnectionGeneration: 3,
	}
	cases := issue749CommandCases(t)
	for _, index := range []int{2, 3, 4} {
		test := cases[index]
		t.Run(test.tool, func(t *testing.T) {
			result := issue755CallCommand(t, &issue755StageBoundRouter{
				outcome: eebusruntime.RawMutationOutcomeV1{Runtime: &binding},
				terminal: eebusraw.NewErrorV1(
					eebusraw.ErrorCodeV1Disconnected,
					"operation lost its admitted SHIP session",
					true,
					eebusraw.SourceLayerV1("eebusreg-coordinator"),
				),
			}, test)
			issue755AssertTerminalEnvelope(
				t,
				result.envelope,
				eebusraw.ErrorCodeV1Disconnected,
				eebusraw.SourceLayerV1("eebusreg-coordinator"),
				&binding,
			)
		})
	}
}

func TestIssue755MutationGetNotFoundStaysCanonicalAndUnbound(t *testing.T) {
	test := issue749CommandCases(t)[3]
	result := issue755CallCommand(t, &issue755StageBoundRouter{
		terminal: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1NotFound,
			"mutation reference was not found",
			false,
			eebusraw.SourceLayerV1Runtime,
		),
	}, test)
	issue755AssertTerminalEnvelope(
		t,
		result.envelope,
		eebusraw.ErrorCodeV1NotFound,
		eebusraw.SourceLayerV1Runtime,
		nil,
	)
}

func TestIssue755PostDispatchTerminalWithoutRuntimeFailsClosed(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         7,
		ConnectionGeneration: 3,
	}
	postDispatchSources := []eebusraw.SourceLayerV1{
		eebusraw.SourceLayerV1("eebus-go-executor"),
		eebusraw.SourceLayerV1SpineRoundTrip,
		eebusraw.SourceLayerV1("ship-session"),
		eebusraw.SourceLayerV1Remote,
	}
	test := issue749CommandCases(t)[2]
	for _, source := range postDispatchSources {
		t.Run(string(source), func(t *testing.T) {
			result := issue755CallCommand(t, &issue755StageBoundRouter{
				terminal: eebusraw.NewErrorV1(
					eebusraw.ErrorCodeV1Disconnected,
					"post-dispatch terminal omitted its runtime",
					true,
					source,
				),
			}, test)
			issue755AssertTerminalEnvelope(
				t,
				result.envelope,
				eebusraw.ErrorCodeV1Internal,
				eebusV1SourceLayerGatewayRouter,
				nil,
			)

			bound := issue755CallCommand(t, &issue755StageBoundRouter{
				outcome: eebusruntime.RawMutationOutcomeV1{Runtime: &binding},
				terminal: eebusraw.NewErrorV1(
					eebusraw.ErrorCodeV1Disconnected,
					"post-dispatch terminal retained its runtime",
					true,
					source,
				),
			}, test)
			issue755AssertTerminalEnvelope(
				t,
				bound.envelope,
				eebusraw.ErrorCodeV1Disconnected,
				source,
				&binding,
			)
		})
	}
}

func TestIssue755MutationAndOutcomeRuntimeMustMatch(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         7,
		ConnectionGeneration: 3,
	}
	test := issue749CommandCases(t)[2]
	request := issue749DecodeArguments[eebusraw.FeatureDataSetRequestV1](t, test.arguments)
	mutation := issue749PreparedMutation(t, request, binding)

	cases := []struct {
		name    string
		runtime *eebusraw.RuntimeBindingV1
	}{
		{name: "missing"},
		{
			name: "different",
			runtime: &eebusraw.RuntimeBindingV1{
				RuntimeEpoch:         binding.RuntimeEpoch,
				ConnectionGeneration: binding.ConnectionGeneration + 1,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := issue755CallCommand(t, &issue755StageBoundRouter{
				outcome: eebusruntime.RawMutationOutcomeV1{
					Mutation: mutation,
					Runtime:  testCase.runtime,
				},
			}, test)
			issue755AssertTerminalEnvelope(
				t,
				result.envelope,
				eebusraw.ErrorCodeV1Internal,
				eebusV1SourceLayerGatewayRouter,
				nil,
			)
		})
	}

	result := issue755CallCommand(t, &issue755StageBoundRouter{
		outcome: eebusruntime.RawMutationOutcomeV1{
			Mutation: mutation,
			Runtime:  &binding,
		},
	}, test)
	if result.isError || result.envelope["error"] != nil || result.envelope["data"] == nil {
		t.Fatalf("matching mutation outcome failed: %#v", result.envelope)
	}
	assertIssue749CommandMeta(t, result.envelope, test.tool, test.scope, binding)
}

func TestIssue755FeatureDataGetZeroDataEnforcesSourcePartition(t *testing.T) {
	sourceCases := []struct {
		source     eebusraw.SourceLayerV1
		wantCode   eebusraw.ErrorCodeV1
		wantSource eebusraw.SourceLayerV1
	}{
		{
			source:     eebusV1SourceLayerMCP,
			wantCode:   eebusraw.ErrorCodeV1Disconnected,
			wantSource: eebusV1SourceLayerMCP,
		},
		{
			source:     eebusV1SourceLayerGatewayRouter,
			wantCode:   eebusraw.ErrorCodeV1Disconnected,
			wantSource: eebusV1SourceLayerGatewayRouter,
		},
		{
			source:     eebusraw.SourceLayerV1Runtime,
			wantCode:   eebusraw.ErrorCodeV1Disconnected,
			wantSource: eebusraw.SourceLayerV1Runtime,
		},
		{
			source:     eebusraw.SourceLayerV1("eebusreg-coordinator"),
			wantCode:   eebusraw.ErrorCodeV1Disconnected,
			wantSource: eebusraw.SourceLayerV1("eebusreg-coordinator"),
		},
		{
			source:     eebusraw.SourceLayerV1Executor,
			wantCode:   eebusraw.ErrorCodeV1Internal,
			wantSource: eebusV1SourceLayerGatewayRouter,
		},
		{
			source:     eebusraw.SourceLayerV1SpineRoundTrip,
			wantCode:   eebusraw.ErrorCodeV1Internal,
			wantSource: eebusV1SourceLayerGatewayRouter,
		},
		{
			source:     eebusraw.SourceLayerV1("ship-session"),
			wantCode:   eebusraw.ErrorCodeV1Internal,
			wantSource: eebusV1SourceLayerGatewayRouter,
		},
		{
			source:     eebusraw.SourceLayerV1Remote,
			wantCode:   eebusraw.ErrorCodeV1Internal,
			wantSource: eebusV1SourceLayerGatewayRouter,
		},
	}
	test := issue749CommandCases(t)[1]
	for _, testCase := range sourceCases {
		t.Run(string(testCase.source), func(t *testing.T) {
			result := issue755CallCommand(t, &issue755StageBoundRouter{
				dataGetTerminal: eebusraw.NewErrorV1(
					eebusraw.ErrorCodeV1Disconnected,
					"feature data read ended without a captured runtime",
					true,
					testCase.source,
				),
			}, test)
			issue755AssertTerminalEnvelope(
				t,
				result.envelope,
				testCase.wantCode,
				testCase.wantSource,
				nil,
			)
		})
	}
}

func TestIssue755FeatureDataGetPartialResultWithBoundDataRemainsLegal(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         7,
		ConnectionGeneration: 3,
	}
	test := issue749CommandCases(t)[1]
	arguments := cloneIssue749Arguments(t, test.arguments)
	targets := arguments["targets"].([]any)
	secondTarget := cloneIssue749Arguments(t, targets[0].(map[string]any))
	secondTarget["feature_address"] = float64(3)
	arguments["targets"] = append(targets, secondTarget)
	test.arguments = arguments
	request := issue749DecodeArguments[eebusraw.FeatureDataGetRequestV1](t, arguments)

	result := issue755CallCommand(t, &issue755StageBoundRouter{
		dataGetData: issue749PartialData(t, request, binding),
		dataGetTerminal: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1PartialResult,
			"one bound read target timed out",
			true,
			eebusraw.SourceLayerV1SpineRoundTrip,
		),
	}, test)
	if !result.isError || result.envelope["data"] == nil {
		t.Fatalf("bound partial read was not retained: %#v", result.envelope)
	}
	publicError := msp06Map(t, result.envelope["error"], "partial read error")
	if publicError["code"] != string(eebusraw.ErrorCodeV1PartialResult) ||
		publicError["source_layer"] != string(eebusraw.SourceLayerV1SpineRoundTrip) {
		t.Fatalf("partial read terminal = %#v", publicError)
	}
	assertIssue749CommandMeta(t, result.envelope, test.tool, test.scope, binding)
}

func TestIssue755MutationToolsRejectPartialResultCategorically(t *testing.T) {
	binding := eebusraw.RuntimeBindingV1{
		RuntimeEpoch:         7,
		ConnectionGeneration: 3,
	}
	commandCases := issue749CommandCases(t)
	setRequest := issue749DecodeArguments[eebusraw.FeatureDataSetRequestV1](
		t,
		commandCases[2].arguments,
	)
	mutation := issue749PreparedMutation(t, setRequest, binding)
	sources := []eebusraw.SourceLayerV1{
		eebusV1SourceLayerMCP,
		eebusV1SourceLayerGatewayRouter,
		eebusraw.SourceLayerV1Runtime,
		eebusraw.SourceLayerV1("eebusreg-coordinator"),
		eebusraw.SourceLayerV1Executor,
		eebusraw.SourceLayerV1SpineRoundTrip,
		eebusraw.SourceLayerV1("ship-session"),
		eebusraw.SourceLayerV1Remote,
	}
	outcomes := []struct {
		name    string
		outcome eebusruntime.RawMutationOutcomeV1
	}{
		{name: "zero-unbound"},
		{
			name: "zero-bound",
			outcome: eebusruntime.RawMutationOutcomeV1{
				Runtime: &binding,
			},
		},
		{
			name: "mutation-bound",
			outcome: eebusruntime.RawMutationOutcomeV1{
				Mutation: mutation,
				Runtime:  &binding,
			},
		},
	}

	for _, commandIndex := range []int{2, 3, 4} {
		test := commandCases[commandIndex]
		t.Run(test.tool, func(t *testing.T) {
			for _, source := range sources {
				t.Run(string(source), func(t *testing.T) {
					for _, outcome := range outcomes {
						t.Run(outcome.name, func(t *testing.T) {
							result := issue755CallCommand(t, &issue755StageBoundRouter{
								outcome: outcome.outcome,
								terminal: eebusraw.NewErrorV1(
									eebusraw.ErrorCodeV1PartialResult,
									"partial mutation results are forbidden",
									true,
									source,
								),
							}, test)
							issue755AssertTerminalEnvelope(
								t,
								result.envelope,
								eebusraw.ErrorCodeV1Internal,
								eebusV1SourceLayerGatewayRouter,
								nil,
							)
						})
					}
				})
			}
		})
	}
}

func issue755CallCommand(
	t *testing.T,
	router EEBusV1CommandRouter,
	test issue749CommandCase,
) msp06CallResult {
	t.Helper()
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	return msp06Call(
		t,
		issue743OperatorHandler(t, server),
		test.tool,
		cloneIssue749Arguments(t, test.arguments),
	)
}

func issue755AssertTerminalEnvelope(
	t *testing.T,
	envelope map[string]any,
	code eebusraw.ErrorCodeV1,
	source eebusraw.SourceLayerV1,
	runtime *eebusraw.RuntimeBindingV1,
) {
	t.Helper()
	msp06AssertKeys(t, envelope, "terminal envelope", "meta", "request", "data", "error")
	if envelope["data"] != nil {
		t.Fatalf("terminal envelope exposed data: %#v", envelope["data"])
	}
	publicError := msp06Map(t, envelope["error"], "terminal error")
	msp06AssertKeys(
		t,
		publicError,
		"terminal error",
		"code",
		"message",
		"retriable",
		"source_layer",
	)
	if publicError["code"] != string(code) ||
		publicError["source_layer"] != string(source) {
		t.Fatalf("terminal error = %#v, want code=%s source=%s",
			publicError, code, source)
	}
	meta := msp06Map(t, envelope["meta"], "terminal meta")
	msp06AssertKeys(
		t,
		meta,
		"terminal meta",
		"contract",
		"tool",
		"scope",
		"mask_tier",
		"auth_scope",
		"data_timestamp",
		"data_hash",
		"runtime",
	)
	hash, _ := meta["data_hash"].(string)
	if !strings.HasPrefix(hash, "sha256:") || len(hash) != len("sha256:")+64 {
		t.Fatalf("terminal data_hash = %q", hash)
	}
	if runtime == nil {
		if meta["runtime"] != nil {
			t.Fatalf("unbound terminal fabricated runtime: %#v", meta["runtime"])
		}
		return
	}
	got := msp06Map(t, meta["runtime"], "terminal runtime")
	msp06AssertKeys(
		t,
		got,
		"terminal runtime",
		"runtime_epoch",
		"connection_generation",
	)
	if fmt.Sprint(got["runtime_epoch"]) != fmt.Sprint(runtime.RuntimeEpoch) ||
		fmt.Sprint(got["connection_generation"]) != fmt.Sprint(runtime.ConnectionGeneration) {
		t.Fatalf("terminal runtime = %#v, want %#v", got, runtime)
	}
}
