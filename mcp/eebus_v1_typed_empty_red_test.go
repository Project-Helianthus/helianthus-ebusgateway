package mcp

import (
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

func TestIssue766TypedEmptyKeepsTheReleasedRawReadTerminalContract(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	router := &issue749TypedRouter{
		dataGetError: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1TypedEmpty,
			"raw READ response contained valid empty typed data",
			false,
			eebusraw.SourceLayerV1Remote,
		),
	}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}

	command := issue749CommandCases(t)[1]
	result := msp06Call(t, issue743OperatorHandler(t, server), command.tool, command.arguments)
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("typed-empty envelope = %#v", result.envelope)
	}
	errorData := msp06Map(t, result.envelope["error"], "typed-empty error")
	if errorData["code"] != string(eebusraw.ErrorCodeV1TypedEmpty) ||
		errorData["retriable"] != false ||
		errorData["source_layer"] != string(eebusraw.SourceLayerV1Remote) {
		t.Fatalf("typed-empty terminal = %#v", errorData)
	}
	for _, forbidden := range []string{"read_token", "before", "mutation_ref", "continuation"} {
		if strings.Contains(result.raw, forbidden) {
			t.Fatalf("typed-empty terminal leaked %q: %s", forbidden, result.raw)
		}
	}
}

func TestIssue766TypedEmptyFailsClosedUnlessNonRetriableRemote(t *testing.T) {
	command := issue749CommandCases(t)[1]
	tests := []struct {
		name      string
		retriable bool
		source    eebusraw.SourceLayerV1
	}{
		{
			name:   "runtime source",
			source: eebusraw.SourceLayerV1Runtime,
		},
		{
			name:      "retriable remote",
			retriable: true,
			source:    eebusraw.SourceLayerV1Remote,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
			server, _ := msp06TestServer(t, provider)
			router := &issue749TypedRouter{
				dataGetError: eebusraw.NewErrorV1(
					eebusraw.ErrorCodeV1TypedEmpty,
					"raw READ response contained valid empty typed data",
					test.retriable,
					test.source,
				),
			}
			if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
				t.Fatal(err)
			}

			result := msp06Call(t, issue743OperatorHandler(t, server), command.tool, command.arguments)
			if !result.isError || result.envelope["data"] != nil {
				t.Fatalf("malformed typed-empty envelope = %#v", result.envelope)
			}
			errorData := msp06Map(t, result.envelope["error"], "malformed typed-empty error")
			if errorData["code"] != string(eebusraw.ErrorCodeV1Internal) ||
				errorData["retriable"] != false ||
				errorData["source_layer"] != string(eebusV1SourceLayerGatewayRouter) {
				t.Fatalf("malformed typed-empty terminal = %#v", errorData)
			}
		})
	}
}

func TestIssue766TypedEmptyIsFeatureDataGetOnly(t *testing.T) {
	commandCases := issue749CommandCases(t)
	tests := []struct {
		name      string
		command   issue749CommandCase
		configure func(*issue749TypedRouter, *eebusraw.ErrorV1)
	}{
		{
			name:    "features get",
			command: commandCases[0],
			configure: func(router *issue749TypedRouter, terminal *eebusraw.ErrorV1) {
				router.featuresGetError = terminal
			},
		},
		{
			name:    "mutation get",
			command: commandCases[3],
			configure: func(router *issue749TypedRouter, terminal *eebusraw.ErrorV1) {
				router.mutationError = terminal
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
			server, _ := msp06TestServer(t, provider)
			router := &issue749TypedRouter{}
			test.configure(router, eebusraw.NewErrorV1(
				eebusraw.ErrorCodeV1TypedEmpty,
				"typed-empty is invalid for this tool",
				false,
				eebusraw.SourceLayerV1Runtime,
			))
			if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
				t.Fatal(err)
			}

			result := msp06Call(
				t,
				issue743OperatorHandler(t, server),
				test.command.tool,
				test.command.arguments,
			)
			issue755AssertTerminalEnvelope(
				t,
				result.envelope,
				eebusraw.ErrorCodeV1Internal,
				eebusV1SourceLayerGatewayRouter,
				nil,
			)
		})
	}
}

func TestIssue766MixedTypedEmptyRemainsPartialResult(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	command := issue749CommandCases(t)[1]
	arguments := cloneIssue749Arguments(t, command.arguments)
	targets := arguments["targets"].([]any)
	second := cloneIssue749Arguments(t, targets[0].(map[string]any))
	second["feature_address"] = float64(3)
	arguments["targets"] = append(targets, second)
	request := issue749DecodeArguments[eebusraw.FeatureDataGetRequestV1](t, arguments)
	binding := eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3}
	data := issue749PartialData(t, request, binding)
	data.Failures[0].Error = *eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1TypedEmpty,
		"raw READ response contained valid empty typed data",
		false,
		eebusraw.SourceLayerV1Remote,
	)
	router := &issue749TypedRouter{
		dataGetData: data,
		dataGetError: eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1PartialResult,
			"fixed partial result",
			false,
			eebusraw.SourceLayerV1Runtime,
		),
	}
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}

	result := msp06Call(t, issue743OperatorHandler(t, server), command.tool, arguments)
	if !result.isError || result.envelope["data"] == nil ||
		issue749EnvelopeErrorCode(t, result.envelope) != string(eebusraw.ErrorCodeV1PartialResult) {
		t.Fatalf("mixed typed-empty envelope = %#v", result.envelope)
	}
	dataEnvelope := msp06Map(t, result.envelope["data"], "mixed typed-empty data")
	if dataEnvelope["complete"] != false {
		t.Fatalf("mixed typed-empty complete = %#v, want false", dataEnvelope["complete"])
	}
	failures, ok := dataEnvelope["failures"].([]any)
	if !ok || len(failures) != 1 {
		t.Fatalf("mixed typed-empty failures = %#v", dataEnvelope["failures"])
	}
	failure := msp06Map(t, failures[0], "mixed typed-empty failure")
	failureError := msp06Map(t, failure["error"], "mixed typed-empty failure error")
	if failureError["code"] != string(eebusraw.ErrorCodeV1TypedEmpty) ||
		failureError["retriable"] != false ||
		failureError["source_layer"] != string(eebusraw.SourceLayerV1Remote) {
		t.Fatalf("mixed typed-empty failure terminal = %#v", failureError)
	}
}
