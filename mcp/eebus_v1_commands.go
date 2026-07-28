package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const eebusV1RawFeatureContract = "helianthus.eebus.raw-feature-runtime.v1"

const (
	eebusV1SourceLayerMCP           = eebusraw.SourceLayerV1("mcp")
	eebusV1SourceLayerGatewayRouter = eebusraw.SourceLayerV1("gateway-router")
	eebusV1MaximumSafeInteger       = uint64(9007199254740991)
)

// EEBusV1CommandRouter is the sole MCP dependency for typed raw eeBUS
// feature reads, mutations, status, and rollback.
type EEBusV1CommandRouter interface {
	FeaturesGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeaturesGetRequestV1) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1)
	FeaturesDataGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1)
	FeaturesDataSet(context.Context, eebusraw.WriteAuthorizationV1, eebusraw.FeatureDataSetRequestV1) (eebusraw.MutationV1, *eebusraw.ErrorV1)
	MutationsGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.MutationGetRequestV1) (eebusraw.MutationV1, *eebusraw.ErrorV1)
	MutationsRollback(context.Context, eebusraw.WriteAuthorizationV1, eebusraw.MutationRollbackRequestV1) (eebusraw.MutationV1, *eebusraw.ErrorV1)
}

type eebusV1CommandSpec struct {
	tool        eebusraw.ToolV1
	scope       eebusraw.AuthScopeV1
	description string
	inputSchema map[string]any
}

var eebusV1CommandSpecs = []eebusV1CommandSpec{
	{
		tool: eebusraw.ToolV1FeaturesGet, scope: eebusraw.AuthScopeV1RawRead,
		description: "Get one exact raw eeBUS feature and its full function declarations.",
		inputSchema: eebusV1FeaturesGetSchema(),
	},
	{
		tool: eebusraw.ToolV1FeaturesDataGet, scope: eebusraw.AuthScopeV1RawRead,
		description: "Read complete typed data from one to sixteen exact raw eeBUS feature functions.",
		inputSchema: eebusV1FeaturesDataGetSchema(),
	},
	{
		tool: eebusraw.ToolV1FeaturesDataSet, scope: eebusraw.AuthScopeV1RawWrite,
		description: "Apply one guarded complete typed raw eeBUS feature mutation.",
		inputSchema: eebusV1FeaturesDataSetSchema(),
	},
	{
		tool: eebusraw.ToolV1MutationsGet, scope: eebusraw.AuthScopeV1RawRead,
		description: "Get one durable raw eeBUS mutation record.",
		inputSchema: eebusV1MutationsGetSchema(),
	},
	{
		tool: eebusraw.ToolV1MutationsRollback, scope: eebusraw.AuthScopeV1RawWrite,
		description: "Rollback one durable raw eeBUS mutation through the guarded command path.",
		inputSchema: eebusV1MutationsRollbackSchema(),
	},
}

type eebusV1CommandMeta struct {
	Contract      string               `json:"contract"`
	Tool          eebusraw.ToolV1      `json:"tool"`
	Scope         eebusraw.AuthScopeV1 `json:"scope"`
	MaskTier      eebusraw.MaskTier    `json:"mask_tier"`
	AuthScope     eebusraw.AuthScopeV1 `json:"auth_scope"`
	DataTimestamp string               `json:"data_timestamp"`
	DataHash      eebusraw.HashV1      `json:"data_hash"`
	// Nil is reserved for failures that occur before any runtime binding is known.
	Runtime *eebusraw.RuntimeBindingV1 `json:"runtime"`
}

type eebusV1CommandEnvelope struct {
	Meta    eebusV1CommandMeta `json:"meta"`
	Request any                `json:"request"`
	Data    any                `json:"data"`
	Error   *eebusraw.ErrorV1  `json:"error"`
}

type eebusV1CommandHashView struct {
	Contract  string                     `json:"contract"`
	Tool      eebusraw.ToolV1            `json:"tool"`
	Scope     eebusraw.AuthScopeV1       `json:"scope"`
	MaskTier  eebusraw.MaskTier          `json:"mask_tier"`
	AuthScope eebusraw.AuthScopeV1       `json:"auth_scope"`
	Runtime   *eebusraw.RuntimeBindingV1 `json:"runtime"`
	Request   any                        `json:"request"`
	Data      any                        `json:"data"`
	Error     *eebusraw.ErrorV1          `json:"error"`
}

func (server *Server) RegisterEEBusV1CommandRouter(router EEBusV1CommandRouter) error {
	if server == nil {
		return errors.New("eeBUS MCP server is nil")
	}
	if eebusV1NilCommandRouter(router) {
		return errors.New("eeBUS MCP command router is nil")
	}
	server.eebusV1Mu.Lock()
	defer server.eebusV1Mu.Unlock()
	if server.eebusV1CommandRouter != nil {
		return errors.New("eeBUS MCP command router is already registered")
	}
	server.eebusV1CommandRouter = router
	return nil
}

func eebusV1NilCommandRouter(router EEBusV1CommandRouter) bool {
	if router == nil {
		return true
	}
	value := reflect.ValueOf(router)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func eebusV1CommandSpecForName(name string) (eebusV1CommandSpec, bool) {
	for _, spec := range eebusV1CommandSpecs {
		if string(spec.tool) == name {
			return spec, true
		}
	}
	return eebusV1CommandSpec{}, false
}

func eebusV1CommandTools() []Tool {
	tools := make([]Tool, 0, len(eebusV1CommandSpecs))
	for _, spec := range eebusV1CommandSpecs {
		tools = append(tools, Tool{
			Name: string(spec.tool), Description: spec.description, InputSchema: spec.inputSchema,
		})
	}
	return tools
}

func (server *Server) handleEEBusV1CommandRawCall(
	ctx context.Context,
	spec eebusV1CommandSpec,
	arguments json.RawMessage,
) any {
	request, terminal := eebusV1DecodeCommandRequest(spec.tool, arguments)
	if terminal != nil {
		return eebusV1CommandResult(spec, nil, nil, terminal, nil)
	}

	if eebusV1BoundaryFromContext(ctx) != eebusV1OperatorBoundary {
		terminal = eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1PermissionDenied,
			"raw eeBUS commands require the owner-only operator boundary",
			false,
			eebusV1SourceLayerGatewayRouter,
		)
		return eebusV1CommandResult(spec, request, nil, terminal, nil)
	}

	server.eebusV1Mu.RLock()
	router := server.eebusV1CommandRouter
	server.eebusV1Mu.RUnlock()
	if eebusV1NilCommandRouter(router) {
		terminal = eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1Internal,
			"raw eeBUS command router is unavailable",
			false,
			eebusV1SourceLayerGatewayRouter,
		)
		return eebusV1CommandResult(spec, request, nil, terminal, nil)
	}

	data, terminal, runtime := eebusV1DispatchCommand(ctx, router, spec, request)
	if terminal != nil &&
		(spec.tool != eebusraw.ToolV1FeaturesDataGet ||
			terminal.Code != eebusraw.ErrorCodeV1PartialResult) {
		data = nil
	}
	return eebusV1CommandResult(spec, request, data, terminal, runtime)
}

func eebusV1MalformedCommandResult(spec eebusV1CommandSpec) any {
	return eebusV1CommandResult(
		spec,
		nil,
		nil,
		eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1InvalidArgument,
			"command request does not match the closed tool shape",
			false,
			eebusV1SourceLayerMCP,
		),
		nil,
	)
}

func eebusV1DecodeCommandRequest(
	tool eebusraw.ToolV1,
	arguments json.RawMessage,
) (any, *eebusraw.ErrorV1) {
	invalid := func() (any, *eebusraw.ErrorV1) {
		return nil, eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1InvalidArgument,
			"command request does not match the closed tool shape",
			false,
			eebusV1SourceLayerMCP,
		)
	}
	if len(arguments) == 0 || eebusV1JSONHasDuplicateKeys(arguments) {
		return invalid()
	}
	decode := func(request any) (any, *eebusraw.ErrorV1) {
		decoder := json.NewDecoder(bytes.NewReader(arguments))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(request); err != nil {
			return invalid()
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return invalid()
		}
		typed := reflect.ValueOf(request).Elem().Interface()
		if terminal := eebusV1ValidateCommandRequest(tool, typed); terminal != nil {
			if terminal.Code == eebusraw.ErrorCodeV1SecretDetected {
				return nil, eebusraw.NewErrorV1(
					eebusraw.ErrorCodeV1InvalidArgument,
					"command request was rejected by the protected input boundary",
					false,
					eebusV1SourceLayerMCP,
				)
			}
			cloned := terminal.Clone()
			cloned.SourceLayer = eebusV1SourceLayerMCP
			cloned.Message = "command request failed canonical validation"
			return typed, &cloned
		}
		return typed, nil
	}
	switch tool {
	case eebusraw.ToolV1FeaturesGet:
		return decode(&eebusraw.FeaturesGetRequestV1{})
	case eebusraw.ToolV1FeaturesDataGet:
		return decode(&eebusraw.FeatureDataGetRequestV1{})
	case eebusraw.ToolV1FeaturesDataSet:
		return decode(&eebusraw.FeatureDataSetRequestV1{})
	case eebusraw.ToolV1MutationsGet:
		return decode(&eebusraw.MutationGetRequestV1{})
	case eebusraw.ToolV1MutationsRollback:
		return decode(&eebusraw.MutationRollbackRequestV1{})
	default:
		return invalid()
	}
}

func eebusV1JSONHasDuplicateKeys(document []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	duplicate, err := eebusV1WalkJSONValue(decoder)
	if err != nil {
		return false
	}
	if duplicate {
		return true
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false
	}
	return false
}

func eebusV1WalkJSONValue(decoder *json.Decoder) (bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return false, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return false, nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			rawKey, err := decoder.Token()
			if err != nil {
				return false, err
			}
			key, ok := rawKey.(string)
			if !ok {
				return false, errors.New("JSON object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return true, nil
			}
			seen[key] = struct{}{}
			duplicate, err := eebusV1WalkJSONValue(decoder)
			if err != nil || duplicate {
				return duplicate, err
			}
		}
		_, err := decoder.Token()
		return false, err
	case '[':
		for decoder.More() {
			duplicate, err := eebusV1WalkJSONValue(decoder)
			if err != nil || duplicate {
				return duplicate, err
			}
		}
		_, err := decoder.Token()
		return false, err
	default:
		return false, errors.New("unexpected JSON delimiter")
	}
}

func eebusV1ValidateCommandRequest(tool eebusraw.ToolV1, request any) *eebusraw.ErrorV1 {
	switch tool {
	case eebusraw.ToolV1FeaturesGet:
		typed, ok := request.(eebusraw.FeaturesGetRequestV1)
		if !ok {
			return eebusraw.NewErrorV1(eebusraw.ErrorCodeV1InvalidArgument, "invalid request", false, eebusV1SourceLayerMCP)
		}
		return eebusraw.ValidateFeaturesGetRequestV1(typed)
	case eebusraw.ToolV1FeaturesDataGet:
		typed, ok := request.(eebusraw.FeatureDataGetRequestV1)
		if !ok {
			return eebusraw.NewErrorV1(eebusraw.ErrorCodeV1InvalidArgument, "invalid request", false, eebusV1SourceLayerMCP)
		}
		return eebusraw.ValidateFeatureDataGetRequestV1(typed)
	case eebusraw.ToolV1FeaturesDataSet:
		typed, ok := request.(eebusraw.FeatureDataSetRequestV1)
		if !ok {
			return eebusraw.NewErrorV1(eebusraw.ErrorCodeV1InvalidArgument, "invalid request", false, eebusV1SourceLayerMCP)
		}
		return eebusraw.ValidateFeatureDataSetRequestV1(typed)
	case eebusraw.ToolV1MutationsGet:
		typed, ok := request.(eebusraw.MutationGetRequestV1)
		if !ok {
			return eebusraw.NewErrorV1(eebusraw.ErrorCodeV1InvalidArgument, "invalid request", false, eebusV1SourceLayerMCP)
		}
		return eebusraw.ValidateMutationGetRequestV1(typed)
	case eebusraw.ToolV1MutationsRollback:
		typed, ok := request.(eebusraw.MutationRollbackRequestV1)
		if !ok {
			return eebusraw.NewErrorV1(eebusraw.ErrorCodeV1InvalidArgument, "invalid request", false, eebusV1SourceLayerMCP)
		}
		return eebusraw.ValidateMutationRollbackRequestV1(typed)
	default:
		return eebusraw.NewErrorV1(eebusraw.ErrorCodeV1InvalidArgument, "unknown raw eeBUS command", false, eebusV1SourceLayerMCP)
	}
}

func eebusV1DispatchCommand(
	ctx context.Context,
	router EEBusV1CommandRouter,
	spec eebusV1CommandSpec,
	request any,
) (any, *eebusraw.ErrorV1, *eebusraw.RuntimeBindingV1) {
	var data any
	var terminal *eebusraw.ErrorV1
	switch spec.tool {
	case eebusraw.ToolV1FeaturesGet:
		typedRequest := request.(eebusraw.FeaturesGetRequestV1)
		typedData, typedTerminal := router.FeaturesGet(ctx, eebusV1ReadAuthorization(spec), typedRequest)
		data, terminal = typedData, typedTerminal
	case eebusraw.ToolV1FeaturesDataGet:
		typedRequest := request.(eebusraw.FeatureDataGetRequestV1)
		typedData, typedTerminal := router.FeaturesDataGet(ctx, eebusV1ReadAuthorization(spec), typedRequest)
		data, terminal = typedData, typedTerminal
	case eebusraw.ToolV1FeaturesDataSet:
		typedRequest := request.(eebusraw.FeatureDataSetRequestV1)
		typedData, typedTerminal := router.FeaturesDataSet(ctx, eebusV1WriteAuthorization(spec), typedRequest)
		data, terminal = typedData, typedTerminal
	case eebusraw.ToolV1MutationsGet:
		typedRequest := request.(eebusraw.MutationGetRequestV1)
		typedData, typedTerminal := router.MutationsGet(ctx, eebusV1ReadAuthorization(spec), typedRequest)
		data, terminal = typedData, typedTerminal
	case eebusraw.ToolV1MutationsRollback:
		typedRequest := request.(eebusraw.MutationRollbackRequestV1)
		typedData, typedTerminal := router.MutationsRollback(ctx, eebusV1WriteAuthorization(spec), typedRequest)
		data, terminal = typedData, typedTerminal
	default:
		return nil, eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1Internal,
			"raw eeBUS command dispatch failed",
			false,
			eebusV1SourceLayerGatewayRouter,
		), nil
	}

	safeTerminal, terminalFailure := eebusV1ValidateRouterTerminal(terminal)
	if terminalFailure != nil {
		return nil, terminalFailure, nil
	}
	terminal = safeTerminal
	runtime, validationFailure := eebusV1ValidateCommandOutcome(spec.tool, request, data, terminal)
	if validationFailure != nil {
		return nil, validationFailure, nil
	}
	if runtime == nil && terminal != nil {
		if eebusV1TerminalRequiresRuntime(terminal.Code) {
			return nil, eebusV1ContractViolation(), nil
		}
		terminal.SourceLayer = eebusV1SourceLayerGatewayRouter
	}
	return data, terminal, runtime
}

func eebusV1ReadAuthorization(spec eebusV1CommandSpec) eebusraw.ReadAuthorizationV1 {
	return eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          eebusraw.AuthScopeV1RawRead,
		Tool:           spec.tool,
		MaskTier:       eebusraw.MaskTierRaw,
	}
}

func eebusV1WriteAuthorization(spec eebusV1CommandSpec) eebusraw.WriteAuthorizationV1 {
	return eebusraw.WriteAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          eebusraw.AuthScopeV1RawWrite,
		Tool:           spec.tool,
		MaskTier:       eebusraw.MaskTierRaw,
	}
}

func eebusV1FeatureDataRuntime(data eebusraw.FeatureDataGetDataV1) *eebusraw.RuntimeBindingV1 {
	var binding *eebusraw.RuntimeBindingV1
	for _, result := range data.Results {
		current := eebusV1BoundRuntime(result.Runtime)
		if current == nil {
			return nil
		}
		if binding == nil {
			binding = current
			continue
		}
		if *binding != *current {
			return nil
		}
	}
	return binding
}

func eebusV1BoundRuntime(runtime eebusraw.RuntimeBindingV1) *eebusraw.RuntimeBindingV1 {
	if runtime.RuntimeEpoch == 0 ||
		runtime.RuntimeEpoch > eebusV1MaximumSafeInteger ||
		runtime.ConnectionGeneration == 0 ||
		runtime.ConnectionGeneration > eebusV1MaximumSafeInteger {
		return nil
	}
	bound := runtime
	return &bound
}

func eebusV1ValidateCommandOutcome(
	tool eebusraw.ToolV1,
	request any,
	data any,
	terminal *eebusraw.ErrorV1,
) (*eebusraw.RuntimeBindingV1, *eebusraw.ErrorV1) {
	switch tool {
	case eebusraw.ToolV1FeaturesGet:
		typedRequest := request.(eebusraw.FeaturesGetRequestV1)
		typedData := data.(eebusraw.FeaturesGetDataV1)
		if reflect.ValueOf(typedData).IsZero() {
			if terminal == nil {
				return nil, eebusV1ContractViolation()
			}
			return nil, nil
		}
		if failure := eebusraw.ValidateFeaturesGetDataV1(typedRequest, typedData); failure != nil {
			return nil, eebusV1CanonicalValidationFailure(failure)
		}
		runtime := eebusV1BoundRuntime(typedData.Runtime)
		if runtime == nil {
			return nil, eebusV1ContractViolation()
		}
		return runtime, nil
	case eebusraw.ToolV1FeaturesDataGet:
		typedRequest := request.(eebusraw.FeatureDataGetRequestV1)
		typedData := data.(eebusraw.FeatureDataGetDataV1)
		if terminal != nil && terminal.Code != eebusraw.ErrorCodeV1PartialResult {
			if !reflect.ValueOf(typedData).IsZero() {
				return nil, eebusV1ContractViolation()
			}
			return nil, nil
		}
		if failure := eebusraw.ValidateFeatureDataGetDataV1(typedRequest, typedData, terminal); failure != nil {
			return nil, eebusV1CanonicalValidationFailure(failure)
		}
		runtime := eebusV1FeatureDataRuntime(typedData)
		if runtime == nil {
			return nil, eebusV1ContractViolation()
		}
		return runtime, nil
	case eebusraw.ToolV1FeaturesDataSet,
		eebusraw.ToolV1MutationsGet,
		eebusraw.ToolV1MutationsRollback:
		typedData := data.(eebusraw.MutationV1)
		if reflect.ValueOf(typedData).IsZero() {
			if terminal == nil {
				return nil, eebusV1ContractViolation()
			}
			return nil, nil
		}
		if failure := eebusraw.ValidateMutationV1(typedData); failure != nil {
			return nil, eebusV1CanonicalValidationFailure(failure)
		}
		if !eebusV1MutationMatchesRequest(tool, request, typedData) {
			return nil, eebusV1ContractViolation()
		}
		runtime := eebusV1BoundRuntime(typedData.Runtime)
		if runtime == nil {
			return nil, eebusV1ContractViolation()
		}
		return runtime, nil
	default:
		return nil, eebusV1ContractViolation()
	}
}

func eebusV1MutationMatchesRequest(
	tool eebusraw.ToolV1,
	request any,
	mutation eebusraw.MutationV1,
) bool {
	switch tool {
	case eebusraw.ToolV1FeaturesDataSet:
		typed := request.(eebusraw.FeatureDataSetRequestV1)
		if !reflect.DeepEqual(typed.Target, mutation.Target) {
			return false
		}
		requestedHash, requestErr := typed.Value.ComputeHash()
		mutationHash, mutationErr := mutation.Requested.ComputeHash()
		return requestErr == nil && mutationErr == nil && requestedHash == mutationHash
	case eebusraw.ToolV1MutationsGet:
		return request.(eebusraw.MutationGetRequestV1).MutationRef == mutation.MutationRef
	case eebusraw.ToolV1MutationsRollback:
		return request.(eebusraw.MutationRollbackRequestV1).MutationRef == mutation.MutationRef
	default:
		return false
	}
}

func eebusV1ValidateRouterTerminal(
	terminal *eebusraw.ErrorV1,
) (*eebusraw.ErrorV1, *eebusraw.ErrorV1) {
	if terminal == nil {
		return nil, nil
	}
	cloned := terminal.Clone()
	if _, err := eebusraw.CanonicalSHA256V1(cloned); err != nil {
		if errors.Is(err, eebusraw.ErrSecretDetected) {
			return nil, eebusV1SecretFailure()
		}
		return nil, eebusV1ContractViolation()
	}
	if !eebusV1KnownErrorCode(cloned.Code) ||
		!eebusV1KnownSourceLayer(cloned.SourceLayer) ||
		!eebusV1BoundedString(cloned.Message, 512) {
		return nil, eebusV1ContractViolation()
	}
	if cloned.Details != nil {
		if cloned.Details.TargetIndex != nil && *cloned.Details.TargetIndex > 15 ||
			!eebusV1OptionalBoundedString(cloned.Details.Classification, 128) ||
			len(cloned.Details.Unknown) > 256 {
			return nil, eebusV1ContractViolation()
		}
		for _, unknown := range cloned.Details.Unknown {
			if !eebusV1BoundedString(unknown.Path, 1024) ||
				!eebusV1BoundedString(unknown.Source, 256) ||
				unknown.Value.Validate() != nil {
				return nil, eebusV1ContractViolation()
			}
		}
	}
	return &cloned, nil
}

func eebusV1BoundedString(value string, maximum int) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" &&
		utf8.ValidString(trimmed) &&
		utf8.RuneCountInString(trimmed) <= maximum
}

func eebusV1OptionalBoundedString(value string, maximum int) bool {
	return value == "" || eebusV1BoundedString(value, maximum)
}

func eebusV1CanonicalValidationFailure(terminal *eebusraw.ErrorV1) *eebusraw.ErrorV1 {
	if terminal != nil && terminal.Code == eebusraw.ErrorCodeV1SecretDetected {
		return eebusV1SecretFailure()
	}
	return eebusV1ContractViolation()
}

func eebusV1SecretFailure() *eebusraw.ErrorV1 {
	return eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1SecretDetected,
		"raw eeBUS response was rejected by the protected output boundary",
		false,
		eebusV1SourceLayerGatewayRouter,
	)
}

func eebusV1ContractViolation() *eebusraw.ErrorV1 {
	return eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1Internal,
		"raw eeBUS command router returned an invalid contract result",
		false,
		eebusV1SourceLayerGatewayRouter,
	)
}

func eebusV1TerminalRequiresRuntime(code eebusraw.ErrorCodeV1) bool {
	switch code {
	case eebusraw.ErrorCodeV1ConstraintsUnknown,
		eebusraw.ErrorCodeV1ConstraintFailure,
		eebusraw.ErrorCodeV1StaleReadToken,
		eebusraw.ErrorCodeV1CASMismatch,
		eebusraw.ErrorCodeV1RuntimeEpochMismatch,
		eebusraw.ErrorCodeV1ConnectionGenerationMismatch,
		eebusraw.ErrorCodeV1IdempotencyConflict,
		eebusraw.ErrorCodeV1WriterBusy,
		eebusraw.ErrorCodeV1Disconnected,
		eebusraw.ErrorCodeV1Timeout,
		eebusraw.ErrorCodeV1Cancelled,
		eebusraw.ErrorCodeV1RemoteError,
		eebusraw.ErrorCodeV1DecodeError,
		eebusraw.ErrorCodeV1PartialResult,
		eebusraw.ErrorCodeV1NoEffect,
		eebusraw.ErrorCodeV1OutcomeUnknown,
		eebusraw.ErrorCodeV1Conflict,
		eebusraw.ErrorCodeV1RollbackFailed:
		return true
	default:
		return false
	}
}

func eebusV1KnownErrorCode(code eebusraw.ErrorCodeV1) bool {
	switch code {
	case eebusraw.ErrorCodeV1PermissionDenied,
		eebusraw.ErrorCodeV1InvalidArgument,
		eebusraw.ErrorCodeV1UnsupportedOperation,
		eebusraw.ErrorCodeV1PartialOperationForbidden,
		eebusraw.ErrorCodeV1ConstraintsUnknown,
		eebusraw.ErrorCodeV1ConstraintFailure,
		eebusraw.ErrorCodeV1StaleReadToken,
		eebusraw.ErrorCodeV1CASMismatch,
		eebusraw.ErrorCodeV1RuntimeEpochMismatch,
		eebusraw.ErrorCodeV1ConnectionGenerationMismatch,
		eebusraw.ErrorCodeV1IdempotencyConflict,
		eebusraw.ErrorCodeV1WriterBusy,
		eebusraw.ErrorCodeV1Disconnected,
		eebusraw.ErrorCodeV1Timeout,
		eebusraw.ErrorCodeV1Cancelled,
		eebusraw.ErrorCodeV1RemoteError,
		eebusraw.ErrorCodeV1DecodeError,
		eebusraw.ErrorCodeV1PartialResult,
		eebusraw.ErrorCodeV1OutcomeUnknown,
		eebusraw.ErrorCodeV1Conflict,
		eebusraw.ErrorCodeV1RollbackFailed,
		eebusraw.ErrorCodeV1NoEffect,
		eebusraw.ErrorCodeV1NotFound,
		eebusraw.ErrorCodeV1SecretDetected,
		eebusraw.ErrorCodeV1Internal:
		return true
	default:
		return false
	}
}

func eebusV1KnownSourceLayer(layer eebusraw.SourceLayerV1) bool {
	switch string(layer) {
	case "mcp",
		"gateway-router",
		"eebusreg-runtime",
		"eebusreg-coordinator",
		"eebus-go-executor",
		"spine-go-round-trip",
		"ship-session",
		"remote":
		return true
	default:
		return false
	}
}

func eebusV1CommandResult(
	spec eebusV1CommandSpec,
	request any,
	data any,
	terminal *eebusraw.ErrorV1,
	runtime *eebusraw.RuntimeBindingV1,
) any {
	if terminal != nil {
		cloned := terminal.Clone()
		terminal = &cloned
	}
	if terminal != nil && terminal.Code == eebusraw.ErrorCodeV1SecretDetected {
		request = nil
		data = nil
		runtime = nil
	}
	hashView := eebusV1CommandHashView{
		Contract: eebusV1RawFeatureContract,
		Tool:     spec.tool, Scope: spec.scope, MaskTier: eebusraw.MaskTierRaw,
		AuthScope: spec.scope, Runtime: runtime, Request: request, Data: data, Error: terminal,
	}
	hash, err := eebusraw.CanonicalSHA256V1(hashView)
	if err != nil {
		request = nil
		data = nil
		runtime = nil
		if errors.Is(err, eebusraw.ErrSecretDetected) {
			terminal = eebusV1SecretFailure()
		} else {
			terminal = eebusV1ContractViolation()
		}
		hashView.Runtime = nil
		hashView.Request = nil
		hashView.Data = nil
		hashView.Error = terminal
		hash, _ = eebusraw.CanonicalSHA256V1(hashView)
	}
	envelope := eebusV1CommandEnvelope{
		Meta: eebusV1CommandMeta{
			Contract: eebusV1RawFeatureContract,
			Tool:     spec.tool, Scope: spec.scope, MaskTier: eebusraw.MaskTierRaw,
			AuthScope: spec.scope, DataTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
			DataHash: hash, Runtime: runtime,
		},
		Request: request, Data: data, Error: terminal,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		fallbackTerminal := eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1Internal,
			"raw eeBUS response encoding failed",
			false,
			eebusV1SourceLayerGatewayRouter,
		)
		fallbackView := eebusV1CommandHashView{
			Contract: eebusV1RawFeatureContract,
			Tool:     spec.tool, Scope: spec.scope, MaskTier: eebusraw.MaskTierRaw,
			AuthScope: spec.scope, Error: fallbackTerminal,
		}
		fallbackHash, _ := eebusraw.CanonicalSHA256V1(fallbackView)
		fallbackEnvelope := eebusV1CommandEnvelope{
			Meta: eebusV1CommandMeta{
				Contract: eebusV1RawFeatureContract,
				Tool:     spec.tool, Scope: spec.scope, MaskTier: eebusraw.MaskTierRaw,
				AuthScope: spec.scope, DataTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
				DataHash: fallbackHash,
			},
			Error: fallbackTerminal,
		}
		encoded, _ = json.Marshal(fallbackEnvelope)
		return callToolResultText(string(encoded), true)
	}
	return callToolResultText(string(encoded), terminal != nil)
}

func eebusV1FeaturesGetSchema() map[string]any {
	return eebusV1ClosedSchema(
		map[string]any{"target": eebusV1FeatureLocatorSchema()},
		[]string{"target"},
	)
}

func eebusV1FeaturesDataGetSchema() map[string]any {
	return eebusV1ClosedSchema(
		map[string]any{
			"targets": map[string]any{
				"type": "array", "minItems": 1, "maxItems": 16,
				"items": eebusV1FeatureTargetSchema(eebusraw.OperationV1Read),
			},
			"timeout_ms": map[string]any{"type": "integer", "minimum": 1, "maximum": 30000},
		},
		[]string{"targets"},
	)
}

func eebusV1FeaturesDataSetSchema() map[string]any {
	schema := eebusV1ClosedSchema(
		map[string]any{
			"target": eebusV1FeatureTargetSchema(eebusraw.OperationV1Write),
			"value":  eebusV1TypedValueSchema(1),
			"read_token": map[string]any{
				"type": "string", "pattern": eebusV1TokenPattern,
			},
			"expected_current": eebusV1TypedValueSchema(1),
			"idempotency_key": map[string]any{
				"type": "string", "minLength": 16, "maxLength": 128,
				"pattern": `^[A-Za-z0-9._~-]+$`,
			},
			"mode":              map[string]any{"type": "string", "enum": []string{"apply", "probe"}},
			"probe_ttl_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 900},
			"constraints_override": eebusV1ClosedSchema(
				map[string]any{
					"profile_id":    map[string]any{"type": "string", "minLength": 1, "maxLength": 128},
					"justification": map[string]any{"type": "string", "minLength": 1, "maxLength": 1024},
					"expires_at":    map[string]any{"type": "string", "format": "date-time", "pattern": `Z$`},
				},
				[]string{"profile_id", "justification", "expires_at"},
			),
		},
		[]string{"target", "value", "read_token", "idempotency_key", "mode"},
	)
	schema["allOf"] = []any{
		map[string]any{
			"if": map[string]any{
				"properties": map[string]any{"mode": map[string]any{"const": "probe"}},
				"required":   []string{"mode"},
			},
			"then": map[string]any{"required": []string{"probe_ttl_seconds"}},
			"else": map[string]any{"not": map[string]any{"required": []string{"probe_ttl_seconds"}}},
		},
	}
	return schema
}

func eebusV1MutationsGetSchema() map[string]any {
	return eebusV1ClosedSchema(
		map[string]any{"mutation_ref": map[string]any{"type": "string", "pattern": eebusV1TokenPattern}},
		[]string{"mutation_ref"},
	)
}

func eebusV1MutationsRollbackSchema() map[string]any {
	return eebusV1ClosedSchema(
		map[string]any{
			"mutation_ref": map[string]any{"type": "string", "pattern": eebusV1TokenPattern},
			"idempotency_key": map[string]any{
				"type": "string", "minLength": 16, "maxLength": 128,
				"pattern": `^[A-Za-z0-9._~-]+$`,
			},
		},
		[]string{"mutation_ref", "idempotency_key"},
	)
}

func eebusV1FeatureLocatorSchema() map[string]any {
	return eebusV1ClosedSchema(
		map[string]any{
			"remote_ski":      map[string]any{"type": "string", "minLength": 40, "maxLength": 40, "pattern": `^[0-9a-f]{40}$`},
			"ship_id":         map[string]any{"type": "string", "minLength": 1, "maxLength": 256},
			"device_address":  map[string]any{"type": "string", "minLength": 1, "maxLength": 64},
			"entity_address":  map[string]any{"type": "array", "minItems": 1, "maxItems": 16, "items": eebusV1SafeIntegerSchema(0)},
			"feature_address": eebusV1SafeIntegerSchema(0),
			"feature_type":    map[string]any{"type": "string", "minLength": 1, "maxLength": 128},
			"feature_role":    map[string]any{"type": "string", "enum": []string{"client", "server", "special"}},
		},
		[]string{
			"remote_ski", "ship_id", "device_address", "entity_address",
			"feature_address", "feature_type", "feature_role",
		},
	)
}

func eebusV1FeatureTargetSchema(operation eebusraw.OperationV1) map[string]any {
	locator := eebusV1FeatureLocatorSchema()
	properties := locator["properties"].(map[string]any)
	properties["function"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 256}
	properties["operation"] = map[string]any{"type": "string", "const": string(operation)}
	required := append(locator["required"].([]string), "function", "operation")
	locator["required"] = required
	return locator
}

func eebusV1ClosedSchema(properties map[string]any, required []string) map[string]any {
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
		"x-secret-classifier":  "eebusraw.canonical.v1",
	}
	if len(required) != 0 {
		schema["required"] = required
	}
	return schema
}

func eebusV1SafeIntegerSchema(minimum int64) map[string]any {
	return map[string]any{
		"type": "integer", "minimum": minimum, "maximum": int64(9007199254740991),
	}
}

func eebusV1TypedValueSchema(depth int) map[string]any {
	scalar := map[string]any{
		"oneOf": []any{
			map[string]any{"type": "null"},
			map[string]any{"type": "boolean"},
			eebusV1SafeIntegerSchema(-9007199254740991),
			map[string]any{"type": "string", "maxLength": 16384},
		},
	}
	if depth > 4 {
		return scalar
	}
	return map[string]any{
		"oneOf": []any{
			scalar,
			map[string]any{
				"type": "array", "maxItems": 256, "items": eebusV1TypedValueSchema(depth + 1),
			},
			map[string]any{
				"type": "object", "maxProperties": 256,
				"propertyNames":        map[string]any{"type": "string", "maxLength": 256},
				"additionalProperties": eebusV1TypedValueSchema(depth + 1),
			},
		},
	}
}
