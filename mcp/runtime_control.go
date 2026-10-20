package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// The G5 runtime lifecycle tools (D4). They are host tools on the ordinary
// MCP surface, not driver contributions, and carry no principal, authority,
// idempotency key or receipt.
const (
	RuntimeControlListTool    = "helianthus.runtime.list"
	RuntimeControlGetTool     = "helianthus.runtime.get"
	RuntimeControlStartTool   = "helianthus.runtime.start"
	RuntimeControlStopTool    = "helianthus.runtime.stop"
	RuntimeControlRestartTool = "helianthus.runtime.restart"
)

// runtimeControlCallTimeout bounds a mutation whose transport context has no
// deadline; an earlier caller deadline is kept. It exceeds the store's bounded
// namespace-wide serialization wait, so contention answers TRANSITION_BUSY
// rather than DEADLINE_EXCEEDED, and it never adds an argument member.
const runtimeControlCallTimeout = 10 * time.Second

const (
	runtimeControlDriverIDPattern         = `^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`
	runtimeControlDriverIDMaxBytes        = 128
	runtimeControlExpectedRevisionPattern = `^[1-9][0-9]{0,19}$`
)

// RuntimeControlService is the consumer-side view of the internal
// desired-state service; *runtimecontrol.Service implements it. Reads are
// cached views that construct nothing and perform no endpoint I/O. Control
// applies the service's closed request decoding and then Decide.
type RuntimeControlService interface {
	List() runtimecontrol.Catalog
	Get(id runtimecontrol.DriverID) (runtimecontrol.DriverStatus, error)
	Control(ctx context.Context, method runtimecontrol.Method, body []byte) (runtimecontrol.DesiredView, error)
}

var (
	errRuntimeControlServerMissing = errors.New("runtime control tools need an MCP server")
	errRuntimeControlRegistered    = errors.New("runtime control tools are already registered")
)

// RegisterRuntimeControlTools appends the five lifecycle tools to the ordinary
// tool inventory. Registration does not depend on driver state or on eeBUS;
// with a nil service the tools stay listed and answer UNAVAILABLE.
func RegisterRuntimeControlTools(server *Server, service RuntimeControlService) error {
	if server == nil {
		return errRuntimeControlServerMissing
	}
	if runtimeControlServiceNil(service) {
		service = nil
	}
	server.runtimeControlMu.Lock()
	defer server.runtimeControlMu.Unlock()
	if server.runtimeControlRegistered {
		return errRuntimeControlRegistered
	}
	server.runtimeControlRegistered = true
	server.runtimeControl = service
	server.tools = append(server.tools, runtimeControlTools()...)
	return nil
}

func runtimeControlServiceNil(service RuntimeControlService) bool {
	if service == nil {
		return true
	}
	value := reflect.ValueOf(service)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *Server) runtimeControlService() RuntimeControlService {
	s.runtimeControlMu.RLock()
	defer s.runtimeControlMu.RUnlock()
	return s.runtimeControl
}

func runtimeControlMethod(name string) (runtimecontrol.Method, bool) {
	switch name {
	case RuntimeControlStartTool:
		return runtimecontrol.MethodStart, true
	case RuntimeControlStopTool:
		return runtimecontrol.MethodStop, true
	case RuntimeControlRestartTool:
		return runtimecontrol.MethodRestart, true
	default:
		return "", false
	}
}

func isRuntimeControlTool(name string) bool {
	if name == RuntimeControlListTool || name == RuntimeControlGetTool {
		return true
	}
	_, ok := runtimeControlMethod(name)
	return ok
}

func runtimeControlDriverIDSchema() map[string]any {
	return map[string]any{
		"type":        "string",
		"description": "Canonical configured driver identity.",
		"pattern":     runtimeControlDriverIDPattern,
		"maxLength":   runtimeControlDriverIDMaxBytes,
	}
}

func runtimeControlMutationSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"DriverID": runtimeControlDriverIDSchema(),
			"ExpectedRevision": map[string]any{
				"type":        "string",
				"description": "The driver's desired_revision read back from list or get: a canonical nonzero unsigned 64-bit decimal string.",
				"pattern":     runtimeControlExpectedRevisionPattern,
			},
		},
		"required":             []string{"DriverID", "ExpectedRevision"},
		"additionalProperties": false,
	}
}

func runtimeControlTools() []Tool {
	return []Tool{
		{
			Name:        RuntimeControlListTool,
			Description: "List every registered runtime driver from one coherent cached view, sorted by driver_id.",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		},
		{
			Name:        RuntimeControlGetTool,
			Description: "Get one registered runtime driver from the coherent cached view.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"DriverID": runtimeControlDriverIDSchema(),
				},
				"required":             []string{"DriverID"},
				"additionalProperties": false,
			},
		},
		{
			Name:        RuntimeControlStartTool,
			Description: "Request desired RUNNING for one driver. Answers after durable acceptance with desired state and revision only; observe completion through list or get.",
			InputSchema: runtimeControlMutationSchema(),
		},
		{
			Name:        RuntimeControlStopTool,
			Description: "Request desired STOPPED for one driver. Answers after durable acceptance with desired state and revision only; observe completion through list or get.",
			InputSchema: runtimeControlMutationSchema(),
		},
		{
			Name:        RuntimeControlRestartTool,
			Description: "Request one replacement of a driver whose desired state is RUNNING. Every accepted call takes a new desired revision; after a lost response, read back before retrying.",
			InputSchema: runtimeControlMutationSchema(),
		},
	}
}

// runtimeControlDriverV1 is the wire projection of one cached DriverStatus,
// in the contract's field order.
type runtimeControlDriverV1 struct {
	DriverID          string `json:"driver_id"`
	Enabled           bool   `json:"enabled"`
	DesiredState      string `json:"desired_state"`
	DesiredRevision   string `json:"desired_revision"`
	ObservedState     string `json:"observed_state"`
	Reason            string `json:"reason"`
	SafetyQuarantined bool   `json:"safety_quarantined"`
	Revision          string `json:"revision"`
}

type runtimeControlListDataV1 struct {
	CatalogRevision string                   `json:"catalog_revision"`
	Drivers         []runtimeControlDriverV1 `json:"drivers"`
}

type runtimeControlGetDataV1 struct {
	CatalogRevision string                 `json:"catalog_revision"`
	Driver          runtimeControlDriverV1 `json:"driver"`
}

func runtimeControlDriver(status runtimecontrol.DriverStatus) runtimeControlDriverV1 {
	return runtimeControlDriverV1{
		DriverID:          string(status.DriverID),
		Enabled:           status.Enabled,
		DesiredState:      string(status.DesiredState),
		DesiredRevision:   strconv.FormatUint(uint64(status.DesiredRevision), 10),
		ObservedState:     string(status.ObservedState),
		Reason:            string(status.Reason),
		SafetyQuarantined: status.SafetyQuarantined,
		Revision:          strconv.FormatUint(uint64(status.Revision), 10),
	}
}

func runtimeControlListData(catalog runtimecontrol.Catalog) runtimeControlListDataV1 {
	drivers := make([]runtimeControlDriverV1, 0, len(catalog.Drivers))
	for _, status := range catalog.Drivers {
		drivers = append(drivers, runtimeControlDriver(status))
	}
	return runtimeControlListDataV1{
		CatalogRevision: strconv.FormatUint(uint64(catalog.Revision), 10),
		Drivers:         drivers,
	}
}

// runtimeControlArguments is the closed decoding of refusal-table row 1 for
// the reads: one JSON object with exactly the named members, each a JSON
// string, no duplicate member and no trailing data. Duplicates are detected
// on the token stream, before any decoding could discard one.
func runtimeControlArguments(raw json.RawMessage, names ...string) (map[string]string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}
	members := make(map[string]string, len(names))
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, false
		}
		if _, duplicate := members[key]; duplicate {
			return nil, false
		}
		valueToken, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		value, ok := valueToken.(string)
		if !ok {
			return nil, false
		}
		members[key] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	if len(members) != len(names) {
		return nil, false
	}
	for _, name := range names {
		if _, ok := members[name]; !ok {
			return nil, false
		}
	}
	return members, true
}

// runtimeControlToolError is the envelope error of a lifecycle refusal. Its
// message is categorical; refusal reasons and raw errors never reach the wire
// because they can carry storage paths or native detail.
type runtimeControlToolError struct {
	code      string
	message   string
	retriable bool
}

func runtimeControlInvalidArgument() *runtimeControlToolError {
	return runtimeControlRefusalError(&runtimecontrol.Refusal{Code: runtimecontrol.RefusalInvalidArgument, Row: 1})
}

func runtimeControlUnavailable() *runtimeControlToolError {
	return runtimeControlRefusalError(&runtimecontrol.Refusal{Code: runtimecontrol.RefusalUnavailable, Row: 3})
}

func runtimeControlRefusalError(refusal *runtimecontrol.Refusal) *runtimeControlToolError {
	message := ""
	switch refusal.Code {
	case runtimecontrol.RefusalInvalidArgument:
		message = "arguments are not the closed object this tool accepts"
		if refusal.Row == 5 {
			message = "call has no finite deadline"
		}
	case runtimecontrol.RefusalNotFound:
		message = "driver is not registered"
	case runtimecontrol.RefusalFailedPrecondition:
		switch refusal.Row {
		case 3:
			message = "driver control state is unusable until offline repair"
		case 4:
			message = "driver is disabled by configuration"
		case 6:
			message = "RESTART requires desired RUNNING"
		case 7:
			message = "quarantine or unresolved recovery blocks admission"
		default:
			message = "lifecycle precondition is not met"
		}
	case runtimecontrol.RefusalUnavailable:
		message = "control admission is closed; read back before retrying"
		if refusal.Row == 0 {
			message = "persistence failed before commit; nothing was accepted"
		}
	case runtimecontrol.RefusalDeadlineExceeded:
		message = "deadline expired before a decision; read back if delivery was uncertain"
	case runtimecontrol.RefusalRevisionConflict:
		message = "expected revision is not the desired revision; read back before deciding again"
	case runtimecontrol.RefusalTransitionBusy:
		message = "decision path is busy; retry later"
	case runtimecontrol.RefusalResourceExhausted:
		message = "control storage capacity is exhausted"
	case runtimecontrol.RefusalCounterExhausted:
		message = "a required revision counter cannot advance"
	default:
		return runtimeControlUnknownOutcome()
	}
	return &runtimeControlToolError{code: string(refusal.Code), message: message, retriable: refusal.Retryable()}
}

// runtimeControlUnknownOutcome answers an error outside the refusal table
// (for example an unresolved readback or a closed service). The decision may
// or may not have persisted, so the caller reads back before retrying.
func runtimeControlUnknownOutcome() *runtimeControlToolError {
	return &runtimeControlToolError{
		code:      string(runtimecontrol.RefusalUnavailable),
		message:   "control decision outcome is unavailable; read back before retrying",
		retriable: true,
	}
}

// runtimeControlFailure maps a service error to its envelope error and data.
// Only REVISION_CONFLICT carries data: the refusal's cached desired view.
func runtimeControlFailure(err error) (any, *runtimeControlToolError) {
	var refusal *runtimecontrol.Refusal
	if errors.As(err, &refusal) && refusal != nil {
		toolErr := runtimeControlRefusalError(refusal)
		if refusal.Code == runtimecontrol.RefusalRevisionConflict && refusal.View != nil {
			return *refusal.View, toolErr
		}
		return nil, toolErr
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, runtimeControlRefusalError(&runtimecontrol.Refusal{Code: runtimecontrol.RefusalDeadlineExceeded, Row: 5})
	}
	return nil, runtimeControlUnknownOutcome()
}

func runtimeControlResult(data any, toolErr *runtimeControlToolError) map[string]any {
	envelope := newToolEnvelope(data, nil)
	if toolErr == nil {
		return callToolResultText(mustJSON(envelope), false)
	}
	envelope["error"] = map[string]any{
		"code":         toolErr.code,
		"message":      toolErr.message,
		"retriable":    toolErr.retriable,
		"source_layer": "gateway",
	}
	return callToolResultText(mustJSON(envelope), true)
}

// handleRuntimeControlRawCall dispatches one lifecycle tool from the raw call
// parameters. malformedParams reports duplicate members anywhere in the call
// parameters. Omitted arguments read as the empty object; an explicit null or
// any other non-object is refused at row 1.
func (s *Server) handleRuntimeControlRawCall(ctx context.Context, name string, arguments json.RawMessage, malformedParams bool) map[string]any {
	if malformedParams {
		return runtimeControlResult(nil, runtimeControlInvalidArgument())
	}
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	service := s.runtimeControlService()
	switch name {
	case RuntimeControlListTool:
		if _, ok := runtimeControlArguments(arguments); !ok {
			return runtimeControlResult(nil, runtimeControlInvalidArgument())
		}
		if service == nil {
			return runtimeControlResult(nil, runtimeControlUnavailable())
		}
		return runtimeControlResult(runtimeControlListData(service.List()), nil)
	case RuntimeControlGetTool:
		members, ok := runtimeControlArguments(arguments, "DriverID")
		if !ok {
			return runtimeControlResult(nil, runtimeControlInvalidArgument())
		}
		if service == nil {
			return runtimeControlResult(nil, runtimeControlUnavailable())
		}
		status, err := service.Get(runtimecontrol.DriverID(members["DriverID"]))
		if err != nil {
			return runtimeControlResult(runtimeControlFailure(err))
		}
		return runtimeControlResult(runtimeControlGetDataV1{
			CatalogRevision: strconv.FormatUint(uint64(status.Revision), 10),
			Driver:          runtimeControlDriver(status),
		}, nil)
	}
	method, ok := runtimeControlMethod(name)
	if !ok {
		return runtimeControlResult(nil, runtimeControlInvalidArgument())
	}
	if service == nil {
		if _, err := runtimecontrol.ParseRequest(arguments); err != nil {
			return runtimeControlResult(runtimeControlFailure(err))
		}
		return runtimeControlResult(nil, runtimeControlUnavailable())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := context.WithTimeout(ctx, runtimeControlCallTimeout)
	defer cancel()
	view, err := service.Control(callCtx, method, arguments)
	if err != nil {
		return runtimeControlResult(runtimeControlFailure(err))
	}
	return runtimeControlResult(view, nil)
}
