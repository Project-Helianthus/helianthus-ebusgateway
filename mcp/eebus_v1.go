package mcp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	eebusV1RuntimeStatusTool   = "eebus.v1.runtime.status.get"
	eebusV1ServicesListTool    = "eebus.v1.services.list"
	eebusV1ServicesGetTool     = "eebus.v1.services.get"
	eebusV1SessionsListTool    = "eebus.v1.sessions.list"
	eebusV1SessionsGetTool     = "eebus.v1.sessions.get"
	eebusV1TopologyGetTool     = "eebus.v1.topology.get"
	eebusV1SnapshotCaptureTool = "eebus.v1.snapshot.capture"
	eebusV1SnapshotDropTool    = "eebus.v1.snapshot.drop"
	eebusV1PairingStatusTool   = "eebus.v1.pairing.status.get"

	eebusV1DefaultLiveTimeout = 3 * time.Second
	eebusV1MaxLiveWorkers     = 8
	eebusV1TokenPattern       = `^[A-Za-z0-9_-]{43}$`
)

// EEBusV1Provider is the MSP-06 typed, read-only seam and the only MCP
// production boundary allowed to import the eeBUS runtime package.
type EEBusV1Provider interface {
	Snapshot() (eebusruntime.SnapshotV1, error)
}

type eebusV1RegistrationOptions struct {
	now          func() time.Time
	entropy      io.Reader
	pseudonymKey []byte
	liveTimeout  time.Duration
}

type eebusV1Runtime struct {
	provider     EEBusV1Provider
	now          func() time.Time
	pseudonymKey []byte
	liveTimeout  time.Duration
	liveWorkers  chan struct{}
	store        *eebusV1SnapshotStore
}

type eebusV1ToolSpec struct {
	name        string
	scope       string
	description string
	properties  []string
	required    []string
}

var eebusV1ToolSpecs = []eebusV1ToolSpec{
	{name: eebusV1RuntimeStatusTool, scope: "runtime-status", description: "Get the redacted eeBUS runtime status.", properties: []string{"evidence_ref"}},
	{name: eebusV1ServicesListTool, scope: "services", description: "List redacted eeBUS services.", properties: []string{"evidence_ref"}},
	{name: eebusV1ServicesGetTool, scope: "service", description: "Get one redacted eeBUS service.", properties: []string{"evidence_ref", "id_digest"}, required: []string{"id_digest"}},
	{name: eebusV1SessionsListTool, scope: "sessions", description: "List redacted eeBUS sessions.", properties: []string{"evidence_ref"}},
	{name: eebusV1SessionsGetTool, scope: "session", description: "Get one redacted eeBUS session.", properties: []string{"evidence_ref", "id_digest"}, required: []string{"id_digest"}},
	{name: eebusV1TopologyGetTool, scope: "topology", description: "Get the redacted eeBUS topology.", properties: []string{"evidence_ref"}},
	{name: eebusV1SnapshotCaptureTool, scope: "whole-root", description: "Capture one immutable redacted eeBUS evidence root."},
	{name: eebusV1SnapshotDropTool, scope: "whole-root", description: "Drop one immutable eeBUS evidence root.", properties: []string{"snapshot_ref"}, required: []string{"snapshot_ref"}},
	{name: eebusV1PairingStatusTool, scope: "pairing-status", description: "Get redacted eeBUS pairing status.", properties: []string{"evidence_ref"}},
}

type eebusV1MetaV1 struct {
	Contract      eebusV1ContractV1          `json:"contract"`
	Tool          string                     `json:"tool"`
	Scope         string                     `json:"scope"`
	MaskTier      string                     `json:"mask_tier"`
	AuthScope     string                     `json:"auth_scope"`
	Mode          string                     `json:"mode"`
	DataTimestamp string                     `json:"data_timestamp"`
	DataHash      string                     `json:"data_hash"`
	Runtime       eebusV1RuntimeStatusDataV1 `json:"runtime"`
}

type eebusV1ErrorV1 struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	Retriable   bool   `json:"retriable"`
	SourceLayer string `json:"source_layer"`
}

type eebusV1EnvelopeV1 struct {
	Meta  eebusV1MetaV1   `json:"meta"`
	Data  any             `json:"data"`
	Error *eebusV1ErrorV1 `json:"error"`
}

type eebusV1HashView struct {
	Contract      eebusV1ContractV1         `json:"contract"`
	Tool          string                    `json:"tool"`
	Scope         string                    `json:"scope"`
	MaskTier      string                    `json:"mask_tier"`
	AuthScope     string                    `json:"auth_scope"`
	Mode          string                    `json:"mode"`
	DataTimestamp string                    `json:"data_timestamp"`
	RuntimeState  string                    `json:"runtime_state"`
	Degradation   *eebusV1DegradationDataV1 `json:"degradation"`
	Data          any                       `json:"data"`
	Error         *eebusV1ErrorV1           `json:"error"`
}

func (server *Server) RegisterEEBusV1Provider(provider EEBusV1Provider) error {
	key := make([]byte, sha256.Size)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return errors.New("generate eeBUS MCP pseudonym key")
	}
	return server.registerEEBusV1Provider(provider, eebusV1RegistrationOptions{
		now: time.Now, entropy: rand.Reader, pseudonymKey: key, liveTimeout: eebusV1DefaultLiveTimeout,
	})
}

func (server *Server) registerEEBusV1Provider(provider EEBusV1Provider, options eebusV1RegistrationOptions) error {
	if server == nil {
		return errors.New("eeBUS MCP server is nil")
	}
	if eebusV1NilProvider(provider) {
		return errors.New("eeBUS MCP provider is nil")
	}
	if options.now == nil {
		options.now = time.Now
	}
	if options.entropy == nil {
		options.entropy = rand.Reader
	}
	if options.liveTimeout <= 0 {
		options.liveTimeout = eebusV1DefaultLiveTimeout
	}
	if len(options.pseudonymKey) != sha256.Size {
		return errors.New("eeBUS MCP pseudonym key must contain 32 bytes")
	}
	server.eebusV1Mu.Lock()
	defer server.eebusV1Mu.Unlock()
	if server.eebusV1 != nil {
		return errors.New("eeBUS MCP provider is already registered")
	}
	runtime := &eebusV1Runtime{
		provider:     provider,
		now:          options.now,
		pseudonymKey: append([]byte(nil), options.pseudonymKey...),
		liveTimeout:  options.liveTimeout,
		liveWorkers:  make(chan struct{}, eebusV1MaxLiveWorkers),
	}
	runtime.store = newEEBusV1SnapshotStore(runtime.now, options.entropy)
	server.eebusV1 = runtime
	server.tools = append(server.tools, eebusV1Tools()...)
	return nil
}

func eebusV1NilProvider(provider EEBusV1Provider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func eebusV1Tools() []Tool {
	tools := make([]Tool, 0, len(eebusV1ToolSpecs))
	for _, spec := range eebusV1ToolSpecs {
		properties := make(map[string]any, len(spec.properties))
		for _, property := range spec.properties {
			properties[property] = map[string]any{"type": "string", "pattern": eebusV1TokenPattern}
		}
		schema := map[string]any{
			"type":                 "object",
			"properties":           properties,
			"additionalProperties": false,
		}
		if len(spec.required) != 0 {
			schema["required"] = append([]string(nil), spec.required...)
		}
		tools = append(tools, Tool{Name: spec.name, Description: spec.description, InputSchema: schema})
	}
	return tools
}

func (server *Server) handleEEBusV1Call(ctx context.Context, name string, arguments map[string]any) (any, bool) {
	server.eebusV1Mu.RLock()
	runtime := server.eebusV1
	server.eebusV1Mu.RUnlock()
	if runtime == nil {
		return nil, false
	}
	spec, ok := eebusV1Spec(name)
	if !ok {
		return nil, false
	}
	envelope := runtime.call(ctx, spec, arguments)
	encoded, err := json.Marshal(envelope)
	if err != nil {
		envelope = runtime.errorEnvelope(spec, "live", runtime.now().UTC(), eebusV1FallbackRuntime(), "contract_violation")
		encoded, _ = json.Marshal(envelope)
	}
	return callToolResultText(string(encoded), envelope.Error != nil), true
}

func eebusV1Spec(name string) (eebusV1ToolSpec, bool) {
	for _, spec := range eebusV1ToolSpecs {
		if spec.name == name {
			return spec, true
		}
	}
	return eebusV1ToolSpec{}, false
}

func (runtime *eebusV1Runtime) call(ctx context.Context, spec eebusV1ToolSpec, arguments map[string]any) eebusV1EnvelopeV1 {
	validated, code := eebusV1ValidateArguments(spec, arguments)
	if code != "" {
		return runtime.errorEnvelope(spec, "live", runtime.now().UTC(), eebusV1FallbackRuntime(), code)
	}
	if spec.name == eebusV1SnapshotDropTool {
		result := runtime.store.drop(validated.snapshotRef)
		return runtime.envelope(spec, "live", runtime.now().UTC(), eebusV1FallbackRuntime(), result, nil)
	}
	if validated.evidenceRef != "" {
		lookup := runtime.store.lookup(validated.evidenceRef, spec.name, spec.scope)
		status, timestamp := lookup.Runtime, lookup.Timestamp
		if timestamp == "" {
			timestamp = eebusV1Timestamp(runtime.now())
			status = eebusV1FallbackRuntime()
		}
		if lookup.ErrorCode != "" {
			return runtime.errorEnvelopeAt(spec, "evidence", timestamp, status, lookup.ErrorCode)
		}
		data, code := eebusV1DataForTool(spec.name, *lookup.Projection, validated.idDigest)
		if code != "" {
			return runtime.errorEnvelopeAt(spec, "evidence", timestamp, status, code)
		}
		return runtime.envelopeAt(spec, "evidence", timestamp, status, data, nil)
	}

	return runtime.liveCall(ctx, spec, validated)
}

func (runtime *eebusV1Runtime) liveCall(ctx context.Context, spec eebusV1ToolSpec, validated eebusV1ValidatedArguments) eebusV1EnvelopeV1 {
	if ctx == nil {
		ctx = context.Background()
	}
	liveCtx, cancel := context.WithTimeout(ctx, runtime.liveTimeout)
	defer cancel()

	select {
	case runtime.liveWorkers <- struct{}{}:
	case <-liveCtx.Done():
		return runtime.errorEnvelope(spec, "live", runtime.now(), eebusV1FallbackRuntime(), "timeout")
	}
	if liveCtx.Err() != nil {
		<-runtime.liveWorkers
		return runtime.errorEnvelope(spec, "live", runtime.now(), eebusV1FallbackRuntime(), "timeout")
	}

	result := make(chan eebusV1EnvelopeV1, 1)
	go func() {
		defer func() { <-runtime.liveWorkers }()
		result <- runtime.buildLiveEnvelope(spec, validated)
	}()

	select {
	case envelope := <-result:
		return envelope
	case <-liveCtx.Done():
		return runtime.errorEnvelope(spec, "live", runtime.now(), eebusV1FallbackRuntime(), "timeout")
	}
}

func (runtime *eebusV1Runtime) buildLiveEnvelope(spec eebusV1ToolSpec, validated eebusV1ValidatedArguments) eebusV1EnvelopeV1 {
	projection, code := runtime.liveProjection()
	if code != "" {
		return runtime.errorEnvelope(spec, "live", runtime.now(), eebusV1FallbackRuntime(), code)
	}
	if spec.name == eebusV1SnapshotCaptureTool {
		captured, code := runtime.store.capture(projection)
		if code != "" {
			return runtime.errorEnvelopeAt(spec, "live", projection.DataTimestamp, projection.Runtime, code)
		}
		return runtime.envelopeAt(spec, "live", projection.DataTimestamp, projection.Runtime, captured, nil)
	}
	data, code := eebusV1DataForTool(spec.name, projection, validated.idDigest)
	if code != "" {
		return runtime.errorEnvelopeAt(spec, "live", projection.DataTimestamp, projection.Runtime, code)
	}
	return runtime.envelopeAt(spec, "live", projection.DataTimestamp, projection.Runtime, data, nil)
}

type eebusV1ValidatedArguments struct {
	evidenceRef string
	snapshotRef string
	idDigest    string
}

func eebusV1ValidateArguments(spec eebusV1ToolSpec, arguments map[string]any) (eebusV1ValidatedArguments, string) {
	allowed := make(map[string]bool, len(spec.properties))
	for _, property := range spec.properties {
		allowed[property] = true
	}
	for key := range arguments {
		if !allowed[key] {
			return eebusV1ValidatedArguments{}, "invalid_argument"
		}
	}
	for _, required := range spec.required {
		if _, exists := arguments[required]; !exists {
			return eebusV1ValidatedArguments{}, "invalid_argument"
		}
	}

	var result eebusV1ValidatedArguments
	if raw, exists := arguments["evidence_ref"]; exists {
		value, ok := eebusV1ParseCanonicalToken(raw)
		if !ok {
			return eebusV1ValidatedArguments{}, "invalid_argument"
		}
		result.evidenceRef = value
	}
	if raw, exists := arguments["snapshot_ref"]; exists {
		value, ok := eebusV1ParseCanonicalToken(raw)
		if !ok {
			return eebusV1ValidatedArguments{}, "invalid_argument"
		}
		result.snapshotRef = value
	}
	if raw, exists := arguments["id_digest"]; exists {
		value, ok := eebusV1ParseCanonicalToken(raw)
		if !ok {
			return eebusV1ValidatedArguments{}, "invalid_argument"
		}
		result.idDigest = value
	}
	return result, ""
}

func (runtime *eebusV1Runtime) liveProjection() (eebusV1Projection, string) {
	snapshot, err := runtime.provider.Snapshot()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return eebusV1Projection{}, "timeout"
		}
		return eebusV1Projection{}, "backend_unavailable"
	}
	if err := eebusV1ValidateProviderCollectionBounds(snapshot); err != nil {
		return eebusV1Projection{}, "contract_violation"
	}
	projection, err := eebusV1ProjectSnapshot(snapshot, runtime.pseudonymKey)
	if err != nil {
		return eebusV1Projection{}, "contract_violation"
	}
	return projection, ""
}

func eebusV1ValidateProviderCollectionBounds(snapshot eebusruntime.SnapshotV1) error {
	if err := eebusV1ValidateCollectionSizes(
		len(snapshot.Pairing),
		len(snapshot.Services),
		len(snapshot.Sessions),
		len(snapshot.Topology.Devices),
		len(snapshot.Raw),
	); err != nil {
		return err
	}
	for _, pairing := range snapshot.Pairing {
		if err := eebusV1ValidateCollectionSizes(len(pairing.Raw)); err != nil {
			return err
		}
	}
	for _, service := range snapshot.Services {
		if err := eebusV1ValidateCollectionSizes(len(service.Raw)); err != nil {
			return err
		}
	}
	for _, session := range snapshot.Sessions {
		if err := eebusV1ValidateCollectionSizes(len(session.Raw)); err != nil {
			return err
		}
	}
	for _, device := range snapshot.Topology.Devices {
		if err := eebusV1ValidateCollectionSizes(len(device.Entities), len(device.UseCaseClaims), len(device.Raw)); err != nil {
			return err
		}
		for _, entity := range device.Entities {
			if err := eebusV1ValidateCollectionSizes(len(entity.Features), len(entity.Raw)); err != nil {
				return err
			}
			for _, feature := range entity.Features {
				if err := eebusV1ValidateCollectionSizes(len(feature.Raw)); err != nil {
					return err
				}
			}
		}
		for _, claim := range device.UseCaseClaims {
			if err := eebusV1ValidateCollectionSizes(len(claim.Raw)); err != nil {
				return err
			}
		}
	}
	return nil
}

func eebusV1DataForTool(name string, projection eebusV1Projection, idDigest string) (any, string) {
	switch name {
	case eebusV1RuntimeStatusTool:
		return projection.Runtime, ""
	case eebusV1ServicesListTool:
		return eebusV1ServicesListDataV1{Services: projection.Snapshot.Services}, ""
	case eebusV1ServicesGetTool:
		for _, service := range projection.Snapshot.Services {
			if service.ID.Digest == idDigest {
				return service, ""
			}
		}
		return nil, "not_found"
	case eebusV1SessionsListTool:
		return eebusV1SessionsListDataV1{Sessions: projection.Snapshot.Sessions}, ""
	case eebusV1SessionsGetTool:
		for _, session := range projection.Snapshot.Sessions {
			if session.ID.Digest == idDigest {
				return session, ""
			}
		}
		return nil, "not_found"
	case eebusV1TopologyGetTool:
		return projection.Snapshot.Topology, ""
	case eebusV1PairingStatusTool:
		return eebusV1PairingStatusDataV1{Pairing: projection.Snapshot.Pairing}, ""
	default:
		return nil, "contract_violation"
	}
}

func (runtime *eebusV1Runtime) errorEnvelope(spec eebusV1ToolSpec, mode string, timestamp time.Time, status eebusV1RuntimeStatusDataV1, code string) eebusV1EnvelopeV1 {
	return runtime.errorEnvelopeAt(spec, mode, eebusV1Timestamp(timestamp), status, code)
}

func (runtime *eebusV1Runtime) errorEnvelopeAt(spec eebusV1ToolSpec, mode, timestamp string, status eebusV1RuntimeStatusDataV1, code string) eebusV1EnvelopeV1 {
	public, ok := eebusV1PublicErrorForCode(code)
	if !ok {
		public, _ = eebusV1PublicErrorForCode("contract_violation")
	}
	return runtime.envelopeAt(spec, mode, timestamp, status, nil, &public)
}

func (runtime *eebusV1Runtime) envelope(spec eebusV1ToolSpec, mode string, timestamp time.Time, status eebusV1RuntimeStatusDataV1, data any, publicError *eebusV1ErrorV1) eebusV1EnvelopeV1 {
	return runtime.envelopeAt(spec, mode, eebusV1Timestamp(timestamp), status, data, publicError)
}

func (runtime *eebusV1Runtime) envelopeAt(spec eebusV1ToolSpec, mode, timestamp string, status eebusV1RuntimeStatusDataV1, data any, publicError *eebusV1ErrorV1) eebusV1EnvelopeV1 {
	view := eebusV1HashView{
		Contract: eebusV1Contract, Tool: spec.name, Scope: spec.scope,
		MaskTier: eebusV1MaskTier, AuthScope: eebusV1AuthScope, Mode: mode,
		DataTimestamp: timestamp, RuntimeState: status.State, Degradation: status.Degradation,
		Data: data, Error: publicError,
	}
	encoded, err := json.Marshal(view)
	hash := ""
	if err == nil {
		exclusions := []string(nil)
		if spec.name == eebusV1SnapshotCaptureTool {
			exclusions = []string{
				"/data/snapshot_ref", "/data/expires_at", "/data/evidence_refs", "/data/snapshot/meta/captured_at",
			}
		}
		_, hash, err = eebusV1CanonicalHashJSON(encoded, exclusions...)
	}
	if err != nil {
		public, _ := eebusV1PublicErrorForCode("contract_violation")
		publicError = &public
		data = nil
		fallback := eebusV1HashView{
			Contract: eebusV1Contract, Tool: spec.name, Scope: spec.scope,
			MaskTier: eebusV1MaskTier, AuthScope: eebusV1AuthScope, Mode: mode,
			DataTimestamp: timestamp, RuntimeState: status.State, Degradation: status.Degradation,
			Data: nil, Error: publicError,
		}
		encoded, _ = json.Marshal(fallback)
		_, hash, _ = eebusV1CanonicalHashJSON(encoded)
	}
	return eebusV1EnvelopeV1{
		Meta: eebusV1MetaV1{
			Contract: eebusV1Contract, Tool: spec.name, Scope: spec.scope,
			MaskTier: eebusV1MaskTier, AuthScope: eebusV1AuthScope, Mode: mode,
			DataTimestamp: timestamp, DataHash: hash, Runtime: status,
		},
		Data: data, Error: publicError,
	}
}

func eebusV1FallbackRuntime() eebusV1RuntimeStatusDataV1 {
	return eebusV1RuntimeStatusDataV1{State: "stopped"}
}

func eebusV1PublicErrorForCode(code string) (eebusV1ErrorV1, bool) {
	errorsByCode := map[string]eebusV1ErrorV1{
		"invalid_argument":    {Code: "invalid_argument", Message: "invalid argument", SourceLayer: "mcp"},
		"not_found":           {Code: "not_found", Message: "not found", SourceLayer: "mcp"},
		"permission_denied":   {Code: "permission_denied", Message: "permission denied", SourceLayer: "policy"},
		"admin_required":      {Code: "admin_required", Message: "administrator authorization required", SourceLayer: "policy"},
		"backend_unavailable": {Code: "backend_unavailable", Message: "eeBUS runtime unavailable", Retriable: true, SourceLayer: "eebusruntime"},
		"timeout":             {Code: "timeout", Message: "eeBUS runtime request timed out", Retriable: true, SourceLayer: "eebusruntime"},
		"snapshot_gone":       {Code: "snapshot_gone", Message: "snapshot no longer available", SourceLayer: "snapshot-store"},
		"quota_exceeded":      {Code: "quota_exceeded", Message: "snapshot quota exceeded", Retriable: true, SourceLayer: "snapshot-store"},
		"contract_violation":  {Code: "contract_violation", Message: "eeBUS MCP contract violation", SourceLayer: "mcp"},
	}
	public, ok := errorsByCode[code]
	return public, ok
}

func (runtime *eebusV1Runtime) String() string {
	return fmt.Sprintf("eeBUS MCP v1 runtime (%d tools)", len(eebusV1ToolSpecs))
}
