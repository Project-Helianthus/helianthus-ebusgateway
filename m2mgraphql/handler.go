// Package m2mgraphql exposes the single, versioned machine-to-machine
// canonical-PV query. It intentionally is not a general GraphQL engine.
package m2mgraphql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
)

const (
	route             = "/graphql/m2m/v1"
	contractID        = "PUBLIC_GRAPHQL_M2M_V1"
	maxRequestBytes   = 16 << 10
	maxResponseBytes  = 1 << 20
	maxJSONDepth      = 64
	maxQueryDepth     = 8
	maxSelectedFields = 256
	maxFacts          = 256
	maxProjectionRows = 512
)

const fixedQuery = `query M2MCurrentSnapshot($request: M2MCurrentSnapshotRequest!) {
  m2mCurrentSnapshot(request: $request) {
    contractId canonicalContractId assetRef generation producedAt
    evaluatedMonotonicNs sourceTimeState currentSourceOriginRef
    facts {
      factId
      dimension {
        ... on M2MScopeDimension { scope }
        ... on M2MPhaseDimension { phase }
        ... on M2MPhasePairDimension { phasePair }
        ... on M2MInputDimension { inputId }
        ... on M2MSensorDimension { sensorId }
      }
      value {
        ... on M2MDecimalValue { coefficient scale }
        ... on M2MEnumValue { symbol }
        ... on M2MBitfieldValue { symbols }
      }
      unit quality availability freshness receiptMonotonicNs
      freshUntilMonotonicNs retainUntilMonotonicNs freshnessPolicy originRef
      continuity {
        __typename
        ... on M2MBaselineContinuity { baseline }
        ... on M2MContiguousContinuity { delta { coefficient scale } }
        ... on M2MRolloverContinuity {
          delta { coefficient scale } modulus { coefficient scale }
          rolloverEvidenceRef
        }
        ... on M2MResetContinuity { resetEvidenceRef }
        ... on M2MDiscontinuityContinuity { discontinuityEvidenceRef }
      }
    }
    capabilities { id outcome }
    provenance {
      originRef sourceProtocol sourceProfileId sourceProfileVersion sourceValidity
      sourceRegistryRef sourceObservationRef evidenceRef
    }
    requestedOutputs { sourceRef requestedOutputRef }
    projectionReport {
      __typename
      ... on M2MMappedProjectionReportEntry {
        sourceRef requestedOutputRef factId
        dimension {
          ... on M2MScopeDimension { scope }
          ... on M2MPhaseDimension { phase }
          ... on M2MPhasePairDimension { phasePair }
          ... on M2MInputDimension { inputId }
          ... on M2MSensorDimension { sensorId }
        }
      }
      ... on M2MWithheldProjectionReportEntry { sourceRef requestedOutputRef }
      ... on M2MUnrepresentableProjectionReportEntry { sourceRef requestedOutputRef }
    }
  }
}
`

type Config struct {
	SnapshotByAssetAt     func(context.Context, string) (pv.Snapshot, time.Time, bool)
	AssetExists           func(string) bool
	AllowedAssets         map[string]struct{}
	MonotonicMilliseconds func() int64
}

type handler struct {
	cfg                 Config
	canonicalQueryShape string
	mu                  sync.Mutex
	principals          map[string]*principalLimit
}
type principalLimit struct {
	inFlight bool
	tokens   int
	at       int64
}
type principalKey struct{}

func WithMTLSPrincipal(ctx context.Context, fingerprint string) context.Context {
	return context.WithValue(ctx, principalKey{}, fingerprint)
}

func NewHandler(cfg Config) (http.Handler, error) {
	if cfg.SnapshotByAssetAt == nil || cfg.AssetExists == nil {
		return nil, errors.New("authoritative asset and snapshot providers are required")
	}
	if cfg.AllowedAssets == nil {
		cfg.AllowedAssets = map[string]struct{}{}
	}
	if cfg.MonotonicMilliseconds == nil {
		started := time.Now()
		cfg.MonotonicMilliseconds = func() int64 { return time.Since(started).Milliseconds() }
	}
	canonicalShape, _, _, err := queryDocumentShape(fixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded canonical query")
	}
	return &handler{cfg: cfg, canonicalQueryShape: canonicalShape, principals: make(map[string]*principalLimit)}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "REQUEST_INVALID")
		return
	}
	if r.URL.Path != route || r.URL.RawQuery != "" {
		writeError(w, "QUERY_REJECTED")
		return
	}
	principal, _ := r.Context().Value(principalKey{}).(string)
	if strings.TrimSpace(principal) == "" {
		writeError(w, "REQUEST_INVALID")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil {
		writeError(w, "REQUEST_INVALID")
		return
	}
	if len(body) > maxRequestBytes {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err := validateJSON(body); err != nil {
		writeError(w, "REQUEST_INVALID")
		return
	}
	request, err := decodeClosedRequest(body)
	if err != nil {
		writeError(w, "REQUEST_INVALID")
		return
	}
	queryShape, queryDepth, queryFields, err := queryDocumentShape(request.Query)
	if queryDepth > maxQueryDepth || queryFields > maxSelectedFields {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err != nil || request.OperationName != "M2MCurrentSnapshot" || queryShape != h.canonicalQueryShape {
		writeError(w, "QUERY_REJECTED")
		return
	}
	if request.Variables.Request.ContractID != contractID {
		writeError(w, "CONTRACT_INCOMPATIBLE")
		return
	}
	asset := request.Variables.Request.AssetRef
	if _, ok := h.cfg.AllowedAssets[asset]; !ok {
		writeError(w, "ASSET_FORBIDDEN")
		return
	}
	if h.cfg.AssetExists != nil && !h.cfg.AssetExists(asset) {
		writeError(w, "ASSET_NOT_FOUND")
		return
	}
	if !h.admit(principal) {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	defer h.release(principal)
	snapshot, producedAt, ok := h.cfg.SnapshotByAssetAt(r.Context(), asset)
	if !ok {
		writeError(w, "SOURCE_UNAVAILABLE")
		return
	}
	if snapshot.AssetRef != asset {
		writeError(w, "SOURCE_UNAVAILABLE")
		return
	}
	if len(snapshot.Facts) > maxFacts || len(snapshot.Origins) > maxFacts || len(snapshot.RequestedOutputs) > maxProjectionRows || len(snapshot.ProjectionReport) > maxProjectionRows {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err := validatePublicSnapshot(snapshot); err != nil {
		writeError(w, "SOURCE_UNAVAILABLE")
		return
	}
	wire, err := mcpCurrentSnapshotAt(snapshot, producedAt)
	if err != nil {
		writeError(w, "SOURCE_UNAVAILABLE")
		return
	}
	encoded, err := json.Marshal(map[string]any{"data": map[string]any{"m2mCurrentSnapshot": wire}})
	if err != nil || len(encoded) > maxResponseBytes {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

func (h *handler) admit(principal string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.cfg.MonotonicMilliseconds()
	limit := h.principals[principal]
	if limit == nil {
		limit = &principalLimit{tokens: 2, at: now}
		h.principals[principal] = limit
	}
	if now > limit.at {
		elapsedIntervals := (now - limit.at) / 1000
		limit.tokens = min(2, limit.tokens+int(elapsedIntervals))
		limit.at += elapsedIntervals * 1000
	}
	if limit.inFlight || limit.tokens == 0 {
		return false
	}
	limit.inFlight = true
	limit.tokens--
	return true
}
func (h *handler) release(principal string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if limit := h.principals[principal]; limit != nil {
		limit.inFlight = false
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeError(w http.ResponseWriter, code string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": nil, "errors": []any{map[string]any{"message": "M2M request failed", "path": []string{"m2mCurrentSnapshot"}, "extensions": map[string]string{"code": code}}}})
}

func validateJSON(data []byte) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	if err := walkJSON(decoder, 0); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("extra JSON value")
	}
	_, err := decoder.Token()
	if err != io.EOF {
		return errors.New("extra JSON value")
	}
	return nil
}

type closedRequest struct {
	OperationName string
	Query         string
	Variables     struct {
		Request struct {
			ContractID string
			AssetRef   string
		}
	}
}

func decodeClosedRequest(data []byte) (closedRequest, error) {
	var request closedRequest
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil || !hasExactKeys(root, "operationName", "query", "variables") {
		return request, errors.New("invalid request envelope")
	}
	if err := json.Unmarshal(root["operationName"], &request.OperationName); err != nil || request.OperationName == "" {
		return request, errors.New("invalid operation name")
	}
	if err := json.Unmarshal(root["query"], &request.Query); err != nil || request.Query == "" {
		return request, errors.New("invalid query")
	}
	var variables map[string]json.RawMessage
	if err := json.Unmarshal(root["variables"], &variables); err != nil || !hasExactKeys(variables, "request") {
		return request, errors.New("invalid variables")
	}
	var input map[string]json.RawMessage
	if err := json.Unmarshal(variables["request"], &input); err != nil || !hasExactKeys(input, "contractId", "assetRef") {
		return request, errors.New("invalid request input")
	}
	if err := json.Unmarshal(input["contractId"], &request.Variables.Request.ContractID); err != nil {
		return request, errors.New("invalid contract ID")
	}
	if err := json.Unmarshal(input["assetRef"], &request.Variables.Request.AssetRef); err != nil {
		return request, errors.New("invalid asset reference")
	}
	return request, nil
}

func hasExactKeys(values map[string]json.RawMessage, expected ...string) bool {
	if len(values) != len(expected) {
		return false
	}
	for _, key := range expected {
		if _, ok := values[key]; !ok {
			return false
		}
	}
	return true
}
func walkJSON(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON depth")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return errors.New("object key")
				}
				if _, duplicate := seen[name]; duplicate {
					return errors.New("duplicate key")
				}
				seen[name] = struct{}{}
				if err := walkJSON(decoder, depth+1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walkJSON(decoder, depth+1); err != nil {
					return err
				}
			}
		default:
			return errors.New("unexpected delimiter")
		}
		_, err = decoder.Token()
		return err
	default:
		return nil
	}
}
func queryDocumentShape(query string) (string, int, int, error) {
	document, err := parser.Parse(parser.ParseParams{Source: query, Options: parser.ParseOptions{NoLocation: true, NoSource: true}})
	if err != nil {
		return "", 0, 0, err
	}
	maximumDepth, selectedFields := 0, 0
	var walk func(*ast.SelectionSet, int)
	walk = func(selectionSet *ast.SelectionSet, parentDepth int) {
		if selectionSet == nil {
			return
		}
		for _, selection := range selectionSet.Selections {
			depth := parentDepth + 1
			if depth > maximumDepth {
				maximumDepth = depth
			}
			if _, ok := selection.(*ast.Field); ok {
				selectedFields++
			}
			walk(selection.GetSelectionSet(), depth)
		}
	}
	for _, node := range document.Definitions {
		definition, ok := node.(ast.Definition)
		if !ok {
			continue
		}
		walk(definition.GetSelectionSet(), 0)
	}
	printed, ok := printer.Print(document).(string)
	if !ok {
		return "", maximumDepth, selectedFields, fmt.Errorf("unexpected printed query type")
	}
	return printed, maximumDepth, selectedFields, nil
}

// MCPCurrentSnapshot is the one lossless public wire projection shared by the
// dedicated GraphQL endpoint and the MCP-first semantic surface.
func MCPCurrentSnapshot(snapshot pv.Snapshot, producedAt time.Time) (map[string]any, error) {
	return mcpCurrentSnapshotAt(snapshot, producedAt)
}

func mcpCurrentSnapshotAt(snapshot pv.Snapshot, producedAt time.Time) (map[string]any, error) {
	if err := validatePublicSnapshot(snapshot); err != nil {
		return nil, err
	}
	if producedAt.IsZero() {
		return nil, errors.New("publication time is unavailable")
	}
	result := map[string]any{"contractId": contractID, "canonicalContractId": pv.ContractV1, "assetRef": snapshot.AssetRef, "generation": u64(snapshot.Generation), "producedAt": producedAt.UTC().Format(time.RFC3339Nano), "evaluatedMonotonicNs": i64(snapshot.Evaluated), "sourceTimeState": string(snapshot.SourceTimeState), "currentSourceOriginRef": string(snapshot.Source.SourceObservationRef), "capabilities": []any{map[string]any{"id": snapshot.Capability.ID, "outcome": string(snapshot.Capability.Outcome)}}}
	facts := make([]pv.Fact, 0, len(snapshot.Facts))
	for _, fact := range snapshot.Facts {
		facts = append(facts, fact)
	}
	sort.Slice(facts, func(i, j int) bool {
		return string(pv.NewFactKey(facts[i].ID, facts[i].Dimensions)) < string(pv.NewFactKey(facts[j].ID, facts[j].Dimensions))
	})
	result["facts"] = projectFacts(facts)
	origins := make([]pv.Digest, 0, len(snapshot.Origins))
	currentOrigin := snapshot.Source.SourceObservationRef
	for ref := range snapshot.Origins {
		if ref != currentOrigin {
			origins = append(origins, ref)
		}
	}
	if len(origins) == 0 && snapshot.Source.SourceObservationRef != "" {
		snapshot.Origins = map[pv.Digest]pv.Provenance{snapshot.Source.SourceObservationRef: snapshot.Source}
	}
	sort.Slice(origins, func(i, j int) bool { return origins[i] < origins[j] })
	if currentOrigin != "" {
		origins = append([]pv.Digest{currentOrigin}, origins...)
	}
	provenance := make([]any, 0, len(origins))
	for _, ref := range origins {
		provenance = append(provenance, projectProvenance(ref, snapshot.Origins[ref]))
	}
	result["provenance"] = provenance
	requested := append([]pv.RequestedOutput(nil), snapshot.RequestedOutputs...)
	sort.Slice(requested, func(i, j int) bool {
		return string(requested[i].SourceRef)+string(requested[i].RequestedOutputRef) < string(requested[j].SourceRef)+string(requested[j].RequestedOutputRef)
	})
	rows := make([]any, 0, len(requested))
	for _, item := range requested {
		rows = append(rows, map[string]any{"sourceRef": string(item.SourceRef), "requestedOutputRef": string(item.RequestedOutputRef)})
	}
	result["requestedOutputs"] = rows
	report := append([]pv.Projection(nil), snapshot.ProjectionReport...)
	sort.Slice(report, func(i, j int) bool {
		return string(report[i].SourceRef)+string(report[i].RequestedOutputRef) < string(report[j].SourceRef)+string(report[j].RequestedOutputRef)
	})
	reports := make([]any, 0, len(report))
	for _, item := range report {
		row := map[string]any{"sourceRef": string(item.SourceRef), "requestedOutputRef": string(item.RequestedOutputRef)}
		switch item.Outcome {
		case pv.ProjectionMapped:
			row["__typename"] = "M2MMappedProjectionReportEntry"
			row["factId"] = string(item.FactID)
			if item.Dimensions != nil {
				row["dimension"] = projectDimension(*item.Dimensions)
			}
		case pv.ProjectionWithheld:
			row["__typename"] = "M2MWithheldProjectionReportEntry"
		default:
			row["__typename"] = "M2MUnrepresentableProjectionReportEntry"
		}
		reports = append(reports, row)
	}
	result["projectionReport"] = reports
	return result, nil
}

func validatePublicSnapshot(snapshot pv.Snapshot) error {
	if snapshot.ContractID != "" && snapshot.ContractID != pv.ContractV1 {
		return errors.New("unexpected canonical contract")
	}
	if snapshot.AssetRef == "" || snapshot.SourceTimeState != pv.SourceTimeUnavailable && snapshot.SourceTimeState != pv.SourceTimeValid && snapshot.SourceTimeState != pv.SourceTimeInvalid {
		return errors.New("invalid snapshot identity")
	}
	if snapshot.Capability.ID != pv.CapabilityThreePhaseTelemetryV1 || snapshot.Capability.Outcome != pv.CapabilitySatisfied && snapshot.Capability.Outcome != pv.CapabilityNotSatisfied {
		return errors.New("invalid capability")
	}
	current := snapshot.Source.SourceObservationRef
	if current.Validate() != nil {
		return errors.New("invalid current source")
	}
	if len(snapshot.Origins) == 0 {
		snapshot.Origins = map[pv.Digest]pv.Provenance{current: snapshot.Source}
	}
	for ref, provenance := range snapshot.Origins {
		if ref.Validate() != nil || provenance.SourceObservationRef != ref || provenance.SourceRegistryRef.Validate() != nil || provenance.SourceShadowRef.Validate() != nil || provenance.EvidenceRef.Validate() != nil || provenance.Validity != pv.SourceTerminalVerified || provenance.Protocol == "" || provenance.ProfileID == "" || provenance.ProfileVersion == "" {
			return errors.New("invalid provenance")
		}
	}
	if provenance, ok := snapshot.Origins[current]; !ok || provenance != snapshot.Source {
		return errors.New("current source is not in provenance")
	}
	catalog := pv.CatalogV1()
	for key, fact := range snapshot.Facts {
		definition, known := catalog.Facts[fact.ID]
		if !known || key != pv.NewFactKey(fact.ID, fact.Dimensions) || catalog.ValidateCandidate(pv.FactCandidate{ID: fact.ID, Dimensions: fact.Dimensions, Value: fact.Value, Unit: fact.Unit}) != nil || definition.Policy != fact.Temporal.Policy {
			return errors.New("invalid fact")
		}
		if _, ok := snapshot.Origins[fact.OriginRef]; !ok {
			return errors.New("fact origin unavailable")
		}
		if fact.Quality != pv.QualityGood && fact.Quality != pv.QualitySuspect && fact.Quality != pv.QualityBad {
			return errors.New("invalid fact quality")
		}
		if fact.Availability != pv.AvailabilityAvailable && fact.Availability != pv.AvailabilityUnavailable && fact.Availability != pv.AvailabilityUnsupported {
			return errors.New("invalid fact availability")
		}
		if fact.Freshness != pv.FreshnessFresh && fact.Freshness != pv.FreshnessStale && fact.Freshness != pv.FreshnessExpired {
			return errors.New("invalid fact freshness")
		}
		if err := validateContinuity(definition.Accumulator, fact.Continuity); err != nil {
			return err
		}
	}
	return validateProjectionAccounting(snapshot)
}

func validateContinuity(accumulator bool, continuity *pv.Continuity) error {
	if continuity == nil {
		return nil
	}
	if !accumulator {
		return errors.New("non-accumulator fact carries continuity")
	}
	validDecimal := func(value *pv.Decimal) bool { return value != nil && value.Validate() == nil }
	switch continuity.State {
	case pv.ContinuityBaseline:
		if continuity.Delta != nil || continuity.Modulus != nil || continuity.EvidenceRef != "" {
			return errors.New("invalid baseline continuity")
		}
	case pv.ContinuityContiguous:
		if !validDecimal(continuity.Delta) || continuity.Modulus != nil || continuity.EvidenceRef != "" {
			return errors.New("invalid contiguous continuity")
		}
	case pv.ContinuityRollover:
		if !validDecimal(continuity.Delta) || !validDecimal(continuity.Modulus) || continuity.EvidenceRef.Validate() != nil {
			return errors.New("invalid rollover continuity")
		}
	case pv.ContinuityReset:
		if continuity.Delta != nil || continuity.Modulus != nil || continuity.EvidenceRef.Validate() != nil {
			return errors.New("invalid reset continuity")
		}
	case pv.ContinuityDiscontinuity:
		if continuity.Delta != nil || continuity.Modulus != nil || continuity.EvidenceRef != "" && continuity.EvidenceRef.Validate() != nil {
			return errors.New("invalid discontinuity continuity")
		}
	default:
		return errors.New("invalid continuity state")
	}
	return nil
}

func validateProjectionAccounting(snapshot pv.Snapshot) error {
	type projectionKey struct{ source, output pv.Digest }
	requested := make(map[projectionKey]struct{}, len(snapshot.RequestedOutputs))
	for _, item := range snapshot.RequestedOutputs {
		key := projectionKey{item.SourceRef, item.RequestedOutputRef}
		if item.SourceRef.Validate() != nil || item.RequestedOutputRef.Validate() != nil {
			return errors.New("invalid requested output")
		}
		if _, duplicate := requested[key]; duplicate {
			return errors.New("duplicate requested output")
		}
		requested[key] = struct{}{}
	}
	reported := make(map[projectionKey]struct{}, len(snapshot.ProjectionReport))
	for _, item := range snapshot.ProjectionReport {
		key := projectionKey{item.SourceRef, item.RequestedOutputRef}
		if _, ok := requested[key]; !ok {
			return errors.New("unrequested projection")
		}
		if _, duplicate := reported[key]; duplicate {
			return errors.New("duplicate projection")
		}
		reported[key] = struct{}{}
		switch item.Outcome {
		case pv.ProjectionMapped:
			if item.Dimensions == nil {
				return errors.New("mapped projection without dimensions")
			}
			fact, ok := snapshot.Facts[pv.NewFactKey(item.FactID, *item.Dimensions)]
			if !ok || fact.OriginRef != item.SourceRef {
				return errors.New("mapped projection does not resolve")
			}
		case pv.ProjectionWithheld, pv.ProjectionUnrepresentable:
			if item.FactID != "" || item.Dimensions != nil || item.SourceRef != snapshot.Source.SourceObservationRef {
				return errors.New("non-mapped projection carries mapped fields")
			}
		default:
			return errors.New("invalid projection outcome")
		}
	}
	if len(reported) != len(requested) {
		return errors.New("incomplete projection accounting")
	}
	return nil
}
func projectFacts(facts []pv.Fact) []any {
	out := make([]any, 0, len(facts))
	for _, fact := range facts {
		row := map[string]any{"factId": string(fact.ID), "dimension": projectDimension(fact.Dimensions), "value": projectValue(fact.Value), "unit": string(fact.Unit), "quality": string(fact.Quality), "availability": string(fact.Availability), "freshness": string(fact.Freshness), "receiptMonotonicNs": i64(fact.Temporal.Receipt), "freshUntilMonotonicNs": i64(fact.Temporal.FreshUntil), "retainUntilMonotonicNs": i64(fact.Temporal.RetainUntil), "freshnessPolicy": string(fact.Temporal.Policy), "originRef": string(fact.OriginRef), "continuity": nil}
		if fact.Continuity != nil {
			row["continuity"] = projectContinuity(*fact.Continuity)
		}
		out = append(out, row)
	}
	return out
}
func projectDimension(d pv.Dimensions) map[string]any {
	switch {
	case d.Scope != "":
		return map[string]any{"scope": string(d.Scope)}
	case d.Phase != "":
		return map[string]any{"phase": string(d.Phase)}
	case d.PhasePair != "":
		return map[string]any{"phasePair": string(d.PhasePair)}
	case d.InputID != "":
		return map[string]any{"inputId": d.InputID}
	default:
		return map[string]any{"sensorId": d.SensorID}
	}
}
func projectValue(v pv.FactValue) map[string]any {
	switch v.Kind {
	case pv.ValueKindDecimal:
		if v.Decimal != nil {
			return map[string]any{"coefficient": v.Decimal.Coefficient, "scale": v.Decimal.Scale}
		}
	case pv.ValueKindEnum:
		return map[string]any{"symbol": v.Symbol}
	case pv.ValueKindBitfield:
		symbols := append([]string(nil), v.Symbols...)
		sort.Strings(symbols)
		return map[string]any{"symbols": symbols}
	}
	return map[string]any{}
}
func projectContinuity(c pv.Continuity) map[string]any {
	switch c.State {
	case pv.ContinuityBaseline:
		return map[string]any{"__typename": "M2MBaselineContinuity", "baseline": "BASELINE"}
	case pv.ContinuityContiguous:
		return map[string]any{"__typename": "M2MContiguousContinuity", "delta": projectDecimal(c.Delta)}
	case pv.ContinuityRollover:
		return map[string]any{"__typename": "M2MRolloverContinuity", "delta": projectDecimal(c.Delta), "modulus": projectDecimal(c.Modulus), "rolloverEvidenceRef": string(c.EvidenceRef)}
	case pv.ContinuityReset:
		return map[string]any{"__typename": "M2MResetContinuity", "resetEvidenceRef": string(c.EvidenceRef)}
	default:
		return map[string]any{"__typename": "M2MDiscontinuityContinuity", "discontinuityEvidenceRef": string(c.EvidenceRef)}
	}
}
func projectDecimal(d *pv.Decimal) any {
	if d == nil {
		return nil
	}
	return map[string]any{"coefficient": d.Coefficient, "scale": d.Scale}
}
func projectProvenance(ref pv.Digest, p pv.Provenance) map[string]any {
	return map[string]any{"originRef": string(ref), "sourceProtocol": p.Protocol, "sourceProfileId": p.ProfileID, "sourceProfileVersion": p.ProfileVersion, "sourceValidity": p.Validity, "sourceRegistryRef": string(p.SourceRegistryRef), "sourceObservationRef": string(p.SourceObservationRef), "evidenceRef": string(p.EvidenceRef)}
}
func u64(value uint64) string            { return json.Number(stringify(value)).String() }
func i64(value pv.MonotonicNanos) string { return stringify(int64(value)) }
func stringify(value any) string         { encoded, _ := json.Marshal(value); return string(encoded) }
