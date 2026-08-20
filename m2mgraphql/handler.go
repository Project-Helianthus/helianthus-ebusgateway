// Package m2mgraphql exposes the single, versioned machine-to-machine
// canonical-PV query. It intentionally is not a general GraphQL engine.
package m2mgraphql

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
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
	SnapshotByAsset       func(context.Context, string) (pv.Snapshot, bool)
	SnapshotByAssetAt     func(context.Context, string) (pv.Snapshot, time.Time, bool)
	AssetExists           func(string) bool
	AllowedAssets         map[string]struct{}
	MonotonicMilliseconds func() int64
}

type handler struct {
	cfg        Config
	mu         sync.Mutex
	principals map[string]*principalLimit
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
	if cfg.AllowedAssets == nil {
		cfg.AllowedAssets = map[string]struct{}{}
	}
	if cfg.MonotonicMilliseconds == nil {
		cfg.MonotonicMilliseconds = func() int64 { return time.Now().UnixMilli() }
	}
	return &handler{cfg: cfg, principals: make(map[string]*principalLimit)}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "REQUEST_INVALID")
		return
	}
	if r.URL.Path != route {
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
	var request struct {
		OperationName string `json:"operationName"`
		Query         string `json:"query"`
		Variables     struct {
			Request struct {
				ContractID string `json:"contractId"`
				AssetRef   string `json:"assetRef"`
			} `json:"request"`
		} `json:"variables"`
	}
	if json.Unmarshal(body, &request) != nil {
		writeError(w, "REQUEST_INVALID")
		return
	}
	if strings.HasPrefix(strings.TrimSpace(request.Query), "subscription") || strings.HasPrefix(strings.TrimSpace(request.Query), "mutation") {
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
	if queryDepth(request.Query) > maxQueryDepth || selectedFields(request.Query) > maxSelectedFields {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if request.OperationName != "M2MCurrentSnapshot" || request.Query != fixedQuery {
		writeError(w, "QUERY_REJECTED")
		return
	}
	if !h.admit(principal) {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	defer h.release(principal)
	if h.cfg.SnapshotByAsset == nil && h.cfg.SnapshotByAssetAt == nil {
		writeError(w, "SOURCE_UNAVAILABLE")
		return
	}
	var snapshot pv.Snapshot
	var producedAt time.Time
	var ok bool
	if h.cfg.SnapshotByAssetAt != nil {
		snapshot, producedAt, ok = h.cfg.SnapshotByAssetAt(r.Context(), asset)
	} else {
		snapshot, ok = h.cfg.SnapshotByAsset(r.Context(), asset)
	}
	if !ok {
		writeError(w, "SOURCE_UNAVAILABLE")
		return
	}
	if len(snapshot.Facts) > maxFacts || len(snapshot.ProjectionReport) > maxProjectionRows {
		writeError(w, "REQUEST_LIMIT_EXCEEDED")
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
		limit.tokens = min(2, limit.tokens+int((now-limit.at)/1000))
		limit.at = now
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
func queryDepth(query string) int {
	depth, maximum := 0, 0
	for _, char := range query {
		if char == '{' {
			depth++
			if depth > maximum {
				maximum = depth
			}
		}
		if char == '}' {
			depth--
		}
	}
	return maximum
}
func selectedFields(query string) int {
	return len(strings.Fields(query))
}

// MCPCurrentSnapshot is the one lossless public wire projection shared by the
// dedicated GraphQL endpoint and the MCP-first semantic surface.
func MCPCurrentSnapshot(snapshot pv.Snapshot) (map[string]any, error) {
	return mcpCurrentSnapshotAt(snapshot, time.Unix(0, int64(snapshot.Evaluated)).UTC())
}

func mcpCurrentSnapshotAt(snapshot pv.Snapshot, producedAt time.Time) (map[string]any, error) {
	if snapshot.ContractID != "" && snapshot.ContractID != pv.ContractV1 {
		return nil, errors.New("unexpected canonical contract")
	}
	if producedAt.IsZero() {
		producedAt = time.Unix(0, int64(snapshot.Evaluated)).UTC()
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
	for ref := range snapshot.Origins {
		origins = append(origins, ref)
	}
	if len(origins) == 0 && snapshot.Source.SourceObservationRef != "" {
		origins = append(origins, snapshot.Source.SourceObservationRef)
		snapshot.Origins = map[pv.Digest]pv.Provenance{snapshot.Source.SourceObservationRef: snapshot.Source}
	}
	sort.Slice(origins, func(i, j int) bool { return origins[i] < origins[j] })
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
func projectFacts(facts []pv.Fact) []any {
	out := make([]any, 0, len(facts))
	for _, fact := range facts {
		row := map[string]any{"factId": string(fact.ID), "dimension": projectDimension(fact.Dimensions), "value": projectValue(fact.Value), "unit": string(fact.Unit), "quality": string(fact.Quality), "availability": string(fact.Availability), "freshness": string(fact.Freshness), "receiptMonotonicNs": i64(fact.Temporal.Receipt), "freshUntilMonotonicNs": i64(fact.Temporal.FreshUntil), "retainUntilMonotonicNs": i64(fact.Temporal.RetainUntil), "freshnessPolicy": string(fact.Temporal.Policy), "originRef": string(fact.OriginRef)}
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
		return map[string]any{"__typename": "M2MScopeDimension", "scope": string(d.Scope)}
	case d.Phase != "":
		return map[string]any{"__typename": "M2MPhaseDimension", "phase": string(d.Phase)}
	case d.PhasePair != "":
		return map[string]any{"__typename": "M2MPhasePairDimension", "phasePair": string(d.PhasePair)}
	case d.InputID != "":
		return map[string]any{"__typename": "M2MInputDimension", "inputId": d.InputID}
	default:
		return map[string]any{"__typename": "M2MSensorDimension", "sensorId": d.SensorID}
	}
}
func projectValue(v pv.FactValue) map[string]any {
	switch v.Kind {
	case pv.ValueKindDecimal:
		if v.Decimal != nil {
			return map[string]any{"__typename": "M2MDecimalValue", "coefficient": v.Decimal.Coefficient, "scale": stringify(v.Decimal.Scale)}
		}
	case pv.ValueKindEnum:
		return map[string]any{"__typename": "M2MEnumValue", "symbol": v.Symbol}
	case pv.ValueKindBitfield:
		symbols := append([]string(nil), v.Symbols...)
		sort.Strings(symbols)
		return map[string]any{"__typename": "M2MBitfieldValue", "symbols": symbols}
	}
	return map[string]any{}
}
func projectContinuity(c pv.Continuity) map[string]any {
	switch c.State {
	case pv.ContinuityBaseline:
		return map[string]any{"__typename": "M2MBaselineContinuity", "baseline": true}
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
	return map[string]any{"coefficient": d.Coefficient, "scale": stringify(d.Scale)}
}
func projectProvenance(ref pv.Digest, p pv.Provenance) map[string]any {
	return map[string]any{"originRef": string(ref), "sourceProtocol": p.Protocol, "sourceProfileId": p.ProfileID, "sourceProfileVersion": p.ProfileVersion, "sourceValidity": p.Validity, "sourceRegistryRef": string(p.SourceRegistryRef), "sourceObservationRef": string(p.SourceObservationRef), "evidenceRef": string(p.EvidenceRef)}
}
func u64(value uint64) string            { return json.Number(stringify(value)).String() }
func i64(value pv.MonotonicNanos) string { return stringify(int64(value)) }
func stringify(value any) string         { encoded, _ := json.Marshal(value); return string(encoded) }
