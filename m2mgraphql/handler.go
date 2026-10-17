// Package m2mgraphql exposes the single, versioned SemReg PV query.
package m2mgraphql

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
)

const (
	route                     = "/graphql/m2m/v1"
	semanticPVContractID      = "PUBLIC_GRAPHQL_SEMANTIC_PV_V1"
	semanticStorageContractID = "PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1"
	semanticEVSEContractID    = "PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1"
	maxRequestBytes           = 16 << 10
	maxResponseBytes          = 1 << 20
	maxJSONDepth              = 64
	maxQueryDepth             = 8
	maxSelectedFields         = 256
)

// The request-budget table is bounded because remote hosts are not
// authenticated: a client rotating source addresses (for example across an
// IPv6 prefix) would otherwise create one permanent entry per address.
//
// rateLimitTableCapacity bounds the budget entries. Inserting a new key at
// capacity first evicts idle entries, then the least recently used entry; a
// new key always receives an entry and is never refused for table pressure.
// An entry whose request is in flight is never evicted, by the idle sweep or
// by LRU, so its one-in-flight guard always holds. When every entry is in
// flight the table grows past capacity instead; the table is therefore bounded
// by rateLimitTableCapacity plus the number of in-flight requests, which the
// HTTP server already bounds, and the next insertion of a new key trims it
// back to capacity once those requests are released. rateLimitIdleMillis is
// the idle window after which an untouched entry is evicted. Any entry idle
// for longer than two refill intervals is already back at its full budget, so
// idle eviction cannot change admission; only LRU eviction under pressure can,
// and it can only grant a returning idle key a fresh budget, never deny one.
// Explicit in-process partitions ("key:") share the same table and policy.
const (
	rateLimitTableCapacity = 4096
	rateLimitIdleMillis    = 60_000
	rateLimitBurst         = 2
	rateLimitRefillMillis  = 1000
)

const semanticPVFixedQuery = `query SemanticPVCurrent($request: M2MCurrentSnapshotRequest!) {
  semanticPVCurrent(request: $request) { snapshot evaluation selections projection }
}`

const semanticStorageFixedQuery = `query SemanticStorageCurrent($request: M2MCurrentSnapshotRequest!) {
  semanticStorageCurrent(request: $request) { snapshot evaluation selections projection }
}`

const semanticEVSEFixedQuery = `query SemanticEVSECurrent($request: M2MCurrentSnapshotRequest!) {
  semanticEVSECurrent(request: $request) { snapshot evaluation selections projection }
}`

// Config supplies the one immutable SemReg evaluation used by every public PV
// consumer. The legacy canonical-PV provider is deliberately absent.
type Config struct {
	AllowedAssets          map[string]struct{}
	MonotonicMilliseconds  func() int64
	SemanticPVCurrent      func(context.Context, string) (json.RawMessage, bool)
	SemanticStorageCurrent func(context.Context, string) (json.RawMessage, bool)
	SemanticEVSECurrent    func(context.Context, string) (json.RawMessage, bool)
}

type handler struct {
	cfg               Config
	queryShape        string
	storageQueryShape string
	evseQueryShape    string
	mu                sync.Mutex
	limits            map[string]*list.Element // value: *requestLimit
	lru               *list.List               // front = most recently used
	limitCapacity     int
}

type requestLimit struct {
	key      string
	inFlight bool
	tokens   int
	at       int64
	lastUsed int64
}

type rateLimitKey struct{}

// WithRateLimitKey lets an in-process caller choose the request-budget
// partition. It carries no identity and grants no access. Requests from the
// network listener never carry it and are partitioned by remote host.
func WithRateLimitKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, rateLimitKey{}, key)
}

// WithMTLSPrincipal is a compatibility alias of WithRateLimitKey kept for
// in-process callers until the M2M cleanup removes it; despite its name, no
// client principal is derived or required.
func WithMTLSPrincipal(ctx context.Context, key string) context.Context {
	return WithRateLimitKey(ctx, key)
}

// requestRateLimitKey partitions the request budget by the remote host (the
// port is ignored so reconnecting does not refill the budget), unless an
// in-process caller supplied an explicit partition.
func requestRateLimitKey(r *http.Request) string {
	if key, ok := r.Context().Value(rateLimitKey{}).(string); ok && strings.TrimSpace(key) != "" {
		return "key:" + key
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "remote:" + host
}

func NewHandler(cfg Config) (http.Handler, error) {
	if cfg.SemanticPVCurrent == nil && cfg.SemanticStorageCurrent == nil && cfg.SemanticEVSECurrent == nil {
		return nil, errors.New("an authoritative SemReg projection provider is required")
	}
	if cfg.AllowedAssets == nil {
		cfg.AllowedAssets = map[string]struct{}{}
	}
	if cfg.MonotonicMilliseconds == nil {
		started := time.Now()
		cfg.MonotonicMilliseconds = func() int64 { return time.Since(started).Milliseconds() }
	}
	shape, _, _, err := queryDocumentShape(semanticPVFixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded SemReg PV query")
	}
	storageShape, _, _, err := queryDocumentShape(semanticStorageFixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded SemReg storage query")
	}
	evseShape, _, _, err := queryDocumentShape(semanticEVSEFixedQuery)
	if err != nil {
		return nil, errors.New("invalid embedded SemReg EVSE query")
	}
	return &handler{cfg: cfg, queryShape: shape, storageQueryShape: storageShape, evseQueryShape: evseShape, limits: make(map[string]*list.Element), lru: list.New(), limitCapacity: rateLimitTableCapacity}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	if r.URL.Path != route || r.URL.RawQuery != "" {
		writeError(w, "", "QUERY_REJECTED")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	if len(body) > maxRequestBytes {
		writeError(w, "", "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err := validateJSON(body); err != nil {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	request, err := decodeClosedRequest(body)
	if err != nil {
		writeError(w, "", "REQUEST_INVALID")
		return
	}
	queryShape, queryDepth, queryFields, err := queryDocumentShape(request.Query)
	if queryDepth > maxQueryDepth || queryFields > maxSelectedFields {
		writeError(w, "", "REQUEST_LIMIT_EXCEEDED")
		return
	}
	if err != nil {
		writeError(w, "", "QUERY_REJECTED")
		return
	}
	root, contract, provider := "", "", (func(context.Context, string) (json.RawMessage, bool))(nil)
	switch request.OperationName {
	case "SemanticPVCurrent":
		root, contract, provider = "semanticPVCurrent", semanticPVContractID, h.cfg.SemanticPVCurrent
	case "SemanticStorageCurrent":
		root, contract, provider = "semanticStorageCurrent", semanticStorageContractID, h.cfg.SemanticStorageCurrent
	case "SemanticEVSECurrent":
		root, contract, provider = "semanticEVSECurrent", semanticEVSEContractID, h.cfg.SemanticEVSECurrent
	default:
		writeError(w, "", "QUERY_REJECTED")
		return
	}
	if (request.OperationName == "SemanticPVCurrent" && queryShape != h.queryShape) || (request.OperationName == "SemanticStorageCurrent" && queryShape != h.storageQueryShape) || (request.OperationName == "SemanticEVSECurrent" && queryShape != h.evseQueryShape) || provider == nil {
		writeError(w, root, "QUERY_REJECTED")
		return
	}
	if request.Variables.Request.ContractID != contract {
		writeError(w, root, "CONTRACT_INCOMPATIBLE")
		return
	}
	asset := request.Variables.Request.AssetRef
	if _, ok := h.cfg.AllowedAssets[asset]; !ok {
		writeError(w, root, "ASSET_FORBIDDEN")
		return
	}
	limit := h.admit(requestRateLimitKey(r))
	if limit == nil {
		writeError(w, root, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	defer h.release(limit)
	data, ok := provider(r.Context(), asset)
	if !ok || !json.Valid(data) {
		writeError(w, root, "SOURCE_UNAVAILABLE")
		return
	}
	var projection map[string]json.RawMessage
	if err := json.Unmarshal(data, &projection); err != nil || !hasExactKeys(projection, "snapshot", "evaluation", "selections", "projection") {
		writeError(w, root, "SOURCE_UNAVAILABLE")
		return
	}
	encoded, err := json.Marshal(map[string]any{"data": map[string]any{root: map[string]json.RawMessage{
		"snapshot": projection["snapshot"], "evaluation": projection["evaluation"], "selections": projection["selections"], "projection": projection["projection"],
	}}})
	if err != nil || len(encoded) > maxResponseBytes {
		writeError(w, root, "REQUEST_LIMIT_EXCEEDED")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

// admit returns the admitted budget entry, or nil when the key's budget is
// exhausted or a request for the key is already in flight.
func (h *handler) admit(key string) *requestLimit {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.cfg.MonotonicMilliseconds()
	h.evictIdleLocked(now)
	var limit *requestLimit
	if element := h.limits[key]; element != nil {
		limit = element.Value.(*requestLimit)
		h.lru.MoveToFront(element)
	} else {
		for h.lru.Len() >= h.limitCapacity {
			victim := h.lruVictimLocked()
			if victim == nil {
				// Every entry is in flight: exceed capacity rather than
				// drop a guard or refuse the new key.
				break
			}
			h.removeLocked(victim)
		}
		limit = &requestLimit{key: key, tokens: rateLimitBurst, at: now}
		h.limits[key] = h.lru.PushFront(limit)
	}
	limit.lastUsed = now
	if now > limit.at {
		elapsedIntervals := (now - limit.at) / rateLimitRefillMillis
		limit.tokens = min(rateLimitBurst, limit.tokens+int(elapsedIntervals))
		limit.at += elapsedIntervals * rateLimitRefillMillis
	}
	if limit.inFlight || limit.tokens == 0 {
		return nil
	}
	limit.inFlight = true
	limit.tokens--
	return limit
}

// release clears the in-flight mark on the entry admit returned. In-flight
// entries are never evicted; the entry is still addressed by pointer so that a
// release can never clear or touch a different entry for the same key.
func (h *handler) release(limit *requestLimit) {
	h.mu.Lock()
	defer h.mu.Unlock()
	limit.inFlight = false
	if element := h.limits[limit.key]; element != nil && element.Value == limit {
		limit.lastUsed = h.cfg.MonotonicMilliseconds()
		h.lru.MoveToFront(element)
	}
}

// evictIdleLocked drops entries untouched for longer than the idle window,
// skipping entries with a request in flight. The list is ordered by last use,
// so expired entries sit at the back and the sweep costs O(1) amortised per
// admission plus one step per in-flight entry passed over.
func (h *handler) evictIdleLocked(now int64) {
	for element := h.lru.Back(); element != nil; {
		limit := element.Value.(*requestLimit)
		if now-limit.lastUsed <= rateLimitIdleMillis {
			return
		}
		prev := element.Prev()
		if !limit.inFlight {
			h.removeLocked(element)
		}
		element = prev
	}
}

// lruVictimLocked returns the least recently used entry without a request in
// flight, or nil when every entry is in flight.
func (h *handler) lruVictimLocked() *list.Element {
	for element := h.lru.Back(); element != nil; element = element.Prev() {
		if !element.Value.(*requestLimit).inFlight {
			return element
		}
	}
	return nil
}

func (h *handler) removeLocked(element *list.Element) {
	limit := h.lru.Remove(element).(*requestLimit)
	delete(h.limits, limit.key)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeError(w http.ResponseWriter, root, code string) {
	path := []string{}
	if root != "" {
		path = []string{root}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": nil, "errors": []any{map[string]any{"message": "M2M request failed", "path": path, "extensions": map[string]string{"code": code}}}})
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
		if definition, ok := node.(ast.Definition); ok {
			walk(definition.GetSelectionSet(), 0)
		}
	}
	printed, ok := printer.Print(document).(string)
	if !ok {
		return "", maximumDepth, selectedFields, fmt.Errorf("unexpected printed query type")
	}
	return printed, maximumDepth, selectedFields, nil
}
