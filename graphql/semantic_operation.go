package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/handler"
)

// SemanticOperationRouter is the injected bounded dispatcher of the one
// generic semantic operation route (helianthus-docs-ebus
// api/semantic-operation-route.md). The GraphQL adapter performs no admission,
// executor selection or native dispatch of its own.
type SemanticOperationRouter interface {
	Route(context.Context, semanticoperations.Request) (semanticoperations.Result, error)
}

const (
	semanticOperationFieldName = "semanticOperation"
	semanticOperationMaxDepth  = 32
	// semanticOperationMaxSelections bounds the selections the request guard
	// visits while counting semanticOperation fields.
	semanticOperationMaxSelections = 4096
)

// semanticOperationError is a request-level refusal surfaced on
// errors[].extensions.code. Every such refusal happens before dispatch.
type semanticOperationError struct {
	code   semanticoperations.Code
	detail string
}

func (e semanticOperationError) Error() string {
	return "semantic operation: " + string(e.code) + ": " + e.detail
}

func (e semanticOperationError) Extensions() map[string]interface{} {
	return map[string]interface{}{"code": string(e.code)}
}

func semanticOperationInvalid(detail string) error {
	return semanticOperationError{code: semanticoperations.CodeInvalidArgument, detail: detail}
}

func semanticOperationExhausted(detail string) error {
	return semanticOperationError{code: semanticoperations.CodeResourceExhausted, detail: detail}
}

// semanticOperationErrorFrom keeps only the closed request-level code of a
// router error; any other error is an unwired or failed provider.
func semanticOperationErrorFrom(err error) error {
	var routeErr *semanticoperations.Error
	if errors.As(err, &routeErr) {
		return semanticOperationError{code: routeErr.Code, detail: routeErr.Detail}
	}
	var adapterErr semanticOperationError
	if errors.As(err, &adapterErr) {
		return adapterErr
	}
	return semanticOperationError{code: semanticoperations.CodeUnavailable, detail: "operation provider failed"}
}

// semanticOperationField registers the one additive mutation. The result is
// nullable: a request-level refusal nulls it and reports the code.
func semanticOperationField(builder *Builder) *graphqlgo.Field {
	inputs := map[reflect.Type]*graphqlgo.InputObject{}
	input := graphqlgo.NewInputObject(graphqlgo.InputObjectConfig{
		Name: "SemanticOperationInput",
		Fields: graphqlgo.InputObjectConfigFieldMap{
			"assetId":                            &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"bindingId":                          &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"capabilityInstanceId":               &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"operation":                          &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(semanticInputType(reflect.TypeOf(semreg.DefinitionRef{}), inputs))},
			"arguments":                          &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(semanticInputType(reflect.TypeOf([]semreg.TypedField{}), inputs))},
			"expectedSemanticRevision":           &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"expectedCapabilityRevision":         &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"expectedCapabilityInstanceRevision": &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"expectedSourceEpochId":              &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"expectedDriverGeneration":           &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"preconditions":                      &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(semanticInputType(reflect.TypeOf([]operation.Precondition{}), inputs))},
			"timeoutMs":                          &graphqlgo.InputObjectFieldConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
		},
	})
	result := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "SemanticOperationResult",
		Fields: graphqlgo.Fields{
			"status":           &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"kernelOutcome":    &graphqlgo.Field{Type: graphqlgo.String},
			"delivery":         &graphqlgo.Field{Type: graphqlgo.String},
			"acknowledgement":  &graphqlgo.Field{Type: graphqlgo.String},
			"readbackRelation": &graphqlgo.Field{Type: graphqlgo.String},
			"reason":           &graphqlgo.Field{Type: graphqlgo.String},
		},
	})
	return &graphqlgo.Field{
		Type: result,
		Args: graphqlgo.FieldConfigArgument{"input": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(input)}},
		Resolve: func(p graphqlgo.ResolveParams) (any, error) {
			ctx := p.Context
			if ctx == nil {
				ctx = context.Background()
			}
			marker, _ := ctx.Value(semanticOperationMarkerKey{}).(*semanticOperationMarker)
			// Only the strict single-JSON-body path decodes the input; GET,
			// form, raw GraphQL and in-process paths fail closed here.
			if marker != nil && marker.oversize {
				return nil, semanticOperationExhausted("request body exceeds the byte bound")
			}
			if marker == nil || marker.request == nil {
				return nil, semanticOperationInvalid("requires one strict JSON request body")
			}
			if args, ok := p.Args["input"].(map[string]interface{}); !ok || args["assetId"] != string(marker.request.AssetID) {
				return nil, semanticOperationInvalid("input does not match the inspected request")
			}
			router := builder.semanticOperationRouter()
			if router == nil {
				return nil, semanticOperationError{code: semanticoperations.CodeUnavailable, detail: "operation provider unwired"}
			}
			routed, err := router.Route(ctx, *marker.request)
			if err != nil {
				return nil, semanticOperationErrorFrom(err)
			}
			return semanticOperationResultValue(routed), nil
		},
	}
}

func semanticOperationResultValue(result semanticoperations.Result) map[string]interface{} {
	return map[string]interface{}{
		"status":           string(result.Status),
		"kernelOutcome":    semanticOptional(result.KernelOutcome),
		"delivery":         semanticOptional(result.Delivery),
		"acknowledgement":  semanticOptional(result.Acknowledgement),
		"readbackRelation": semanticOptional(result.ReadbackRelation),
		"reason":           semanticOptional(result.Reason),
	}
}

// semanticOptional keeps an unevidenced member as JSON null.
func semanticOptional[T ~string](value *T) interface{} {
	if value == nil {
		return nil
	}
	return string(*value)
}

// semanticInputType derives string-only input records from the pinned SemReg
// types with the same recursive snake_case to camelCase mapping as reads.
func semanticInputType(t reflect.Type, objects map[reflect.Type]*graphqlgo.InputObject) graphqlgo.Input {
	if t.Kind() == reflect.Pointer {
		return semanticInputType(t.Elem(), objects)
	}
	switch t.Kind() {
	case reflect.String:
		return graphqlgo.String
	case reflect.Bool:
		return graphqlgo.Boolean
	case reflect.Int32:
		return graphqlgo.Int
	case reflect.Slice:
		return graphqlgo.NewList(graphqlgo.NewNonNull(semanticInputType(t.Elem(), objects)))
	case reflect.Struct:
		if object := objects[t]; object != nil {
			return object
		}
		fields := graphqlgo.InputObjectConfigFieldMap{}
		for name, member := range semanticInputMembers(t) {
			input := semanticInputType(member.kind, objects)
			if !member.optional {
				input = graphqlgo.NewNonNull(input)
			}
			fields[name] = &graphqlgo.InputObjectFieldConfig{Type: input}
		}
		object := graphqlgo.NewInputObject(graphqlgo.InputObjectConfig{Name: "Semantic" + t.Name() + "Input", Fields: fields})
		objects[t] = object
		return object
	default:
		panic("unsupported SemReg input scalar: " + t.String())
	}
}

type semanticInputMember struct {
	tag      string
	kind     reflect.Type
	optional bool
}

func semanticInputMembers(t reflect.Type) map[string]semanticInputMember {
	members := map[string]semanticInputMember{}
	for index := 0; index < t.NumField(); index++ {
		field := t.Field(index)
		tag := field.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "" || name == "-" {
			continue
		}
		members[semanticCamel(name)] = semanticInputMember{
			tag: name, kind: field.Type,
			optional: field.Type.Kind() == reflect.Pointer || strings.Contains(tag, ",omitempty"),
		}
	}
	return members
}

var semanticIntegerToken = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// semanticSnakeValue converts one camelCase input record into its exact SemReg
// JSON member names, enforcing string-only identifiers and exact scalar types.
func semanticSnakeValue(raw any, t reflect.Type) (any, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		object, ok := raw.(map[string]any)
		if !ok {
			return nil, semanticOperationInvalid("input record must be an object")
		}
		members := semanticInputMembers(t)
		converted := make(map[string]any, len(object))
		for name, value := range object {
			member, known := members[name]
			if !known {
				return nil, semanticOperationInvalid("unknown input member")
			}
			if value == nil {
				if member.optional {
					continue
				}
				return nil, semanticOperationInvalid("null required input member")
			}
			snake, err := semanticSnakeValue(value, member.kind)
			if err != nil {
				return nil, err
			}
			converted[member.tag] = snake
		}
		for name, member := range members {
			if _, present := object[name]; !present && !member.optional {
				return nil, semanticOperationInvalid("missing input member")
			}
		}
		return converted, nil
	case reflect.Slice:
		list, ok := raw.([]any)
		if !ok {
			return nil, semanticOperationInvalid("input member must be a list")
		}
		converted := make([]any, 0, len(list))
		for _, item := range list {
			if item == nil {
				return nil, semanticOperationInvalid("null list entry")
			}
			snake, err := semanticSnakeValue(item, t.Elem())
			if err != nil {
				return nil, err
			}
			converted = append(converted, snake)
		}
		return converted, nil
	case reflect.String:
		text, ok := raw.(string)
		if !ok {
			return nil, semanticOperationInvalid("input member must be a string")
		}
		if semanticoperations.IsIdentifierType(t) && len(text) > semanticoperations.MaxOpaqueIDBytes {
			return nil, semanticOperationExhausted("opaque identifier exceeds the byte bound")
		}
		return text, nil
	case reflect.Bool:
		value, ok := raw.(bool)
		if !ok {
			return nil, semanticOperationInvalid("input member must be a boolean")
		}
		return value, nil
	case reflect.Int32:
		number, ok := raw.(json.Number)
		if !ok || !semanticIntegerToken.MatchString(string(number)) {
			return nil, semanticOperationInvalid("input member must be an integer")
		}
		if _, err := strconv.ParseInt(string(number), 10, 32); err != nil {
			return nil, semanticOperationInvalid("input member exceeds Int")
		}
		return number, nil
	}
	return nil, semanticOperationInvalid("unsupported input member")
}

func decodeSemanticRecord[T semreg.Record](raw any) (T, error) {
	var zero T
	snake, err := semanticSnakeValue(raw, reflect.TypeOf(zero))
	if err != nil {
		return zero, err
	}
	encoded, err := json.Marshal(snake)
	if err != nil {
		return zero, semanticOperationInvalid("input record encoding")
	}
	record, err := semreg.Decode[T](encoded)
	if err != nil {
		if semreg.ErrorIdentifier(err) == semreg.BoundsExceeded {
			return zero, semanticOperationExhausted("SemReg record bound")
		}
		return zero, semanticOperationInvalid("SemReg record")
	}
	return record, nil
}

var semanticOperationInputMembers = map[string]bool{
	"assetId": true, "bindingId": true, "capabilityInstanceId": true, "operation": true, "arguments": true,
	"expectedSemanticRevision": true, "expectedCapabilityRevision": true, "expectedCapabilityInstanceRevision": true,
	"expectedSourceEpochId": true, "expectedDriverGeneration": true, "preconditions": true, "timeoutMs": true,
}

// decodeSemanticOperationInput decodes the closed input member set from raw
// JSON-typed values, so GraphQL's lenient variable coercion never applies.
func decodeSemanticOperationInput(raw any) (semanticoperations.Request, error) {
	var request semanticoperations.Request
	object, ok := raw.(map[string]any)
	if !ok {
		return request, semanticOperationInvalid("input must be an object")
	}
	for name := range object {
		if !semanticOperationInputMembers[name] {
			return request, semanticOperationInvalid("unknown input member")
		}
	}
	for name := range semanticOperationInputMembers {
		if value, present := object[name]; !present || value == nil {
			return request, semanticOperationInvalid("missing or null input member")
		}
	}
	text := func(name string, identifier bool) (string, error) {
		value, ok := object[name].(string)
		if !ok {
			return "", semanticOperationInvalid("input member must be a string")
		}
		if identifier && len(value) > semanticoperations.MaxOpaqueIDBytes {
			return "", semanticOperationExhausted("opaque identifier exceeds the byte bound")
		}
		return value, nil
	}
	members := map[string]*string{}
	for _, member := range []struct {
		name       string
		identifier bool
	}{
		{"assetId", true}, {"bindingId", true}, {"capabilityInstanceId", true}, {"expectedSourceEpochId", true},
		{"expectedSemanticRevision", false}, {"expectedCapabilityRevision", false},
		{"expectedCapabilityInstanceRevision", false}, {"expectedDriverGeneration", false},
	} {
		value, err := text(member.name, member.identifier)
		if err != nil {
			return request, err
		}
		members[member.name] = &value
	}
	request.AssetID = semreg.AssetID(*members["assetId"])
	request.BindingID = semreg.NativeBindingID(*members["bindingId"])
	request.CapabilityInstanceID = semreg.CapabilityInstanceID(*members["capabilityInstanceId"])
	request.ExpectedSourceEpochID = semreg.SourceEpochID(*members["expectedSourceEpochId"])
	request.ExpectedSemanticRevision = semreg.Uint64(*members["expectedSemanticRevision"])
	request.ExpectedCapabilityRevision = semreg.Uint64(*members["expectedCapabilityRevision"])
	request.ExpectedCapabilityInstanceRevision = semreg.Uint64(*members["expectedCapabilityInstanceRevision"])
	request.ExpectedDriverGeneration = semreg.Uint64(*members["expectedDriverGeneration"])

	definition, err := decodeSemanticRecord[semreg.DefinitionRef](object["operation"])
	if err != nil {
		return request, err
	}
	request.Operation = definition

	arguments, ok := object["arguments"].([]any)
	if !ok {
		return request, semanticOperationInvalid("arguments must be a list")
	}
	if len(arguments) > semanticoperations.MaxArguments {
		return request, semanticOperationExhausted("arguments exceed the entry bound")
	}
	request.Arguments = make([]semreg.TypedField, 0, len(arguments))
	for _, rawArgument := range arguments {
		argument, err := decodeSemanticRecord[semreg.TypedField](rawArgument)
		if err != nil {
			return request, err
		}
		request.Arguments = append(request.Arguments, argument)
	}

	preconditions, ok := object["preconditions"].([]any)
	if !ok {
		return request, semanticOperationInvalid("preconditions must be a list")
	}
	if len(preconditions) > semanticoperations.MaxPreconditions {
		return request, semanticOperationExhausted("preconditions exceed the entry bound")
	}
	request.Preconditions = make([]operation.Precondition, 0, len(preconditions))
	for _, rawPrecondition := range preconditions {
		precondition, err := decodeSemanticRecord[operation.Precondition](rawPrecondition)
		if err != nil {
			return request, err
		}
		request.Preconditions = append(request.Preconditions, precondition)
	}

	timeout, ok := object["timeoutMs"].(json.Number)
	if !ok || !semanticIntegerToken.MatchString(string(timeout)) {
		return request, semanticOperationInvalid("timeoutMs must be an integer")
	}
	milliseconds, err := strconv.ParseInt(string(timeout), 10, 64)
	if err != nil || milliseconds < 1 || milliseconds > semanticoperations.MaxTimeout.Milliseconds() {
		return request, semanticOperationInvalid("timeoutMs out of range")
	}
	request.Timeout = time.Duration(milliseconds) * time.Millisecond
	if err := request.Validate(); err != nil {
		return request, semanticOperationErrorFrom(err)
	}
	return request, nil
}

// semanticOperationMarker carries the guard's verdict for one HTTP request.
type semanticOperationMarker struct {
	request  *semanticoperations.Request
	oversize bool
}

type semanticOperationMarkerKey struct{}

type semanticReplayBody struct {
	io.Reader
	io.Closer
}

type semanticBodyReadError struct{ err error }

func (r semanticBodyReadError) Read([]byte) (int, error) { return 0, r.err }

// guardSemanticOperationRequests bounds and strictly inspects requests that
// select semanticOperation before GraphQL parsing or coercion runs. Every other
// request reaches the handler with a byte-identical body; an oversized body
// only carries a marker that the semanticOperation resolver alone reads.
func guardSemanticOperationRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mirror the handler's request-option selection: URL query, raw GraphQL
		// and form bodies never carry a strict semantic operation.
		if r.Method != http.MethodPost || r.Body == nil || r.URL.Query().Get("query") != "" {
			next.ServeHTTP(w, r)
			return
		}
		switch strings.Split(r.Header.Get("Content-Type"), ";")[0] {
		case handler.ContentTypeGraphQL, handler.ContentTypeFormURLEncoded:
			next.ServeHTTP(w, r)
			return
		}
		original := r.Body
		body, err := io.ReadAll(io.LimitReader(original, int64(semanticoperations.MaxRequestBytes)+1))
		if err != nil {
			r.Body = semanticReplayBody{Reader: io.MultiReader(bytes.NewReader(body), semanticBodyReadError{err: err}), Closer: original}
			next.ServeHTTP(w, r)
			return
		}
		if len(body) > semanticoperations.MaxRequestBytes {
			// A body past the bound is replayed in full to the stock handler so
			// other requests keep their handling; a semantic one is refused by
			// the resolver before any owner, capture or dispatch.
			r.Body = semanticReplayBody{Reader: io.MultiReader(bytes.NewReader(body), original), Closer: original}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), semanticOperationMarkerKey{}, &semanticOperationMarker{oversize: true})))
			return
		}
		r.Body = semanticReplayBody{Reader: bytes.NewReader(body), Closer: original}
		request, semantic, err := inspectSemanticOperationRequest(r.Header, body)
		if !semantic {
			next.ServeHTTP(w, r)
			return
		}
		if err != nil {
			writeSemanticOperationError(w, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), semanticOperationMarkerKey{}, &semanticOperationMarker{request: &request})))
	})
}

func writeSemanticOperationError(w http.ResponseWriter, err error) {
	refusal, ok := semanticOperationErrorFrom(err).(semanticOperationError)
	if !ok {
		refusal = semanticOperationError{code: semanticoperations.CodeInvalidArgument, detail: "request"}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"data": nil,
		"errors": []interface{}{map[string]interface{}{
			"message":    refusal.Error(),
			"extensions": refusal.Extensions(),
		}},
	})
}

// inspectSemanticOperationRequest strictly parses the body before any lenient
// decoding and reports whether it names semanticOperation. Safety never
// depends on this detection: the resolver refuses any execution that did not
// pass through it.
func inspectSemanticOperationRequest(header http.Header, body []byte) (semanticoperations.Request, bool, error) {
	var none semanticoperations.Request
	envelope := parseSemanticEnvelope(body)
	named := false
	for _, query := range envelope.queries {
		named = named || strings.Contains(query, semanticOperationFieldName)
	}
	if !named {
		return none, false, nil
	}
	if envelope.err != nil {
		return none, true, envelope.err
	}
	object, ok := envelope.root.(map[string]any)
	if !ok {
		return none, true, semanticOperationInvalid("batched or non-object request bodies are not supported")
	}
	query, ok := object["query"].(string)
	if !ok {
		return none, true, semanticOperationInvalid("request query must be a string member named query")
	}
	operationName, ok := object["operationName"].(string)
	if !ok && object["operationName"] != nil {
		return none, true, semanticOperationInvalid("operationName must be a string")
	}
	document, err := parser.Parse(parser.ParseParams{Source: query})
	if err != nil {
		return none, true, semanticOperationInvalid("GraphQL document")
	}
	selected, fragments, ok := selectSemanticOperationDefinition(document, operationName)
	if !ok {
		return none, true, semanticOperationInvalid("operation selection")
	}
	collector := semanticFieldCollector{fragments: fragments, active: map[string]bool{}}
	if !collector.collect(selected.SelectionSet, 0) {
		return none, true, semanticOperationInvalid("fragment structure")
	}
	fields := collector.fields
	if len(fields) == 0 {
		return none, false, nil
	}
	variables, err := strictSemanticOperationMembers(object)
	if err != nil {
		return none, true, err
	}
	if err := strictSemanticOperationHeaders(header); err != nil {
		return none, true, err
	}
	if len(fields) != 1 {
		return none, true, semanticOperationInvalid("at most one semanticOperation field per request")
	}
	if selected.Operation != ast.OperationTypeMutation {
		return none, true, semanticOperationInvalid("semanticOperation is a mutation")
	}
	raw, err := semanticOperationRawInput(fields[0], selected, variables)
	if err != nil {
		return none, true, err
	}
	request, err := decodeSemanticOperationInput(raw)
	return request, true, err
}

func selectSemanticOperationDefinition(document *ast.Document, name string) (*ast.OperationDefinition, map[string]*ast.FragmentDefinition, bool) {
	fragments := map[string]*ast.FragmentDefinition{}
	var selected *ast.OperationDefinition
	for _, definition := range document.Definitions {
		switch typed := definition.(type) {
		case *ast.OperationDefinition:
			if name == "" {
				if selected != nil {
					return nil, nil, false
				}
				selected = typed
			} else if typed.Name != nil && typed.Name.Value == name {
				selected = typed
			}
		case *ast.FragmentDefinition:
			if typed.Name != nil {
				fragments[typed.Name.Value] = typed
			}
		}
	}
	return selected, fragments, selected != nil
}

// semanticFieldCollector counts every root selection of the field, through
// aliases, inline fragments and each fragment spread, whatever its
// directives. It stops at a second field, which is refused anyway, and bounds
// the selections it visits, so repeated fragment spreads cannot make the
// inspection exponential. A fragment cycle is malformed.
type semanticFieldCollector struct {
	fragments map[string]*ast.FragmentDefinition
	active    map[string]bool
	fields    []*ast.Field
	visits    int
}

func (c *semanticFieldCollector) collect(set *ast.SelectionSet, depth int) bool {
	if set == nil {
		return true
	}
	if depth > semanticOperationMaxDepth {
		return false
	}
	for _, selection := range set.Selections {
		if len(c.fields) > 1 {
			return true
		}
		if c.visits++; c.visits > semanticOperationMaxSelections {
			return false
		}
		switch typed := selection.(type) {
		case *ast.Field:
			if typed.Name != nil && typed.Name.Value == semanticOperationFieldName {
				c.fields = append(c.fields, typed)
			}
		case *ast.InlineFragment:
			if !c.collect(typed.SelectionSet, depth+1) {
				return false
			}
		case *ast.FragmentSpread:
			if typed.Name == nil {
				continue
			}
			name := typed.Name.Value
			if c.active[name] {
				return false
			}
			fragment := c.fragments[name]
			if fragment == nil {
				continue
			}
			c.active[name] = true
			if !c.collect(fragment.SelectionSet, depth+1) {
				return false
			}
			delete(c.active, name)
		}
	}
	return true
}

func strictSemanticOperationHeaders(header http.Header) error {
	mediaType, parameters, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return semanticOperationInvalid("requires an application/json request body")
	}
	for name, value := range parameters {
		if name != "charset" || !strings.EqualFold(value, "utf-8") {
			return semanticOperationInvalid("requires a UTF-8 JSON request body")
		}
	}
	for _, encoding := range header.Values("Content-Encoding") {
		if !strings.EqualFold(strings.TrimSpace(encoding), "identity") {
			return semanticOperationInvalid("requires an identity-encoded request body")
		}
	}
	return nil
}

// semanticEnvelope is the strict parse of one request body. Every query-like
// member value is kept, including duplicates and case variants that a lenient
// decoder would merge; err holds the first strict violation.
type semanticEnvelope struct {
	root    any
	queries []string
	err     error
}

// parseSemanticEnvelope rejects duplicate member names at every level after
// JSON escape decoding, before any object that discards duplicates exists.
func parseSemanticEnvelope(body []byte) semanticEnvelope {
	var envelope semanticEnvelope
	if !utf8.Valid(body) {
		envelope.err = semanticOperationInvalid("request body must be UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	root, err := readStrictSemanticJSON(decoder, 0, true, &envelope)
	if err != nil {
		if envelope.err == nil {
			envelope.err = err
		}
		return envelope
	}
	if _, err := decoder.Token(); err != io.EOF && envelope.err == nil {
		envelope.err = semanticOperationInvalid("request body must hold one JSON value")
	}
	envelope.root = root
	return envelope
}

func readStrictSemanticJSON(decoder *json.Decoder, depth int, collect bool, envelope *semanticEnvelope) (any, error) {
	if depth > semanticOperationMaxDepth {
		return nil, semanticOperationInvalid("request nesting")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, semanticOperationInvalid("request body JSON")
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, semanticOperationInvalid("request body JSON")
			}
			name, ok := key.(string)
			if !ok {
				return nil, semanticOperationInvalid("request body JSON")
			}
			if _, duplicate := object[name]; duplicate && envelope.err == nil {
				envelope.err = semanticOperationInvalid("duplicate JSON member")
			}
			value, err := readStrictSemanticJSON(decoder, depth+1, false, envelope)
			if err != nil {
				return nil, err
			}
			if text, isText := value.(string); collect && isText && strings.EqualFold(name, "query") {
				envelope.queries = append(envelope.queries, text)
			}
			object[name] = value
		}
		if _, err := decoder.Token(); err != nil {
			return nil, semanticOperationInvalid("request body JSON")
		}
		return object, nil
	case '[':
		list := []any{}
		for decoder.More() {
			value, err := readStrictSemanticJSON(decoder, depth+1, collect && depth == 0, envelope)
			if err != nil {
				return nil, err
			}
			list = append(list, value)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, semanticOperationInvalid("request body JSON")
		}
		return list, nil
	}
	return nil, semanticOperationInvalid("request body JSON")
}

// strictSemanticOperationMembers admits only the GraphQL-over-HTTP request
// members with their exact names and JSON types.
func strictSemanticOperationMembers(envelope map[string]any) (map[string]any, error) {
	var variables map[string]any
	for name, value := range envelope {
		switch name {
		case "query", "operationName":
		case "variables":
			if value != nil {
				var ok bool
				if variables, ok = value.(map[string]any); !ok {
					return nil, semanticOperationInvalid("variables must be an object")
				}
			}
		case "extensions":
			if _, ok := value.(map[string]any); !ok && value != nil {
				return nil, semanticOperationInvalid("extensions must be an object")
			}
		default:
			return nil, semanticOperationInvalid("unknown request member")
		}
	}
	return variables, nil
}

// semanticOperationRawInput resolves the one input argument from inline
// literals and the strictly parsed variables, never from coerced values.
func semanticOperationRawInput(field *ast.Field, selected *ast.OperationDefinition, variables map[string]any) (any, error) {
	var input ast.Value
	for _, argument := range field.Arguments {
		if argument.Name == nil || argument.Name.Value != "input" || input != nil {
			return nil, semanticOperationInvalid("semanticOperation takes exactly one input argument")
		}
		input = argument.Value
	}
	if input == nil {
		return nil, semanticOperationInvalid("semanticOperation takes exactly one input argument")
	}
	return semanticRawValue(input, selected, variables, 0)
}

func semanticRawValue(value ast.Value, selected *ast.OperationDefinition, variables map[string]any, depth int) (any, error) {
	if depth > semanticOperationMaxDepth {
		return nil, semanticOperationInvalid("input nesting")
	}
	switch typed := value.(type) {
	case *ast.Variable:
		name := typed.Name.Value
		if raw, present := variables[name]; present {
			return raw, nil
		}
		for _, definition := range selected.VariableDefinitions {
			if definition.Variable != nil && definition.Variable.Name.Value == name && definition.DefaultValue != nil {
				return semanticRawValue(definition.DefaultValue, selected, nil, depth+1)
			}
		}
		return nil, semanticOperationInvalid("input variable not provided")
	case *ast.ObjectValue:
		object := map[string]any{}
		for _, field := range typed.Fields {
			name := field.Name.Value
			if _, duplicate := object[name]; duplicate {
				return nil, semanticOperationInvalid("duplicate input member")
			}
			raw, err := semanticRawValue(field.Value, selected, variables, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = raw
		}
		return object, nil
	case *ast.ListValue:
		list := make([]any, 0, len(typed.Values))
		for _, item := range typed.Values {
			raw, err := semanticRawValue(item, selected, variables, depth+1)
			if err != nil {
				return nil, err
			}
			list = append(list, raw)
		}
		return list, nil
	case *ast.StringValue:
		return typed.Value, nil
	case *ast.IntValue:
		return json.Number(typed.Value), nil
	case *ast.FloatValue:
		return json.Number(typed.Value), nil
	case *ast.BooleanValue:
		return typed.Value, nil
	}
	return nil, semanticOperationInvalid("unsupported input literal")
}
