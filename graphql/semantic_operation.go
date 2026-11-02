package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"mime"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/ast"
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
	// semanticOperationMaxSelections bounds the selections the resolver visits
	// while counting semanticOperation fields of the parsed operation.
	semanticOperationMaxSelections = 4096
	// semanticOperationTransportRootKey holds, in the HTTP root value, the
	// request-line and header facts the semanticOperation resolver checks.
	semanticOperationTransportRootKey = "semanticOperationTransport"
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
			// Every limit holds on the parsed operation before admission; a
			// refusal here never reaches the router, so nothing is sent.
			request, err := semanticOperationParsedRequest(ctx, p)
			if err != nil {
				return nil, semanticOperationErrorFrom(err)
			}
			router := builder.semanticOperationRouter()
			if router == nil {
				return nil, semanticOperationError{code: semanticoperations.CodeUnavailable, detail: "operation provider unwired"}
			}
			routed, err := router.Route(ctx, request)
			if err != nil {
				return nil, semanticOperationErrorFrom(err)
			}
			return semanticOperationResultValue(routed), nil
		},
	}
}

// semanticOperationParsedRequest applies the route's request limits to the
// operation the stock GraphQL handler has already parsed: the transport facts
// recorded with the HTTP root value, at most one semanticOperation selection
// in the executing operation, the aggregate input size, and the strict typed
// decode of the input as written. The input is rebuilt from the document's
// literals and the request variables as the handler decoded them, never from
// GraphQL-coerced arguments. No request body is read or scanned here.
func semanticOperationParsedRequest(ctx context.Context, p graphqlgo.ResolveParams) (semanticoperations.Request, error) {
	var none semanticoperations.Request
	transport, ok := semanticOperationRootTransport(p.Info.RootValue)
	if !ok {
		return none, semanticOperationInvalid("requires one JSON request body")
	}
	if transport.err != nil {
		return none, transport.err
	}
	operationDefinition, ok := p.Info.Operation.(*ast.OperationDefinition)
	if !ok || operationDefinition == nil || len(p.Info.FieldASTs) != 1 {
		return none, semanticOperationInvalid("operation selection")
	}
	fragments := map[string]*ast.FragmentDefinition{}
	for name, fragment := range p.Info.Fragments {
		if typed, ok := fragment.(*ast.FragmentDefinition); ok {
			fragments[name] = typed
		}
	}
	collector := semanticFieldCollector{fragments: fragments, active: map[string]bool{}}
	if !collector.collect(operationDefinition.SelectionSet, 0) {
		return none, semanticOperationInvalid("fragment structure")
	}
	if len(collector.fields) != 1 {
		return none, semanticOperationInvalid("at most one semanticOperation field per request")
	}
	if operationDefinition.Operation != ast.OperationTypeMutation {
		return none, semanticOperationInvalid("semanticOperation is a mutation")
	}
	variables, ok := ctx.Value(semanticOperationVariablesKey{}).(*semanticOperationVariables)
	if !ok || variables == nil {
		return none, semanticOperationInvalid("request variables unavailable")
	}
	raw, err := semanticOperationRawInput(p.Info.FieldASTs[0], operationDefinition, variables.values)
	if err != nil {
		return none, err
	}
	meter := semanticInputMeter{remaining: semanticoperations.MaxRequestBytes}
	if err := meter.measure(raw, 0); err != nil {
		return none, err
	}
	return decodeSemanticOperationInput(raw)
}

// semanticOperationTransport records whether the stock handler took the
// operation from one UTF-8, identity-encoded JSON POST body. It is derived
// from the request line and headers alone and only the semanticOperation
// resolver reads it; every other field ignores it.
type semanticOperationTransport struct{ err error }

// semanticOperationTransportOf mirrors the stock handler's request-option
// selection: a URL query, a non-POST method, a raw GraphQL or form body, or a
// non-JSON media type never carries a semantic operation.
func semanticOperationTransportOf(r *http.Request) *semanticOperationTransport {
	if r == nil || r.Method != http.MethodPost || r.URL == nil || r.URL.Query().Get("query") != "" {
		return &semanticOperationTransport{err: semanticOperationInvalid("requires one JSON request body")}
	}
	return &semanticOperationTransport{err: strictSemanticOperationHeaders(r.Header)}
}

func semanticOperationRootTransport(root any) (*semanticOperationTransport, bool) {
	values, ok := root.(map[string]any)
	if !ok {
		return nil, false
	}
	transport, ok := values[semanticOperationTransportRootKey].(*semanticOperationTransport)
	return transport, ok && transport != nil
}

// semanticOperationVariables holds the request variables exactly as the
// caller of graphql.Do supplied them (for HTTP, as the stock handler decoded
// the body), before GraphQL input coercion.
type semanticOperationVariables struct{ values map[string]any }

type semanticOperationVariablesKey struct{}

// semanticOperationVariablesExtension keeps a reference to the supplied
// variables in the execution context so the semanticOperation resolver can
// decode its input without GraphQL's lenient coercion (numbers to strings,
// truncated integers, singleton lists). It never reads, rejects or alters a
// request and adds nothing to any result.
type semanticOperationVariablesExtension struct{}

func (semanticOperationVariablesExtension) Init(ctx context.Context, p *graphqlgo.Params) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, semanticOperationVariablesKey{}, &semanticOperationVariables{values: p.VariableValues})
}

func (semanticOperationVariablesExtension) Name() string { return "semanticOperationVariables" }

func (semanticOperationVariablesExtension) ParseDidStart(ctx context.Context) (context.Context, graphqlgo.ParseFinishFunc) {
	return ctx, func(error) {}
}

func (semanticOperationVariablesExtension) ValidationDidStart(ctx context.Context) (context.Context, graphqlgo.ValidationFinishFunc) {
	return ctx, func([]gqlerrors.FormattedError) {}
}

func (semanticOperationVariablesExtension) ExecutionDidStart(ctx context.Context) (context.Context, graphqlgo.ExecutionFinishFunc) {
	return ctx, func(*graphqlgo.Result) {}
}

func (semanticOperationVariablesExtension) ResolveFieldDidStart(ctx context.Context, _ *graphqlgo.ResolveInfo) (context.Context, graphqlgo.ResolveFieldFinishFunc) {
	return ctx, func(interface{}, error) {}
}

func (semanticOperationVariablesExtension) HasResult() bool { return false }

func (semanticOperationVariablesExtension) GetResult(context.Context) interface{} { return nil }

// semanticInputMeter measures the compact encoding/json form of the raw
// input, inline literals and variable values alike, and stops as soon as the
// route's byte bound is exceeded, so an aggregate of individually valid
// members cannot pass.
type semanticInputMeter struct{ remaining int }

func (m *semanticInputMeter) add(bytes int) error {
	if m.remaining -= bytes; m.remaining < 0 {
		return semanticOperationExhausted("input exceeds the byte bound")
	}
	return nil
}

func (m *semanticInputMeter) measure(value any, depth int) error {
	if depth > semanticOperationMaxDepth {
		return semanticOperationInvalid("input nesting")
	}
	switch typed := value.(type) {
	case map[string]any:
		if err := m.add(2 + max(len(typed)-1, 0)); err != nil {
			return err
		}
		for name, member := range typed {
			if err := m.text(name); err != nil {
				return err
			}
			if err := m.add(1); err != nil {
				return err
			}
			if err := m.measure(member, depth+1); err != nil {
				return err
			}
		}
		return nil
	case []any:
		if err := m.add(2 + max(len(typed)-1, 0)); err != nil {
			return err
		}
		for _, member := range typed {
			if err := m.measure(member, depth+1); err != nil {
				return err
			}
		}
		return nil
	case string:
		return m.text(typed)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return semanticOperationInvalid("input value")
	}
	return m.add(len(encoded))
}

// text never encodes a string longer than the remaining budget: its encoding
// is at least its bytes plus the two quotes.
func (m *semanticInputMeter) text(value string) error {
	if len(value)+2 > m.remaining {
		return semanticOperationExhausted("input exceeds the byte bound")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return semanticOperationInvalid("input value")
	}
	return m.add(len(encoded))
}

// semanticIntegerText returns the decimal text of an integer input: an
// integer literal of the document, or an integral JSON number of the
// variables. Fractions, strings and booleans are never converted.
func semanticIntegerText(raw any) (string, bool) {
	switch number := raw.(type) {
	case json.Number:
		return string(number), semanticIntegerToken.MatchString(string(number))
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) || number != math.Trunc(number) {
			return "", false
		}
		return strconv.FormatFloat(number, 'f', -1, 64), true
	}
	return "", false
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
		number, ok := semanticIntegerText(raw)
		if !ok {
			return nil, semanticOperationInvalid("input member must be an integer")
		}
		if _, err := strconv.ParseInt(number, 10, 32); err != nil {
			return nil, semanticOperationInvalid("input member exceeds Int")
		}
		return json.Number(number), nil
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

// decodeSemanticOperationInput decodes the closed input member set from the
// raw values of the parsed operation, so GraphQL's lenient variable coercion
// never applies.
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

	timeout, ok := semanticIntegerText(object["timeoutMs"])
	if !ok {
		return request, semanticOperationInvalid("timeoutMs must be an integer")
	}
	milliseconds, err := strconv.ParseInt(timeout, 10, 64)
	if err != nil || milliseconds < 1 || milliseconds > semanticoperations.MaxTimeout.Milliseconds() {
		return request, semanticOperationInvalid("timeoutMs out of range")
	}
	request.Timeout = time.Duration(milliseconds) * time.Millisecond
	if err := request.Validate(); err != nil {
		return request, semanticOperationErrorFrom(err)
	}
	return request, nil
}

// semanticFieldCollector counts every root selection of the field, through
// aliases, inline fragments and each fragment spread, whatever its
// directives. It stops at a second field, which is refused anyway, and bounds
// the selections it visits, so repeated fragment spreads cannot make the
// count exponential. A fragment cycle is malformed.
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

// strictSemanticOperationHeaders admits only an identity-encoded
// application/json body with no parameter other than a UTF-8 charset.
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

// semanticOperationRawInput resolves the one input argument from the
// document's literals and the variables as supplied, never from coerced values.
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
