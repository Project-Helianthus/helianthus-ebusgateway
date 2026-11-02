package graphql

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// SemanticSnapshotProvider captures publication pointers, evaluation context and
// catalog coherently. It must apply public-exposure and lifecycle gates before
// returning inputs; Capture performs no native acquisition or operation dispatch.
type SemanticSnapshotProvider interface {
	Capture(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)
}

type semanticReadError string

func (e semanticReadError) Error() string { return "semantic snapshot read: " + string(e) }
func (e semanticReadError) Extensions() map[string]interface{} {
	return map[string]interface{}{"code": string(e)}
}

const semanticSnapshotRootKey = "_semanticSnapshotCache"

type semanticSnapshotCache struct {
	provider SemanticSnapshotProvider
	once     sync.Once
	view     semanticview.View
	err      error
}

func semanticSnapshotField(builder *Builder) *graphqlgo.Field {
	types := map[reflect.Type]*graphqlgo.Object{}
	output := semanticRecordType(reflect.TypeOf(semanticview.View{}), types)
	return &graphqlgo.Field{Type: output, Args: graphqlgo.FieldConfigArgument{
		"assetIds": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.ID)))},
	}, Resolve: func(p graphqlgo.ResolveParams) (any, error) {
		ids, err := semanticAssetIDs(p.Args["assetIds"])
		if err != nil {
			return nil, err
		}
		cache := &semanticSnapshotCache{provider: builder.semanticSnapshotProvider()}
		if root, ok := p.Info.RootValue.(map[string]any); ok {
			if shared, ok := root[semanticSnapshotRootKey].(*semanticSnapshotCache); ok {
				cache = shared
			}
		}
		cache.once.Do(func() {
			union, err := semanticOperationIDs(p.Info)
			if err != nil {
				cache.err = err
				return
			}
			if cache.provider == nil {
				cache.err = semanticReadError("UNAVAILABLE")
				return
			}
			metadata, inputs, err := cache.provider.Capture(p.Context, union)
			if err != nil {
				cache.err = semanticReadError("UNAVAILABLE")
				return
			}
			// Do not permit a provider to omit, substitute or add an asset.
			selected := map[semreg.AssetID]bool{}
			for _, id := range union {
				selected[id] = true
			}
			if len(inputs) != len(union) {
				cache.err = semanticReadError("CONFLICT")
				return
			}
			for _, input := range inputs {
				if !selected[input.AssetID] {
					cache.err = semanticReadError("CONFLICT")
					return
				}
				delete(selected, input.AssetID)
			}
			cache.view, err = semanticview.Build(metadata, inputs)
			if err != nil {
				code := "CONFLICT"
				switch semreg.ErrorIdentifier(err) {
				case semreg.RevisionConflict, semreg.DigestMismatch:
					code = "CONFLICT"
				case semreg.BoundsExceeded:
					code = "RESOURCE_EXHAUSTED"
				case semreg.DefinitionOwnerMissing:
					code = "UNAVAILABLE"
				}
				cache.err = semanticReadError(code)
			}
		})
		if cache.err != nil {
			return nil, cache.err
		}
		selected := map[semreg.AssetID]bool{}
		for _, id := range ids {
			selected[id] = true
		}
		view := semanticview.View{Assets: []semanticview.Asset{}}
		for _, asset := range cache.view.Assets {
			if selected[asset.AssetID] {
				view.Assets = append(view.Assets, asset)
			}
		}
		raw, err := json.Marshal(view)
		if err != nil {
			return nil, semanticReadError("CONFLICT")
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, semanticReadError("CONFLICT")
		}
		return semanticCamelValue(value), nil
	}}
}

func semanticAssetIDs(raw any) ([]semreg.AssetID, error) {
	values, ok := raw.([]interface{})
	if !ok || len(values) < 1 || len(values) > 32 {
		return nil, semanticReadError("INVALID_ARGUMENT")
	}
	ids := make([]semreg.AssetID, 0, len(values))
	seen := map[semreg.AssetID]bool{}
	for _, value := range values {
		text, ok := value.(string)
		id := semreg.AssetID(text)
		if !ok || len(text) > 256 || strings.Contains(text, "://") || id.Validate() != nil || seen[id] {
			return nil, semanticReadError("INVALID_ARGUMENT")
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, nil
}

// Walk only root selections, including active fragments and directives. Nested
// fields cannot contribute assets to the operation-wide capture.
func semanticOperationIDs(info graphqlgo.ResolveInfo) ([]semreg.AssetID, error) {
	union := map[semreg.AssetID]bool{}
	visited := map[string]bool{}
	var walk func(*ast.SelectionSet) error
	active := func(ds []*ast.Directive) bool {
		for _, d := range ds {
			for _, a := range d.Arguments {
				if a.Name.Value != "if" {
					continue
				}
				var value any
				switch v := a.Value.(type) {
				case *ast.BooleanValue:
					value = v.Value
				case *ast.Variable:
					value = info.VariableValues[v.Name.Value]
				}
				b, _ := value.(bool)
				if (d.Name.Value == "skip" && b) || (d.Name.Value == "include" && !b) {
					return false
				}
			}
		}
		return true
	}
	walk = func(set *ast.SelectionSet) error {
		if set == nil {
			return nil
		}
		for _, selection := range set.Selections {
			switch s := selection.(type) {
			case *ast.Field:
				if s.Name.Value != "semanticSnapshot" || !active(s.Directives) {
					continue
				}
				var raw any
				for _, a := range s.Arguments {
					if a.Name.Value != "assetIds" {
						continue
					}
					raw = semanticASTValue(a.Value, info.VariableValues)
				}
				ids, err := semanticAssetIDs(raw)
				if err != nil {
					return err
				}
				for _, id := range ids {
					union[id] = true
				}
			case *ast.InlineFragment:
				if active(s.Directives) {
					if err := walk(s.SelectionSet); err != nil {
						return err
					}
				}
			case *ast.FragmentSpread:
				if !visited[s.Name.Value] && active(s.Directives) {
					visited[s.Name.Value] = true
					if fragment := info.Fragments[s.Name.Value]; fragment != nil {
						if err := walk(fragment.GetSelectionSet()); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}
	if err := walk(info.Operation.GetSelectionSet()); err != nil {
		return nil, err
	}
	if len(union) < 1 || len(union) > 32 {
		return nil, semanticReadError("INVALID_ARGUMENT")
	}
	ids := make([]semreg.AssetID, 0, len(union))
	for id := range union {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
func semanticASTValue(value ast.Value, variables map[string]interface{}) any {
	switch v := value.(type) {
	case *ast.Variable:
		return variables[v.Name.Value]
	case *ast.StringValue:
		return []interface{}{v.Value} // GraphQL singleton list coercion
	case *ast.IntValue:
		return []interface{}{v.Value}
	case *ast.ListValue:
		out := make([]interface{}, 0, len(v.Values))
		for _, item := range v.Values {
			switch x := item.(type) {
			case *ast.StringValue:
				out = append(out, x.Value)
			case *ast.IntValue:
				out = append(out, x.Value)
			case *ast.Variable:
				out = append(out, variables[x.Name.Value])
			default:
				out = append(out, nil)
			}
		}
		return out
	}
	return nil
}

func semanticCamel(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}
func semanticCamelValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, v := range x {
			out[semanticCamel(k)] = semanticCamelValue(v)
		}
		return out
	case []any:
		for i := range x {
			x[i] = semanticCamelValue(x[i])
		}
	}
	return v
}

// Derive typed objects from the pinned public records, retaining JSON presence
// and scalar types. Prefix names to avoid collisions with legacy eBUS objects.
func semanticRecordType(t reflect.Type, objects map[reflect.Type]*graphqlgo.Object) graphqlgo.Output {
	if t.Kind() == reflect.Pointer {
		return semanticRecordType(t.Elem(), objects)
	}
	switch t.Kind() {
	case reflect.String:
		return graphqlgo.String
	case reflect.Bool:
		return graphqlgo.Boolean
	case reflect.Int, reflect.Int32, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16:
		return graphqlgo.Int
	case reflect.Slice:
		return graphqlgo.NewList(graphqlgo.NewNonNull(semanticRecordType(t.Elem(), objects)))
	case reflect.Struct:
		if object := objects[t]; object != nil {
			return object
		}
		name := "Semantic" + t.Name()
		if t == reflect.TypeOf(semanticview.View{}) {
			name = "SemanticSnapshotRead"
		}
		if t == reflect.TypeOf(semanticview.Asset{}) {
			name = "AssetSnapshotRead"
		}
		object := graphqlgo.NewObject(graphqlgo.ObjectConfig{Name: name, Fields: graphqlgo.FieldsThunk(func() graphqlgo.Fields {
			fields := graphqlgo.Fields{}
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				tag := strings.Split(f.Tag.Get("json"), ",")
				if tag[0] == "" || tag[0] == "-" {
					continue
				}
				output := semanticRecordType(f.Type, objects)
				optional := f.Type.Kind() == reflect.Pointer || strings.Contains(f.Tag.Get("json"), ",omitempty")
				if !optional {
					output = graphqlgo.NewNonNull(output)
				}
				fields[semanticCamel(tag[0])] = &graphqlgo.Field{Type: output}
			}
			return fields
		})})
		objects[t] = object
		return object
	default:
		panic("unsupported SemReg record scalar: " + t.String())
	}
}
