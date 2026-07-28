package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const issue749FrozenM6ManifestSHA256 = "4bbbe92ba4a0294b82092d6b735261725fd45bb1f7b1fef76aca54894ec921b4"

var issue749AdditiveToolNames = []string{
	"eebus.v1.features.get",
	"eebus.v1.features.data.get",
	"eebus.v1.features.data.set",
	"eebus.v1.mutations.get",
	"eebus.v1.mutations.rollback",
}

type issue749CommandRuntime struct {
	mu    sync.Mutex
	calls []eebusraw.ToolV1
}

func TestIssue749ExactAdditiveInventoryPreservesPublicM6(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	commandRuntime := &issue749CommandRuntime{}
	issue749RegisterCommandRouter(t, server, commandRuntime)

	beforePublicList := provider.callCount()
	publicTools := issue749EEBusTools(t, server.Handler())
	if after := provider.callCount(); after != beforePublicList {
		t.Fatalf("public tools/list contacted provider: %d -> %d", beforePublicList, after)
	}
	beforeOperatorList := provider.callCount()
	operatorTools := issue749EEBusTools(t, issue743OperatorHandler(t, server))
	if after := provider.callCount(); after != beforeOperatorList {
		t.Fatalf("operator tools/list contacted provider: %d -> %d", beforeOperatorList, after)
	}
	if calls := commandRuntime.callCount(); calls != 0 {
		t.Fatalf("tools/list contacted command runtime %d times", calls)
	}

	publicNames := issue749ToolNames(publicTools)
	wantPublicNames := append([]string(nil), msp06ToolNames...)
	sort.Strings(wantPublicNames)

	if !reflect.DeepEqual(publicNames, wantPublicNames) {
		t.Fatalf("public eeBUS tools = %v, want exact frozen M6 inventory %v", publicNames, wantPublicNames)
	}

	publicManifest, err := json.Marshal(publicTools)
	if err != nil {
		t.Fatal(err)
	}
	gotDigest := sha256.Sum256(publicManifest)
	if got := fmt.Sprintf("%x", gotDigest); got != issue749FrozenM6ManifestSHA256 {
		t.Fatalf("public M6 manifest SHA-256 = %s, want frozen %s\nmanifest: %s",
			got, issue749FrozenM6ManifestSHA256, publicManifest)
	}

	operatorM6Manifest, err := json.Marshal(issue749SelectTools(operatorTools, msp06ToolNames))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(publicManifest, operatorM6Manifest) {
		t.Fatalf("operator changed the frozen M6 tools:\npublic: %s\noperator M6: %s",
			publicManifest, operatorM6Manifest)
	}

	wantOperatorNames := append(append([]string(nil), msp06ToolNames...), issue749AdditiveToolNames...)
	sort.Strings(wantOperatorNames)
	operatorNames := issue749ToolNames(operatorTools)
	if !reflect.DeepEqual(operatorNames, wantOperatorNames) {
		t.Fatalf("operator eeBUS tools = %v, want exact 14-tool inventory %v",
			operatorNames, wantOperatorNames)
	}

	operatorManifest, err := json.Marshal(operatorTools)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{
		[]byte("candidate_ref"),
		[]byte("eebus.v2."),
		[]byte("eebus.v1.features.read"),
	} {
		if bytes.Contains(operatorManifest, forbidden) {
			t.Fatalf("operator manifest exposes forbidden token %q: %s", forbidden, operatorManifest)
		}
	}
}

func TestIssue749PublicBoundaryDeniesCommandsBeforeProvider(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	commandRuntime := &issue749CommandRuntime{}
	issue749RegisterCommandRouter(t, server, commandRuntime)

	for _, test := range issue749CommandCases(t) {
		t.Run(test.tool, func(t *testing.T) {
			before := provider.callCount()
			params, err := json.Marshal(map[string]any{
				"name":      test.tool,
				"arguments": test.arguments,
			})
			if err != nil {
				t.Fatal(err)
			}
			response := doRPC(t, server.Handler(), rpcRequest{
				JSONRPC: "2.0",
				ID:      1,
				Method:  "tools/call",
				Params:  params,
			})
			if response.Error != nil {
				t.Fatalf("public call returned RPC error %+v, want content-level permission_denied", response.Error)
			}
			result := msp06Map(t, response.Result, "public command result")
			if isError, _ := result["isError"].(bool); !isError {
				t.Fatal("public denial did not set isError=true")
			}
			content := msp06Slice(t, result["content"], "public command content")
			if len(content) != 1 {
				t.Fatalf("public command content count = %d, want 1", len(content))
			}
			text, _ := msp06Map(t, content[0], "public command content[0]")["text"].(string)
			var envelope map[string]any
			if err := json.Unmarshal([]byte(text), &envelope); err != nil {
				t.Fatalf("decode public denial envelope: %v; text=%q", err, text)
			}
			meta := msp06Map(t, envelope["meta"], "public command meta")
			if got, _ := meta["tool"].(string); got != test.tool {
				t.Fatalf("public command meta.tool = %q, want %q", got, test.tool)
			}
			if got, _ := meta["scope"].(string); got != test.scope {
				t.Fatalf("public command meta.scope = %q, want %q", got, test.scope)
			}
			if got, _ := meta["auth_scope"].(string); got != test.scope {
				t.Fatalf("public command meta.auth_scope = %q, want %q", got, test.scope)
			}
			if got, _ := meta["mask_tier"].(string); got != "raw" {
				t.Fatalf("public command meta.mask_tier = %q, want raw", got)
			}
			if envelope["data"] != nil {
				t.Fatalf("public denial data = %#v, want null", envelope["data"])
			}
			publicError := msp06Map(t, envelope["error"], "public command error")
			if code, _ := publicError["code"].(string); code != "permission_denied" {
				t.Fatalf("public command error code = %q, want permission_denied", code)
			}
			if calls := commandRuntime.callCount(); calls != 0 {
				t.Fatalf("public call reached command runtime %d times", calls)
			}
			if after := provider.callCount(); after != before {
				t.Fatalf("public call contacted provider: %d -> %d", before, after)
			}
		})
	}
}

func TestIssue749OperatorCommandsRouteOnlyThroughCommandRouter(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	commandRuntime := &issue749CommandRuntime{}
	issue749RegisterCommandRouter(t, server, commandRuntime)
	operator := issue743OperatorHandler(t, server)

	for index, test := range issue749CommandCases(t) {
		t.Run(test.tool, func(t *testing.T) {
			beforeProvider := provider.callCount()
			result := msp06Call(t, operator, test.tool, test.arguments)
			if !result.isError {
				t.Fatal("recording runtime terminal response did not set isError=true")
			}
			publicError := msp06Map(t, result.envelope["error"], "operator command error")
			if code, _ := publicError["code"].(string); code != "invalid_argument" {
				t.Fatalf("operator command error code = %q, want recording invalid_argument", code)
			}
			if got := commandRuntime.callCount(); got != index+1 {
				t.Fatalf("command runtime calls = %d, want %d", got, index+1)
			}
			if got := commandRuntime.lastCall(); got != eebusraw.ToolV1(test.tool) {
				t.Fatalf("command runtime last call = %q, want %q", got, test.tool)
			}
			if after := provider.callCount(); after != beforeProvider {
				t.Fatalf("operator command contacted snapshot provider: %d -> %d", beforeProvider, after)
			}
		})
	}
}

func TestIssue749ServerExposesTypedCommandRouterRegistration(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)
	if method := reflect.ValueOf(server).MethodByName("RegisterEEBusV1CommandRouter"); !method.IsValid() {
		t.Fatal("MCP server lacks RegisterEEBusV1CommandRouter(EEBusV1CommandRouter) registration boundary")
	}
}

func issue749RegisterCommandRouter(t *testing.T, server *Server, router any) {
	t.Helper()
	method := reflect.ValueOf(server).MethodByName("RegisterEEBusV1CommandRouter")
	if !method.IsValid() {
		t.Fatal("MCP server lacks RegisterEEBusV1CommandRouter(EEBusV1CommandRouter) registration boundary")
	}
	results := method.Call([]reflect.Value{reflect.ValueOf(router)})
	if len(results) != 1 {
		t.Fatalf("RegisterEEBusV1CommandRouter result count = %d, want 1", len(results))
	}
	if !results[0].IsNil() {
		t.Fatalf("register eeBUS command router: %v", results[0].Interface())
	}
}

func issue749EEBusTools(t *testing.T, handler http.Handler) []map[string]any {
	t.Helper()
	response := doRPC(t, handler, rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if response.Error != nil {
		t.Fatalf("tools/list error = %+v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T, want map", response.Result)
	}
	rawTools, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T, want []any", result["tools"])
	}

	tools := make([]map[string]any, 0, len(msp06ToolNames)+len(issue749AdditiveToolNames))
	for _, rawTool := range rawTools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			t.Fatalf("tool type = %T, want map", rawTool)
		}
		name, _ := tool["name"].(string)
		if strings.HasPrefix(name, "eebus.") {
			tools = append(tools, tool)
		}
	}
	sort.Slice(tools, func(left, right int) bool {
		leftName, _ := tools[left]["name"].(string)
		rightName, _ := tools[right]["name"].(string)
		return leftName < rightName
	})
	return tools
}

func issue749ToolNames(tools []map[string]any) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		names = append(names, name)
	}
	return names
}

func issue749SelectTools(tools []map[string]any, names []string) []map[string]any {
	selectedNames := make(map[string]struct{}, len(names))
	for _, name := range names {
		selectedNames[name] = struct{}{}
	}
	selected := make([]map[string]any, 0, len(names))
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		if _, ok := selectedNames[name]; ok {
			selected = append(selected, tool)
		}
	}
	return selected
}

type issue749CommandCase struct {
	tool      string
	scope     string
	arguments map[string]any
}

func issue749CommandCases(t *testing.T) []issue749CommandCase {
	t.Helper()
	locator := eebusraw.FeatureLocatorV1{
		RemoteSKI: strings.Repeat("a", 40), SHIPID: "fixture-ship-id",
		DeviceAddress: "0", EntityAddress: []uint64{1}, FeatureAddress: 2,
		FeatureType: "Measurement", FeatureRole: eebusraw.FeatureRoleV1Server,
	}
	readTarget := eebusraw.FeatureTargetV1{
		RemoteSKI: locator.RemoteSKI, SHIPID: locator.SHIPID,
		DeviceAddress: locator.DeviceAddress, EntityAddress: locator.EntityAddress,
		FeatureAddress: locator.FeatureAddress, FeatureType: locator.FeatureType,
		FeatureRole: locator.FeatureRole, Function: "measurementListData",
		Operation: eebusraw.OperationV1Read,
	}
	writeTarget := readTarget.Clone()
	writeTarget.Operation = eebusraw.OperationV1Write
	value, err := eebusraw.NewTypedValueV1("fixture-value")
	if err != nil {
		t.Fatal(err)
	}

	return []issue749CommandCase{
		{
			tool:  string(eebusraw.ToolV1FeaturesGet),
			scope: string(eebusraw.AuthScopeV1RawRead),
			arguments: issue749Arguments(t, eebusraw.FeaturesGetRequestV1{
				Target: locator,
			}),
		},
		{
			tool:  string(eebusraw.ToolV1FeaturesDataGet),
			scope: string(eebusraw.AuthScopeV1RawRead),
			arguments: issue749Arguments(t, eebusraw.FeatureDataGetRequestV1{
				Targets: []eebusraw.FeatureTargetV1{readTarget},
			}),
		},
		{
			tool:  string(eebusraw.ToolV1FeaturesDataSet),
			scope: string(eebusraw.AuthScopeV1RawWrite),
			arguments: issue749Arguments(t, eebusraw.FeatureDataSetRequestV1{
				Target: writeTarget, Value: value, ReadToken: strings.Repeat("E", 43),
				IdempotencyKey: "fixture-set-key-01", Mode: eebusraw.ModeV1Apply,
			}),
		},
		{
			tool:  string(eebusraw.ToolV1MutationsGet),
			scope: string(eebusraw.AuthScopeV1RawRead),
			arguments: issue749Arguments(t, eebusraw.MutationGetRequestV1{
				MutationRef: strings.Repeat("M", 43),
			}),
		},
		{
			tool:  string(eebusraw.ToolV1MutationsRollback),
			scope: string(eebusraw.AuthScopeV1RawWrite),
			arguments: issue749Arguments(t, eebusraw.MutationRollbackRequestV1{
				MutationRef: strings.Repeat("M", 43), IdempotencyKey: "fixture-rollback-key-01",
			}),
		},
	}
}

func issue749Arguments(t *testing.T, request any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var arguments map[string]any
	if err := json.Unmarshal(encoded, &arguments); err != nil {
		t.Fatal(err)
	}
	return arguments
}

func (runtime *issue749CommandRuntime) terminal(tool eebusraw.ToolV1) *eebusraw.ErrorV1 {
	runtime.mu.Lock()
	runtime.calls = append(runtime.calls, tool)
	runtime.mu.Unlock()
	return eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1InvalidArgument,
		"recording command runtime terminal",
		false,
		eebusraw.SourceLayerV1Validation,
	)
}

func (runtime *issue749CommandRuntime) callCount() int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return len(runtime.calls)
}

func (runtime *issue749CommandRuntime) lastCall() eebusraw.ToolV1 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.calls) == 0 {
		return ""
	}
	return runtime.calls[len(runtime.calls)-1]
}

func (runtime *issue749CommandRuntime) FeaturesGet(
	_ context.Context,
	_ eebusraw.ReadAuthorizationV1,
	_ eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeaturesGetDataV1{}, runtime.terminal(eebusraw.ToolV1FeaturesGet)
}

func (runtime *issue749CommandRuntime) FeaturesDataGet(
	_ context.Context,
	_ eebusraw.ReadAuthorizationV1,
	_ eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeatureDataGetDataV1{}, runtime.terminal(eebusraw.ToolV1FeaturesDataGet)
}

func (runtime *issue749CommandRuntime) FeaturesDataSet(
	_ context.Context,
	_ eebusraw.WriteAuthorizationV1,
	_ eebusraw.FeatureDataSetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	return eebusraw.MutationV1{}, runtime.terminal(eebusraw.ToolV1FeaturesDataSet)
}

func (runtime *issue749CommandRuntime) MutationsGet(
	_ context.Context,
	_ eebusraw.ReadAuthorizationV1,
	_ eebusraw.MutationGetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	return eebusraw.MutationV1{}, runtime.terminal(eebusraw.ToolV1MutationsGet)
}

func (runtime *issue749CommandRuntime) MutationsRollback(
	_ context.Context,
	_ eebusraw.WriteAuthorizationV1,
	_ eebusraw.MutationRollbackRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	return eebusraw.MutationV1{}, runtime.terminal(eebusraw.ToolV1MutationsRollback)
}
