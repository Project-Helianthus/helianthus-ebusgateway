package main

import (
	"context"
	"testing"

	gatewaygraphql "github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	graphqlgo "github.com/graphql-go/graphql"
)

type recordingLateScheduleWriter struct{ calls int }

func (writer *recordingLateScheduleWriter) SetZoneTimeProgram(context.Context, int, int, []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	writer.calls++
	return &mcp.TimeProgramWriteResult{Success: true}, nil
}

func (writer *recordingLateScheduleWriter) SetDhwTimeProgram(context.Context, int, []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	writer.calls++
	return &mcp.TimeProgramWriteResult{Success: true}, nil
}

type recordingLateConfigWriter struct{ calls int }

func (writer *recordingLateConfigWriter) SetSystemConfig(context.Context, string, string) mcp.ConfigSetResult {
	writer.calls++
	return mcp.ConfigSetResult{Success: true}
}

func (writer *recordingLateConfigWriter) SetBoilerConfig(context.Context, string, string) mcp.ConfigSetResult {
	writer.calls++
	return mcp.ConfigSetResult{Success: true}
}

type staticLateWatchProvider struct{ snapshot mcp.WatchSummary }

func (provider staticLateWatchProvider) Snapshot() mcp.WatchSummary { return provider.snapshot }

type recordingLateGraphQLWriter struct{ calls int }

func (writer *recordingLateGraphQLWriter) SetBoilerConfig(context.Context, string, string) gatewaygraphql.BoilerConfigMutationResult {
	writer.calls++
	return gatewaygraphql.BoilerConfigMutationResult{Success: true}
}

func (writer *recordingLateGraphQLWriter) SetSystemConfig(context.Context, string, string) gatewaygraphql.ConfigMutationResult {
	writer.calls++
	return gatewaygraphql.ConfigMutationResult{Success: true}
}

func (writer *recordingLateGraphQLWriter) SetZoneTimeProgram(context.Context, int, int, []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	writer.calls++
	return &mcp.TimeProgramWriteResult{Success: true}, nil
}

func (writer *recordingLateGraphQLWriter) SetDhwTimeProgram(context.Context, int, []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	writer.calls++
	return &mcp.TimeProgramWriteResult{Success: true}, nil
}

type staticLateGraphQLWatchProvider struct{ snapshot gatewaygraphql.WatchSummary }

func (provider staticLateGraphQLWatchProvider) Snapshot() gatewaygraphql.WatchSummary {
	return provider.snapshot
}

type emptyLateInvokeRegistry struct{}

func (emptyLateInvokeRegistry) Lookup(byte) (registry.DeviceEntry, bool) {
	return nil, false
}

type emptyLateInvoker struct{}

func (emptyLateInvoker) Invoke(context.Context, router.Plane, string, map[string]any) (any, error) {
	return nil, nil
}

func TestIssue851EarlyHTTPBindingsFailClosedThenDelegate(t *testing.T) {
	schedule := &lateEBusScheduleWriter{}
	config := &lateEBusConfigWriter{}
	watch := &lateEBusWatchSummaryProvider{}
	graphQLWriter := &lateEBusGraphQLWriter{}
	graphQLWatch := &lateEBusGraphQLWatchSummaryProvider{}

	if result, err := schedule.SetZoneTimeProgram(context.Background(), 0, 0, nil); err != nil || result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("unbound schedule result = %#v, %v", result, err)
	}
	if result := config.SetSystemConfig(context.Background(), "field", "value"); result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("unbound config result = %#v", result)
	}
	if snapshot := watch.Snapshot(); !snapshot.Degraded.Active || len(snapshot.Degraded.Reasons) != 1 || snapshot.Degraded.Reasons[0] != "driver_unavailable" {
		t.Fatalf("unbound watch snapshot = %#v", snapshot)
	}
	if result := graphQLWriter.SetSystemConfig(context.Background(), "field", "value"); result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("unbound GraphQL config result = %#v", result)
	}
	if snapshot := graphQLWatch.Snapshot(); !snapshot.Degraded.Active || len(snapshot.Degraded.Reasons) != 1 || snapshot.Degraded.Reasons[0] != "driver_unavailable" {
		t.Fatalf("unbound GraphQL watch snapshot = %#v", snapshot)
	}

	recordingSchedule := &recordingLateScheduleWriter{}
	recordingConfig := &recordingLateConfigWriter{}
	recordingGraphQL := &recordingLateGraphQLWriter{}
	wantWatch := mcp.WatchSummary{Inventory: mcp.WatchSummaryInventory{TotalEntries: 3}}
	wantGraphQLWatch := gatewaygraphql.WatchSummary{Inventory: gatewaygraphql.WatchSummaryInventory{TotalEntries: 4}}
	schedule.Bind(recordingSchedule)
	config.Bind(recordingConfig)
	watch.Bind(staticLateWatchProvider{snapshot: wantWatch})
	graphQLWriter.Bind(recordingGraphQL)
	graphQLWatch.Bind(staticLateGraphQLWatchProvider{snapshot: wantGraphQLWatch})

	if result, err := schedule.SetDhwTimeProgram(context.Background(), 0, nil); err != nil || !result.Success || recordingSchedule.calls != 1 {
		t.Fatalf("bound schedule result = %#v, %v calls=%d", result, err, recordingSchedule.calls)
	}
	if result := config.SetBoilerConfig(context.Background(), "field", "value"); !result.Success || recordingConfig.calls != 1 {
		t.Fatalf("bound config result = %#v calls=%d", result, recordingConfig.calls)
	}
	if got := watch.Snapshot(); got.Inventory.TotalEntries != wantWatch.Inventory.TotalEntries {
		t.Fatalf("bound watch snapshot = %#v", got)
	}
	if result := graphQLWriter.SetSystemConfig(context.Background(), "field", "value"); !result.Success || recordingGraphQL.calls != 1 {
		t.Fatalf("bound GraphQL config result = %#v calls=%d", result, recordingGraphQL.calls)
	}
	if got := graphQLWatch.Snapshot(); got.Inventory.TotalEntries != wantGraphQLWatch.Inventory.TotalEntries {
		t.Fatalf("bound GraphQL watch snapshot = %#v", got)
	}
}

func TestIssue851EarlyGraphQLMutationSurfaceRemainsStableAcrossBinding(t *testing.T) {
	writer := &lateEBusGraphQLWriter{}
	builder := gatewaygraphql.NewBuilder(nil, nil)
	builder.SetBoilerConfigWriter(writer)
	builder.SetSystemConfigWriter(writer)
	builder.SetScheduleWriter(writer)
	schema, err := gatewaygraphql.NewSchema(builder, emptyLateInvokeRegistry{}, emptyLateInvoker{}, nil)
	if err != nil {
		t.Fatalf("NewSchema() error = %v", err)
	}

	execute := func() map[string]any {
		t.Helper()
		result := graphqlgo.Do(graphqlgo.Params{
			Schema:        schema,
			RequestString: `mutation { setSystemConfig(field: "field", value: "value") { success error } }`,
		})
		if len(result.Errors) != 0 {
			t.Fatalf("GraphQL mutation errors = %v", result.Errors)
		}
		root, ok := result.Data.(map[string]any)
		if !ok {
			t.Fatalf("GraphQL data = %#v", result.Data)
		}
		mutation, ok := root["setSystemConfig"].(map[string]any)
		if !ok {
			t.Fatalf("GraphQL mutation result = %#v", root["setSystemConfig"])
		}
		return mutation
	}

	unbound := execute()
	if unbound["success"] != false || unbound["error"] != semanticWriterSourceNotAdmittedError {
		t.Fatalf("unbound mutation = %#v", unbound)
	}
	recording := &recordingLateGraphQLWriter{}
	writer.Bind(recording)
	bound := execute()
	if bound["success"] != true || bound["error"] != nil || recording.calls != 1 {
		t.Fatalf("bound mutation = %#v calls=%d", bound, recording.calls)
	}
}
