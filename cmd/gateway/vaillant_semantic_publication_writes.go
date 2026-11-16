package main

import (
	"context"
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// The poller's configuration and time-program writers return native
// results; the writer adapters below gate them behind the admitted source
// and map the native results to the GraphQL and MCP forms with the same
// field names.

// admittedGraphQLSemanticWriter gates the poller's writers behind the
// admitted source; it maps the native configuration write results to their
// GraphQL form, and its schedule path maps MCP slots to the native schedule
// writer and the native write results back to their MCP form.
type admittedGraphQLSemanticWriter struct {
	boiler   vaillantBoilerConfigWriter
	system   vaillantSystemConfigWriter
	schedule vaillantScheduleWriter
	admitted admittedSourceProvider
}

func (writer admittedGraphQLSemanticWriter) SetBoilerConfig(ctx context.Context, fieldName string, rawValue string) graphql.BoilerConfigMutationResult {
	if !admittedSourceActive(writer.admitted) {
		return graphql.BoilerConfigMutationResult{Success: false, Error: semanticWriterSourceNotAdmittedError}
	}
	if writer.boiler == nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "boiler config writer unavailable"}
	}
	return graphqlBoilerConfigResult(writer.boiler.SetBoilerConfig(ctx, fieldName, rawValue))
}

func (writer admittedGraphQLSemanticWriter) SetSystemConfig(ctx context.Context, fieldName string, rawValue string) graphql.ConfigMutationResult {
	if !admittedSourceActive(writer.admitted) {
		return graphql.ConfigMutationResult{Success: false, Error: semanticWriterSourceNotAdmittedError}
	}
	if writer.system == nil {
		return graphql.ConfigMutationResult{Success: false, Error: "system config writer unavailable"}
	}
	return graphqlSystemConfigResult(writer.system.SetSystemConfig(ctx, fieldName, rawValue))
}

func (writer admittedGraphQLSemanticWriter) SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	if !admittedSourceActive(writer.admitted) {
		return &mcp.TimeProgramWriteResult{Success: false, Error: semanticWriterSourceNotAdmittedError}, nil
	}
	if writer.schedule == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "schedule writer unavailable"}, nil
	}
	return mcpTimeProgramWriteResult(writer.schedule.SetZoneTimeProgram(ctx, zone, weekday, timeProgramSlotsFromMCP(slots)))
}

func (writer admittedGraphQLSemanticWriter) SetDhwTimeProgram(ctx context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	if !admittedSourceActive(writer.admitted) {
		return &mcp.TimeProgramWriteResult{Success: false, Error: semanticWriterSourceNotAdmittedError}, nil
	}
	if writer.schedule == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "schedule writer unavailable"}, nil
	}
	return mcpTimeProgramWriteResult(writer.schedule.SetDhwTimeProgram(ctx, weekday, timeProgramSlotsFromMCP(slots)))
}

// mcpConfigWriterAdapter adapts the semantic poller's config write methods
// to the MCP ConfigWriter interface and maps their native results.
type mcpConfigWriterAdapter struct {
	poller *vaillantSemanticPoller
}

func (a *mcpConfigWriterAdapter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	if a == nil || a.poller == nil {
		return m8sourcestate.CommandRoutingFragment{}, fmt.Errorf("config writer unavailable")
	}
	return m8sourcestate.CommandRoutingFragment{Routes: []m8sourcestate.CommandRoute{
		{SemanticPath: "/mcp/ebus_v1_semantic_boiler_status_set_config", Source: "ebus", Available: true},
		{SemanticPath: "/mcp/ebus_v1_semantic_system_set_config", Source: "ebus", Available: true},
	}}, nil
}

func (a *mcpConfigWriterAdapter) SetSystemConfig(ctx context.Context, field string, value string) mcp.ConfigSetResult {
	if a == nil || a.poller == nil {
		return mcp.ConfigSetResult{Error: "system config writer unavailable"}
	}
	return mcpConfigSetResult(a.poller.SetSystemConfig(ctx, field, value))
}

func (a *mcpConfigWriterAdapter) SetBoilerConfig(ctx context.Context, field string, value string) mcp.ConfigSetResult {
	if a == nil || a.poller == nil {
		return mcp.ConfigSetResult{Error: "boiler config writer unavailable"}
	}
	return mcpConfigSetResult(a.poller.SetBoilerConfig(ctx, field, value))
}

// graphqlBoilerConfigResult maps a native configuration write result to the
// GraphQL boiler mutation result.
func graphqlBoilerConfigResult(result vaillantConfigWriteResult) graphql.BoilerConfigMutationResult {
	return graphql.BoilerConfigMutationResult{Success: result.Success, Error: result.Error}
}

// graphqlSystemConfigResult maps a native configuration write result to the
// GraphQL system mutation result.
func graphqlSystemConfigResult(result vaillantConfigWriteResult) graphql.ConfigMutationResult {
	return graphql.ConfigMutationResult{Success: result.Success, Error: result.Error}
}

// mcpConfigSetResult maps a native configuration write result to the MCP
// configuration set result.
func mcpConfigSetResult(result vaillantConfigWriteResult) mcp.ConfigSetResult {
	return mcp.ConfigSetResult{Success: result.Success, Error: result.Error}
}
