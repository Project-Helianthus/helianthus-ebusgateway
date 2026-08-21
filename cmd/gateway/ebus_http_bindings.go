package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// These bindings keep the HTTP/MCP surface stable while eBUS startup and
// source selection continue behind it. Before binding, mutations fail closed;
// after binding, calls delegate to the same admitted writers used previously.
type lateEBusScheduleWriter struct {
	mu       sync.RWMutex
	delegate mcp.ScheduleWriter
}

func (writer *lateEBusScheduleWriter) Bind(delegate mcp.ScheduleWriter) {
	writer.mu.Lock()
	writer.delegate = delegate
	writer.mu.Unlock()
}

func (writer *lateEBusScheduleWriter) current() mcp.ScheduleWriter {
	writer.mu.RLock()
	delegate := writer.delegate
	writer.mu.RUnlock()
	return delegate
}

func (writer *lateEBusScheduleWriter) SetZoneTimeProgram(ctx context.Context, zone, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	delegate := writer.current()
	if delegate == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: semanticWriterSourceNotAdmittedError}, nil
	}
	return delegate.SetZoneTimeProgram(ctx, zone, weekday, slots)
}

func (writer *lateEBusScheduleWriter) SetDhwTimeProgram(ctx context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	delegate := writer.current()
	if delegate == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: semanticWriterSourceNotAdmittedError}, nil
	}
	return delegate.SetDhwTimeProgram(ctx, weekday, slots)
}

func (writer *lateEBusScheduleWriter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	delegate := writer.current()
	owner, ok := delegate.(m8CommandRoutingSnapshotter)
	if !ok {
		return m8sourcestate.CommandRoutingFragment{}, fmt.Errorf("schedule writer unavailable")
	}
	return owner.M8CommandRoutingState()
}

type lateEBusConfigWriter struct {
	mu       sync.RWMutex
	delegate mcp.ConfigWriter
}

func (writer *lateEBusConfigWriter) Bind(delegate mcp.ConfigWriter) {
	writer.mu.Lock()
	writer.delegate = delegate
	writer.mu.Unlock()
}

func (writer *lateEBusConfigWriter) current() mcp.ConfigWriter {
	writer.mu.RLock()
	delegate := writer.delegate
	writer.mu.RUnlock()
	return delegate
}

func (writer *lateEBusConfigWriter) SetSystemConfig(ctx context.Context, field, value string) mcp.ConfigSetResult {
	delegate := writer.current()
	if delegate == nil {
		return mcp.ConfigSetResult{Success: false, Error: semanticWriterSourceNotAdmittedError}
	}
	return delegate.SetSystemConfig(ctx, field, value)
}

func (writer *lateEBusConfigWriter) SetBoilerConfig(ctx context.Context, field, value string) mcp.ConfigSetResult {
	delegate := writer.current()
	if delegate == nil {
		return mcp.ConfigSetResult{Success: false, Error: semanticWriterSourceNotAdmittedError}
	}
	return delegate.SetBoilerConfig(ctx, field, value)
}

func (writer *lateEBusConfigWriter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	delegate := writer.current()
	owner, ok := delegate.(m8CommandRoutingSnapshotter)
	if !ok {
		return m8sourcestate.CommandRoutingFragment{}, fmt.Errorf("config writer unavailable")
	}
	return owner.M8CommandRoutingState()
}

type lateEBusWatchSummaryProvider struct {
	mu       sync.RWMutex
	delegate mcp.WatchSummaryProvider
}

type graphQLSemanticWriter interface {
	graphql.BoilerConfigWriter
	graphql.SystemConfigWriter
	graphql.ScheduleWriter
}

// lateEBusGraphQLWriter is installed before the GraphQL schema is built. The
// schema therefore keeps the same mutation fields for the lifetime of the
// process; calls fail closed until the semantic poller is bound and admitted.
type lateEBusGraphQLWriter struct {
	mu       sync.RWMutex
	delegate graphQLSemanticWriter
}

func (writer *lateEBusGraphQLWriter) Bind(delegate graphQLSemanticWriter) {
	writer.mu.Lock()
	writer.delegate = delegate
	writer.mu.Unlock()
}

func (writer *lateEBusGraphQLWriter) current() graphQLSemanticWriter {
	writer.mu.RLock()
	delegate := writer.delegate
	writer.mu.RUnlock()
	return delegate
}

func (writer *lateEBusGraphQLWriter) SetBoilerConfig(ctx context.Context, fieldName, rawValue string) graphql.BoilerConfigMutationResult {
	delegate := writer.current()
	if delegate == nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: semanticWriterSourceNotAdmittedError}
	}
	return delegate.SetBoilerConfig(ctx, fieldName, rawValue)
}

func (writer *lateEBusGraphQLWriter) SetSystemConfig(ctx context.Context, fieldName, rawValue string) graphql.ConfigMutationResult {
	delegate := writer.current()
	if delegate == nil {
		return graphql.ConfigMutationResult{Success: false, Error: semanticWriterSourceNotAdmittedError}
	}
	return delegate.SetSystemConfig(ctx, fieldName, rawValue)
}

func (writer *lateEBusGraphQLWriter) SetZoneTimeProgram(ctx context.Context, zone, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	delegate := writer.current()
	if delegate == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: semanticWriterSourceNotAdmittedError}, nil
	}
	return delegate.SetZoneTimeProgram(ctx, zone, weekday, slots)
}

func (writer *lateEBusGraphQLWriter) SetDhwTimeProgram(ctx context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	delegate := writer.current()
	if delegate == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: semanticWriterSourceNotAdmittedError}, nil
	}
	return delegate.SetDhwTimeProgram(ctx, weekday, slots)
}

type lateEBusGraphQLWatchSummaryProvider struct {
	mu       sync.RWMutex
	delegate graphql.WatchSummaryProvider
}

func (provider *lateEBusGraphQLWatchSummaryProvider) Bind(delegate graphql.WatchSummaryProvider) {
	provider.mu.Lock()
	provider.delegate = delegate
	provider.mu.Unlock()
}

func (provider *lateEBusGraphQLWatchSummaryProvider) Snapshot() graphql.WatchSummary {
	provider.mu.RLock()
	delegate := provider.delegate
	provider.mu.RUnlock()
	if delegate == nil {
		return graphql.WatchSummary{Degraded: graphql.WatchSummaryDegraded{
			Active:  true,
			Reasons: []string{"driver_unavailable"},
		}}
	}
	return delegate.Snapshot()
}

func (provider *lateEBusWatchSummaryProvider) Bind(delegate mcp.WatchSummaryProvider) {
	provider.mu.Lock()
	provider.delegate = delegate
	provider.mu.Unlock()
}

func (provider *lateEBusWatchSummaryProvider) Snapshot() mcp.WatchSummary {
	provider.mu.RLock()
	delegate := provider.delegate
	provider.mu.RUnlock()
	if delegate == nil {
		return mcp.WatchSummary{Degraded: mcp.WatchSummaryDegraded{
			Active:  true,
			Reasons: []string{"driver_unavailable"},
		}}
	}
	return delegate.Snapshot()
}
