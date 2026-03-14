package graphql

import (
	"context"
	"sync"
)

type Builder struct {
	registry Registry
	changes  <-chan struct{}
	status   StatusProvider
	semantic SemanticProvider
	bus      BusObservabilityProvider
	watch    WatchSummaryProvider
	boiler   BoilerConfigWriter
	schedule ScheduleWriter

	mu       sync.RWMutex
	schema   Schema
	revision uint64
}

func NewBuilder(reg Registry, changes <-chan struct{}) *Builder {
	return &Builder{
		registry: reg,
		changes:  changes,
		status:   staticStatusProvider{},
		semantic: staticSemanticProvider{},
		bus:      staticBusObservabilityProvider{},
		watch:    staticWatchSummaryProvider{},
	}
}

func (b *Builder) Start(ctx context.Context) error {
	if err := b.Rebuild(); err != nil {
		return err
	}
	if b.changes == nil {
		return nil
	}

	// Goroutine exits when ctx.Done() closes or when changes closes.
	go b.watchChanges(ctx)

	return nil
}

func (b *Builder) watchChanges(ctx context.Context) {
	if b == nil || b.changes == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-b.changes:
			if !ok {
				return
			}
			_ = b.Rebuild()
		}
	}
}

func (b *Builder) Rebuild() error {
	if b == nil {
		return nil
	}

	schema, err := BuildSchema(b.registry)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.schema = schema
	b.revision++
	b.mu.Unlock()

	return nil
}

func (b *Builder) Schema() Schema {
	if b == nil {
		return Schema{}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return cloneSchema(b.schema)
}

func (b *Builder) FreshSchema() Schema {
	if b == nil {
		return Schema{}
	}

	// Registry mutations can arrive outside explicit builder change hooks,
	// for example from semantic discovery after boot. Rebuild on read so
	// schema-backed device views do not stay pinned to an empty startup snapshot.
	_ = b.Rebuild()

	b.mu.RLock()
	defer b.mu.RUnlock()
	return cloneSchema(b.schema)
}

func (b *Builder) Revision() uint64 {
	if b == nil {
		return 0
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.revision
}

func (b *Builder) SetStatusProvider(provider StatusProvider) {
	if b == nil || provider == nil {
		return
	}
	b.mu.Lock()
	b.status = provider
	b.mu.Unlock()
}

func (b *Builder) statusProvider() StatusProvider {
	if b == nil {
		return staticStatusProvider{}
	}
	b.mu.RLock()
	provider := b.status
	b.mu.RUnlock()
	if provider == nil {
		return staticStatusProvider{}
	}
	return provider
}

func (b *Builder) SetSemanticProvider(provider SemanticProvider) {
	if b == nil || provider == nil {
		return
	}
	b.mu.Lock()
	b.semantic = provider
	b.mu.Unlock()
}

func (b *Builder) semanticProvider() SemanticProvider {
	if b == nil {
		return staticSemanticProvider{}
	}
	b.mu.RLock()
	provider := b.semantic
	b.mu.RUnlock()
	if provider == nil {
		return staticSemanticProvider{}
	}
	return provider
}

func (b *Builder) SetBusObservabilityProvider(provider BusObservabilityProvider) {
	if b == nil || provider == nil {
		return
	}
	b.mu.Lock()
	b.bus = provider
	b.mu.Unlock()
}

func (b *Builder) busObservabilityProvider() BusObservabilityProvider {
	if b == nil {
		return staticBusObservabilityProvider{}
	}
	b.mu.RLock()
	provider := b.bus
	b.mu.RUnlock()
	if provider == nil {
		return staticBusObservabilityProvider{}
	}
	return provider
}

func (b *Builder) SetWatchSummaryProvider(provider WatchSummaryProvider) {
	if b == nil || provider == nil {
		return
	}
	b.mu.Lock()
	b.watch = provider
	b.mu.Unlock()
}

func (b *Builder) watchSummaryProvider() WatchSummaryProvider {
	if b == nil {
		return staticWatchSummaryProvider{}
	}
	b.mu.RLock()
	provider := b.watch
	b.mu.RUnlock()
	if provider == nil {
		return staticWatchSummaryProvider{}
	}
	return provider
}

func (b *Builder) SetBoilerConfigWriter(writer BoilerConfigWriter) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.boiler = writer
	b.mu.Unlock()
}

func (b *Builder) boilerConfigWriter() BoilerConfigWriter {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	writer := b.boiler
	b.mu.RUnlock()
	return writer
}

func (b *Builder) SetScheduleWriter(writer ScheduleWriter) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.schedule = writer
	b.mu.Unlock()
}

func (b *Builder) scheduleWriter() ScheduleWriter {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	writer := b.schedule
	b.mu.RUnlock()
	return writer
}
