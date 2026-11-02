package graphql

import (
	"context"
	"reflect"
	"sync"
)

type Builder struct {
	registry               Registry
	changes                <-chan struct{}
	status                 StatusProvider
	identity               GatewayIdentityProvider
	semantic               SemanticProvider
	semanticSnapshot       SemanticSnapshotProvider
	semanticOperation      SemanticOperationRouter
	bus                    BusObservabilityProvider
	identificationEvidence IdentificationEvidenceProvider
	watch                  WatchSummaryProvider
	boiler                 BoilerConfigWriter
	system                 SystemConfigWriter
	schedule               ScheduleWriter
	vaillantB503           VaillantB503Provider
	mutationSource         byte
	mutationSourceAdmitted bool
	mutationSourceProvider func() (byte, bool)

	mu       sync.RWMutex
	schema   Schema
	revision uint64
}

func NewBuilder(reg Registry, changes <-chan struct{}) *Builder {
	return &Builder{
		registry: reg,
		changes:  changes,
		status:   staticStatusProvider{},
		identity: staticGatewayIdentityProvider{},
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

func (b *Builder) SetGatewayIdentityProvider(provider GatewayIdentityProvider) {
	if b == nil || provider == nil {
		return
	}
	b.mu.Lock()
	b.identity = provider
	b.mu.Unlock()
}

func (b *Builder) gatewayIdentityProvider() GatewayIdentityProvider {
	if b == nil {
		return staticGatewayIdentityProvider{}
	}
	b.mu.RLock()
	provider := b.identity
	b.mu.RUnlock()
	if provider == nil {
		return staticGatewayIdentityProvider{}
	}
	return provider
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
	if evidence, ok := provider.(IdentificationEvidenceProvider); ok {
		b.identificationEvidence = evidence
	}
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

func (b *Builder) SetSystemConfigWriter(writer SystemConfigWriter) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.system = writer
	b.mu.Unlock()
}

func (b *Builder) SetAdmittedMutationSource(source byte) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.mutationSource = source
	b.mutationSourceAdmitted = source != 0
	b.mu.Unlock()
}

func (b *Builder) ClearAdmittedMutationSource() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.mutationSource = 0
	b.mutationSourceAdmitted = false
	b.mu.Unlock()
}

// SetAdmittedMutationSourceProvider installs the live request-time authority
// used by GraphQL mutations. The builder's selected source remains separate so
// source-selection state can survive a temporary driver withdrawal.
func (b *Builder) SetAdmittedMutationSourceProvider(provider func() (byte, bool)) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.mutationSourceProvider = provider
	b.mu.Unlock()
}

func (b *Builder) admittedMutationSource() (byte, bool) {
	if b == nil {
		return 0, false
	}
	b.mu.RLock()
	source := b.mutationSource
	admitted := b.mutationSourceAdmitted
	b.mu.RUnlock()
	return source, admitted
}

func (b *Builder) AdmittedMutationSource() (byte, bool) {
	return b.admittedMutationSource()
}

func (b *Builder) mutationSourceForRequest() (byte, bool) {
	if b == nil {
		return 0, false
	}
	b.mu.RLock()
	provider := b.mutationSourceProvider
	b.mu.RUnlock()
	if provider != nil {
		return provider()
	}
	return b.admittedMutationSource()
}

func (b *Builder) systemConfigWriter() SystemConfigWriter {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	writer := b.system
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

// SetSemanticSnapshotProvider supplies coherent, already public-safe SemReg captures.
// Nil providers, including typed nils, clear the registration.
func (b *Builder) SetSemanticSnapshotProvider(provider SemanticSnapshotProvider) {
	if b == nil {
		return
	}
	if provider != nil {
		value := reflect.ValueOf(provider)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				provider = nil
			}
		}
	}
	b.mu.Lock()
	b.semanticSnapshot = provider
	b.mu.Unlock()
}

func (b *Builder) semanticSnapshotProvider() SemanticSnapshotProvider {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.semanticSnapshot
}

func (b *Builder) systemConfigBound() *ConfigBound {
	system := b.semanticProvider().System()
	if system == nil || system.Config.DhwBivalenceBound == nil {
		return nil
	}
	bound := *system.Config.DhwBivalenceBound
	return &bound
}

// SetSemanticOperationRouter installs the bounded dispatcher of the one
// semanticOperation mutation. Nil routers, including typed nils, clear the
// registration; the mutation then answers UNAVAILABLE without dispatch.
func (b *Builder) SetSemanticOperationRouter(router SemanticOperationRouter) {
	if b == nil {
		return
	}
	if router != nil {
		value := reflect.ValueOf(router)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				router = nil
			}
		}
	}
	b.mu.Lock()
	b.semanticOperation = router
	b.mu.Unlock()
}

func (b *Builder) semanticOperationRouter() SemanticOperationRouter {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.semanticOperation
}
