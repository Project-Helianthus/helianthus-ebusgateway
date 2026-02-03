package graphql

import (
	"context"
	"sync"
)

type Builder struct {
	registry Registry
	changes  <-chan struct{}
	status   StatusProvider

	mu       sync.RWMutex
	schema   Schema
	revision uint64
}

func NewBuilder(reg Registry, changes <-chan struct{}) *Builder {
	return &Builder{
		registry: reg,
		changes:  changes,
		status:   staticStatusProvider{},
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
