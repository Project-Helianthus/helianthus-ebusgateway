package graphql

import (
	"context"
	"sync"
)

type Builder struct {
	registry Registry
	changes  <-chan struct{}

	mu       sync.RWMutex
	schema   Schema
	revision uint64
}

func NewBuilder(reg Registry, changes <-chan struct{}) *Builder {
	return &Builder{
		registry: reg,
		changes:  changes,
	}
}

func (b *Builder) Start(ctx context.Context) error {
	if err := b.Rebuild(); err != nil {
		return err
	}
	if b.changes == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Goroutine exits when ctx.Done() closes.
	go func() {
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
	}()

	return nil
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
