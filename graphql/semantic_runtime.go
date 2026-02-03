package graphql

import (
	"context"

	"github.com/d3vi1/helianthus-ebusreg/router"
)

// SemanticRuntime bridges router broadcasts into semantic snapshots and subscriptions.
type SemanticRuntime struct {
	router   *router.BusEventRouter
	provider *LiveSemanticProvider
	hub      *BroadcastHub
}

func NewSemanticRuntime(router *router.BusEventRouter, provider *LiveSemanticProvider, hub *BroadcastHub) *SemanticRuntime {
	if provider == nil {
		provider = NewLiveSemanticProvider()
	}
	return &SemanticRuntime{
		router:   router,
		provider: provider,
		hub:      hub,
	}
}

// WireSemantic attaches a live semantic provider to the builder and returns a runtime bridge.
func WireSemantic(builder *Builder, router *router.BusEventRouter, hub *BroadcastHub) *SemanticRuntime {
	runtime := NewSemanticRuntime(router, nil, hub)
	if builder != nil {
		builder.SetSemanticProvider(runtime.Provider())
	}
	return runtime
}

func (runtime *SemanticRuntime) Provider() *LiveSemanticProvider {
	if runtime == nil {
		return nil
	}
	return runtime.provider
}

func (runtime *SemanticRuntime) Start(ctx context.Context) {
	if runtime == nil || runtime.router == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go runtime.run(ctx)
}

func (runtime *SemanticRuntime) run(ctx context.Context) {
	events := runtime.router.Events()
	if events == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if runtime.provider == nil {
				continue
			}
			if totals, updated := runtime.provider.ApplyBroadcast(event); updated {
				if runtime.hub != nil {
					runtime.hub.PublishEnergyUpdate(totals)
				}
			}
		}
	}
}
