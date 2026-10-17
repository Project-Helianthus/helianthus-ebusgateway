package main

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// controllerB524Identity overlays only complete, fresh discovery data from the
// same registry identity, generation and scan source. It never sends a request.
func (p *vaillantSemanticPoller) controllerB524Identity() vaillantsystem.B524ControllerIdentity {
	if p == nil {
		return vaillantsystem.B524ControllerIdentity{}
	}
	p.mu.Lock()
	controller, source := p.controller, p.source
	p.mu.Unlock()
	identity, generation, ok := b524RegistryIdentity(p.reg, controller)
	if !ok {
		return vaillantsystem.B524ControllerIdentity{}
	}
	return storedB524InterfaceIdentity(p.reg, controller, source, identity, generation, time.Now())
}

func configBoundEquals(a, b *graphql.ConfigBound) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func mcpConfigBound(bound *graphql.ConfigBound) *mcp.ConfigBound {
	if bound == nil {
		return nil
	}
	return &mcp.ConfigBound{
		NarrowedOnly: bound.NarrowedOnly,
		MinC:         bound.MinC,
		MaxC:         bound.MaxC,
		Source:       bound.Source,
		Reason:       bound.Reason,
		Profile:      bound.Profile,
	}
}
