package main

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// controllerB524Identity overlays only complete discovery data from the
// same registry identity, controller observation and scan source. It never sends a request.
func (p *vaillantSemanticPoller) controllerB524Identity() vaillantsystem.B524ControllerIdentity {
	identity, _ := p.controllerB524IdentityAndAddress()
	return identity
}

func (p *vaillantSemanticPoller) controllerB524IdentityAndAddress() (vaillantsystem.B524ControllerIdentity, byte) {
	if p == nil {
		return vaillantsystem.B524ControllerIdentity{}, 0
	}
	p.mu.Lock()
	controller, source := p.controller, p.source
	p.mu.Unlock()
	identity, revision, ok := b524RegistryIdentity(p.reg, controller)
	if !ok {
		return vaillantsystem.B524ControllerIdentity{}, controller
	}
	return storedB524InterfaceIdentity(p.reg, controller, source, identity, revision), controller
}

func (p *vaillantSemanticPoller) controllerConfigBound() graphql.ConfigBound {
	identity, address := p.controllerB524IdentityAndAddress()
	bound := systemconfig.ResolveDHWBivalenceBound(identity)
	bound.ControllerAddress = address
	return bound
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
