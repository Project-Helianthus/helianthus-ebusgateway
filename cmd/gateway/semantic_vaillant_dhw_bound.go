package main

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
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

// controllerConfigBound resolves the native hot water bivalence bound for the
// discovered controller; the publication owner maps it to its GraphQL and MCP
// forms.
func (p *vaillantSemanticPoller) controllerConfigBound() systemconfig.ConfigBound {
	// Fence the complete resolution, including retained interface qualifiers.
	var before systemconfig.BoundRegistryIdentity
	if p != nil {
		p.mu.Lock()
		address := p.controller
		p.mu.Unlock()
		before = systemconfig.CaptureBoundRegistryIdentity(p.reg, address)
	}
	identity, address := p.controllerB524IdentityAndAddress()
	bound := systemconfig.ResolveDHWBivalenceBound(identity)
	bound.ControllerAddress = address
	if p != nil && before.Valid && before == systemconfig.CaptureBoundRegistryIdentity(p.reg, address) {
		bound.ControllerRegistryIdentity = before
	}
	return bound
}
