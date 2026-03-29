package graphql

type GatewayIdentity struct {
	InstanceGUID string
}

type GatewayIdentityProvider interface {
	GatewayIdentity() GatewayIdentity
}

type staticGatewayIdentityProvider struct{}

func (staticGatewayIdentityProvider) GatewayIdentity() GatewayIdentity {
	return GatewayIdentity{}
}
