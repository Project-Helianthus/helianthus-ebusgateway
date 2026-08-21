package ebusgateway

import (
	"context"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// OpenEBusDriverTransport constructs one fresh eBUS transport generation.
// Lifecycle ownership remains with the caller; the returned close function is
// invoked only by the adapter-owned DriverRuntime close worker.
func OpenEBusDriverTransport(ctx context.Context, cfg Config) (transport.RawTransport, func() error, error) {
	cfg = applyDefaults(cfg)
	return resolveTransport(ctx, cfg)
}

// OpenEBusDriverPassiveTransport constructs the generation-coupled passive
// observer connection used by proxy-like ENH/ENS configurations.
func OpenEBusDriverPassiveTransport(ctx context.Context, cfg Config) (transport.RawTransport, error) {
	cfg = applyDefaults(cfg)
	return resolvePassiveTransport(ctx, cfg)
}
