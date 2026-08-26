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

// EBusDriverTransportProtocol resolves URI scheme overrides through the same
// normalization used by transport construction. Invalid endpoints retain the
// canonical configured fallback so their error remains driver-local.
func EBusDriverTransportProtocol(config TransportConfig) TransportProtocol {
	normalized, err := NormalizeEBusDriverTransportConfig(config)
	if err == nil {
		if isAdapterDirectTransportProtocol(normalized.Protocol) {
			return TransportAdapterDirect
		}
		return normalized.Protocol
	}
	protocol := canonicalTransportProtocol(config.Protocol)
	if isAdapterDirectTransportProtocol(protocol) {
		return TransportAdapterDirect
	}
	return protocol
}

// NormalizeEBusDriverTransportConfig resolves a valid endpoint URI into the
// canonical protocol/network/address tuple used by both the process shell and
// transport construction. Callers must retain an invalid descriptor unchanged
// so its failure remains isolated to the eBUS driver lifecycle.
func NormalizeEBusDriverTransportConfig(config TransportConfig) (TransportConfig, error) {
	return normalizeTransportConfig(config)
}
