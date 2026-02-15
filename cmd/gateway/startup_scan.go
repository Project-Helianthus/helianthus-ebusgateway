package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
)

type timeoutBus struct {
	bus     registry.ScanBus
	timeout time.Duration
}

func (b *timeoutBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("scan timeout bus missing")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if b.timeout <= 0 {
		return b.bus.Send(ctx, frame)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= b.timeout {
			return b.bus.Send(ctx, frame)
		}
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.bus.Send(ctxTimeout, frame)
}

func startDiscoveryScanLoop(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder) {
	if !cfg.ScanOnStart || gateway == nil || gateway.Bus == nil || gateway.Registry == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	interval := cfg.ScanInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	go func() {
		previousTotal := 0
		for {
			scanCtx := ctx
			cancel := func() {}
			if cfg.ScanTimeout > 0 {
				scanCtx, cancel = context.WithTimeout(ctx, cfg.ScanTimeout)
			}
			scanBus := &timeoutBus{bus: gateway.Bus, timeout: cfg.ScanRequestTimeout}
			devices, err := registry.Scan(scanCtx, scanBus, gateway.Registry, cfg.ScanSource, nil)
			cancel()

			if err != nil && ctx.Err() == nil {
				log.Printf("startup scan error: %v", err)
			}
			total := countRegistryDevices(gateway.Registry)
			log.Printf("startup scan: pass=%d device(s), total=%d", len(devices), total)

			if total > 0 && total != previousTotal {
				previousTotal = total
				gateway.RefreshRouterPlanes()
				if builder != nil {
					if err := builder.Rebuild(); err != nil {
						log.Printf("graphql schema rebuild failed after scan: %v", err)
					}
				}
			}

			if err == nil && total > 0 {
				return
			}

			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func countRegistryDevices(reg *registry.DeviceRegistry) int {
	if reg == nil {
		return 0
	}
	count := 0
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		count++
		return true
	})
	return count
}
