package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
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

type scanStats struct {
	ok         int
	timeouts   int
	collisions int
	nacks      int
	crcErrors  int
	otherErrs  int
}

type statsBus struct {
	bus   registry.ScanBus
	stats scanStats
}

func (b *statsBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("scan stats bus missing")
	}
	response, err := b.bus.Send(ctx, frame)
	if err == nil {
		b.stats.ok++
		return response, nil
	}

	switch {
	case errors.Is(err, ebuserrors.ErrTimeout) || errors.Is(err, context.DeadlineExceeded):
		b.stats.timeouts++
	case errors.Is(err, ebuserrors.ErrBusCollision):
		b.stats.collisions++
	case errors.Is(err, ebuserrors.ErrNACK):
		b.stats.nacks++
	case errors.Is(err, ebuserrors.ErrCRCMismatch):
		b.stats.crcErrors++
	default:
		b.stats.otherErrs++
	}
	return response, err
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
			scanBus := &statsBus{
				bus: &timeoutBus{bus: gateway.Bus, timeout: cfg.ScanRequestTimeout},
			}
			targets := ([]byte)(nil)
			if cfg.TransportConfig.Protocol == ebusgateway.TransportEbusdTCP && cfg.TransportConfig.Network == "tcp" {
				if scanTargets, err := ebusdScanResultTargets(scanCtx, cfg.TransportConfig); err == nil && len(scanTargets) > 0 {
					targets = scanTargets
				}
			}

			devices, err := registry.Scan(scanCtx, scanBus, gateway.Registry, cfg.ScanSource, targets)
			cancel()

			if err != nil && ctx.Err() == nil {
				log.Printf("startup scan error: %v", err)
			}
			total := countRegistryDevices(gateway.Registry)
			log.Printf("startup scan: pass=%d device(s), total=%d", len(devices), total)
			log.Printf(
				"startup scan stats: ok=%d timeouts=%d collisions=%d nacks=%d crc=%d other=%d",
				scanBus.stats.ok,
				scanBus.stats.timeouts,
				scanBus.stats.collisions,
				scanBus.stats.nacks,
				scanBus.stats.crcErrors,
				scanBus.stats.otherErrs,
			)

			if total > 0 && total != previousTotal {
				previousTotal = total
				gateway.RefreshRouterPlanes()
				if builder != nil {
					if err := builder.Rebuild(); err != nil {
						log.Printf("graphql schema rebuild failed after scan: %v", err)
					}
				}
			}

			if shouldStopDiscoveryScan(total) {
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

func shouldStopDiscoveryScan(total int) bool {
	return total > 0
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

func ebusdScanResultTargets(ctx context.Context, cfg ebusgateway.TransportConfig) ([]byte, error) {
	if cfg.Address == "" || cfg.Network != "tcp" {
		return nil, nil
	}

	dial := cfg.Dial
	if dial == nil {
		dial = dialContext
	}

	dialCtx := ctx
	cancel := func() {}
	if dialCtx == nil {
		dialCtx = context.Background()
	}
	if cfg.DialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(dialCtx, cfg.DialTimeout)
	}
	defer cancel()

	conn, err := dial(dialCtx, cfg.Network, cfg.Address, cfg.DialTimeout)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	if _, err := conn.Write([]byte("scan result\n")); err != nil {
		return nil, err
	}

	var targets []byte
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) {
				return targets, err
			}
			return targets, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "err") {
			continue
		}
		fields := strings.Split(line, ";")
		if len(fields) == 0 {
			continue
		}
		raw := strings.TrimSpace(fields[0])
		if raw == "" {
			continue
		}
		value, parseErr := strconv.ParseUint(raw, 16, 8)
		if parseErr != nil {
			continue
		}
		targets = append(targets, byte(value))
	}

	return targets, nil
}

func dialContext(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}
