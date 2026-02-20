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

type ebusdScanResultRow struct {
	Address         byte
	Manufacturer    string
	DeviceID        string
	SoftwareVersion string
	HardwareVersion string
	SerialNumber    string
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
			targetLabel := ""
			var targetConfig *ebusgateway.TransportConfig
			for _, candidate := range ebusdScanTargetCandidates(cfg.TransportConfig) {
				scanTargets, err := ebusdScanResultTargets(scanCtx, candidate)
				if err != nil || len(scanTargets) == 0 {
					continue
				}
				targets = scanTargets
				targetLabel = candidate.Address
				candidateCopy := candidate
				targetConfig = &candidateCopy
				break
			}
			if len(targets) > 0 {
				log.Printf("startup scan: using %d target(s) from ebusd scan result at %s", len(targets), targetLabel)
			}

			devices, err := registry.Scan(scanCtx, scanBus, gateway.Registry, cfg.ScanSource, targets)

			if err != nil && ctx.Err() == nil {
				log.Printf("startup scan error: %v", err)
			}

			total := countRegistryDevices(gateway.Registry)
			imported := 0
			if total == 0 && len(devices) == 0 && targetConfig != nil &&
				scanBus.stats.ok == 0 && (scanBus.stats.timeouts > 0 || scanBus.stats.collisions > 0) {
				infos, infoErr := ebusdScanResultInfos(scanCtx, *targetConfig)
				if infoErr != nil {
					log.Printf("startup scan fallback error: %v", infoErr)
				} else if len(infos) > 0 {
					for _, info := range infos {
						gateway.Registry.Register(info)
					}
					imported = len(infos)
					total = countRegistryDevices(gateway.Registry)
				}
			}
			if imported > 0 {
				log.Printf("startup scan fallback: imported %d device(s) from ebusd scan result", imported)
			}
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
			cancel()

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

func ebusdScanTargetCandidates(config ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
	candidates := make([]ebusgateway.TransportConfig, 0, 2)

	if config.Protocol == ebusgateway.TransportEbusdTCP && strings.EqualFold(config.Network, "tcp") {
		candidates = append(candidates, config)
	}

	fallback := ebusgateway.TransportConfig{
		Network:     "tcp",
		Address:     "127.0.0.1:8888",
		DialTimeout: config.DialTimeout,
		Dial:        config.Dial,
	}
	if fallback.DialTimeout <= 0 || fallback.DialTimeout > 2*time.Second {
		fallback.DialTimeout = 2 * time.Second
	}

	alreadyPresent := false
	for _, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.Network), fallback.Network) &&
			strings.TrimSpace(candidate.Address) == fallback.Address {
			alreadyPresent = true
			break
		}
	}
	if !alreadyPresent {
		candidates = append(candidates, fallback)
	}

	return candidates
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
	rows, err := ebusdScanResultRows(ctx, cfg)
	if err != nil {
		return nil, err
	}
	targets := make([]byte, 0, len(rows))
	for _, row := range rows {
		targets = append(targets, row.Address)
	}
	return targets, nil
}

func ebusdScanResultInfos(ctx context.Context, cfg ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
	rows, err := ebusdScanResultRows(ctx, cfg)
	if err != nil {
		return nil, err
	}

	infos := make([]registry.DeviceInfo, 0, len(rows))
	for _, row := range rows {
		infos = append(infos, registry.DeviceInfo{
			Address:         row.Address,
			Manufacturer:    row.Manufacturer,
			DeviceID:        row.DeviceID,
			SoftwareVersion: row.SoftwareVersion,
			HardwareVersion: row.HardwareVersion,
			SerialNumber:    row.SerialNumber,
		})
	}
	return infos, nil
}

func ebusdScanResultRows(ctx context.Context, cfg ebusgateway.TransportConfig) ([]ebusdScanResultRow, error) {
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

	rows := make([]ebusdScanResultRow, 0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) {
				return rows, err
			}
			return rows, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		row, ok := parseEbusdScanResultLine(line)
		if !ok {
			continue
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func parseEbusdScanResultLine(line string) (ebusdScanResultRow, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(strings.ToLower(line), "err") {
		return ebusdScanResultRow{}, false
	}

	fields := strings.Split(line, ";")
	if len(fields) < 5 {
		return ebusdScanResultRow{}, false
	}

	rawAddress := strings.TrimSpace(fields[0])
	value, err := strconv.ParseUint(rawAddress, 16, 8)
	if err != nil {
		return ebusdScanResultRow{}, false
	}

	serialParts := make([]string, 0, len(fields)-5)
	for _, field := range fields[5:] {
		trimmed := strings.TrimSpace(field)
		if trimmed == "" {
			continue
		}
		serialParts = append(serialParts, trimmed)
	}

	return ebusdScanResultRow{
		Address:         byte(value),
		Manufacturer:    strings.TrimSpace(fields[1]),
		DeviceID:        strings.TrimSpace(fields[2]),
		SoftwareVersion: strings.TrimSpace(fields[3]),
		HardwareVersion: strings.TrimSpace(fields[4]),
		SerialNumber:    strings.Join(serialParts, "-"),
	}, true
}

func dialContext(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}
