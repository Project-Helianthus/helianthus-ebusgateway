package ebusgateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type PassiveTapEventKind uint8

const (
	PassiveTapEventConnected PassiveTapEventKind = iota + 1
	PassiveTapEventDisconnected
	PassiveTapEventSymbol
	PassiveTapEventReset
	PassiveTapEventDecodeFault
)

type PassiveEndpointState string

const (
	PassiveEndpointStateUnknown                    PassiveEndpointState = "unknown"
	PassiveEndpointStateConnected                  PassiveEndpointState = "connected"
	PassiveEndpointStateTemporarilyDisconnected    PassiveEndpointState = "temporarily_disconnected"
	PassiveEndpointStateUnsupportedOrMisconfigured PassiveEndpointState = "unsupported_or_misconfigured"
	PassiveEndpointStateClosed                     PassiveEndpointState = "closed"
)

type PassiveTapEvent struct {
	Kind       PassiveTapEventKind
	Symbol     byte
	ObservedAt time.Time
	Err        error
}

type PassiveTapConsumer interface {
	OnPassiveTapEvent(PassiveTapEvent)
}

type PassiveTapStatus struct {
	Connected          bool
	EndpointState      PassiveEndpointState
	LastError          string
	ConnectCount       uint64
	DisconnectCount    uint64
	ResetCount         uint64
	DecodeFaultCount   uint64
	LastConnectAt      time.Time
	LastDisconnectAt   time.Time
	LastObservedSymbol time.Time
}

type PassiveBusTap struct {
	cfg      Config
	consumer PassiveTapConsumer
	wrap     func(transport.RawTransport) transport.RawTransport

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	transportMu sync.Mutex
	transport   transport.RawTransport

	statusMu sync.RWMutex
	status   PassiveTapStatus
}

type passiveEscapeDecoder struct {
	escape bool
}

func StartPassiveBusTap(ctx context.Context, cfg Config, consumer PassiveTapConsumer) (*PassiveBusTap, error) {
	return StartPassiveBusTapWithTransport(ctx, cfg, consumer, nil)
}

func StartPassiveBusTapWithTransport(ctx context.Context, cfg Config, consumer PassiveTapConsumer, wrap func(transport.RawTransport) transport.RawTransport) (*PassiveBusTap, error) {
	if consumer == nil {
		return nil, fmt.Errorf("passive bus tap missing consumer: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cfg = applyDefaults(cfg)
	tapCtx, cancel := context.WithCancel(ctx)
	tap := &PassiveBusTap{
		cfg:      cfg,
		consumer: consumer,
		wrap:     wrap,
		ctx:      tapCtx,
		cancel:   cancel,
		status: PassiveTapStatus{
			EndpointState: PassiveEndpointStateUnknown,
		},
	}

	if err := tap.connect(tapCtx); err != nil {
		cancel()
		return nil, err
	}

	tap.wg.Add(1)
	go tap.run()
	return tap, nil
}

func (tap *PassiveBusTap) Close() error {
	if tap == nil {
		return nil
	}

	tap.cancel()
	tap.closeCurrentTransport()
	tap.wg.Wait()
	return nil
}

func (tap *PassiveBusTap) Snapshot() PassiveTapStatus {
	if tap == nil {
		return PassiveTapStatus{}
	}

	tap.statusMu.RLock()
	defer tap.statusMu.RUnlock()
	return tap.status
}

func (tap *PassiveBusTap) run() {
	defer tap.wg.Done()

	retryAttempt := 0
	for {
		tr := tap.currentTransport()
		if tr == nil {
			if err := tap.ctx.Err(); err != nil {
				tap.markClosed()
				return
			}

			if err := tap.connect(tap.ctx); err != nil {
				state := PassiveEndpointStateTemporarilyDisconnected
				if tap.Snapshot().ConnectCount == 0 {
					state = PassiveEndpointStateUnsupportedOrMisconfigured
				}
				tap.recordDisconnect(err, state)
				if !tap.sleepReconnectDelay(retryAttempt) {
					tap.markClosed()
					return
				}
				retryAttempt++
				continue
			}
			retryAttempt = 0
			continue
		}

		err := tap.readLoop(tr)
		tap.closeCurrentTransport()
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || tap.ctx.Err() != nil {
			tap.markClosed()
			return
		}

		tap.recordDisconnect(err, PassiveEndpointStateTemporarilyDisconnected)
		if !tap.sleepReconnectDelay(retryAttempt) {
			tap.markClosed()
			return
		}
		retryAttempt++
	}
}

func (tap *PassiveBusTap) readLoop(tr transport.RawTransport) error {
	decoder := passiveEscapeDecoder{}
	lastSymbolAt := time.Now()

	for {
		if err := tap.ctx.Err(); err != nil {
			return err
		}

		event, err := readPassiveTransportEvent(tr)
		if err != nil {
			if errors.Is(err, ebuserrors.ErrTimeout) {
				threshold := tap.cfg.PassiveAbsenceThreshold
				if threshold > 0 && time.Since(lastSymbolAt) >= threshold {
					return fmt.Errorf("passive tap absence threshold exceeded after %s: %w", threshold, ebuserrors.ErrTimeout)
				}
				continue
			}
			if decoder.escape {
				tap.recordDecodeFault(fmt.Errorf("incomplete escape sequence before disconnect: %w", ebuserrors.ErrInvalidPayload))
				decoder.reset()
			}
			return err
		}

		now := time.Now()
		switch event.Kind {
		case transport.StreamEventReset:
			if decoder.escape {
				tap.recordDecodeFault(fmt.Errorf("incomplete escape sequence before reset: %w", ebuserrors.ErrInvalidPayload))
				decoder.reset()
			}
			tap.recordReset(now)
		case transport.StreamEventByte:
			symbol, ok, decodeErr := decoder.push(event.Byte)
			if decodeErr != nil {
				tap.recordDecodeFault(decodeErr)
				continue
			}
			if !ok {
				continue
			}

			lastSymbolAt = now
			tap.recordSymbol(now)
			tap.emit(PassiveTapEvent{
				Kind:       PassiveTapEventSymbol,
				Symbol:     symbol,
				ObservedAt: now,
			})
		}
	}
}

func (tap *PassiveBusTap) connect(ctx context.Context) error {
	tr, err := resolvePassiveTransport(ctx, tap.cfg)
	if err != nil {
		return err
	}
	if tap.wrap != nil {
		tr = tap.wrap(tr)
	}

	tap.transportMu.Lock()
	tap.transport = tr
	tap.transportMu.Unlock()

	tap.statusMu.Lock()
	tap.status.Connected = true
	tap.status.EndpointState = PassiveEndpointStateConnected
	tap.status.ConnectCount++
	tap.status.LastConnectAt = time.Now()
	tap.status.LastError = ""
	tap.statusMu.Unlock()

	tap.emit(PassiveTapEvent{
		Kind:       PassiveTapEventConnected,
		ObservedAt: time.Now(),
	})
	return nil
}

func (tap *PassiveBusTap) currentTransport() transport.RawTransport {
	tap.transportMu.Lock()
	defer tap.transportMu.Unlock()
	return tap.transport
}

func (tap *PassiveBusTap) closeCurrentTransport() {
	tap.transportMu.Lock()
	tr := tap.transport
	tap.transport = nil
	tap.transportMu.Unlock()
	if tr != nil {
		_ = tr.Close()
	}
}

func (tap *PassiveBusTap) recordDisconnect(err error, state PassiveEndpointState) {
	tap.statusMu.Lock()
	tap.status.Connected = false
	tap.status.EndpointState = state
	tap.status.DisconnectCount++
	tap.status.LastDisconnectAt = time.Now()
	if err != nil {
		tap.status.LastError = err.Error()
	} else {
		tap.status.LastError = ""
	}
	tap.statusMu.Unlock()

	tap.emit(PassiveTapEvent{
		Kind:       PassiveTapEventDisconnected,
		ObservedAt: time.Now(),
		Err:        err,
	})
}

func (tap *PassiveBusTap) recordReset(observedAt time.Time) {
	tap.statusMu.Lock()
	tap.status.ResetCount++
	tap.statusMu.Unlock()
	tap.emit(PassiveTapEvent{
		Kind:       PassiveTapEventReset,
		ObservedAt: observedAt,
	})
}

func (tap *PassiveBusTap) recordDecodeFault(err error) {
	tap.statusMu.Lock()
	tap.status.DecodeFaultCount++
	if err != nil {
		tap.status.LastError = err.Error()
	}
	tap.statusMu.Unlock()
	tap.emit(PassiveTapEvent{
		Kind:       PassiveTapEventDecodeFault,
		ObservedAt: time.Now(),
		Err:        err,
	})
}

func (tap *PassiveBusTap) recordSymbol(observedAt time.Time) {
	tap.statusMu.Lock()
	tap.status.LastObservedSymbol = observedAt
	tap.statusMu.Unlock()
}

func (tap *PassiveBusTap) markClosed() {
	tap.statusMu.Lock()
	tap.status.Connected = false
	tap.status.EndpointState = PassiveEndpointStateClosed
	tap.statusMu.Unlock()
}

func (tap *PassiveBusTap) sleepReconnectDelay(attempt int) bool {
	delay := reconnectDelay(attempt, tap.cfg.PassiveReconnectInitialDelay, tap.cfg.PassiveReconnectMaxDelay)
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-tap.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (tap *PassiveBusTap) emit(event PassiveTapEvent) {
	if tap == nil || tap.consumer == nil {
		return
	}
	if event.ObservedAt.IsZero() {
		event.ObservedAt = time.Now()
	}
	tap.consumer.OnPassiveTapEvent(event)
}

func reconnectDelay(attempt int, initial, max time.Duration) time.Duration {
	if initial <= 0 {
		initial = time.Second
	}
	if max <= 0 {
		max = 30 * time.Second
	}
	delay := initial
	for i := 0; i < attempt; i++ {
		if delay >= max {
			return max
		}
		delay *= 2
		if delay >= max {
			return max
		}
	}
	return delay
}

func readPassiveTransportEvent(tr transport.RawTransport) (transport.StreamEvent, error) {
	if reader, ok := tr.(transport.StreamEventReader); ok {
		return reader.ReadEvent()
	}

	value, err := tr.ReadByte()
	if err != nil {
		return transport.StreamEvent{}, err
	}
	return transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: value,
	}, nil
}

func resolvePassiveTransport(ctx context.Context, cfg Config) (transport.RawTransport, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	config, err := normalizeTransportConfig(cfg.TransportConfig)
	if err != nil {
		return nil, fmt.Errorf("passive transport config: %w", err)
	}
	if config.Protocol == TransportEbusdTCP {
		return nil, fmt.Errorf("passive transport unsupported on %s: %w", config.Protocol, ebuserrors.ErrInvalidPayload)
	}
	if config.Network == "" {
		return nil, fmt.Errorf("passive transport missing network: %w", ebuserrors.ErrInvalidPayload)
	}
	if config.Address == "" {
		return nil, fmt.Errorf("passive transport missing address: %w", ebuserrors.ErrInvalidPayload)
	}

	dial := config.Dial
	if dial == nil {
		dial = dialContext
	}

	conn, err := dial(ctx, config.Network, config.Address, config.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("passive transport dial failed: %w", err)
	}
	if err := enablePassiveKeepalive(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	tr, err := transportFromConn(config.Protocol, conn, config.ReadTimeout, config.WriteTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tr, nil
}

func enablePassiveKeepalive(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return fmt.Errorf("passive transport keepalive enable failed: %w", err)
	}
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	return nil
}

func (decoder *passiveEscapeDecoder) push(raw byte) (byte, bool, error) {
	if decoder.escape {
		decoder.escape = false
		switch raw {
		case 0x00:
			return protocol.SymbolEscape, true, nil
		case 0x01:
			return protocol.SymbolSyn, true, nil
		default:
			return 0, false, fmt.Errorf("passive tap invalid escape sequence 0x%02x: %w", raw, ebuserrors.ErrInvalidPayload)
		}
	}

	if raw == protocol.SymbolEscape {
		decoder.escape = true
		return 0, false, nil
	}
	return raw, true, nil
}

func (decoder *passiveEscapeDecoder) reset() {
	decoder.escape = false
}
