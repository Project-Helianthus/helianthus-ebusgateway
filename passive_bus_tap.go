package ebusgateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
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
	PassiveTapEventReadTimeout
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

	// WasEscaped is true iff Symbol was produced by the eBUS byte-
	// stuffing decoder from a wire `0xA9 0x00` (→ logical 0xA9) or
	// `0xA9 0x01` (→ logical 0xAA). False means EITHER (a) a raw
	// passthrough byte that the local decoder saw on the wire, OR
	// (b) an upstream-logical byte where the transport did not expose
	// escape provenance. For F-23 transports that do expose
	// transport.StreamEvent.WasEscaped, the passive tap preserves the
	// upstream wire-side ground truth instead of hardcoding false.
	//
	// F-19d (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-13-batch17.md):
	// the passive transaction reconstructor uses this flag to
	// disambiguate logical 0xAA bytes — escape-decoded data vs wire
	// SYN frame-terminator — replacing the heuristic
	// isMidRequestFrame() that mis-classified ~9 events/hour into
	// next-frame cascades.
	WasEscaped bool
}

type PassiveTapConsumer interface {
	OnPassiveTapEvent(PassiveTapEvent)
}

type PassiveTapStatus struct {
	Connected           bool
	EndpointState       PassiveEndpointState
	LastError           string
	ConnectAttemptCount uint64
	ConnectCount        uint64
	ConnectFailureCount uint64
	DisconnectCount     uint64
	ResetCount          uint64
	DecodeFaultCount    uint64
	ObservedSymbolCount uint64
	LastConnectAt       time.Time
	LastDisconnectAt    time.Time
	LastObservedSymbol  time.Time
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
	decodeWireEscapes := passiveTapDecodesWireEscapes(tap.cfg)
	if passiveTransportDeliversLogicalBytes(tr) {
		decodeWireEscapes = false
	}
	lastSymbolAt := time.Now()
	absenceDisconnect := passiveTapEnforcesAbsenceDisconnect(tap.cfg)

	for {
		if err := tap.ctx.Err(); err != nil {
			return err
		}

		event, err := readPassiveTransportEvent(tap.ctx, tr)
		if err != nil {
			if errors.Is(err, ebuserrors.ErrTimeout) {
				tap.emit(PassiveTapEvent{
					Kind:       PassiveTapEventReadTimeout,
					ObservedAt: time.Now(),
					Err:        err,
				})
				threshold := tap.cfg.PassiveAbsenceThreshold
				if absenceDisconnect && threshold > 0 && time.Since(lastSymbolAt) >= threshold {
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
			if decodeWireEscapes && decoder.escape {
				tap.recordDecodeFault(fmt.Errorf("incomplete escape sequence before reset: %w", ebuserrors.ErrInvalidPayload))
				decoder.reset()
			}
			tap.recordReset(now)
		case transport.StreamEventByte:
			symbol := event.Byte
			// F-19d (batch-17) / F-23 (batch-19, 2026-05-13):
			// WasEscaped carries the wire-side ground truth.
			//
			// Path-1 (decodeWireEscapes=true): the local escape
			// decoder runs on raw wire bytes and authoritatively
			// produces wasEscaped from the wire-pair observation.
			//
			// Path-2 (decodeWireEscapes=false; already-logical
			// streams — adapter-direct via PassiveTransport, ENH/
			// ENS proxy-like, or remote ENH direct-adapter): the
			// upstream layer decoded escapes and surfaces the
			// per-byte WasEscaped flag via transport.StreamEvent
			// (added in helianthus-ebusgo PR #154 / F-23). Source
			// it directly instead of hardcoding false — this is
			// the F-23 consumer-side cleanup that closes the
			// Pattern A/B unexpected_symbol abandons from
			// batch-19. Pre-F-23 the upstream ENH transport
			// leaked escape pairs as raw bytes and the gateway
			// passive tap saw them as bare 0xA9 0x00 sequences;
			// post-F-23 PR-1 the upstream delivers logical 0xA9
			// with WasEscaped=true and logical 0xAA payload as
			// WasEscaped=true (distinct from raw wire SYN 0xAA
			// with WasEscaped=false). Honoring this flag here is
			// what lets the reconstructor distinguish payload
			// 0xAA from wire SYN at every emission site.
			wasEscaped := event.WasEscaped
			if decodeWireEscapes {
				var ok bool
				var decodeErr error
				symbol, ok, wasEscaped, decodeErr = decoder.push(event.Byte)
				if decodeErr != nil {
					tap.recordDecodeFault(decodeErr)
					continue
				}
				if !ok {
					continue
				}
			}

			lastSymbolAt = now
			tap.recordSymbol(now)
			tap.emit(PassiveTapEvent{
				Kind:       PassiveTapEventSymbol,
				Symbol:     symbol,
				ObservedAt: now,
				WasEscaped: wasEscaped,
			})
		}
	}
}

func (tap *PassiveBusTap) connect(ctx context.Context) error {
	tap.statusMu.Lock()
	tap.status.ConnectAttemptCount++
	tap.statusMu.Unlock()

	var tr transport.RawTransport
	var err error

	if tap.cfg.PassiveTransport != nil {
		// Adapter-direct mode: use pre-configured passive transport
		// from the multiplexer (symbols are already logical, no dial needed).
		tr = tap.cfg.PassiveTransport
	} else {
		tr, err = resolvePassiveTransport(ctx, tap.cfg)
		if err != nil {
			tap.statusMu.Lock()
			tap.status.ConnectFailureCount++
			tap.status.LastError = err.Error()
			tap.statusMu.Unlock()
			return err
		}
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
	tap.status.ObservedSymbolCount++
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

func readPassiveTransportEvent(ctx context.Context, tr transport.RawTransport) (transport.StreamEvent, error) {
	if reader, ok := tr.(interface {
		ReadEventContext(context.Context) (transport.StreamEvent, error)
	}); ok {
		return reader.ReadEventContext(ctx)
	}
	if reader, ok := tr.(transport.StreamEventReader); ok {
		return reader.ReadEvent()
	}
	if reader, ok := tr.(interface {
		ReadByteContext(context.Context) (byte, error)
	}); ok {
		value, err := reader.ReadByteContext(ctx)
		if err != nil {
			return transport.StreamEvent{}, err
		}
		return transport.StreamEvent{Kind: transport.StreamEventByte, Byte: value}, nil
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

func passiveTapEnforcesAbsenceDisconnect(cfg Config) bool {
	// Adapter-direct mode: multiplexer handles reconnection internally.
	if cfg.PassiveTransport != nil {
		return false
	}
	return !passiveTapUsesProxyLikeObserverTransport(cfg)
}

func passiveTapDecodesWireEscapes(cfg Config) bool {
	return !passiveTapObserverStreamAlreadyLogical(cfg)
}

func passiveTransportDeliversLogicalBytes(tr transport.RawTransport) bool {
	escapeAware, ok := tr.(transport.EscapeAware)
	return ok && escapeAware.BytesAreUnescaped()
}

func passiveTapObserverStreamAlreadyLogical(cfg Config) bool {
	// Adapter-direct mode: multiplexer delivers logical bytes
	// (post-ENH-decode), no escape decoding needed.
	if cfg.PassiveTransport != nil {
		return true
	}
	if passiveTapUsesProxyLikeObserverTransport(cfg) {
		return true
	}

	config, err := normalizeTransportConfig(cfg.TransportConfig)
	if err != nil {
		return false
	}

	switch config.Protocol {
	case TransportENH, TransportENS:
	default:
		return false
	}
	if strings.ToLower(strings.TrimSpace(config.Network)) != "tcp" {
		return false
	}

	// Remote direct adapter sessions surface observer bytes as logical eBUS
	// symbols inside ENH RECEIVED frames. Running the local escape decoder on
	// that already-decoded stream corrupts any 0xA9 data bytes.
	return passiveObserveFirstDirectAdapterEndpoint(config.Network, config.Address)
}

func passiveTapUsesProxyLikeObserverTransport(cfg Config) bool {
	config, err := normalizeTransportConfig(cfg.TransportConfig)
	if err != nil {
		return false
	}

	switch config.Protocol {
	case TransportENH, TransportENS:
	default:
		return false
	}
	if strings.ToLower(strings.TrimSpace(config.Network)) != "tcp" {
		return false
	}

	if passiveObserveFirstDirectAdapterEndpoint(config.Network, config.Address) {
		return false
	}

	host, port, err := net.SplitHostPort(strings.TrimSpace(config.Address))
	if err != nil {
		return false
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if port == "9999" {
		return false
	}
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return false
		}
	}

	portValue, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	return portValue >= 19001 && portValue < 20000
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

// push consumes one raw wire byte and returns the decoded logical
// byte. The third return value is true iff the decoded byte was
// produced by an escape sequence (raw `0xA9 0x00` → logical `0xA9`,
// raw `0xA9 0x01` → logical `0xAA`). False means the byte was a raw
// passthrough — including raw wire SYN (0xAA) bytes which the
// reconstructor must distinguish from escape-decoded data 0xAA per
// F-19d (batch-17 EBUSD-VERIFICATION). Reference: eBUS byte-stuffing
// rule + john30/ebusd `symbol.h:79-82`.
//
// NOTE: this decoder only runs in the Path-1 (decodeWireEscapes=true)
// observe-the-raw-wire configuration. In Path-2 (already-logical
// observer streams: adapter-direct, ENH/ENS proxy-like), the upstream
// layer has already decoded escapes; F-23 transports surface the
// original provenance through transport.StreamEvent.WasEscaped.
func (decoder *passiveEscapeDecoder) push(raw byte) (decoded byte, ok bool, wasEscaped bool, err error) {
	if decoder.escape {
		decoder.escape = false
		switch raw {
		case 0x00:
			return protocol.SymbolEscape, true, true, nil
		case 0x01:
			return protocol.SymbolSyn, true, true, nil
		default:
			return 0, false, false, fmt.Errorf("passive tap invalid escape sequence 0x%02x: %w", raw, ebuserrors.ErrInvalidPayload)
		}
	}

	if raw == protocol.SymbolEscape {
		decoder.escape = true
		return 0, false, false, nil
	}
	return raw, true, false, nil
}

func (decoder *passiveEscapeDecoder) reset() {
	decoder.escape = false
}
