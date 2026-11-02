package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

// What one ebus.primary construction attempt proves about its failure. A
// configuration that local validation rejected before any dial is the only
// failure the configuration alone decides; it is classified CONFIG_INVALID.
// Everything after a dial is the peer's outcome: an INIT the endpoint rejects
// or does not confirm, a malformed payload, a closed connection. A degraded
// or restarting adapter answers like that, so these are retryable, and the
// attempt's log line carries what the INIT handshake saw.

// ebusConfigRejection is a configuration that local validation rejected
// before any dial. It unwraps to the validation error, so its text and its
// sentinel are the validation's own.
type ebusConfigRejection struct{ err error }

func (rejection *ebusConfigRejection) Error() string { return rejection.err.Error() }

func (rejection *ebusConfigRejection) Unwrap() error { return rejection.err }

// rejectEBusConfig marks err, the verdict of a local validation that ran
// before any dial, as a rejected configuration.
func rejectEBusConfig(err error) error {
	if err == nil {
		return nil
	}
	return &ebusConfigRejection{err: err}
}

// validateEBusDriverTransport runs the local validation of a proxy-class
// transport configuration (ENH, ENS, plain TCP or UDP, ebusd TCP) before any
// dial. The endpoint must normalize, name a network and an address, and use
// a protocol the gateway can open over that network. These are the checks
// the transport constructor applies, part of them only after its dial;
// running them first means a rejected configuration never contacts the
// endpoint, and every failure after the dial is the peer's outcome. Like the
// constructor it first fills empty fields with the configuration defaults.
// It returns the normalized configuration. An injected transport has nothing
// to validate.
func validateEBusDriverTransport(cfg ebusgateway.Config) (ebusgateway.TransportConfig, error) {
	if cfg.Transport != nil {
		return cfg.TransportConfig, nil
	}
	config := cfg.TransportConfig
	defaults := ebusgateway.DefaultConfig().TransportConfig
	if config.Protocol == "" {
		config.Protocol = defaults.Protocol
	}
	if config.Network == "" {
		config.Network = defaults.Network
	}
	if config.Address == "" {
		config.Address = defaults.Address
	}
	config, err := ebusgateway.NormalizeEBusDriverTransportConfig(config)
	if err != nil {
		return config, err
	}
	if config.Network == "" {
		return config, fmt.Errorf("gateway transport missing network: %w", ebuserrors.ErrInvalidPayload)
	}
	if config.Address == "" {
		return config, fmt.Errorf("gateway transport missing address: %w", ebuserrors.ErrInvalidPayload)
	}
	switch config.Protocol {
	case ebusgateway.TransportENH, ebusgateway.TransportENS, ebusgateway.TransportTCPPlain, ebusgateway.TransportEbusdTCP:
		return config, nil
	case ebusgateway.TransportUDPPlain:
		switch config.Network {
		case "udp", "udp4", "udp6":
			return config, nil
		}
		return config, fmt.Errorf("gateway transport %q requires a udp network: %w", config.Protocol, ebuserrors.ErrInvalidPayload)
	}
	return config, fmt.Errorf("gateway transport unsupported protocol %q: %w", config.Protocol, ebuserrors.ErrInvalidPayload)
}

// observeEBusDial wraps the dial of one construction's transport
// configuration and reports whether it established a connection. Without a
// configured dial it dials like the transport constructor does.
func observeEBusDial(config *ebusgateway.TransportConfig) *atomic.Bool {
	connected := &atomic.Bool{}
	dial := config.Dial
	if dial == nil {
		dial = func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			dialer := net.Dialer{Timeout: timeout}
			return dialer.DialContext(ctx, network, address)
		}
	}
	config.Dial = func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
		conn, err := dial(ctx, network, address, timeout)
		if err == nil {
			connected.Store(true)
		}
		return conn, err
	}
	return connected
}

// ebusHandshakeOutcome is what one construction attempt saw of the INIT
// handshake: the INIT attempts it made, the features byte of every answer
// that did not confirm the requested features, and the payload class of
// every INIT attempt that failed.
type ebusHandshakeOutcome struct {
	attempts int
	features []byte
	payloads []string
}

func (outcome ebusHandshakeOutcome) String() string {
	features := "none"
	if len(outcome.features) > 0 {
		values := make([]string, len(outcome.features))
		for i, value := range outcome.features {
			values[i] = fmt.Sprintf("0x%02X", value)
		}
		features = strings.Join(values, "/")
	}
	payloads := "none"
	if len(outcome.payloads) > 0 {
		payloads = strings.Join(outcome.payloads, "/")
	}
	return fmt.Sprintf("init_attempts=%d, features_seen=%s, payload_class=%s", outcome.attempts, features, payloads)
}

// ebusHandshakeFailure is a failed construction whose connection reached the
// INIT handshake, with what the handshake saw. It unwraps to the failure, so
// its error class and its classification are the failure's own.
type ebusHandshakeFailure struct {
	err     error
	outcome ebusHandshakeOutcome
}

func (failure *ebusHandshakeFailure) Error() string { return failure.err.Error() }

func (failure *ebusHandshakeFailure) Unwrap() error { return failure.err }

// ebusHandshakePayloadClasses names the closed classes of a failed INIT
// answer by the protocol sentinel it carries.
var ebusHandshakePayloadClasses = []struct {
	sentinel error
	class    string
}{
	{ebuserrors.ErrInvalidPayload, "invalid_payload"},
	{ebuserrors.ErrTimeout, "timeout"},
	{ebuserrors.ErrTransportClosed, "transport_closed"},
	{ebuserrors.ErrAdapterHostError, "adapter_host_error"},
	{ebuserrors.ErrAdapterReset, "adapter_reset"},
}

// ebusHandshakePayloadClass is the class of a failed INIT answer.
func ebusHandshakePayloadClass(err error) string {
	for _, entry := range ebusHandshakePayloadClasses {
		if errors.Is(err, entry.sentinel) {
			return entry.class
		}
	}
	return "failed"
}

// ebusHandshakePayloadClassText is the class of a failed INIT attempt that
// the multiplexer reports only as text: the sentinel whose text it carries.
func ebusHandshakePayloadClassText(text string) string {
	for _, entry := range ebusHandshakePayloadClasses {
		if strings.Contains(text, entry.sentinel.Error()) {
			return entry.class
		}
	}
	return "failed"
}

// ebusProxyHandshakeFailure attaches the outcome of the proxy-class INIT
// handshake, a single exchange, to the failure of a construction whose dial
// succeeded.
func ebusProxyHandshakeFailure(err error) error {
	return &ebusHandshakeFailure{err: err, outcome: ebusHandshakeOutcome{attempts: 1, payloads: []string{ebusHandshakePayloadClass(err)}}}
}

// ebusHandshakeLine matches the adapter-direct multiplexer's line for one
// INIT attempt that did not confirm the requested features: an answer with
// other features, or a failure with its cause.
var ebusHandshakeLine = regexp.MustCompile(`adaptermux: INIT attempt ([0-9]+)(?:: features=0x([0-9A-Fa-f]{2}) \(want 0x[0-9A-Fa-f]{2}\)| failed: (.*) \(retrying\))`)

// ebusHandshakeTrace observes the INIT handshake of one adapter-direct
// construction. The multiplexer retries INIT inside a construction and
// reports each attempt that does not confirm the requested features only in
// its log, so the trace is the multiplexer's logger: every line reaches the
// process log unchanged, and the INIT attempt lines are also recorded until
// the construction returns.
type ebusHandshakeTrace struct {
	mu sync.Mutex
	// finished is set once the construction returned; the multiplexer keeps
	// the logger for its lifetime, and later lines are only passed on.
	finished atomic.Bool
	outcome  ebusHandshakeOutcome
}

// logger is the multiplexer logger of the traced construction, with the
// process logger's prefix and flags.
func (trace *ebusHandshakeTrace) logger() *log.Logger {
	return log.New(ebusHandshakeTraceWriter{trace: trace}, log.Prefix(), log.Flags())
}

type ebusHandshakeTraceWriter struct{ trace *ebusHandshakeTrace }

func (writer ebusHandshakeTraceWriter) Write(p []byte) (int, error) {
	writer.trace.observe(string(p))
	return log.Writer().Write(p)
}

func (trace *ebusHandshakeTrace) observe(line string) {
	if trace.finished.Load() {
		return
	}
	match := ebusHandshakeLine.FindStringSubmatch(line)
	if match == nil {
		return
	}
	attempt, err := strconv.Atoi(match[1])
	if err != nil {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.finished.Load() {
		return
	}
	if attempt > trace.outcome.attempts {
		trace.outcome.attempts = attempt
	}
	if match[2] != "" {
		value, _ := strconv.ParseUint(match[2], 16, 8)
		trace.outcome.features = append(trace.outcome.features, byte(value))
		return
	}
	trace.outcome.payloads = append(trace.outcome.payloads, ebusHandshakePayloadClassText(match[3]))
}

// finish ends the trace when the construction returns and attaches the
// observed handshake to err, the construction's failure. A failure that
// never reached INIT is returned unchanged.
func (trace *ebusHandshakeTrace) finish(err error) error {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.finished.Store(true)
	if err == nil || trace.outcome.attempts == 0 {
		return err
	}
	outcome := ebusHandshakeOutcome{
		attempts: trace.outcome.attempts,
		features: append([]byte(nil), trace.outcome.features...),
		payloads: append([]string(nil), trace.outcome.payloads...),
	}
	return &ebusHandshakeFailure{err: err, outcome: outcome}
}
