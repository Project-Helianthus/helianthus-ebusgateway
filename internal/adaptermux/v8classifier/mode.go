// Package v8classifier implements the frame-atomic-visibility v8
// classifier surface for helianthus-ebusgateway's adapter
// multiplexer. The classifier interposes between the gateway's
// adapter-facing stream (ebusgo ENHTransport / ENSTransport) and
// the mux's existing read path, filtering AA-injection bytes and
// other wire-level artifacts that the v8 design (see
// helianthus-docs-ebus/architecture/adaptermux/frame-atomic-visibility-v8.md)
// identifies as proxy-mediated-mode invariants.
//
// Phase 3 Step B3.2 (this file): mode definition + parsing only.
// No classification logic yet. Subsequent stacked PRs (B3.3..B3.7)
// add the escape decoder wiring, FSM per-session instances, pacer,
// L_rtt EMA, admin channel, and shadow-mode comparison logic.
//
// Mode rollout (per v8 §1.10):
//
//	off     — classifier is dormant. No observation, no counters,
//	          no behavioral effect. Production default until B3.7
//	          live-bus validation closes.
//	shadow  — classifier observes every wire byte AND admin event
//	          from the upstream transport and computes its
//	          classification decisions, but DOES NOT alter the
//	          byte stream forwarded to sessions. Divergences vs
//	          the current pre-v8 path are logged to the admin
//	          channel for operator inspection. Used during live-
//	          bus validation (Step C / B3.7).
//	enforce — classifier replaces the pre-v8 read path. Round-7
//	          mitigations (betweenWritesSyn, queueJustDrained,
//	          payloadAaAutoSyn*) become unreachable code paths;
//	          v8 round-9 absorb counter
//	          (Bus.Round9AbsorbEntered) should stay at 0 under
//	          a healthy proxy. The Prometheus alert
//	          HelianthusRound9FiredUnderProxy (v8 §1.12) is the
//	          primary safety net.
package v8classifier

import (
	"fmt"
	"strings"
)

// Mode is the rollout state of the v8 classifier on a given gateway
// instance. The mode is set at startup time via configuration; it
// is NOT dynamically reconfigurable in this PR (B3.2 scaffold).
// Future B3.x PRs may add a Reload signal if operator-driven
// transitions between modes become necessary.
type Mode int

const (
	// ModeOff disables the classifier entirely. No observation,
	// no counters, no behavioral effect on the byte stream. This
	// is the production default while v8 is still in development.
	ModeOff Mode = iota

	// ModeShadow runs the classifier as a passive observer. It
	// computes its classification decisions on every wire byte but
	// does NOT alter the byte stream forwarded to sessions. Used
	// for live-bus validation (v8 §14): operators compare the
	// classifier's decisions against the current pre-v8 path and
	// surface divergences via the admin channel.
	ModeShadow

	// ModeEnforce makes the v8 classifier the authoritative read
	// path. AA-injection bytes are filtered, escape-decoder admin
	// events propagate via the gateway's admin channel, and the
	// pre-v8 round-7 mitigations become unreachable. Only enabled
	// after Step C live-bus validation passes (B3.7).
	ModeEnforce
)

// String returns the canonical lowercase label for the mode.
// Stable for config parsing, log lines, and Prometheus labels.
func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "off"
	case ModeShadow:
		return "shadow"
	case ModeEnforce:
		return "enforce"
	default:
		return "unknown"
	}
}

// ParseMode parses a case-insensitive mode label from configuration.
// Whitespace around the label is trimmed. The empty string parses to
// ModeOff (the safe default; an unset env var or addon option must
// not silently enable the classifier).
//
// Errors:
//   - returns a non-nil error for any unrecognized non-empty label.
//     Callers SHOULD fail loudly on configuration error rather than
//     silently fall back to ModeOff, so an operator typo (e.g.
//     "enfource") does not silently disable a planned shadow run.
func ParseMode(s string) (Mode, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return ModeOff, nil
	}
	switch strings.ToLower(trimmed) {
	case "off", "disabled", "0", "false", "no":
		return ModeOff, nil
	case "shadow":
		return ModeShadow, nil
	case "enforce":
		return ModeEnforce, nil
	default:
		return ModeOff, fmt.Errorf("v8classifier: unknown mode %q (expected off|shadow|enforce)", trimmed)
	}
}

// EnvVarName is the canonical environment variable consulted by the
// gateway startup glue to determine the classifier mode. The value
// is parsed via ParseMode; an unset variable defaults to ModeOff.
// Documented here so any future addon-config / env-var consumer
// agrees on the name.
const EnvVarName = "HELIANTHUS_V8_CLASSIFIER_MODE"
