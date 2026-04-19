package ebus_standard

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// EnvelopeContract names the MCP envelope contract this package emits.
const (
	EnvelopeContractName  = "helianthus-ebus-mcp"
	EnvelopeContractMajor = 1
	EnvelopeContractMinor = 0
)

// NewEnvelope wraps data / err in the meta/data/error envelope used by the
// ebus_standard MCP surfaces. meta.data_hash is computed via canonical JSON
// (sorted keys, stable number formatting, no whitespace) + SHA-256 so
// structurally-equivalent-but-reordered inputs yield identical hashes.
//
// The timestamp is caller-supplied so tests remain deterministic; callers
// that need "now" pass time.Now().UTC().
func NewEnvelope(data any, err error, ts time.Time) map[string]any {
	meta := map[string]any{
		"contract": map[string]any{
			"name":  EnvelopeContractName,
			"major": EnvelopeContractMajor,
			"minor": EnvelopeContractMinor,
		},
		// consistency.mode pins the data-freshness contract shared with the
		// rest of ebus.v1.* surfaces. ebus_standard is a catalog read
		// (static L7 service/command tables) so the mode is always "LIVE";
		// there is no shadow-snapshot mode for catalog reads.
		"consistency": map[string]any{
			"mode": "LIVE",
		},
		"data_timestamp": ts.UTC().Format(time.RFC3339Nano),
		"data_hash":      DataHash(data),
	}
	var envelopeError any
	if err != nil {
		envelopeError = map[string]any{
			"code":      classifyErr(err),
			"message":   err.Error(),
			"retriable": false,
		}
	}
	return map[string]any{
		"meta":  meta,
		"data":  data,
		"error": envelopeError,
	}
}

// DataHash computes SHA-256 over the canonical JSON rendering of v.
// Canonical rendering: sorted keys, compact numbers, no whitespace.
//
// Contract: data_hash is ALWAYS a 64-hex SHA-256 string, regardless of
// whether data is nil. A nil data value is canonically serialized as the
// JSON literal "null" (4 bytes) and hashed. Clients that consume
// meta.data_hash may therefore rely on a stable 64-hex length across every
// ebus.v1.* envelope.
func DataHash(v any) string {
	var buf strings.Builder
	writeCanonical(&buf, v)
	sum := sha256.Sum256([]byte(buf.String()))
	return hex.EncodeToString(sum[:])
}

func writeCanonical(w *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		w.WriteString("null")
	case bool:
		if x {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
	case string:
		writeCanonicalString(w, x)
	case int:
		w.WriteString(strconv.FormatInt(int64(x), 10))
	case int64:
		w.WriteString(strconv.FormatInt(x, 10))
	case int32:
		w.WriteString(strconv.FormatInt(int64(x), 10))
	case uint8:
		w.WriteString(strconv.FormatUint(uint64(x), 10))
	case float64:
		// Integer-valued floats encode as integers for stability across
		// Go marshallers that elide trailing zeros.
		if x == float64(int64(x)) {
			w.WriteString(strconv.FormatInt(int64(x), 10))
		} else {
			w.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
		}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		w.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				w.WriteByte(',')
			}
			writeCanonicalString(w, k)
			w.WriteByte(':')
			writeCanonical(w, x[k])
		}
		w.WriteByte('}')
	case []any:
		w.WriteByte('[')
		for i, el := range x {
			if i > 0 {
				w.WriteByte(',')
			}
			writeCanonical(w, el)
		}
		w.WriteByte(']')
	case []map[string]any:
		w.WriteByte('[')
		for i, el := range x {
			if i > 0 {
				w.WriteByte(',')
			}
			writeCanonical(w, el)
		}
		w.WriteByte(']')
	case []string:
		w.WriteByte('[')
		for i, el := range x {
			if i > 0 {
				w.WriteByte(',')
			}
			writeCanonicalString(w, el)
		}
		w.WriteByte(']')
	case []int:
		w.WriteByte('[')
		for i, el := range x {
			if i > 0 {
				w.WriteByte(',')
			}
			w.WriteString(strconv.Itoa(el))
		}
		w.WriteByte(']')
	default:
		// Fallback: stringify. Callers should pass canonical-friendly
		// types; this path exists so unknown primitives do not panic.
		w.WriteString(fmt.Sprintf("%q", fmt.Sprintf("%v", v)))
	}
}

func writeCanonicalString(w *strings.Builder, s string) {
	w.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			w.WriteString(`\"`)
		case '\\':
			w.WriteString(`\\`)
		case '\n':
			w.WriteString(`\n`)
		case '\r':
			w.WriteString(`\r`)
		case '\t':
			w.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(w, `\u%04x`, r)
			} else {
				w.WriteRune(r)
			}
		}
	}
	w.WriteByte('"')
}

func classifyErr(err error) string {
	switch {
	case errors.Is(err, ErrUnknownCommand):
		return "UNKNOWN_COMMAND"
	case errors.Is(err, ErrInvalidPayload):
		return "INVALID_PAYLOAD"
	default:
		return "INTERNAL"
	}
}
