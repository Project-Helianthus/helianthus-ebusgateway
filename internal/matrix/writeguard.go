package matrix

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// The write guard is the allowlist every client driven by the adaptermux-v2
// harness passes before a byte leaves the host: identification and read
// requests only. A request is refused when it is not provably one of the
// allowlisted request types; unknown means refused. There is no override.

// ErrWriteGuardRefused marks a request refused by the write guard.
var ErrWriteGuardRefused = errors.New("matrix write guard refused request")

// RequestType names an allowlisted request type.
type RequestType string

const (
	RequestIdentification RequestType = "identification-0704"
	RequestB509Read       RequestType = "b509-read"
	RequestB524Read       RequestType = "b524-read"
	RequestB555Read       RequestType = "b555-read"
)

const ebusMaxDataLen = 16

// GuardTelegram is an outbound initiator telegram without CRC.
type GuardTelegram struct {
	Target    byte
	Primary   byte
	Secondary byte
	Data      []byte
}

// ClassifyTelegram returns the allowlisted request type of a telegram or an
// error wrapping ErrWriteGuardRefused. The read selectors mirror the
// gateway's own request-intent classification (observe_first_family_policy.go):
//
//   - 07 04 with no data: identification;
//   - B5 09 with first data byte 0x0D or 0x29: register read;
//   - B5 24 with opcode 0x02 or 0x06 and operation 0x00, or opcode 0x03: read;
//   - B5 55 with first data byte 0xA3, 0xA4 or 0xA5: timer read.
func ClassifyTelegram(telegram GuardTelegram) (RequestType, error) {
	refuse := func(format string, args ...any) (RequestType, error) {
		return "", fmt.Errorf("%w: %s", ErrWriteGuardRefused, fmt.Sprintf(format, args...))
	}
	switch telegram.Target {
	case protocol.AddressBroadcast:
		return refuse("broadcast target 0x%02X", telegram.Target)
	case protocol.SymbolSyn, protocol.SymbolEscape:
		return refuse("reserved target symbol 0x%02X", telegram.Target)
	}
	if protocol.IsInitiatorCapableAddress(telegram.Target) {
		return refuse("target 0x%02X is an initiator address", telegram.Target)
	}
	if len(telegram.Data) > ebusMaxDataLen {
		return refuse("data length %d exceeds %d", len(telegram.Data), ebusMaxDataLen)
	}
	data := telegram.Data
	switch {
	case telegram.Primary == 0x07 && telegram.Secondary == 0x04:
		if len(data) == 0 {
			return RequestIdentification, nil
		}
	case telegram.Primary == 0xB5 && telegram.Secondary == 0x09:
		if len(data) >= 1 && (data[0] == 0x0D || data[0] == 0x29) {
			return RequestB509Read, nil
		}
	case telegram.Primary == 0xB5 && telegram.Secondary == 0x24:
		if len(data) >= 2 && (data[0] == 0x02 || data[0] == 0x06) && data[1] == 0x00 {
			return RequestB524Read, nil
		}
		if len(data) >= 1 && data[0] == 0x03 {
			return RequestB524Read, nil
		}
	case telegram.Primary == 0xB5 && telegram.Secondary == 0x55:
		if len(data) >= 1 && (data[0] == 0xA3 || data[0] == 0xA4 || data[0] == 0xA5) {
			return RequestB555Read, nil
		}
	}
	return refuse("PB 0x%02X SB 0x%02X data % X is not an allowlisted identification or read request", telegram.Primary, telegram.Secondary, data)
}

// ParseHexTelegram decodes ZZ PB SB NN DB... (no source, no CRC) as used by
// the ebusd hex command. NN must equal the number of data bytes.
func ParseHexTelegram(text string) (GuardTelegram, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(text))
	if err != nil {
		return GuardTelegram{}, fmt.Errorf("%w: telegram is not hex: %v", ErrWriteGuardRefused, err)
	}
	if len(raw) < 4 {
		return GuardTelegram{}, fmt.Errorf("%w: telegram shorter than ZZ PB SB NN", ErrWriteGuardRefused)
	}
	if int(raw[3]) != len(raw)-4 {
		return GuardTelegram{}, fmt.Errorf("%w: NN %d does not match %d data bytes", ErrWriteGuardRefused, raw[3], len(raw)-4)
	}
	return GuardTelegram{Target: raw[0], Primary: raw[1], Secondary: raw[2], Data: append([]byte(nil), raw[4:]...)}, nil
}

// CheckEbusdCommand admits one ebusd command-port line or refuses it. The
// admitted forms are:
//
//	state
//	info
//	scan ZZ                (identification scan of one target)
//	hex [-s QQ] ZZPBSBNN.. (telegram must pass ClassifyTelegram)
//
// Every other verb, option or argument is refused, including read and write,
// whose message definitions the host cannot classify.
func CheckEbusdCommand(line string) error {
	if strings.ContainsAny(line, "\r\n") {
		return fmt.Errorf("%w: command contains a line break", ErrWriteGuardRefused)
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return fmt.Errorf("%w: empty command", ErrWriteGuardRefused)
	}
	verb := strings.ToLower(fields[0])
	args := fields[1:]
	switch verb {
	case "state", "info":
		if len(args) == 0 {
			return nil
		}
	case "scan":
		if len(args) == 1 {
			target, err := parseHexByte(args[0])
			if err != nil {
				return err
			}
			_, err = ClassifyTelegram(GuardTelegram{Target: target, Primary: 0x07, Secondary: 0x04})
			return err
		}
	case "hex":
		if len(args) == 3 && args[0] == "-s" {
			source, err := parseHexByte(args[1])
			if err != nil {
				return err
			}
			if !protocol.IsInitiatorCapableAddress(source) {
				return fmt.Errorf("%w: source 0x%02X is not an initiator address", ErrWriteGuardRefused, source)
			}
			args = args[2:]
		}
		if len(args) == 1 {
			telegram, err := ParseHexTelegram(args[0])
			if err != nil {
				return err
			}
			_, err = ClassifyTelegram(telegram)
			return err
		}
	}
	return fmt.Errorf("%w: ebusd command %q is not allowlisted", ErrWriteGuardRefused, line)
}

func parseHexByte(text string) (byte, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(text), "0x"))
	if err != nil || len(raw) != 1 {
		return 0, fmt.Errorf("%w: %q is not one hex byte", ErrWriteGuardRefused, text)
	}
	return raw[0], nil
}
