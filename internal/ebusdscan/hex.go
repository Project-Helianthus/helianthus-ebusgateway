package ebusdscan

import (
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	primaryVaillant           = byte(0xB5)
	secondaryRegister         = byte(0x09)
	secondaryExtendedRegister = byte(0x24)
)

func BuildHex(target byte, entry Entry) ([]byte, error) {
	switch entry.Method {
	case methodGetRegister:
		payload := []byte{0x0D, byte(entry.Addr >> 8), byte(entry.Addr)}
		return append([]byte{target, primaryVaillant, secondaryRegister, byte(len(payload))}, payload...), nil
	case methodGetExtRegister:
		opcode := entry.Opcode
		if opcode == 0 {
			opcode = 0x02
		}
		payload := []byte{opcode, 0x00, entry.Group, entry.Instance, byte(entry.Addr), byte(entry.Addr >> 8)}
		return append([]byte{target, primaryVaillant, secondaryExtendedRegister, byte(len(payload))}, payload...), nil
	default:
		return nil, fmt.Errorf("unsupported method %q", entry.Method)
	}
}

func HexString(data []byte) string {
	return strings.ToUpper(hex.EncodeToString(data))
}
