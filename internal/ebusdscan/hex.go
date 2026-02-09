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
		return append([]byte{target, primaryVaillant, secondaryRegister}, payload...), nil
	case methodGetExtRegister:
		payload := []byte{0x02, 0x00, entry.Group, entry.Instance, byte(entry.Addr >> 8), byte(entry.Addr)}
		return append([]byte{target, primaryVaillant, secondaryExtendedRegister}, payload...), nil
	default:
		return nil, fmt.Errorf("unsupported method %q", entry.Method)
	}
}

func HexString(data []byte) string {
	return strings.ToUpper(hex.EncodeToString(data))
}
