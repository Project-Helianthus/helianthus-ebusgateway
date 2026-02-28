package ebusgateway

import (
	"fmt"
	"strings"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

func decodeDeviceInfoPayload(payload []byte) (map[string]string, error) {
	if len(payload) < 10 {
		return nil, fmt.Errorf("identify short payload: %w", ebuserrors.ErrInvalidPayload)
	}

	manufacturer := fmt.Sprintf("0x%02X", payload[0])
	if payload[0] == 0xB5 {
		manufacturer = "Vaillant"
	}

	return map[string]string{
		"manufacturer": manufacturer,
		"device_id":    strings.Trim(string(payload[1:6]), " \x00"),
		"sw_version":   fmt.Sprintf("%02X%02X", payload[6], payload[7]),
		"hw_version":   fmt.Sprintf("%02X%02X", payload[8], payload[9]),
	}, nil
}
