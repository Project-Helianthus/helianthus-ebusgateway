package main

import (
	"encoding/binary"
	"math"
)

func decodeDATA2c(b []byte) (float64, bool) {
	if len(b) < 2 {
		return 0, false
	}
	raw := int16(binary.LittleEndian.Uint16(b[:2]))
	return float64(raw) / 16.0, true
}

func decodeDATA2b(b []byte) (float64, bool) {
	if len(b) < 2 {
		return 0, false
	}
	raw := binary.LittleEndian.Uint16(b[:2])
	return float64(raw) / 256.0, true
}

func decodeUCH(b []byte) (uint8, bool) {
	if len(b) < 1 {
		return 0, false
	}
	return b[0], true
}

func decodeUIN(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.LittleEndian.Uint16(b[:2]), true
}

func decodeSIN(b []byte) (int16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return int16(binary.LittleEndian.Uint16(b[:2])), true
}

func decodeOnOff(b []byte) (bool, bool) {
	if len(b) < 1 {
		return false, false
	}
	return b[0] != 0xF0, true
}

func decodeHoursum2(b []byte) (float64, bool) {
	if len(b) < 2 {
		return 0, false
	}
	raw := binary.LittleEndian.Uint16(b[:2])
	return float64(raw) * 2.0, true
}

func decodeUIN100(b []byte) (float64, bool) {
	if len(b) < 2 {
		return 0, false
	}
	raw := binary.LittleEndian.Uint16(b[:2])
	return float64(raw) / 100.0, true
}

func decodePercent0(b []byte) (uint8, bool) {
	if len(b) < 1 {
		return 0, false
	}
	return b[0], true
}

func encodeUCH(v float64) ([]byte, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, false
	}
	rounded := math.Round(v)
	if math.Abs(v-rounded) > 1e-9 || rounded < 0 || rounded > math.MaxUint8 {
		return nil, false
	}
	return []byte{byte(rounded)}, true
}

func encodeTempDATA2c(v float64) []byte {
	raw := int16(v * 16.0)
	out := make([]byte, 2)
	binary.LittleEndian.PutUint16(out, uint16(raw))
	return out
}
