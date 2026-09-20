package testfixture

import (
	"encoding/binary"
	"io"
	"math"
	"net"
	"sync"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

// ReadRequest records one request admitted by the accepted SunSpec chain
// fixture so producer tests can continue to verify the bounded FC03 sequence.
type ReadRequest struct {
	UnitID    byte
	Function  modbus.FunctionCode
	Offset    uint16
	WordCount uint16
}

// ServeSunSpecChain serves the accepted deterministic SunSpec word replay used
// by the native producer tests. Callers retain ownership of words and mutate it
// only between completed producer calls when testing refresh behavior.
func ServeSunSpecChain(words []uint16) (net.Listener, func() []ReadRequest, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	var mu sync.Mutex
	var requests []ReadRequest
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		for {
			header := make([]byte, 7)
			if _, err := io.ReadFull(connection, header); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(header[4:6]))
			body := make([]byte, length-1)
			if _, err := io.ReadFull(connection, body); err != nil || len(body) != 5 {
				return
			}
			request := ReadRequest{
				UnitID: header[6], Function: modbus.FunctionCode(body[0]),
				Offset: binary.BigEndian.Uint16(body[1:3]), WordCount: binary.BigEndian.Uint16(body[3:5]),
			}
			mu.Lock()
			requests = append(requests, request)
			mu.Unlock()
			start := int(request.Offset) - 40000
			end := start + int(request.WordCount)
			if start < 0 || end > len(words) {
				return
			}
			response := make([]byte, 9+2*int(request.WordCount))
			copy(response[:2], header[:2])
			binary.BigEndian.PutUint16(response[4:6], uint16(3+2*int(request.WordCount)))
			response[6], response[7], response[8] = request.UnitID, byte(request.Function), byte(2*request.WordCount)
			for index, word := range words[start:end] {
				binary.BigEndian.PutUint16(response[9+2*index:], word)
			}
			if _, err := connection.Write(response); err != nil {
				return
			}
		}
	}()
	return listener, func() []ReadRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]ReadRequest(nil), requests...)
	}, nil
}

// Words builds a bounded model chain from id/length pairs.
func Words(headers ...uint16) []uint16 {
	words := []uint16{0x5375, 0x6e53}
	for index := 0; index < len(headers); index += 2 {
		words = append(words, headers[index], headers[index+1])
		if headers[index] != 0xffff {
			words = append(words, make([]uint16, headers[index+1])...)
		}
	}
	return words
}

// ObservedFroniusFloatWords returns the accepted observed-monitoring replay.
func ObservedFroniusFloatWords() []uint16 {
	return fixtureWords(
		fixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		fixtureModel{113, 60, floatInverterPayload()},
		fixtureModel{120, 26, make([]uint16, 26)},
		fixtureModel{121, 30, make([]uint16, 30)},
		fixtureModel{122, 44, make([]uint16, 44)},
		fixtureModel{160, 88, mpptPayload(4)},
		fixtureModel{124, 24, make([]uint16, 24)},
	)
}

// ObservedFroniusFloatControlsWords returns the accepted replay including the
// standard controls model used by qualification and semantic publication tests.
func ObservedFroniusFloatControlsWords() []uint16 {
	return fixtureWords(
		fixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		fixtureModel{113, 60, floatInverterPayload()},
		fixtureModel{120, 26, make([]uint16, 26)},
		fixtureModel{121, 30, make([]uint16, 30)},
		fixtureModel{122, 44, make([]uint16, 44)},
		fixtureModel{123, 24, make([]uint16, 24)},
		fixtureModel{160, 88, mpptPayload(4)},
		fixtureModel{124, 24, make([]uint16, 24)},
	)
}

// AdmittedFloatChainWords returns the minimal accepted float-inverter chain.
func AdmittedFloatChainWords() []uint16 {
	return fixtureWords(
		fixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		fixtureModel{113, 60, floatInverterPayload()},
	)
}

// SetFloat changes one Model 113 float point between completed replay calls.
func SetFloat(words []uint16, payloadOffset int, value float32) {
	const payloadStart = 2 + 67 + 2
	bits := math.Float32bits(value)
	words[payloadStart+payloadOffset] = uint16(bits >> 16)
	words[payloadStart+payloadOffset+1] = uint16(bits)
}

// PutString updates one fixed-width SunSpec string field in an accepted replay.
func PutString(words []uint16, value string) {
	data := []byte(value)
	for index := range words {
		var high, low byte
		if 2*index < len(data) {
			high = data[2*index]
		}
		if 2*index+1 < len(data) {
			low = data[2*index+1]
		}
		words[index] = uint16(high)<<8 | uint16(low)
	}
}

type fixtureModel struct {
	id, length uint16
	payload    []uint16
}

func fixtureWords(models ...fixtureModel) []uint16 {
	words := []uint16{0x5375, 0x6e53}
	for _, model := range models {
		words = append(words, model.id, model.length)
		words = append(words, model.payload...)
	}
	return append(words, 0xffff, 0)
}

func commonPayload(manufacturer, model, firmware string) []uint16 {
	payload := make([]uint16, 65)
	PutString(payload[0:16], manufacturer)
	PutString(payload[16:32], model)
	PutString(payload[40:48], firmware)
	PutString(payload[48:64], "synthetic")
	return payload
}

func floatInverterPayload() []uint16 {
	payload := make([]uint16, 60)
	// Model 113's status point is word 48 including the two-word header.
	payload[46] = 4
	return payload
}

func mpptPayload(modules uint16) []uint16 {
	payload := make([]uint16, 88)
	// Model 160's N point is word 8 including the two-word header.
	payload[6] = modules
	return payload
}
