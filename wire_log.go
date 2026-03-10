package ebusgateway

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type wireLogger struct {
	mu     sync.Mutex
	writer io.Writer
	close  func() error
}

func newWireLogger(path string) (*wireLogger, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wireLogger{
		writer: file,
		close:  file.Close,
	}, nil
}

func (logger *wireLogger) Close() error {
	if logger == nil || logger.close == nil {
		return nil
	}
	return logger.close()
}

func (logger *wireLogger) logf(format string, args ...any) {
	if logger == nil || logger.writer == nil {
		return
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	_, _ = fmt.Fprintf(logger.writer, format, args...)
}

func (logger *wireLogger) logByte(connLabel, dir string, value byte) {
	ts := time.Now().Format(time.RFC3339Nano)
	logger.logf("%s conn=%s dir=%s byte=0x%02x\n", ts, connLabel, dir, value)
}

func (logger *wireLogger) logError(connLabel, dir string, err error) {
	if err == nil {
		return
	}
	ts := time.Now().Format(time.RFC3339Nano)
	logger.logf("%s conn=%s dir=%s error=%v\n", ts, connLabel, dir, err)
}

func (logger *wireLogger) logFrame(connLabel, dir string, frame protocol.Frame) {
	ts := time.Now().Format(time.RFC3339Nano)
	logger.logf("%s conn=%s dir=%s frame src=0x%02x dst=0x%02x pri=0x%02x sec=0x%02x len=%d data=%x\n",
		ts,
		connLabel,
		dir,
		frame.Source,
		frame.Target,
		frame.Primary,
		frame.Secondary,
		len(frame.Data),
		frame.Data,
	)
	if frame.Primary == 0xB5 && frame.Secondary == 0x12 {
		if len(frame.Data) >= 2 {
			le := binary.LittleEndian.Uint16(frame.Data[:2])
			be := binary.BigEndian.Uint16(frame.Data[:2])
			logger.logf("%s conn=%s dir=%s b5_12 raw=%x u16le=0x%04x u16be=0x%04x b0=0x%02x b1=0x%02x\n",
				ts, connLabel, dir, frame.Data, le, be, frame.Data[0], frame.Data[1])
		} else {
			logger.logf("%s conn=%s dir=%s b5_12 raw=%x len=%d\n", ts, connLabel, dir, frame.Data, len(frame.Data))
		}
	}
}

type wireLogTransport struct {
	inner     transport.RawTransport
	logger    *wireLogger
	connLabel string
	rx        *frameDecoder
	tx        *frameDecoder
}

func newWireLogTransport(inner transport.RawTransport, logger *wireLogger, connLabel string) transport.RawTransport {
	if logger == nil {
		return inner
	}
	return &wireLogTransport{
		inner:     inner,
		logger:    logger,
		connLabel: connLabel,
		rx:        &frameDecoder{},
		tx:        &frameDecoder{},
	}
}

func (wt *wireLogTransport) ReadByte() (byte, error) {
	if wt == nil || wt.inner == nil {
		return 0, fmt.Errorf("wire log transport missing")
	}
	value, err := wt.inner.ReadByte()
	if err != nil {
		wt.logger.logError(wt.connLabel, "rx", err)
		return value, err
	}
	wt.logger.logByte(wt.connLabel, "rx", value)
	if frame, ok := wt.rx.push(value); ok {
		wt.logger.logFrame(wt.connLabel, "rx", frame)
	}
	return value, nil
}

func (wt *wireLogTransport) ReadEvent() (transport.StreamEvent, error) {
	if wt == nil || wt.inner == nil {
		return transport.StreamEvent{}, fmt.Errorf("wire log transport missing")
	}
	if reader, ok := wt.inner.(transport.StreamEventReader); ok {
		event, err := reader.ReadEvent()
		if err != nil {
			wt.logger.logError(wt.connLabel, "rx", err)
			return event, err
		}
		switch event.Kind {
		case transport.StreamEventReset:
			wt.rx.reset()
			wt.logger.logf("%s conn=%s dir=rx reset\n", time.Now().Format(time.RFC3339Nano), wt.connLabel)
		case transport.StreamEventByte:
			wt.logger.logByte(wt.connLabel, "rx", event.Byte)
			if frame, ok := wt.rx.push(event.Byte); ok {
				wt.logger.logFrame(wt.connLabel, "rx", frame)
			}
		}
		return event, nil
	}

	value, err := wt.ReadByte()
	if err != nil {
		return transport.StreamEvent{}, err
	}
	return transport.StreamEvent{Kind: transport.StreamEventByte, Byte: value}, nil
}

func (wt *wireLogTransport) Write(data []byte) (int, error) {
	if wt == nil || wt.inner == nil {
		return 0, fmt.Errorf("wire log transport missing")
	}
	for _, value := range data {
		wt.logger.logByte(wt.connLabel, "tx", value)
		if frame, ok := wt.tx.push(value); ok {
			wt.logger.logFrame(wt.connLabel, "tx", frame)
		}
	}
	written, err := wt.inner.Write(data)
	if err != nil {
		wt.logger.logError(wt.connLabel, "tx", err)
	}
	return written, err
}

func (wt *wireLogTransport) Close() error {
	if wt == nil || wt.inner == nil {
		return nil
	}
	return wt.inner.Close()
}

type frameDecoder struct {
	escape bool
	buffer []byte
}

func (decoder *frameDecoder) push(symbol byte) (protocol.Frame, bool) {
	if decoder == nil {
		return protocol.Frame{}, false
	}
	if decoder.escape {
		decoder.escape = false
		switch symbol {
		case 0x00:
			decoder.buffer = append(decoder.buffer, protocol.SymbolEscape)
		case 0x01:
			decoder.buffer = append(decoder.buffer, protocol.SymbolSyn)
		default:
			decoder.buffer = decoder.buffer[:0]
		}
		return protocol.Frame{}, false
	}

	switch symbol {
	case protocol.SymbolEscape:
		decoder.escape = true
		return protocol.Frame{}, false
	case protocol.SymbolSyn:
		if len(decoder.buffer) == 0 {
			return protocol.Frame{}, false
		}
		frame, ok := parseFrame(decoder.buffer)
		decoder.buffer = decoder.buffer[:0]
		if ok {
			return frame, true
		}
		return protocol.Frame{}, false
	default:
		decoder.buffer = append(decoder.buffer, symbol)
		return protocol.Frame{}, false
	}
}

func (decoder *frameDecoder) reset() {
	if decoder == nil {
		return
	}
	decoder.escape = false
	decoder.buffer = decoder.buffer[:0]
}

func parseFrame(raw []byte) (protocol.Frame, bool) {
	if len(raw) < 6 {
		return protocol.Frame{}, false
	}
	length := int(raw[4])
	expected := 6 + length
	if len(raw) != expected {
		return protocol.Frame{}, false
	}
	crc := protocol.CRC(raw[:len(raw)-1])
	if crc != raw[len(raw)-1] {
		return protocol.Frame{}, false
	}
	data := append([]byte(nil), raw[5:5+length]...)
	return protocol.Frame{
		Source:    raw[0],
		Target:    raw[1],
		Primary:   raw[2],
		Secondary: raw[3],
		Data:      data,
	}, true
}
