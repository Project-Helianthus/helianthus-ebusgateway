package matrix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// ContenderConfig configures the synthetic ENH contender the harness drives
// on the mux's northbound ENH listener in PX cells.
type ContenderConfig struct {
	Source                 byte
	Telegrams              []GuardTelegram
	StepTimeout            time.Duration
	MaxArbitrationAttempts int
}

// ContenderResult is the contender's record for one cell.
type ContenderResult struct {
	Transactions       int           `json:"transactions"`
	Completed          int           `json:"completed"`
	ArbitrationFailed  int           `json:"arbitration_failed"`
	RequestTypes       []RequestType `json:"request_types"`
	RefusedBeforeWrite bool          `json:"refused_before_write"`
	Error              string        `json:"error,omitempty"`
}

var errContenderProtocol = errors.New("synthetic contender protocol violation")

// IdentificationWorkload is the default contender and ebusd workload: count
// identification requests (07 04) to target.
func IdentificationWorkload(target byte, count int) []GuardTelegram {
	telegrams := make([]GuardTelegram, 0, count)
	for index := 0; index < count; index++ {
		telegrams = append(telegrams, GuardTelegram{Target: target, Primary: 0x07, Secondary: 0x04})
	}
	return telegrams
}

// RunSyntheticContender runs the contender workload on conn. Every telegram
// passes ClassifyTelegram before the first byte, INIT included, is written;
// one refused telegram refuses the whole workload with nothing sent.
func RunSyntheticContender(ctx context.Context, conn net.Conn, config ContenderConfig) (ContenderResult, error) {
	result := ContenderResult{Transactions: len(config.Telegrams)}
	fail := func(err error) (ContenderResult, error) {
		result.Error = err.Error()
		return result, err
	}

	if !protocol.IsInitiatorCapableAddress(config.Source) {
		result.RefusedBeforeWrite = true
		return fail(fmt.Errorf("%w: source 0x%02X is not an initiator address", ErrWriteGuardRefused, config.Source))
	}
	if len(config.Telegrams) == 0 {
		result.RefusedBeforeWrite = true
		return fail(fmt.Errorf("%w: empty contender workload", ErrWriteGuardRefused))
	}
	for _, telegram := range config.Telegrams {
		requestType, err := ClassifyTelegram(telegram)
		if err != nil {
			result.RefusedBeforeWrite = true
			result.RequestTypes = nil
			return fail(err)
		}
		result.RequestTypes = append(result.RequestTypes, requestType)
	}

	if config.StepTimeout <= 0 {
		config.StepTimeout = 2 * time.Second
	}
	if config.MaxArbitrationAttempts <= 0 {
		config.MaxArbitrationAttempts = 5
	}
	session := &enhContenderSession{ctx: ctx, conn: conn, timeout: config.StepTimeout}

	if err := session.send(transport.ENHReqInit, 0x00); err != nil {
		return fail(err)
	}
	if _, err := session.await(func(message transport.ENHMessage) (bool, error) {
		return message.Command == transport.ENHResResetted, nil
	}); err != nil {
		return fail(fmt.Errorf("await RESETTED after INIT: %w", err))
	}

	for _, telegram := range config.Telegrams {
		failed, err := session.transact(config.Source, telegram, config.MaxArbitrationAttempts)
		result.ArbitrationFailed += failed
		if err != nil {
			return fail(err)
		}
		result.Completed++
	}
	return result, nil
}

type enhContenderSession struct {
	ctx     context.Context
	conn    net.Conn
	timeout time.Duration
	parser  transport.ENHParser
	pending []transport.ENHMessage
}

func (session *enhContenderSession) send(command transport.ENHCommand, data byte) error {
	if err := session.ctx.Err(); err != nil {
		return err
	}
	frame := transport.EncodeENH(command, data)
	_ = session.conn.SetWriteDeadline(time.Now().Add(session.timeout))
	_, err := session.conn.Write(frame[:])
	return err
}

func (session *enhContenderSession) next() (transport.ENHMessage, error) {
	buffer := make([]byte, 64)
	for len(session.pending) == 0 {
		if err := session.ctx.Err(); err != nil {
			return transport.ENHMessage{}, err
		}
		_ = session.conn.SetReadDeadline(time.Now().Add(session.timeout))
		count, err := session.conn.Read(buffer)
		if count > 0 {
			messages, parseErr := session.parser.Parse(buffer[:count])
			session.pending = append(session.pending, messages...)
			if parseErr != nil {
				return transport.ENHMessage{}, fmt.Errorf("%w: %v", errContenderProtocol, parseErr)
			}
		}
		if err != nil && len(session.pending) == 0 {
			if errors.Is(err, io.EOF) {
				return transport.ENHMessage{}, fmt.Errorf("listener closed: %w", err)
			}
			return transport.ENHMessage{}, err
		}
	}
	message := session.pending[0]
	session.pending = session.pending[1:]
	if message.Command == transport.ENHResErrorEBUS || message.Command == transport.ENHResErrorHost {
		return message, fmt.Errorf("%w: adapter error 0x%X", errContenderProtocol, byte(message.Command))
	}
	return message, nil
}

// await reads messages until accept returns true.
func (session *enhContenderSession) await(accept func(transport.ENHMessage) (bool, error)) (transport.ENHMessage, error) {
	for {
		message, err := session.next()
		if err != nil {
			return message, err
		}
		done, err := accept(message)
		if err != nil || done {
			return message, err
		}
	}
}

func (session *enhContenderSession) receive() (byte, error) {
	message, err := session.next()
	if err != nil {
		return 0, err
	}
	switch message.Command {
	case transport.ENHResReceived:
		return message.Data, nil
	case transport.ENHResResetted:
		return 0, fmt.Errorf("%w: RESETTED during transaction", errContenderProtocol)
	default:
		return 0, fmt.Errorf("%w: unexpected ENH command 0x%X during transaction", errContenderProtocol, byte(message.Command))
	}
}

// receiveLogical reads one logical byte, decoding an escape pair.
func (session *enhContenderSession) receiveLogical() (byte, error) {
	symbol, err := session.receive()
	if err != nil {
		return 0, err
	}
	if symbol == protocol.SymbolSyn {
		return 0, fmt.Errorf("%w: SYN inside a telegram", errContenderProtocol)
	}
	if symbol != protocol.SymbolEscape {
		return symbol, nil
	}
	suffix, err := session.receive()
	if err != nil {
		return 0, err
	}
	switch suffix {
	case 0x00:
		return protocol.SymbolEscape, nil
	case 0x01:
		return protocol.SymbolSyn, nil
	default:
		return 0, fmt.Errorf("%w: invalid escape suffix 0x%02X", errContenderProtocol, suffix)
	}
}

// sendSymbol sends one wire symbol and verifies its echo.
func (session *enhContenderSession) sendSymbol(symbol byte) error {
	if err := session.send(transport.ENHReqSend, symbol); err != nil {
		return err
	}
	echo, err := session.receive()
	if err != nil {
		return fmt.Errorf("await echo of 0x%02X: %w", symbol, err)
	}
	if echo != symbol {
		return fmt.Errorf("%w: echo 0x%02X for sent 0x%02X", errContenderProtocol, echo, symbol)
	}
	return nil
}

func (session *enhContenderSession) transact(source byte, telegram GuardTelegram, maxAttempts int) (int, error) {
	failed := 0
	for {
		if err := session.send(transport.ENHReqStart, source); err != nil {
			return failed, err
		}
		message, err := session.await(func(message transport.ENHMessage) (bool, error) {
			switch message.Command {
			case transport.ENHResStarted, transport.ENHResFailed:
				return true, nil
			case transport.ENHResResetted:
				return false, fmt.Errorf("%w: RESETTED during arbitration", errContenderProtocol)
			default:
				return false, nil
			}
		})
		if err != nil {
			return failed, fmt.Errorf("await arbitration: %w", err)
		}
		if message.Command == transport.ENHResStarted {
			if message.Data != source {
				return failed, fmt.Errorf("%w: STARTED 0x%02X for source 0x%02X", errContenderProtocol, message.Data, source)
			}
			break
		}
		failed++
		if failed >= maxAttempts {
			return failed, fmt.Errorf("arbitration lost %d times", failed)
		}
	}

	logical := append([]byte{source, telegram.Target, telegram.Primary, telegram.Secondary, byte(len(telegram.Data))}, telegram.Data...)
	logical = append(logical, protocol.CRC(logical))
	for _, symbol := range escapeSymbols(logical[1:]) {
		if err := session.sendSymbol(symbol); err != nil {
			return failed, err
		}
	}

	ack, err := session.receive()
	if err != nil {
		return failed, fmt.Errorf("await target ACK: %w", err)
	}
	if ack != protocol.SymbolAck {
		return failed, fmt.Errorf("target answered 0x%02X instead of ACK", ack)
	}
	length, err := session.receiveLogical()
	if err != nil {
		return failed, fmt.Errorf("await response length: %w", err)
	}
	response := []byte{length}
	for index := 0; index < int(length); index++ {
		value, err := session.receiveLogical()
		if err != nil {
			return failed, fmt.Errorf("await response byte %d: %w", index, err)
		}
		response = append(response, value)
	}
	crc, err := session.receiveLogical()
	if err != nil {
		return failed, fmt.Errorf("await response CRC: %w", err)
	}
	if want := protocol.CRC(response); crc != want {
		return failed, fmt.Errorf("response CRC 0x%02X, want 0x%02X", crc, want)
	}
	if err := session.sendSymbol(protocol.SymbolAck); err != nil {
		return failed, err
	}
	if err := session.sendSymbol(protocol.SymbolSyn); err != nil {
		return failed, err
	}
	return failed, nil
}

// escapeSymbols encodes logical bytes as wire symbols (0xA9 → A9 00,
// 0xAA → A9 01).
func escapeSymbols(logical []byte) []byte {
	symbols := make([]byte, 0, len(logical)+2)
	for _, value := range logical {
		switch value {
		case protocol.SymbolEscape:
			symbols = append(symbols, protocol.SymbolEscape, 0x00)
		case protocol.SymbolSyn:
			symbols = append(symbols, protocol.SymbolEscape, 0x01)
		default:
			symbols = append(symbols, value)
		}
	}
	return symbols
}
