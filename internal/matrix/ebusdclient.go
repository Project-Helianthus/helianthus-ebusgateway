package matrix

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// EbusdWorkloadResult is the record of one guarded ebusd command workload.
type EbusdWorkloadResult struct {
	Commands           []string `json:"commands"`
	Answered           int      `json:"answered"`
	ErrAnswers         int      `json:"err_answers"`
	RefusedBeforeWrite bool     `json:"refused_before_write"`
	Error              string   `json:"error,omitempty"`
}

// EbusdIdentificationCommands is the default ebusd workload against target:
// adapter state, an identification scan of the target, and the
// identification telegram as a hex command.
func EbusdIdentificationCommands(target byte) []string {
	return []string{
		"state",
		fmt.Sprintf("scan %02x", target),
		fmt.Sprintf("hex %02x070400", target),
	}
}

// RunEbusdWorkload sends commands on an ebusd command-port connection. Every
// command passes CheckEbusdCommand before the first one is written; one
// refused command refuses the whole workload with nothing sent.
func RunEbusdWorkload(ctx context.Context, conn net.Conn, commands []string, timeout time.Duration) (EbusdWorkloadResult, error) {
	result := EbusdWorkloadResult{Commands: append([]string(nil), commands...)}
	fail := func(err error) (EbusdWorkloadResult, error) {
		result.Error = err.Error()
		return result, err
	}
	if len(commands) == 0 {
		result.RefusedBeforeWrite = true
		return fail(fmt.Errorf("%w: empty ebusd workload", ErrWriteGuardRefused))
	}
	for _, command := range commands {
		if err := CheckEbusdCommand(command); err != nil {
			result.RefusedBeforeWrite = true
			return fail(err)
		}
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	reader := bufio.NewReader(conn)
	for _, command := range commands {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		if _, err := io.WriteString(conn, command+"\n"); err != nil {
			return fail(fmt.Errorf("write %q: %w", command, err))
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		lines, err := readEbusdAnswer(reader)
		if err != nil {
			return fail(fmt.Errorf("answer to %q: %w", command, err))
		}
		result.Answered++
		if len(lines) > 0 && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(lines[0])), "ERR") {
			result.ErrAnswers++
		}
	}
	if result.ErrAnswers > 0 {
		return fail(fmt.Errorf("%d of %d ebusd commands answered ERR", result.ErrAnswers, result.Answered))
	}
	return result, nil
}

// readEbusdAnswer reads one ebusd answer: lines up to an empty line.
func readEbusdAnswer(reader *bufio.Reader) ([]string, error) {
	lines := make([]string, 0, 2)
	for {
		line, err := reader.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		if err != nil {
			if errors.Is(err, io.EOF) && trimmed == "" && len(lines) > 0 {
				return lines, nil
			}
			return lines, err
		}
		if trimmed == "" {
			return lines, nil
		}
		lines = append(lines, trimmed)
	}
}

// relayRefusalAnswer is what a client receives for a refused command.
const relayRefusalAnswer = "ERR: refused by matrix write guard\n\n"

// GuardedRelayStats counts what a guarded ebusd relay admitted and refused.
type GuardedRelayStats struct {
	mu        sync.Mutex
	forwarded int
	refused   int
	refusals  []string
}

func (stats *GuardedRelayStats) record(forwarded bool, command string) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if forwarded {
		stats.forwarded++
		return
	}
	stats.refused++
	stats.refusals = append(stats.refusals, command)
}

// Snapshot returns a copy of the counters.
func (stats *GuardedRelayStats) Snapshot() (forwarded int, refused int, refusals []string) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	return stats.forwarded, stats.refused, append([]string(nil), stats.refusals...)
}

// ServeGuardedEbusdRelay relays one client connection to the ebusd command
// port. Each client line passes CheckEbusdCommand; a refused line is answered
// locally and never reaches ebusd. The upstream connection is dialed on the
// first admitted line, so a client that only sends refused lines never
// reaches ebusd at all.
func ServeGuardedEbusdRelay(ctx context.Context, client net.Conn, dialUpstream func(context.Context) (net.Conn, error), stats *GuardedRelayStats) error {
	defer func() { _ = client.Close() }()
	stopClose := context.AfterFunc(ctx, func() { _ = client.Close() })
	defer stopClose()
	clientReader := bufio.NewReader(client)
	var upstream net.Conn
	var upstreamReader *bufio.Reader
	defer func() {
		if upstream != nil {
			_ = upstream.Close()
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := clientReader.ReadString('\n')
		command := strings.TrimRight(line, "\r\n")
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if strings.TrimSpace(command) == "" {
			continue
		}
		if guardErr := CheckEbusdCommand(command); guardErr != nil {
			stats.record(false, command)
			if _, err := io.WriteString(client, relayRefusalAnswer); err != nil {
				return err
			}
			continue
		}
		if upstream == nil {
			upstream, err = dialUpstream(ctx)
			if err != nil {
				return fmt.Errorf("dial ebusd command port: %w", err)
			}
			upstreamReader = bufio.NewReader(upstream)
		}
		stats.record(true, command)
		if _, err := io.WriteString(upstream, command+"\n"); err != nil {
			return err
		}
		lines, err := readEbusdAnswer(upstreamReader)
		if err != nil {
			return fmt.Errorf("ebusd answer: %w", err)
		}
		if _, err := io.WriteString(client, strings.Join(lines, "\n")+"\n\n"); err != nil {
			return err
		}
	}
}
