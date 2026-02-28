package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway/internal/ebusdscan"
)

const defaultCommandPort = 8888

type scanConfig struct {
	Host               string
	Port               int
	TSP                string
	Target             byte
	HasTarget          bool
	Source             byte
	HasSource          bool
	Limit              int
	Timeout            time.Duration
	Sleep              time.Duration
	Mode               string
	IdentifyB50928xx   bool
	IdentifyPrefixHint byte
	Verbose            bool
}

type ebusdClient struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	timeout time.Duration
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func loadConfig() (scanConfig, error) {
	var hostFlag string
	var portFlag int
	var tspFlag string
	var targetFlag string
	var sourceFlag string
	var limitFlag int
	var timeoutFlag time.Duration
	var sleepFlag time.Duration
	var modeFlag string
	var agentLocalFlag string
	var identifyFlag bool
	var identifyPrefixFlag string
	var verboseFlag bool

	flag.StringVar(&hostFlag, "host", "", "ebusd host (env: EBUSD_HOST)")
	flag.IntVar(&portFlag, "port", 0, "ebusd command port (env: EBUSD_PORT)")
	flag.StringVar(&tspFlag, "tsp", "", "TSP path or URL (env: EBUSD_TSP)")
	flag.StringVar(&targetFlag, "target", "", "override target address (hex or dec, env: EBUSD_TARGET)")
	flag.StringVar(&sourceFlag, "source", "", "source address for hex command (-s) (hex or dec, env: EBUSD_SOURCE)")
	flag.IntVar(&limitFlag, "limit", 0, "limit number of entries (env: EBUSD_LIMIT)")
	flag.DurationVar(&timeoutFlag, "timeout", 5*time.Second, "command timeout (env: EBUSD_TIMEOUT)")
	flag.DurationVar(&sleepFlag, "sleep", 0, "delay between commands (env: EBUSD_SLEEP)")
	flag.StringVar(&modeFlag, "mode", "hex", "command mode: hex or read-h (env: EBUSD_MODE)")
	flag.StringVar(&agentLocalFlag, "agent-local", "", "path to AGENT-local.md (default: auto-discover)")
	flag.BoolVar(&identifyFlag, "identify-b509-28xx", false, "append B5 09 identify range (env: EBUSD_IDENTIFY_B509_28XX)")
	flag.StringVar(&identifyPrefixFlag, "identify-prefix", "", "prefix for identify range (hex, env: EBUSD_IDENTIFY_PREFIX)")
	flag.BoolVar(&verboseFlag, "verbose", false, "log commands sent")

	flag.Usage = func() {
		out := flag.CommandLine.Output()
		_, _ = fmt.Fprintln(out, "Usage: ebusdscan [options]")
		_, _ = fmt.Fprintln(out, "Scans TSP register entries via ebusd command port (hex or read-h mode).")
		_, _ = fmt.Fprintln(out, "")
		flag.PrintDefaults()
	}

	flag.Parse()

	envHost := strings.TrimSpace(os.Getenv("EBUSD_HOST"))
	envPort := parseIntEnv("EBUSD_PORT")
	envTSP := strings.TrimSpace(os.Getenv("EBUSD_TSP"))
	envTarget := strings.TrimSpace(os.Getenv("EBUSD_TARGET"))
	envSource := strings.TrimSpace(os.Getenv("EBUSD_SOURCE"))
	envLimit := parseIntEnv("EBUSD_LIMIT")
	envTimeout := parseDurationEnv("EBUSD_TIMEOUT")
	envSleep := parseDurationEnv("EBUSD_SLEEP")
	envMode := strings.TrimSpace(os.Getenv("EBUSD_MODE"))
	envIdentify := strings.TrimSpace(os.Getenv("EBUSD_IDENTIFY_B509_28XX"))
	envIdentifyPrefix := strings.TrimSpace(os.Getenv("EBUSD_IDENTIFY_PREFIX"))

	var defaults ebusdscan.AgentDefaults
	if agentLocalFlag == "" {
		if path, err := ebusdscan.FindAgentLocal(""); err == nil {
			if loaded, err := ebusdscan.LoadAgentLocal(path); err == nil {
				defaults = loaded
			}
		}
	} else {
		if loaded, err := ebusdscan.LoadAgentLocal(agentLocalFlag); err == nil {
			defaults = loaded
		} else if !errors.Is(err, ebusdscan.ErrAgentLocalMissing) {
			return scanConfig{}, err
		}
	}

	host := firstNonEmpty(hostFlag, envHost, defaults.Host)
	if host == "" {
		host = "127.0.0.1"
	}

	port := firstNonZero(portFlag, envPort, 0)
	if port == 0 {
		port = defaultCommandPort
	}

	tsp := firstNonEmpty(tspFlag, envTSP, defaults.TSP)
	if tsp == "" {
		return scanConfig{}, fmt.Errorf("tsp source required (flag --tsp or env EBUSD_TSP)")
	}

	targetRaw := firstNonEmpty(targetFlag, envTarget, "")
	var target byte
	hasTarget := false
	if targetRaw != "" {
		parsed, err := parseByteString(targetRaw)
		if err != nil {
			return scanConfig{}, fmt.Errorf("invalid target %q: %w", targetRaw, err)
		}
		target = parsed
		hasTarget = true
	}

	sourceRaw := firstNonEmpty(sourceFlag, envSource, "")
	var source byte
	hasSource := false
	if sourceRaw != "" {
		parsed, err := parseByteString(sourceRaw)
		if err != nil {
			return scanConfig{}, fmt.Errorf("invalid source %q: %w", sourceRaw, err)
		}
		source = parsed
		hasSource = true
	} else if defaults.HasSource {
		source = defaults.Source
		hasSource = true
	}

	limit := firstNonZero(limitFlag, envLimit, 0)

	timeout := timeoutFlag
	if envTimeout > 0 && timeoutFlag == 5*time.Second {
		timeout = envTimeout
	}

	sleep := sleepFlag
	if envSleep > 0 && sleepFlag == 0 {
		sleep = envSleep
	}

	mode := firstNonEmpty(modeFlag, envMode, "hex")
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "hex" && mode != "read-h" {
		return scanConfig{}, fmt.Errorf("unsupported mode %q (use hex or read-h)", mode)
	}

	identify := identifyFlag
	if !identify && envIdentify != "" {
		identify = strings.EqualFold(envIdentify, "1") || strings.EqualFold(envIdentify, "true") || strings.EqualFold(envIdentify, "yes")
	}
	if !identify && defaults.IdentifyB50928xx {
		identify = true
	}

	identifyPrefix := byte(0)
	if identifyPrefixFlag != "" {
		parsed, err := parseByteString(identifyPrefixFlag)
		if err != nil {
			return scanConfig{}, fmt.Errorf("invalid identify-prefix %q: %w", identifyPrefixFlag, err)
		}
		identifyPrefix = parsed
	} else if envIdentifyPrefix != "" {
		parsed, err := parseByteString(envIdentifyPrefix)
		if err != nil {
			return scanConfig{}, fmt.Errorf("invalid identify-prefix %q: %w", envIdentifyPrefix, err)
		}
		identifyPrefix = parsed
	} else if defaults.IdentifyPrefixHint != 0 {
		identifyPrefix = defaults.IdentifyPrefixHint
	}

	return scanConfig{
		Host:               host,
		Port:               port,
		TSP:                tsp,
		Target:             target,
		HasTarget:          hasTarget,
		Source:             source,
		HasSource:          hasSource,
		Limit:              limit,
		Timeout:            timeout,
		Sleep:              sleep,
		Mode:               mode,
		IdentifyB50928xx:   identify,
		IdentifyPrefixHint: identifyPrefix,
		Verbose:            verboseFlag,
	}, nil
}

func run(cfg scanConfig) error {
	result, err := ebusdscan.LoadTSP(cfg.TSP, ebusdscan.Options{
		IdentifyB50928xx: cfg.IdentifyB50928xx,
		IdentifyPrefix:   cfg.IdentifyPrefixHint,
		Limit:            cfg.Limit,
	})
	if err != nil {
		return err
	}

	target := cfg.Target
	if !cfg.HasTarget {
		target = result.Target
	}
	if target == 0 {
		return fmt.Errorf("target address missing (use --target or @zz in TSP)")
	}

	addr := net.JoinHostPort(normalizeDialHost(cfg.Host), strconv.Itoa(cfg.Port))
	conn, err := net.DialTimeout("tcp", addr, cfg.Timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := &ebusdClient{
		conn:    conn,
		reader:  bufio.NewReader(conn),
		writer:  bufio.NewWriter(conn),
		timeout: cfg.Timeout,
	}

	mode := cfg.Mode
	start := time.Now()
	okCount := 0
	errCount := 0
	emptyCount := 0

	fmt.Printf("scan: host=%s port=%d target=0x%02X entries=%d mode=%s\n", cfg.Host, cfg.Port, target, len(result.Entries), mode)

	for _, entry := range result.Entries {
		hexBytes, err := ebusdscan.BuildHex(target, entry)
		if err != nil {
			errCount++
			fmt.Printf("0x%02X %s addr=0x%04X -> build error: %v\n", target, entry.Method, entry.Addr, err)
			continue
		}
		hexText := ebusdscan.HexString(hexBytes)

		cmd := buildCommand(mode, hexText, cfg.HasSource, cfg.Source)
		if cfg.Verbose {
			fmt.Printf("cmd: %s\n", cmd)
		}
		lines, err := client.send(cmd)
		if err != nil && mode == "read-h" {
			lines = nil
		}
		if mode == "read-h" && isUnsupportedCommand(lines, err) {
			mode = "hex"
			cmd = buildCommand(mode, hexText, cfg.HasSource, cfg.Source)
			if cfg.Verbose {
				fmt.Printf("cmd: %s\n", cmd)
			}
			lines, err = client.send(cmd)
		}

		if mode == "hex" {
			if hexLine, ok := firstHexLine(lines); ok {
				lines = []string{hexLine}
				err = nil
			}
		}

		responseText := strings.Join(lines, " | ")
		label := formatEntryLabel(target, entry)
		if err != nil {
			errCount++
			fmt.Printf("%s -> error: %v\n", label, err)
		} else if len(lines) == 0 {
			errCount++
			fmt.Printf("%s -> empty response\n", label)
		} else {
			if isErrorResponse(lines) {
				errCount++
				if isEmptyResponse(lines) {
					emptyCount++
				}
			} else {
				okCount++
			}
			fmt.Printf("%s -> %s\n", label, responseText)
		}

		if cfg.Sleep > 0 {
			time.Sleep(cfg.Sleep)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("summary: total=%d ok=%d err=%d empty=%d elapsed=%s mode=%s\n",
		len(result.Entries), okCount, errCount, emptyCount, elapsed.Truncate(time.Millisecond), mode)
	return nil
}

func firstHexLine(lines []string) (string, bool) {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
			trimmed = strings.TrimSpace(trimmed[2:])
		}
		normalized := strings.Join(strings.Fields(trimmed), "")
		if normalized == "" || len(normalized)%2 != 0 {
			continue
		}
		allHex := true
		for i := 0; i < len(normalized); i++ {
			c := normalized[i]
			if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
				continue
			}
			allHex = false
			break
		}
		if allHex {
			return strings.ToUpper(normalized), true
		}
	}
	return "", false
}

func (c *ebusdClient) send(cmd string) ([]string, error) {
	if c.timeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	}
	if _, err := c.writer.WriteString(cmd + "\n"); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, err
	}

	var lines []string
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return lines, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func buildCommand(mode, hexText string, hasSource bool, source byte) string {
	if mode == "read-h" {
		return fmt.Sprintf("read -h %s", hexText)
	}
	if hasSource {
		return fmt.Sprintf("hex -n -s %02X %s", source, hexText)
	}
	return fmt.Sprintf("hex -n %s", hexText)
}

func formatEntryLabel(target byte, entry ebusdscan.Entry) string {
	if entry.Method == "get_ext_register" {
		return fmt.Sprintf("0x%02X %s group=0x%02X instance=0x%02X addr=0x%04X", target, entry.Method, entry.Group, entry.Instance, entry.Addr)
	}
	return fmt.Sprintf("0x%02X %s addr=0x%04X", target, entry.Method, entry.Addr)
}

func isErrorResponse(lines []string) bool {
	if len(lines) == 0 {
		return true
	}
	line := strings.TrimSpace(lines[0])
	return strings.HasPrefix(line, "ERR:")
}

func normalizeDialHost(host string) string {
	host = strings.TrimSpace(host)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") && len(host) >= 2 {
		return host[1 : len(host)-1]
	}
	return host
}

func isEmptyResponse(lines []string) bool {
	if len(lines) == 0 {
		return true
	}
	line := strings.ToLower(lines[0])
	return strings.Contains(line, "no data") || strings.Contains(line, "empty") || strings.Contains(line, "no answer")
}

func isUnsupportedCommand(lines []string, err error) bool {
	if err != nil {
		return false
	}
	if len(lines) == 0 {
		return false
	}
	line := strings.ToLower(lines[0])
	if strings.Contains(line, "unknown command") {
		return true
	}
	if strings.HasPrefix(line, "usage:") {
		return true
	}
	if strings.Contains(line, "invalid argument") {
		return true
	}
	return false
}

func parseByteString(raw string) (byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty")
	}
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		value, err := strconv.ParseUint(raw, 0, 16)
		if err != nil {
			return 0, err
		}
		if value > 0xFF {
			return 0, fmt.Errorf("out of range")
		}
		return byte(value), nil
	}
	value, err := strconv.ParseUint(raw, 10, 16)
	if err != nil {
		return 0, err
	}
	if value > 0xFF {
		return 0, fmt.Errorf("out of range")
	}
	return byte(value), nil
}

func parseIntEnv(key string) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return value
}

func parseDurationEnv(key string) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
