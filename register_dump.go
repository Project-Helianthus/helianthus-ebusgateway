package ebusgateway

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

type registerDumpEntry struct {
	Method   string
	Group    byte
	Instance byte
	Addr     uint16
	Model    string
	Line     int
}

type baseContext struct {
	Secondary byte
	Prefix    byte
	Group     byte
	Instance  byte
	AddrLo    byte
	HasAddrLo bool
}

func runRegisterDump(ctx context.Context, cfg smokeConfig, gateway *Gateway, entries []registry.DeviceEntry, source byte, logger *wireLogger, logOutput io.Writer) error {
	if strings.TrimSpace(cfg.Smoke.RegisterDumpTSP) == "" {
		return nil
	}
	if gateway == nil {
		return fmt.Errorf("register dump missing gateway")
	}

	content, err := loadRegisterDumpSource(cfg.Smoke.RegisterDumpTSP)
	if err != nil {
		return err
	}

	requests, targetFromTSP, err := parseRegisterDumpTSP(content)
	if err != nil {
		return err
	}
	if len(requests) == 0 {
		return fmt.Errorf("register dump: no ext registers found")
	}

	target := cfg.Smoke.RegisterDumpTarget.Byte()
	if target == 0 {
		target = targetFromTSP
	}
	if target == 0 {
		return fmt.Errorf("register dump target missing")
	}

	entry := findEntryByAddress(entries, target)
	if entry == nil {
		return fmt.Errorf("register dump target 0x%02x not found in scan results", target)
	}

	systemPlane, ok := findSystemPlane(entry.Planes())
	if !ok {
		return fmt.Errorf("register dump target 0x%02x missing system plane", target)
	}

	dumpPath := cfg.Smoke.RegisterDumpOutput
	if strings.TrimSpace(dumpPath) == "" {
		if strings.TrimSpace(cfg.Smoke.WireLogPath) != "" {
			dumpPath = cfg.Smoke.WireLogPath + ".dump.log"
		} else {
			dumpPath = filepath.Join(".", "register_dump.log")
		}
	}

	writer := logOutput
	if writer == nil {
		file, err := os.OpenFile(dumpPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		defer file.Close()
		writer = file
	}

	timeout := time.Duration(cfg.Smoke.RegisterDumpTimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(cfg.Smoke.MethodTimeoutSec) * time.Second
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	limit := cfg.Smoke.RegisterDumpLimit
	if limit > 0 && len(requests) > limit {
		requests = requests[:limit]
	}

	writeDumpLine(writer, "register dump started: target=0x%02x entries=%d tsp=%s", target, len(requests), cfg.Smoke.RegisterDumpTSP)

	for _, req := range requests {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		params := map[string]any{
			"source": source,
			"addr":   req.Addr,
		}
		if req.Method == "get_ext_register" {
			params["group"] = req.Group
			params["instance"] = req.Instance
		}

		ctxMethod, cancel := context.WithTimeout(ctx, timeout)
		result, err := gateway.Router.Invoke(ctxMethod, systemPlane, req.Method, params)
		cancel()
		if err != nil {
			writeDumpLine(writer, "target=0x%02x group=0x%02x instance=0x%02x addr=0x%04x model=%s line=%d error=%v",
				target, req.Group, req.Instance, req.Addr, req.Model, req.Line, err)
			continue
		}

		payload := extractDumpPayload(result)
		writeDumpLine(writer, "target=0x%02x group=0x%02x instance=0x%02x addr=0x%04x method=%s model=%s line=%d payload=%s",
			target, req.Group, req.Instance, req.Addr, req.Method, req.Model, req.Line, payload)
	}

	writeDumpLine(writer, "register dump completed: target=0x%02x entries=%d", target, len(requests))

	if logger != nil {
		logger.logf("%s register dump completed: target=0x%02x entries=%d\n", time.Now().Format(time.RFC3339Nano), target, len(requests))
	}
	return nil
}

func findEntryByAddress(entries []registry.DeviceEntry, target byte) registry.DeviceEntry {
	for _, entry := range entries {
		if entry != nil && entry.Address() == target {
			return entry
		}
	}
	return nil
}

func findSystemPlane(planes []registry.Plane) (router.Plane, bool) {
	for _, plane := range planes {
		if plane == nil {
			continue
		}
		if strings.EqualFold(plane.Name(), "system") {
			if typed, ok := plane.(router.Plane); ok {
				return typed, true
			}
			return nil, false
		}
	}
	return nil, false
}

func loadRegisterDumpSource(source string) ([]byte, error) {
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		resp, err := http.Get(source)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("register dump http status %s", resp.Status)
		}
		return io.ReadAll(resp.Body)
	}
	return os.ReadFile(source)
}

func parseRegisterDumpTSP(content []byte) ([]registerDumpEntry, byte, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	scanner.Buffer(make([]byte, 0, 1024), 1024*1024)

	var entries []registerDumpEntry
	var base *baseContext
	var target byte
	pendingIndex := -1
	seen := make(map[uint32]struct{})

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "@zz(") {
			if args, ok := parseDirectiveArgs(line); ok && len(args) >= 1 {
				if value, ok := parseByteToken(args[0]); ok {
					target = value
				}
			}
			continue
		}

		if strings.HasPrefix(line, "@base(") {
			args, ok := parseDirectiveArgs(line)
			if !ok || len(args) < 6 {
				base = nil
				continue
			}
			baseSecondary, okSecondary := parseUintToken(args[1])
			basePrefix, okPrefix := parseUintToken(args[2])
			if !okSecondary || !okPrefix {
				base = nil
				continue
			}

			ctx := baseContext{
				Secondary: byte(baseSecondary),
				Prefix:    byte(basePrefix),
			}
			if len(args) >= 6 {
				group, okGroup := parseByteToken(args[3])
				instance, okInstance := parseByteToken(args[4])
				addrLo, okAddr := parseByteToken(args[5])
				if okGroup && okInstance && okAddr {
					ctx.Group = group
					ctx.Instance = instance
					ctx.AddrLo = addrLo
					ctx.HasAddrLo = true
				}
			}
			base = &ctx
			continue
		}

		if strings.HasPrefix(line, "@ext(") {
			if base == nil {
				continue
			}
			args, ok := parseDirectiveArgs(line)
			if !ok {
				continue
			}

			entry, ok := buildRegisterEntry(args, base)
			if !ok {
				continue
			}
			entry.Line = lineNumber
			key := uint32(entry.Group)<<24 | uint32(entry.Instance)<<16 | uint32(entry.Addr)
			if _, exists := seen[key]; exists {
				pendingIndex = -1
				continue
			}
			seen[key] = struct{}{}
			entries = append(entries, entry)
			pendingIndex = len(entries) - 1
			continue
		}

		if strings.HasPrefix(line, "model ") && pendingIndex >= 0 {
			name := parseModelName(line)
			if name != "" {
				entries[pendingIndex].Model = name
			}
			pendingIndex = -1
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	return entries, target, nil
}

func buildRegisterEntry(args []string, base *baseContext) (registerDumpEntry, bool) {
	method := ""
	switch base.Secondary {
	case 0x24:
		if base.Prefix == 0x2 {
			method = "get_ext_register"
		}
	case 0x09:
		method = "get_register"
	}
	if method == "" {
		return registerDumpEntry{}, false
	}

	switch len(args) {
	case 1:
		hi, ok := parseByteToken(args[0])
		if !ok {
			return registerDumpEntry{}, false
		}
		if !base.HasAddrLo {
			return registerDumpEntry{}, false
		}
		addr := uint16(hi)<<8 | uint16(base.AddrLo)
		return registerDumpEntry{Method: method, Group: base.Group, Instance: base.Instance, Addr: addr}, true
	case 2:
		hi, ok := parseByteToken(args[0])
		if !ok {
			return registerDumpEntry{}, false
		}
		lo, ok := parseByteToken(args[1])
		if !ok {
			return registerDumpEntry{}, false
		}
		if lo == 0 {
			if !base.HasAddrLo {
				return registerDumpEntry{}, false
			}
			lo = base.AddrLo
		}
		addr := uint16(hi)<<8 | uint16(lo)
		return registerDumpEntry{Method: method, Group: base.Group, Instance: base.Instance, Addr: addr}, true
	case 4:
		group, okGroup := parseByteToken(args[0])
		instance, okInstance := parseByteToken(args[1])
		hi, okHi := parseByteToken(args[2])
		lo, okLo := parseByteToken(args[3])
		if !okGroup || !okInstance || !okHi || !okLo {
			return registerDumpEntry{}, false
		}
		addr := uint16(hi)<<8 | uint16(lo)
		return registerDumpEntry{Method: method, Group: group, Instance: instance, Addr: addr}, true
	default:
		return registerDumpEntry{}, false
	}
}

func parseDirectiveArgs(line string) ([]string, bool) {
	start := strings.Index(line, "(")
	end := strings.LastIndex(line, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, false
	}
	raw := line[start+1 : end]
	parts := strings.Split(raw, ",")
	args := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			args = append(args, value)
		}
	}
	return args, len(args) > 0
}

func parseByteToken(token string) (byte, bool) {
	value, ok := parseUintToken(token)
	if !ok || value > 0xFF {
		return 0, false
	}
	return byte(value), true
}

func parseUintToken(token string) (uint64, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, false
	}
	if strings.HasPrefix(token, "0x") || strings.HasPrefix(token, "0X") {
		value, err := strconv.ParseUint(token, 0, 64)
		if err != nil {
			return 0, false
		}
		return value, true
	}
	if token[0] >= '0' && token[0] <= '9' {
		value, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			return 0, false
		}
		return value, true
	}
	return 0, false
}

func parseModelName(line string) string {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return ""
	}
	if fields[0] != "model" {
		return ""
	}
	name := strings.TrimSuffix(fields[1], "{")
	return strings.TrimSpace(name)
}

func extractDumpPayload(result any) string {
	values, ok := result.(map[string]types.Value)
	if !ok {
		return ""
	}
	value, ok := values["payload"]
	if !ok || !value.Valid {
		return ""
	}
	if payload, ok := value.Value.([]byte); ok {
		return fmt.Sprintf("%x", payload)
	}
	return ""
}

func writeDumpLine(writer io.Writer, format string, args ...any) {
	if writer == nil {
		return
	}
	ts := time.Now().Format(time.RFC3339Nano)
	fmt.Fprintf(writer, "%s %s\n", ts, fmt.Sprintf(format, args...))
}
