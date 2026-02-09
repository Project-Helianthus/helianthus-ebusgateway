package ebusdscan

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	methodGetRegister    = "get_register"
	methodGetExtRegister = "get_ext_register"
)

type Entry struct {
	Method   string
	Opcode   byte
	Group    byte
	Instance byte
	Addr     uint16
	Line     int
}

type Options struct {
	IdentifyB50928xx bool
	IdentifyPrefix   byte
	Limit            int
}

type Result struct {
	Target  byte
	Entries []Entry
}

type baseContext struct {
	Secondary byte
	Prefix    byte
	Group     byte
	Instance  byte
	AddrLo    byte
	HasAddrLo bool
}

func LoadTSP(source string, opts Options) (Result, error) {
	if strings.TrimSpace(source) == "" {
		return Result{}, fmt.Errorf("tsp source missing")
	}

	content, err := loadSourceWithIncludes(source)
	if err != nil {
		return Result{}, err
	}

	entries, target, err := parseTSP(content)
	if err != nil {
		return Result{}, err
	}

	if opts.IdentifyB50928xx {
		prefix := opts.IdentifyPrefix
		if prefix == 0 {
			prefix = 0x28
		}
		entries = appendIdentifyRegisters(entries, prefix)
	}
	if opts.Limit > 0 && len(entries) > opts.Limit {
		entries = entries[:opts.Limit]
	}

	return Result{Target: target, Entries: entries}, nil
}

func loadSource(source string) ([]byte, error) {
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		resp, err := http.Get(source)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("tsp http status %s", resp.Status)
		}
		return io.ReadAll(resp.Body)
	}
	return os.ReadFile(source)
}

func loadSourceWithIncludes(source string) ([]byte, error) {
	content, err := loadSource(source)
	if err != nil {
		return nil, err
	}
	visited := map[string]bool{source: true}
	return expandIncludes(content, source, visited)
}

func expandIncludes(content []byte, base string, visited map[string]bool) ([]byte, error) {
	lines := strings.Split(string(content), "\n")
	var out strings.Builder
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "@include(") {
			args, ok := parseDirectiveArgs(trimmed)
			if ok && len(args) == 1 {
				includeTarget := trimQuotes(args[0])
				if includeTarget != "" {
					if resolved, ok := resolveRelativeSource(base, includeTarget); ok {
						if !visited[resolved] {
							visited[resolved] = true
							data, err := loadSource(resolved)
							if err != nil {
								return nil, err
							}
							expanded, err := expandIncludes(data, resolved, visited)
							if err != nil {
								return nil, err
							}
							out.Write(expanded)
							if len(expanded) > 0 && expanded[len(expanded)-1] != '\n' {
								out.WriteString("\n")
							}
							continue
						}
					}
				}
			}
		}
		out.WriteString(line)
		out.WriteString("\n")
	}
	return []byte(out.String()), nil
}

func resolveRelativeSource(base, rel string) (string, bool) {
	if strings.HasPrefix(rel, "http://") || strings.HasPrefix(rel, "https://") {
		return rel, true
	}
	if filepath.IsAbs(rel) {
		return rel, true
	}
	if strings.HasPrefix(base, "http://") || strings.HasPrefix(base, "https://") {
		baseURL, err := url.Parse(base)
		if err != nil {
			return "", false
		}
		baseURL.Path = path.Clean(path.Join(path.Dir(baseURL.Path), rel))
		return baseURL.String(), true
	}
	if base == "" {
		return "", false
	}
	return filepath.Clean(filepath.Join(filepath.Dir(base), rel)), true
}

func parseTSP(content []byte) ([]Entry, byte, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	scanner.Buffer(make([]byte, 0, 1024), 1024*1024)

	var entries []Entry
	var base *baseContext
	var target byte
	seen := make(map[string]struct{})

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
			if !ok || len(args) < 3 {
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
			entry, ok := buildEntry(args, base)
			if !ok {
				continue
			}
			entry.Line = lineNumber
			key := fmt.Sprintf("%02X:%02X:%02X:%04X", entry.Opcode, entry.Group, entry.Instance, entry.Addr)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			entries = append(entries, entry)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	return entries, target, nil
}

func buildEntry(args []string, base *baseContext) (Entry, bool) {
	method := ""
	switch base.Secondary {
	case 0x24:
		if base.Prefix == 0x2 || base.Prefix == 0x6 {
			method = methodGetExtRegister
		}
	case 0x09:
		method = methodGetRegister
	}
	if method == "" {
		return Entry{}, false
	}

	switch len(args) {
	case 1:
		hi, ok := parseByteToken(args[0])
		if !ok {
			return Entry{}, false
		}
		if !base.HasAddrLo {
			return Entry{}, false
		}
		addr := uint16(hi)<<8 | uint16(base.AddrLo)
		return Entry{Method: method, Opcode: base.Prefix, Group: base.Group, Instance: base.Instance, Addr: addr}, true
	case 2:
		hi, ok := parseByteToken(args[0])
		if !ok {
			return Entry{}, false
		}
		lo, ok := parseByteToken(args[1])
		if !ok {
			return Entry{}, false
		}
		if lo == 0 {
			if !base.HasAddrLo {
				return Entry{}, false
			}
			lo = base.AddrLo
		}
		addr := uint16(hi)<<8 | uint16(lo)
		return Entry{Method: method, Opcode: base.Prefix, Group: base.Group, Instance: base.Instance, Addr: addr}, true
	case 4:
		group, okGroup := parseByteToken(args[0])
		instance, okInstance := parseByteToken(args[1])
		hi, okHi := parseByteToken(args[2])
		lo, okLo := parseByteToken(args[3])
		if !okGroup || !okInstance || !okHi || !okLo {
			return Entry{}, false
		}
		addr := uint16(hi)<<8 | uint16(lo)
		return Entry{Method: method, Opcode: base.Prefix, Group: group, Instance: instance, Addr: addr}, true
	default:
		return Entry{}, false
	}
}

func appendIdentifyRegisters(entries []Entry, prefix byte) []Entry {
	seen := make(map[uint32]struct{}, len(entries))
	for _, entry := range entries {
		extFlag := uint32(0)
		if entry.Method == methodGetExtRegister {
			extFlag = 1
		}
		key := extFlag<<24 | uint32(entry.Addr)
		seen[key] = struct{}{}
	}

	start := uint16(prefix) << 8
	end := start | 0xFF
	for addr := start; addr <= end; addr++ {
		key := uint32(0)<<24 | uint32(addr)
		if _, ok := seen[key]; ok {
			continue
		}
		entries = append(entries, Entry{
			Method: methodGetRegister,
			Addr:   addr,
			Line:   0,
		})
	}
	return entries
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

func trimQuotes(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			return value[1 : len(value)-1]
		}
	}
	return value
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
