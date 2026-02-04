package ebusgateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
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

type registerField struct {
	Name   string
	Type   string
	Base   string
	Scale  float64
	Size   int
	Offset int
}

type modelInfo struct {
	Name   string
	Fields []registerField
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

	content, err := loadRegisterDumpSourceWithIncludes(cfg.Smoke.RegisterDumpTSP)
	if err != nil {
		return err
	}

	templateSources := loadRegisterDumpTemplates(cfg.Smoke.RegisterDumpTSP)
	requests, targetFromTSP, models, err := parseRegisterDumpTSP(content, templateSources)
	if err != nil {
		return err
	}
	if len(requests) == 0 {
		return fmt.Errorf("register dump: no registers found")
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

	if cfg.Smoke.IdentifyB50928xx {
		requests = appendIdentifyRegisters(requests, 0x28)
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
		fields := decodeRegisterFields(req, payload, models)
		writeDumpLine(writer, "target=0x%02x group=0x%02x instance=0x%02x addr=0x%04x method=%s model=%s line=%d payload=%s fields=%s",
			target, req.Group, req.Instance, req.Addr, req.Method, req.Model, req.Line, payload, fields)
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

func loadRegisterDumpSourceWithIncludes(source string) ([]byte, error) {
	content, err := loadRegisterDumpSource(source)
	if err != nil {
		return nil, err
	}
	visited := map[string]bool{source: true}
	return expandRegisterDumpIncludes(content, source, visited)
}

func expandRegisterDumpIncludes(content []byte, base string, visited map[string]bool) ([]byte, error) {
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
							data, err := loadRegisterDumpSource(resolved)
							if err != nil {
								return nil, err
							}
							expanded, err := expandRegisterDumpIncludes(data, resolved, visited)
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

type templateSource struct {
	Path string
	Data []byte
}

func loadRegisterDumpTemplates(source string) []templateSource {
	var templates []templateSource
	if strings.TrimSpace(source) == "" {
		return templates
	}

	paths := []string{"./_templates.tsp", "../_templates.tsp"}
	for _, rel := range paths {
		if resolved, ok := resolveRelativeSource(source, rel); ok {
			templates = append(templates, templateSource{Path: resolved})
		}
	}
	return templates
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

func parseRegisterDumpTSP(content []byte, templates []templateSource) ([]registerDumpEntry, byte, map[string]modelInfo, error) {
	aliasMap := parseScalarAliases(content, templates)
	models := parseModelDefinitions(content, aliasMap)

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
		return nil, 0, nil, err
	}
	return entries, target, models, nil
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

func parseFloatToken(token string) (float64, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return 0, false
	}
	return value, true
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

func decodeRegisterFields(req registerDumpEntry, payloadHex string, models map[string]modelInfo) string {
	if payloadHex == "" {
		return ""
	}
	payload, err := hexToBytes(payloadHex)
	if err != nil {
		return ""
	}
	model, ok := models[req.Model]
	if !ok || len(model.Fields) == 0 {
		return ""
	}

	data := payload
	if req.Method == "get_ext_register" && len(payload) >= 4 {
		data = payload[4:]
	}

	values := make([]string, 0, len(model.Fields))
	for _, field := range model.Fields {
		if field.Size <= 0 || field.Offset+field.Size > len(data) {
			continue
		}
		chunk := data[field.Offset : field.Offset+field.Size]
		value, ok := decodeFieldValue(field, chunk)
		if !ok {
			continue
		}
		values = append(values, fmt.Sprintf("%s=%s", field.Name, value))
	}
	return strings.Join(values, ",")
}

func decodeFieldValue(field registerField, chunk []byte) (string, bool) {
	value, ok := decodeBaseValue(field.Base, chunk)
	if !ok {
		return "", false
	}

	switch typed := value.(type) {
	case float64:
		val := typed
		if field.Scale != 0 && field.Scale != 1 {
			val *= field.Scale
		}
		return fmt.Sprintf("%.4g", val), true
	case float32:
		val := float64(typed)
		if field.Scale != 0 && field.Scale != 1 {
			val *= field.Scale
		}
		return fmt.Sprintf("%.4g", val), true
	case int, int8, int16, int32, int64:
		val := toFloat(typed)
		if field.Scale != 0 && field.Scale != 1 {
			val *= field.Scale
			return fmt.Sprintf("%.4g", val), true
		}
		return fmt.Sprintf("%.0f", val), true
	case uint, uint8, uint16, uint32, uint64:
		val := float64(toUint(typed))
		if field.Scale != 0 && field.Scale != 1 {
			val *= field.Scale
			return fmt.Sprintf("%.4g", val), true
		}
		return fmt.Sprintf("%.0f", val), true
	case string:
		return typed, true
	default:
		return fmt.Sprintf("%v", typed), true
	}
}

func decodeBaseValue(base string, payload []byte) (any, bool) {
	switch base {
	case "EXP":
		value, err := types.EXP{}.Decode(payload)
		if err != nil || !value.Valid {
			return nil, false
		}
		return float64(value.Value.(float32)), true
	case "D2C":
		value, err := types.DATA2c{}.Decode(payload)
		if err != nil || !value.Valid {
			return nil, false
		}
		return value.Value.(float64), true
	case "D2B":
		value, err := types.DATA2b{}.Decode(payload)
		if err != nil || !value.Valid {
			return nil, false
		}
		return value.Value.(float64), true
	case "D1B":
		value, err := types.DATA1b{}.Decode(payload)
		if err != nil || !value.Valid {
			return nil, false
		}
		return int(value.Value.(int8)), true
	case "UIN":
		value, err := types.WORD{}.Decode(payload)
		if err != nil || !value.Valid {
			return nil, false
		}
		return uint16(value.Value.(uint16)), true
	case "BCD":
		value, err := types.BCD{}.Decode(payload)
		if err != nil || !value.Valid {
			return nil, false
		}
		return int(value.Value.(uint8)), true
	case "UCH":
		if len(payload) < 1 {
			return nil, false
		}
		if payload[0] == 0xFF {
			return nil, false
		}
		return payload[0], true
	case "SCH":
		if len(payload) < 1 {
			return nil, false
		}
		v := int8(payload[0])
		if v == -128 {
			return nil, false
		}
		return int(v), true
	case "D1C":
		if len(payload) < 1 {
			return nil, false
		}
		v := int8(payload[0])
		if v == -128 {
			return nil, false
		}
		return float64(v) / 2.0, true
	case "SIN":
		if len(payload) < 2 {
			return nil, false
		}
		raw := int16(binary.LittleEndian.Uint16(payload))
		if raw == -32768 {
			return nil, false
		}
		return int(raw), true
	case "ULG":
		if len(payload) < 4 {
			return nil, false
		}
		raw := binary.LittleEndian.Uint32(payload)
		if raw == 0xFFFFFFFF {
			return nil, false
		}
		return raw, true
	case "FLT":
		if len(payload) < 4 {
			return nil, false
		}
		raw := binary.LittleEndian.Uint32(payload)
		value := math.Float32frombits(raw)
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, false
		}
		return float64(value), true
	case "BCD3":
		if len(payload) < 3 {
			return nil, false
		}
		return decodeBCD(payload[:3])
	case "BCD4":
		if len(payload) < 4 {
			return nil, false
		}
		return decodeBCD(payload[:4])
	}
	return nil, false
}

func decodeBCD(payload []byte) (int, bool) {
	total := 0
	multiplier := 1
	for i := 0; i < len(payload); i++ {
		raw := payload[i]
		tens := raw >> 4
		ones := raw & 0x0F
		if tens > 9 || ones > 9 {
			return 0, false
		}
		total += int(ones) * multiplier
		multiplier *= 10
		total += int(tens) * multiplier
		multiplier *= 10
	}
	return total, true
}

func toFloat(value any) float64 {
	switch typed := value.(type) {
	case int:
		return float64(typed)
	case int8:
		return float64(typed)
	case int16:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func toUint(value any) uint64 {
	switch typed := value.(type) {
	case uint:
		return uint64(typed)
	case uint8:
		return uint64(typed)
	case uint16:
		return uint64(typed)
	case uint32:
		return uint64(typed)
	case uint64:
		return typed
	default:
		return 0
	}
}

func hexToBytes(value string) ([]byte, error) {
	if len(value)%2 != 0 {
		return nil, fmt.Errorf("hex odd length")
	}
	data := make([]byte, len(value)/2)
	for i := 0; i < len(data); i++ {
		parsed, err := strconv.ParseUint(value[i*2:i*2+2], 16, 8)
		if err != nil {
			return nil, err
		}
		data[i] = byte(parsed)
	}
	return data, nil
}

type aliasInfo struct {
	Base  string
	Scale float64
}

func parseScalarAliases(content []byte, templates []templateSource) map[string]aliasInfo {
	aliases := make(map[string]aliasInfo)
	parseScalarAliasesText(content, aliases)
	for _, item := range templates {
		data := item.Data
		if len(data) == 0 && item.Path != "" {
			loaded, err := loadRegisterDumpSource(item.Path)
			if err != nil {
				continue
			}
			data = loaded
		}
		parseScalarAliasesText(data, aliases)
	}
	return aliases
}

func parseScalarAliasesText(content []byte, aliases map[string]aliasInfo) {
	lines := strings.Split(string(content), "\n")
	factor := 1.0
	divisor := 1.0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "//") {
			continue
		}
		if strings.HasPrefix(trimmed, "@factor(") {
			if args, ok := parseDirectiveArgs(trimmed); ok && len(args) == 1 {
				if value, ok := parseFloatToken(args[0]); ok {
					factor = value
				}
			}
			continue
		}
		if strings.HasPrefix(trimmed, "@divisor(") {
			if args, ok := parseDirectiveArgs(trimmed); ok && len(args) == 1 {
				if value, ok := parseFloatToken(args[0]); ok {
					divisor = value
				}
			}
			continue
		}
		if strings.HasPrefix(trimmed, "scalar ") {
			matches := regexp.MustCompile(`scalar\\s+(\\w+)\\s+extends\\s+(\\w+)`).FindStringSubmatch(trimmed)
			if len(matches) == 3 {
				scale := 1.0
				if divisor != 0 {
					scale = factor / divisor
				}
				aliases[matches[1]] = aliasInfo{Base: matches[2], Scale: scale}
			}
			factor = 1.0
			divisor = 1.0
		}
	}
}

func parseModelDefinitions(content []byte, aliases map[string]aliasInfo) map[string]modelInfo {
	models := make(map[string]modelInfo)
	modelDefs := make(map[string]modelDefinition)
	var pendingInherits []string
	var pendingMaxLength int
	var current string
	brace := 0

	lines := strings.Split(string(content), "\n")
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if strings.HasPrefix(line, "@inherit(") {
			args, ok := parseDirectiveArgs(line)
			if ok {
				pendingInherits = args
			}
			continue
		}
		if strings.HasPrefix(line, "@maxLength(") {
			args, ok := parseDirectiveArgs(line)
			if ok && len(args) == 1 {
				if value, ok := parseUintToken(args[0]); ok {
					pendingMaxLength = int(value)
				}
			}
			continue
		}
		if strings.HasPrefix(line, "model ") {
			name := parseModelName(line)
			if name != "" {
				current = name
				modelDefs[name] = modelDefinition{
					Name:     name,
					Inherits: pendingInherits,
				}
				pendingInherits = nil
				brace = strings.Count(line, "{") - strings.Count(line, "}")
				continue
			}
		}
		if current != "" {
			brace += strings.Count(line, "{") - strings.Count(line, "}")
			if isFieldLine(line) {
				fieldName, fieldType := parseFieldLine(line)
				if fieldName != "" && fieldType != "" {
					def := modelDefs[current]
					def.Fields = append(def.Fields, fieldDefinition{
						Name:      fieldName,
						Type:      fieldType,
						MaxLength: pendingMaxLength,
					})
					modelDefs[current] = def
					pendingMaxLength = 0
				}
			}
			if brace <= 0 {
				current = ""
				brace = 0
				pendingMaxLength = 0
			}
		}
	}

	for name := range modelDefs {
		fields := resolveModelFields(name, modelDefs, aliases, map[string]bool{})
		models[name] = modelInfo{Name: name, Fields: fields}
	}

	return models
}

type modelDefinition struct {
	Name     string
	Inherits []string
	Fields   []fieldDefinition
}

type fieldDefinition struct {
	Name      string
	Type      string
	MaxLength int
}

func resolveModelFields(name string, defs map[string]modelDefinition, aliases map[string]aliasInfo, visiting map[string]bool) []registerField {
	if visiting[name] {
		return nil
	}
	def, ok := defs[name]
	if !ok {
		return nil
	}
	visiting[name] = true
	var fields []registerField
	for _, base := range def.Inherits {
		fields = append(fields, resolveModelFields(base, defs, aliases, visiting)...)
	}
	offset := 0
	for _, field := range def.Fields {
		base, scale := resolveAlias(field.Type, aliases)
		size := fieldSize(base, field.MaxLength)
		if size <= 0 {
			break
		}
		fields = append(fields, registerField{
			Name:   field.Name,
			Type:   field.Type,
			Base:   base,
			Scale:  scale,
			Size:   size,
			Offset: offset,
		})
		offset += size
	}
	visiting[name] = false
	return fields
}

func resolveAlias(name string, aliases map[string]aliasInfo) (string, float64) {
	scale := 1.0
	current := name
	visited := make(map[string]bool)
	for {
		info, ok := aliases[current]
		if !ok {
			return current, scale
		}
		if visited[current] {
			return current, scale
		}
		visited[current] = true
		scale *= info.Scale
		current = info.Base
	}
}

func fieldSize(base string, maxLength int) int {
	switch base {
	case "UCH", "SCH", "D1B", "D1C", "BCD", "BDY":
		return 1
	case "UIN", "SIN", "D2B", "D2C", "WORD":
		return 2
	case "BCD3":
		return 3
	case "BCD4":
		return 4
	case "ULG", "SLG", "EXP", "FLT":
		return 4
	case "BTI", "VTI", "VTM":
		return 3
	case "BDA":
		return 4
	case "BDA3", "HDA3":
		return 3
	case "IGN":
		return maxLength
	default:
		return 0
	}
}

func isFieldLine(line string) bool {
	if strings.HasPrefix(line, "@") {
		return false
	}
	if !strings.HasSuffix(line, ";") {
		return false
	}
	return strings.Contains(line, ":")
}

func parseFieldLine(line string) (string, string) {
	parts := strings.Split(line, ":")
	if len(parts) != 2 {
		return "", ""
	}
	name := strings.TrimSpace(parts[0])
	typeValue := strings.TrimSpace(strings.TrimSuffix(parts[1], ";"))
	if name == "" || typeValue == "" {
		return "", ""
	}
	if idx := strings.IndexAny(typeValue, "< "); idx != -1 {
		typeValue = typeValue[:idx]
	}
	return name, typeValue
}

func appendIdentifyRegisters(entries []registerDumpEntry, prefix byte) []registerDumpEntry {
	seen := make(map[uint32]struct{}, len(entries))
	for _, entry := range entries {
		extFlag := uint32(0)
		if entry.Method == "get_ext_register" {
			extFlag = 1
		}
		key := extFlag<<24 | uint32(entry.Addr)
		seen[key] = struct{}{}
	}

	for addr := uint16(prefix)<<8 | 0x00; addr <= uint16(prefix)<<8|0xFF; addr++ {
		key := uint32(0)<<24 | uint32(addr)
		if _, ok := seen[key]; ok {
			continue
		}
		entries = append(entries, registerDumpEntry{
			Method: "get_register",
			Addr:   addr,
			Model:  "identify_b509_28xx",
			Line:   0,
		})
	}
	return entries
}

func writeDumpLine(writer io.Writer, format string, args ...any) {
	if writer == nil {
		return
	}
	ts := time.Now().Format(time.RFC3339Nano)
	fmt.Fprintf(writer, "%s %s\n", ts, fmt.Sprintf(format, args...))
}
