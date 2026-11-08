package growattcanclassify

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const DefaultMaxBytes = 4 << 20
const DefaultMaxLine = 4096
const MaxFrames = 16384

type Options struct {
	MaxBytes      int
	MaxLine       int
	MONInterfaces []string
}

type Candidate struct {
	Family string `json:"family"`
	Grade  string `json:"grade"`
	Reason string `json:"reason"`
}

type Frame struct {
	Line              int         `json:"line"`
	Raw               string      `json:"raw"`
	RawHex            string      `json:"raw_hex"`
	Interface         string      `json:"interface"`
	Timestamp         string      `json:"timestamp"`
	Identifier        string      `json:"identifier"`
	IDE               bool        `json:"ide"`
	Flags             []string    `json:"flags"`
	DLC               int         `json:"dlc"`
	RawDLC            string      `json:"raw_dlc"`
	Payload           string      `json:"payload"`
	Route             string      `json:"route"`
	Endpoint          *int        `json:"endpoint"`
	Group             *int        `json:"group"`
	Slot              *int        `json:"slot"`
	Epoch             *uint64     `json:"epoch"`
	Sequence          *uint64     `json:"sequence"`
	MonotonicTime     *uint64     `json:"monotonic_time"`
	FieldAge          *uint64     `json:"field_age"`
	ProjectCode       string      `json:"project_code,omitempty"`
	ReleaseBytes      string      `json:"release_bytes,omitempty"`
	IdentityGrade     string      `json:"identity_grade"`
	Candidates        []Candidate `json:"candidates"`
	Diagnostics       []string    `json:"diagnostics"`
	PhysicalQualified bool        `json:"physical_qualified"`
	RuntimeAdmission  bool        `json:"runtime_admission"`
}

type InterfaceResult struct {
	Interface string   `json:"interface"`
	Frames    int      `json:"frames"`
	Unknown   int      `json:"unknown"`
	Families  []string `json:"families"`
	Outcome   string   `json:"outcome"`
}

type Result struct {
	Outcome           string            `json:"outcome"`
	Frames            []Frame           `json:"frames"`
	Interfaces        []InterfaceResult `json:"interfaces"`
	PhysicalQualified bool              `json:"physical_qualified"`
	RuntimeAdmission  bool              `json:"runtime_admission"`
}

// Classify consumes local text only. Resource failures return the accepted prefix and an error.
func Classify(r io.Reader, opts Options) (Result, error) {
	result := Result{Outcome: "unknown", Frames: []Frame{}, Interfaces: []InterfaceResult{}}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = DefaultMaxBytes
	}
	if opts.MaxLine == 0 {
		opts.MaxLine = DefaultMaxLine
	}
	if opts.MaxBytes < 1 || opts.MaxBytes > DefaultMaxBytes || opts.MaxLine < 1 || opts.MaxLine > DefaultMaxLine {
		return result, fmt.Errorf("limits must be positive and at most defaults")
	}
	associated := map[string]bool{}
	for _, name := range opts.MONInterfaces {
		associated[name] = true
	}
	reader := bufio.NewReaderSize(io.LimitReader(r, int64(opts.MaxBytes)+1), opts.MaxLine+2)
	total := 0
	var readErr error
	for line := 1; ; line++ {
		raw, err := reader.ReadSlice('\n')
		total += len(raw)
		if total > opts.MaxBytes {
			readErr = fmt.Errorf("input exceeds %d bytes", opts.MaxBytes)
			break
		}
		content := strings.TrimSuffix(string(raw), "\n")
		content = strings.TrimSuffix(content, "\r")
		if len(content) > opts.MaxLine || err == bufio.ErrBufferFull {
			readErr = fmt.Errorf("line %d exceeds %d bytes", line, opts.MaxLine)
			break
		}
		if len(raw) > 0 {
			if len(result.Frames) >= MaxFrames {
				readErr = fmt.Errorf("input exceeds %d records", MaxFrames)
				break
			}
			result.Frames = append(result.Frames, parse(line, content))
		}
		if err != nil {
			if err != io.EOF {
				readErr = err
			}
			break
		}
	}
	tuples := map[string]map[string]bool{}
	for i := range result.Frames {
		f := &result.Frames[i]
		if len(f.Diagnostics) != 0 {
			continue
		}
		if !f.IDE && f.DLC == 8 && (f.Identifier == "311" || f.Identifier == "312" || f.Identifier == "313") {
			if tuples[f.Interface] == nil {
				tuples[f.Interface] = map[string]bool{}
			}
			tuples[f.Interface][f.Identifier] = true
		}
	}
	for i := range result.Frames {
		f := &result.Frames[i]
		if len(f.Diagnostics) != 0 {
			continue
		}
		classifyFrame(f, associated[f.Interface], len(tuples[f.Interface]) == 3)
		if len(f.Candidates) == 0 {
			f.Diagnostics = append(f.Diagnostics, "unknown: no admitted candidate geometry")
		}
	}
	summarize(&result)
	return result, readErr
}

func parse(line int, raw string) Frame {
	f := Frame{Line: line, Raw: raw, RawHex: hex.EncodeToString([]byte(raw)), Flags: []string{}, Candidates: []Candidate{}, Diagnostics: []string{}, IdentityGrade: "Unknown"}
	reject := func(reason string) Frame { f.Diagnostics = append(f.Diagnostics, reason); return f }
	if !utf8.ValidString(raw) {
		return reject("invalid UTF-8 text; exact bytes retained in raw_hex")
	}
	for _, c := range raw {
		if unicode.IsControl(c) && c != '\t' {
			return reject("invalid control character")
		}
	}
	fields := strings.Fields(raw)
	if trimmed := strings.TrimLeft(raw, " \t"); strings.HasPrefix(trimmed, "(") {
		end := strings.IndexByte(trimmed, ')')
		if end < 0 || !validTimestamp(trimmed[1:end]) {
			f.Timestamp = strings.Fields(trimmed)[0]
			return reject("invalid timestamp")
		}
		f.Timestamp = trimmed[:end+1]
		fields = strings.Fields(trimmed[end+1:])
	}
	if len(fields) < 2 {
		return reject("unrecognized line")
	}
	f.Interface = fields[0]
	entry := fields[1]
	var id string
	if index := strings.IndexByte(entry, '#'); index >= 0 {
		id = entry[:index]
		setIdentifier(&f, id)
		f.Payload = entry[index+1:]
		if len(fields) != 2 {
			f.Diagnostics = append(f.Diagnostics, "unexpected compact fields")
		}
		rtrLength := -1
		if !strings.HasPrefix(f.Payload, "#") {
			// Compact classical records may end with _9.._F: the raw DLC of an eight-byte frame.
			if n := len(f.Payload); n >= 2 && f.Payload[n-2] == '_' && strings.ContainsRune("9ABCDEFabcdef", rune(f.Payload[n-1])) {
				f.RawDLC = f.Payload[n-1:]
				f.Payload = f.Payload[:n-2]
			}
		}
		if strings.HasPrefix(f.Payload, "#") {
			f.Flags = append(f.Flags, "FD")
			f.Diagnostics = append(f.Diagnostics, "FD is not telemetry")
			// The character after the FD marker is the flags nibble, not payload.
			if len(f.Payload) < 2 || !strings.ContainsRune("0123456789abcdefABCDEF", rune(f.Payload[1])) {
				f.Diagnostics = append(f.Diagnostics, "invalid FD flags nibble")
				f.Payload = strings.TrimPrefix(f.Payload, "#")
			} else {
				f.Payload = f.Payload[2:]
			}
		} else if strings.HasPrefix(f.Payload, "R") || strings.HasPrefix(f.Payload, "r") {
			f.Flags = append(f.Flags, "RTR")
			f.Diagnostics = append(f.Diagnostics, "RTR is not telemetry")
			// A remote request carries no data; an optional trailing digit is its length.
			rest := f.Payload[1:]
			f.Payload = ""
			rtrLength = 0
			if len(rest) == 1 && rest[0] >= '0' && rest[0] <= '8' {
				rtrLength = int(rest[0] - '0')
			} else if rest != "" {
				f.Diagnostics = append(f.Diagnostics, "invalid RTR length")
			}
		}
		if rtrLength >= 0 {
			f.DLC = rtrLength
		} else {
			f.DLC = len(f.Payload) / 2
		}
	} else {
		id = entry
		setIdentifier(&f, id)
		if len(fields) < 3 {
			return reject("missing DLC")
		}
		f.RawDLC = fields[2]
		f.Payload = strings.Join(fields[3:], " ")
		token := fields[2]
		if len(token) == 3 && token[0] == '{' && token[2] == '}' && strings.ContainsRune("9ABCDEFabcdef", rune(token[1])) {
			// Long-form raw DLC of an eight-byte classical frame (candump -8).
			f.RawDLC = token[1:2]
			f.DLC = 8
			f.Payload = strings.Join(fields[3:], " ")
			for _, b := range fields[3:] {
				if len(b) != 2 {
					f.Diagnostics = append(f.Diagnostics, "invalid byte width")
					break
				}
			}
			data, err := hex.DecodeString(strings.ReplaceAll(f.Payload, " ", ""))
			if err != nil {
				f.Diagnostics = append(f.Diagnostics, "invalid payload hex or length")
			} else if len(data) != 8 {
				f.Diagnostics = append(f.Diagnostics, "payload length does not match classical DLC")
			}
			return f
		}
		if len(token) < 3 || token[0] != '[' || token[len(token)-1] != ']' {
			return reject("invalid DLC syntax")
		}
		digits := token[1 : len(token)-1]
		n, err := strconv.Atoi(digits)
		if err != nil || n < 0 || n > 64 {
			return reject("invalid DLC")
		}
		if n > 8 && len(digits) == 1 {
			return reject("invalid classical DLC")
		}
		if len(digits) != 1 {
			// Classical frames print a one-digit length; a two-digit length is the FD form.
			f.Flags = append(f.Flags, "FD")
			f.Diagnostics = append(f.Diagnostics, "FD is not telemetry")
		}
		f.DLC = n
		f.Payload = strings.Join(fields[3:], " ")
		if strings.EqualFold(f.Payload, "remote request") {
			f.Flags = append(f.Flags, "RTR")
			f.Diagnostics = append(f.Diagnostics, "RTR is not telemetry")
		}
		if !hasFlag(f.Flags, "RTR") {
			for _, b := range fields[3:] {
				if len(b) != 2 {
					f.Diagnostics = append(f.Diagnostics, "invalid byte width")
					break
				}
			}
		}
	}
	if hasFlag(f.Flags, "RTR") {
		// A remote request carries no data bytes, so there is no payload to validate.
		return f
	}
	data, err := hex.DecodeString(strings.ReplaceAll(f.Payload, " ", ""))
	if err != nil {
		f.Diagnostics = append(f.Diagnostics, "invalid payload hex or length")
	} else if hasFlag(f.Flags, "FD") {
		if len(data) != f.DLC && !strings.Contains(raw, "#") {
			f.Diagnostics = append(f.Diagnostics, "payload length does not match DLC")
		}
	} else if len(data) != f.DLC || len(data) > 8 {
		f.Diagnostics = append(f.Diagnostics, "payload length does not match classical DLC")
	}
	return f
}

func hasFlag(flags []string, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}
	return false
}

// validTimestamp accepts the decimal form and the date-time form (candump -t A).
func validTimestamp(s string) bool {
	if date, clock, ok := strings.Cut(s, " "); ok {
		if len(date) != 10 || date[4] != '-' || date[7] != '-' {
			return false
		}
		if !decimalTimestamp(strings.ReplaceAll(date, "-", "")) || strings.Contains(date, ".") {
			return false
		}
		sec, _, _ := strings.Cut(clock, ".")
		if len(sec) != 8 || sec[2] != ':' || sec[5] != ':' {
			return false
		}
		if !decimalTimestamp(strings.ReplaceAll(clock, ":", "")) {
			return false
		}
		_, err := time.Parse("2006-01-02 15:04:05", date+" "+sec)
		return err == nil
	}
	return decimalTimestamp(s)
}

func decimalTimestamp(s string) bool {
	digits, dots := 0, 0
	for _, c := range s {
		if c == '.' {
			dots++
		} else if c >= '0' && c <= '9' {
			digits++
		} else {
			return false
		}
	}
	return digits > 0 && dots <= 1
}

func classifyFrame(f *Frame, mon, tuple bool) {
	add := func(family, reason string) {
		f.Candidates = append(f.Candidates, Candidate{family, "Hypothesis", reason})
	}
	id, _ := strconv.ParseUint(f.Identifier, 16, 32)
	if f.DLC != 8 {
		return
	}
	if !f.IDE {
		if id >= 0x311 && id <= 0x313 && tuple {
			add("V1.04", "complete source-interface standard tuple; ambiguous")
			add("PCS-project-code", "complete source-interface standard tuple; ambiguous")
		}
		group := id & 0xf
		prefix := id >> 4
		if group < 8 && (prefix == 0x50 || prefix == 0x51 || prefix == 0x52 || (prefix >= 0x60 && prefix <= 0x68) || prefix == 0x6c || prefix == 0x6d) {
			f.Group = intPointer(int(group))
			reason := "parallel group geometry; member dictionary and release support unknown"
			if prefix == 0x52 {
				reason = "parallel 52X geometry; requires release11 support, not established by family match"
			}
			add("PCS-parallel", reason)
		}
		return
	}
	page := (id >> 16) & 0xff
	if id>>24 == 0x18 && (id>>8)&0xff == 0xf4 && (page < 0x80 || page > 0x8f) {
		add("LINK", "extended 18xxF4SS full route; source-local only")
		f.Endpoint = intPointer(int(id & 0xff))
		if page == 2 {
			data, _ := hex.DecodeString(strings.ReplaceAll(f.Payload, " ", ""))
			ascii := true
			for _, b := range data[:4] {
				if b < 0x20 || b > 0x7e {
					ascii = false
				}
			}
			if ascii {
				f.ProjectCode = string(data[:4])
				f.ReleaseBytes = hex.EncodeToString(data[4:])
				f.IdentityGrade = "Proven"
			} else {
				f.Diagnostics = append(f.Diagnostics, "1802 project code is not four exact-case ASCII bytes; identity withheld")
			}
		}
	}
	if id>>20 == 0x190 && (id>>8)&0xff == 0xf2 {
		add("PCS-extended", "190nF2SS source route retained")
	}
	if (id>>20 == 0x190 || id>>20 == 0x191) && id&0xffff == 0xdff2 {
		add("PCS-extended", "coordinator route retained independently")
	}
	top := id >> 24
	if mon && (top == 0x04 || top == 0x08 || top == 0x10 || top == 0x14) {
		add("MON", "explicit interface association; BCU/BMU dimensions uninterpreted; descriptive compatibility only")
	}
}

func intPointer(n int) *int { return &n }

func summarize(r *Result) {
	counts := map[string]*InterfaceResult{}
	families := map[string]map[string]bool{}
	for _, f := range r.Frames {
		if counts[f.Interface] == nil {
			counts[f.Interface] = &InterfaceResult{Interface: f.Interface, Families: []string{}, Outcome: "unknown"}
			families[f.Interface] = map[string]bool{}
		}
		c := counts[f.Interface]
		c.Frames++
		if len(f.Candidates) == 0 {
			c.Unknown++
		}
		for _, candidate := range f.Candidates {
			families[f.Interface][candidate.Family] = true
		}
	}
	names := make([]string, 0, len(counts))
	all := map[string]bool{}
	unknown := 0
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		c := counts[name]
		for family := range families[name] {
			c.Families = append(c.Families, family)
			all[family] = true
		}
		sort.Strings(c.Families)
		c.Outcome = outcome(len(c.Families), c.Unknown)
		unknown += c.Unknown
		r.Interfaces = append(r.Interfaces, *c)
	}
	r.Outcome = outcome(len(all), unknown)
}

func outcome(families, unknown int) string {
	if families == 0 {
		return "unknown"
	}
	if families > 1 || unknown > 0 {
		return "mixed"
	}
	return "candidate"
}

func WriteText(w io.Writer, result Result) error {
	if _, err := fmt.Fprintf(w, "outcome=%s physical_qualified=false runtime_admission=false\n", result.Outcome); err != nil {
		return err
	}
	for _, f := range result.Frames {
		if _, err := fmt.Fprintf(w, "line=%d raw=%q raw_hex=%q interface=%q timestamp=%q id=%q ide=%t flags=%q dlc=%d raw_dlc=%q payload=%q route=%q endpoint=%v group=%v slot=unknown epoch=unknown sequence=unknown monotonic_time=unknown field_age=unknown project_code=%q release_bytes=%q identity_grade=%s candidates=%v diagnostics=%q\n", f.Line, f.Raw, f.RawHex, f.Interface, f.Timestamp, f.Identifier, f.IDE, f.Flags, f.DLC, f.RawDLC, f.Payload, f.Route, optionalInt(f.Endpoint), optionalInt(f.Group), f.ProjectCode, f.ReleaseBytes, f.IdentityGrade, f.Candidates, f.Diagnostics); err != nil {
			return err
		}
	}
	for _, c := range result.Interfaces {
		if _, err := fmt.Fprintf(w, "interface=%q frames=%d unknown=%d outcome=%s families=%q\n", c.Interface, c.Frames, c.Unknown, c.Outcome, c.Families); err != nil {
			return err
		}
	}
	return nil
}

func optionalInt(value *int) string {
	if value == nil {
		return "unknown"
	}
	return strconv.Itoa(*value)
}

func setIdentifier(f *Frame, id string) {
	f.Identifier = id
	f.IDE = len(id) == 8
	f.Route = id
	value, err := strconv.ParseUint(id, 16, 32)
	if err != nil || (len(id) != 3 && len(id) != 8) || (len(id) == 3 && value > 0x7ff) {
		f.Diagnostics = append(f.Diagnostics, "invalid identifier width or hex")
	}
	if value&0x20000000 != 0 {
		f.Flags = append(f.Flags, "ERROR")
		f.Diagnostics = append(f.Diagnostics, "error frame is not telemetry")
	}
	if value&0x40000000 != 0 {
		f.Flags = append(f.Flags, "RTR")
		f.Diagnostics = append(f.Diagnostics, "RTR flag is not telemetry")
	}
	if value > 0x1fffffff {
		f.Diagnostics = append(f.Diagnostics, "identifier outside classical range")
	}
}
