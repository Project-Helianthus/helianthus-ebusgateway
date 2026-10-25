package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
)

// Configuration v2 of the Modbus multi-endpoint runtime contract: one closed,
// owner-only JSON document read once at process start through
// -modbus-tcp-endpoints-file. Validation is atomic over the whole document and
// the first failing check decides one static category; nothing is registered,
// constructed or dialed before every check has passed.

const (
	modbusEndpointsContract          = "helianthus.modbus-tcp-endpoints.v2"
	maxModbusEndpointsDocumentBytes  = 16384
	maxModbusTCPEndpoints            = 8
	maxModbusTCPLabelBytes           = 32
	maxModbusTCPEndpointBytes        = 288
	maxModbusTCPUnits                = 32
	minModbusTCPDialTimeout          = 100 * time.Millisecond
	maxModbusTCPDialTimeout          = 30 * time.Second
	defaultModbusTCPDialTimeout      = 5 * time.Second
	modbusTCPDefaultGap              = 200 * time.Millisecond
	modbusTCPDefaultSettle           = time.Second
	modbusTCPDefaultCycle            = 30 * time.Second
	modbusTCPDefaultBackoffFloor     = 5 * time.Second
	modbusTCPDefaultBackoffCeiling   = 300 * time.Second
	modbusEndpointsFlag              = "modbus-tcp-endpoints-file"
	modbusEndpointsRejectionNoEntry  = -1
	modbusEndpointsRejectionPrefix   = "invalid Modbus TCP endpoints document"
	modbusEndpointsDurationMaxDigits = 6
)

// modbusEndpointsCategory is one static category of the contract's rejection
// table. A rejection never echoes a document value, endpoint, label or parser
// text.
type modbusEndpointsCategory string

const (
	modbusEndpointsMixedConfiguration modbusEndpointsCategory = "MIXED_CONFIGURATION"
	modbusEndpointsInvalidDocument    modbusEndpointsCategory = "INVALID_DOCUMENT"
	modbusEndpointsMissingField       modbusEndpointsCategory = "MISSING_FIELD"
	modbusEndpointsUnknownMember      modbusEndpointsCategory = "UNKNOWN_MEMBER"
	modbusEndpointsEmptyList          modbusEndpointsCategory = "EMPTY_LIST"
	modbusEndpointsTooManyEntries     modbusEndpointsCategory = "TOO_MANY_ENTRIES"
	modbusEndpointsInvalidLabel       modbusEndpointsCategory = "INVALID_LABEL"
	modbusEndpointsUnknownFamily      modbusEndpointsCategory = "UNKNOWN_FAMILY"
	modbusEndpointsInvalidEndpoint    modbusEndpointsCategory = "INVALID_ENDPOINT"
	modbusEndpointsInvalidTimeout     modbusEndpointsCategory = "INVALID_TIMEOUT"
	modbusEndpointsInvalidUnits       modbusEndpointsCategory = "INVALID_UNITS"
	modbusEndpointsInvalidPacing      modbusEndpointsCategory = "INVALID_PACING"
	modbusEndpointsDuplicateLabel     modbusEndpointsCategory = "DUPLICATE_LABEL"
	modbusEndpointsDuplicateEndpoint  modbusEndpointsCategory = "DUPLICATE_ENDPOINT"
)

// modbusEndpointsRejection names one category and, for an entry-local case,
// the zero-based entry position.
type modbusEndpointsRejection struct {
	Category modbusEndpointsCategory
	Entry    int
}

func (rejection *modbusEndpointsRejection) Error() string {
	message := modbusEndpointsRejectionPrefix + " (category=" + string(rejection.Category)
	if rejection.Entry >= 0 {
		message += " entry=" + strconv.Itoa(rejection.Entry)
	}
	return message + ")"
}

func rejectModbusEndpoints(category modbusEndpointsCategory, entry int) *modbusEndpointsRejection {
	return &modbusEndpointsRejection{Category: category, Entry: entry}
}

// modbusTCPV1Flags are the configuration v1 flags that the endpoints document
// excludes.
var modbusTCPV1Flags = []string{"modbus-tcp-enabled", "modbus-tcp-endpoint", "modbus-tcp-endpoint-file", "modbus-tcp-dial-timeout"}

// modbusTCPFlagBindings maps each flag-bound Modbus TCP configuration to the
// inputs of its flag set. The process resolves Modbus TCP configuration
// through its single post-parse resolveModbusEndpointFile call, which takes
// the binding to select configuration v1 or v2 from explicitly set flags.
var modbusTCPFlagBindings sync.Map

func bindModbusTCPFlagInputs(config *ebusgateway.ModbusTCPConfig, inputs *gatewayFlagInputs) {
	modbusTCPFlagBindings.Store(config, inputs)
}

func takeModbusTCPFlagBinding(config *ebusgateway.ModbusTCPConfig) (*gatewayFlagInputs, bool) {
	value, ok := modbusTCPFlagBindings.LoadAndDelete(config)
	if !ok {
		return nil, false
	}
	inputs, ok := value.(*gatewayFlagInputs)
	return inputs, ok && inputs != nil && inputs.flags != nil
}

// explicit reports whether any named flag was set on the command line, as
// opposed to keeping its default.
func (inputs *gatewayFlagInputs) explicit(names ...string) bool {
	found := false
	inputs.flags.Visit(func(f *flag.Flag) {
		for _, name := range names {
			if f.Name == name {
				found = true
			}
		}
	})
	return found
}

// resolveModbusEndpointsDocument reads, validates and installs configuration
// v2. A rejection leaves config unchanged.
func resolveModbusEndpointsDocument(config *ebusgateway.ModbusTCPConfig, path string, v1FlagSet bool) error {
	if config == nil {
		return errors.New("invalid Modbus TCP configuration inputs")
	}
	if v1FlagSet {
		return rejectModbusEndpoints(modbusEndpointsMixedConfiguration, modbusEndpointsRejectionNoEntry)
	}
	if path == "" {
		return rejectModbusEndpoints(modbusEndpointsInvalidDocument, modbusEndpointsRejectionNoEntry)
	}
	content, err := readProtectedModbusFile(path, maxModbusEndpointsDocumentBytes)
	if err != nil {
		return rejectModbusEndpoints(modbusEndpointsInvalidDocument, modbusEndpointsRejectionNoEntry)
	}
	entries, rejection := parseModbusEndpointsDocument(content)
	if rejection != nil {
		return rejection
	}
	config.Enabled, config.Endpoint, config.DialTimeout, config.Endpoints = true, "", 0, entries
	return nil
}

type endpointsValueKind uint8

const (
	endpointsObject endpointsValueKind = iota + 1
	endpointsArray
	endpointsString
	endpointsNumber
	endpointsBool
)

// endpointsValue is one decoded JSON value with duplicate keys, null values,
// fractional or exponent numbers and control characters already refused.
type endpointsValue struct {
	kind    endpointsValueKind
	members map[string]*endpointsValue
	items   []*endpointsValue
	text    string
}

// parseModbusEndpointsDocument applies the contract's check order: document,
// top level, each entry in document order, then the cross-entry checks
// (duplicate label, then duplicate endpoint). Byte bounds count the UTF-8
// bytes of the decoded scalar values; an escaped lone surrogate decodes to the
// replacement character and fails its own member's grammar.
func parseModbusEndpointsDocument(content []byte) ([]ebusgateway.ModbusTCPEndpointConfig, *modbusEndpointsRejection) {
	document, ok := decodeModbusEndpointsDocument(content)
	if !ok || document.kind != endpointsObject {
		return nil, rejectModbusEndpoints(modbusEndpointsInvalidDocument, modbusEndpointsRejectionNoEntry)
	}
	for _, member := range []string{"contract", "endpoints"} {
		if document.members[member] == nil {
			return nil, rejectModbusEndpoints(modbusEndpointsMissingField, modbusEndpointsRejectionNoEntry)
		}
	}
	if unknownEndpointsMember(document, "contract", "endpoints") {
		return nil, rejectModbusEndpoints(modbusEndpointsUnknownMember, modbusEndpointsRejectionNoEntry)
	}
	contract, list := document.members["contract"], document.members["endpoints"]
	if contract.kind != endpointsString || contract.text != modbusEndpointsContract || list.kind != endpointsArray {
		return nil, rejectModbusEndpoints(modbusEndpointsInvalidDocument, modbusEndpointsRejectionNoEntry)
	}
	switch {
	case len(list.items) == 0:
		return nil, rejectModbusEndpoints(modbusEndpointsEmptyList, modbusEndpointsRejectionNoEntry)
	case len(list.items) > maxModbusTCPEndpoints:
		return nil, rejectModbusEndpoints(modbusEndpointsTooManyEntries, modbusEndpointsRejectionNoEntry)
	}
	entries := make([]ebusgateway.ModbusTCPEndpointConfig, 0, len(list.items))
	for position, item := range list.items {
		entry, category := parseModbusEndpointsEntry(item)
		if category != "" {
			return nil, rejectModbusEndpoints(category, position)
		}
		entries = append(entries, entry)
	}
	labels := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if labels[entry.Label] {
			return nil, rejectModbusEndpoints(modbusEndpointsDuplicateLabel, modbusEndpointsRejectionNoEntry)
		}
		labels[entry.Label] = true
	}
	endpoints := make(map[string]bool, len(entries))
	for _, entry := range entries {
		normalized, err := normalizeModbusTCPEndpoint(entry.Endpoint)
		if err != nil || endpoints[normalized] {
			return nil, rejectModbusEndpoints(modbusEndpointsDuplicateEndpoint, modbusEndpointsRejectionNoEntry)
		}
		endpoints[normalized] = true
	}
	// Labels are unique and the driver ID derivation is injective, so a
	// validated document cannot yield a duplicate driver ID; the contract has
	// no category for it. Composition still revalidates the instance set.
	return entries, nil
}

var (
	modbusEndpointsEntryMembers  = []string{"label", "family", "endpoint", "dial_timeout", "units", "pacing"}
	modbusEndpointsPacingMembers = []string{"gap_ms", "settle_ms", "cycle_s", "backoff_floor_s", "backoff_ceiling_s"}
)

func parseModbusEndpointsEntry(item *endpointsValue) (ebusgateway.ModbusTCPEndpointConfig, modbusEndpointsCategory) {
	var entry ebusgateway.ModbusTCPEndpointConfig
	if item.kind != endpointsObject {
		return entry, modbusEndpointsInvalidDocument
	}
	for _, member := range []string{"label", "family", "endpoint"} {
		if item.members[member] == nil {
			return entry, modbusEndpointsMissingField
		}
	}
	pacing := item.members["pacing"]
	if unknownEndpointsMember(item, modbusEndpointsEntryMembers...) || (pacing != nil && pacing.kind == endpointsObject && unknownEndpointsMember(pacing, modbusEndpointsPacingMembers...)) {
		return entry, modbusEndpointsUnknownMember
	}
	label := item.members["label"]
	if label.kind != endpointsString || !validModbusTCPLabel(label.text) {
		return entry, modbusEndpointsInvalidLabel
	}
	entry.Label = label.text
	family := item.members["family"]
	if family.kind != endpointsString {
		return entry, modbusEndpointsUnknownFamily
	}
	if _, ok := modbusTCPFamilyTable[modbusTCPFamily(family.text)]; !ok {
		return entry, modbusEndpointsUnknownFamily
	}
	entry.Family = family.text
	endpoint := item.members["endpoint"]
	if endpoint.kind != endpointsString {
		return entry, modbusEndpointsInvalidEndpoint
	}
	if _, err := normalizeModbusTCPEndpoint(endpoint.text); err != nil {
		return entry, modbusEndpointsInvalidEndpoint
	}
	entry.Endpoint = endpoint.text
	entry.DialTimeout = defaultModbusTCPDialTimeout
	if timeout := item.members["dial_timeout"]; timeout != nil {
		parsed, ok := parseModbusTCPDialTimeout(timeout)
		if !ok {
			return entry, modbusEndpointsInvalidTimeout
		}
		entry.DialTimeout = parsed
	}
	if units := item.members["units"]; units != nil {
		parsed, ok := parseModbusTCPUnits(units)
		if !ok || !validModbusTCPUnits(modbusTCPFamily(entry.Family), parsed) {
			return entry, modbusEndpointsInvalidUnits
		}
		entry.Units = parsed
	}
	resolved, ok := parseModbusTCPPacing(modbusTCPFamily(entry.Family), pacing)
	if !ok {
		return entry, modbusEndpointsInvalidPacing
	}
	entry.Pacing = resolved
	return entry, ""
}

func unknownEndpointsMember(object *endpointsValue, known ...string) bool {
	for name := range object.members {
		found := false
		for _, candidate := range known {
			if name == candidate {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}
	return false
}

// decodeModbusEndpointsDocument decodes one UTF-8 JSON object without a
// byte-order mark and refuses duplicate keys at any depth, null values,
// fractional or exponent numbers, control characters in strings (literal or
// escaped) and any data after the object.
func decodeModbusEndpointsDocument(content []byte) (*endpointsValue, bool) {
	if len(content) < 1 || len(content) > maxModbusEndpointsDocumentBytes || !utf8.Valid(content) || bytes.HasPrefix(content, []byte("\xef\xbb\xbf")) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, false
	}
	value, ok := decodeEndpointsValue(decoder, token)
	if !ok {
		return nil, false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return value, true
}

func decodeEndpointsValue(decoder *json.Decoder, token json.Token) (*endpointsValue, bool) {
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			value := &endpointsValue{kind: endpointsObject, members: make(map[string]*endpointsValue)}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, false
				}
				key, ok := keyToken.(string)
				if !ok || containsControl(key) {
					return nil, false
				}
				if _, duplicate := value.members[key]; duplicate {
					return nil, false
				}
				next, err := decoder.Token()
				if err != nil {
					return nil, false
				}
				member, ok := decodeEndpointsValue(decoder, next)
				if !ok {
					return nil, false
				}
				value.members[key] = member
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, false
			}
			return value, true
		case '[':
			value := &endpointsValue{kind: endpointsArray}
			for decoder.More() {
				next, err := decoder.Token()
				if err != nil {
					return nil, false
				}
				item, ok := decodeEndpointsValue(decoder, next)
				if !ok {
					return nil, false
				}
				value.items = append(value.items, item)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, false
			}
			return value, true
		}
		return nil, false
	case string:
		if containsControl(typed) {
			return nil, false
		}
		return &endpointsValue{kind: endpointsString, text: typed}, true
	case json.Number:
		if strings.ContainsAny(string(typed), ".eE") {
			return nil, false
		}
		return &endpointsValue{kind: endpointsNumber, text: string(typed)}, true
	case bool:
		return &endpointsValue{kind: endpointsBool}, true
	}
	// A null value, or any token the grammar does not produce.
	return nil, false
}

func containsControl(text string) bool {
	return strings.IndexFunc(text, unicode.IsControl) >= 0
}

// modbusTCPLabelPattern is the driver-ID segment grammar narrowed to start
// with a letter, use hyphens only as separators and exclude dots.
var modbusTCPLabelPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)

func validModbusTCPLabel(label string) bool {
	return len(label) >= 1 && len(label) <= maxModbusTCPLabelBytes && modbusTCPLabelPattern.MatchString(label)
}

var (
	modbusTCPEndpointPattern = regexp.MustCompile(`^tcp://(\[[0-9A-Fa-f:.]+\]|[^/:?#@\[\]\s]+):([0-9]{1,5})$`)
	modbusTCPHostLabel       = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
)

var errInvalidModbusTCPEndpoint = errors.New("invalid Modbus TCP endpoint")

// normalizeModbusTCPEndpoint validates one endpoint and returns its uniqueness
// key: lowercase host with one trailing dot removed, canonical IP spelling and
// a decimal port without leading zeros. The error never carries the value.
func normalizeModbusTCPEndpoint(endpoint string) (string, error) {
	if len(endpoint) > maxModbusTCPEndpointBytes || endpoint != strings.TrimSpace(endpoint) || containsControl(endpoint) {
		return "", errInvalidModbusTCPEndpoint
	}
	match := modbusTCPEndpointPattern.FindStringSubmatch(endpoint)
	if match == nil {
		return "", errInvalidModbusTCPEndpoint
	}
	port, err := strconv.Atoi(match[2])
	if err != nil || port < 1 || port > 65535 {
		return "", errInvalidModbusTCPEndpoint
	}
	host := match[1]
	if strings.HasPrefix(host, "[") {
		address, err := netip.ParseAddr(host[1 : len(host)-1])
		if err != nil || !address.Is6() || address.Zone() != "" {
			return "", errInvalidModbusTCPEndpoint
		}
		host = "[" + address.String() + "]"
	} else if address, err := netip.ParseAddr(host); err == nil && address.Is4() {
		host = address.String()
	} else {
		host = strings.TrimSuffix(strings.ToLower(host), ".")
		if host == "" || len(host) > 253 {
			return "", errInvalidModbusTCPEndpoint
		}
		for _, label := range strings.Split(host, ".") {
			if !modbusTCPHostLabel.MatchString(label) {
				return "", errInvalidModbusTCPEndpoint
			}
		}
	}
	if err := modbusadapter.ValidateTCPEndpoint(endpoint); err != nil {
		return "", errInvalidModbusTCPEndpoint
	}
	return "tcp://" + host + ":" + strconv.Itoa(port), nil
}

var modbusTCPDurationPattern = regexp.MustCompile(`^([1-9][0-9]*)(ms|s)$`)

func parseModbusTCPDialTimeout(value *endpointsValue) (time.Duration, bool) {
	if value.kind != endpointsString {
		return 0, false
	}
	match := modbusTCPDurationPattern.FindStringSubmatch(value.text)
	if match == nil || len(match[1]) > modbusEndpointsDurationMaxDigits {
		return 0, false
	}
	amount, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	unit := time.Millisecond
	if match[2] == "s" {
		unit = time.Second
	}
	timeout := time.Duration(amount) * unit
	if timeout < minModbusTCPDialTimeout || timeout > maxModbusTCPDialTimeout {
		return 0, false
	}
	return timeout, true
}

func parseModbusTCPUnits(value *endpointsValue) ([]byte, bool) {
	if value.kind != endpointsArray {
		return nil, false
	}
	units := make([]byte, 0, len(value.items))
	for _, item := range value.items {
		unit, ok := endpointsInteger(item)
		if !ok || unit < 1 || unit > 247 {
			return nil, false
		}
		units = append(units, byte(unit))
	}
	return units, true
}

// validModbusTCPUnits checks an allowlist: 1 to 32 distinct units from 1 to
// 247, and for sunspec absent or exactly [1].
func validModbusTCPUnits(family modbusTCPFamily, units []byte) bool {
	if units == nil {
		return true
	}
	if len(units) < 1 || len(units) > maxModbusTCPUnits {
		return false
	}
	seen := make(map[byte]bool, len(units))
	for _, unit := range units {
		if unit < 1 || unit > 247 || seen[unit] {
			return false
		}
		seen[unit] = true
	}
	return family != modbusTCPFamilySunSpec || (len(units) == 1 && units[0] == sunSpecLiveSmokeUnitID)
}

// modbusTCPPacingBound is one pacing member: its bound in its own unit and the
// production default.
type modbusTCPPacingBound struct {
	low, high, fallback int64
	unit                time.Duration
}

var modbusTCPPacingBounds = map[string]modbusTCPPacingBound{
	"gap_ms":            {low: 50, high: 2000, fallback: 200, unit: time.Millisecond},
	"settle_ms":         {low: 0, high: 10000, fallback: 1000, unit: time.Millisecond},
	"cycle_s":           {low: 15, high: 3600, fallback: 30, unit: time.Second},
	"backoff_floor_s":   {low: 1, high: 300, fallback: 5, unit: time.Second},
	"backoff_ceiling_s": {low: 5, high: 3600, fallback: 300, unit: time.Second},
}

// parseModbusTCPPacing resolves the pacing object with the production
// defaults; the sunspec cycle keeps the 15 s it has today.
func parseModbusTCPPacing(family modbusTCPFamily, value *endpointsValue) (ebusgateway.ModbusTCPPacing, bool) {
	pacing := defaultModbusTCPPacing(family)
	if value == nil {
		return pacing, true
	}
	if value.kind != endpointsObject {
		return pacing, false
	}
	for name, member := range value.members {
		bound := modbusTCPPacingBounds[name]
		amount, ok := endpointsInteger(member)
		if !ok || amount < bound.low || amount > bound.high {
			return pacing, false
		}
		resolved := time.Duration(amount) * bound.unit
		switch name {
		case "gap_ms":
			pacing.Gap = resolved
		case "settle_ms":
			pacing.Settle = resolved
		case "cycle_s":
			pacing.Cycle = resolved
		case "backoff_floor_s":
			pacing.BackoffFloor = resolved
		case "backoff_ceiling_s":
			pacing.BackoffCeiling = resolved
		}
	}
	return pacing, validModbusTCPPacing(pacing)
}

func defaultModbusTCPPacing(family modbusTCPFamily) ebusgateway.ModbusTCPPacing {
	cycle := modbusTCPDefaultCycle
	if spec, ok := modbusTCPFamilyTable[family]; ok {
		cycle = spec.cycle
	}
	return ebusgateway.ModbusTCPPacing{Gap: modbusTCPDefaultGap, Settle: modbusTCPDefaultSettle, Cycle: cycle, BackoffFloor: modbusTCPDefaultBackoffFloor, BackoffCeiling: modbusTCPDefaultBackoffCeiling}
}

// validModbusTCPPacing checks resolved pacing against the contract bounds,
// including a floor that is at most the ceiling.
func validModbusTCPPacing(pacing ebusgateway.ModbusTCPPacing) bool {
	within := func(name string, value time.Duration) bool {
		bound := modbusTCPPacingBounds[name]
		return value%bound.unit == 0 && value >= time.Duration(bound.low)*bound.unit && value <= time.Duration(bound.high)*bound.unit
	}
	return within("gap_ms", pacing.Gap) && within("settle_ms", pacing.Settle) && within("cycle_s", pacing.Cycle) &&
		within("backoff_floor_s", pacing.BackoffFloor) && within("backoff_ceiling_s", pacing.BackoffCeiling) &&
		pacing.BackoffFloor <= pacing.BackoffCeiling
}

func endpointsInteger(value *endpointsValue) (int64, bool) {
	if value == nil || value.kind != endpointsNumber {
		return 0, false
	}
	amount, err := strconv.ParseInt(value.text, 10, 64)
	return amount, err == nil
}

// modbusTCPDocumentEndpoints lists the protected endpoints of configuration v2
// for error redaction; configuration v1 lists none.
func modbusTCPDocumentEndpoints(cfg ebusgateway.ModbusTCPConfig) []string {
	endpoints := make([]string, 0, len(cfg.Endpoints))
	for _, entry := range cfg.Endpoints {
		endpoints = append(endpoints, entry.Endpoint)
	}
	return endpoints
}
