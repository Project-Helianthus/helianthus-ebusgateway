package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
)

// Synthetic endpoints only: reserved invalid domains and documentation
// address ranges, never an installation.
const (
	h08EndpointA = "tcp://sunspec-a.example.invalid:502"
	h08EndpointB = "tcp://sunspec-b.example.invalid:1502"
)

func h08Entry(label, family, endpoint string) map[string]any {
	return map[string]any{"label": label, "family": family, "endpoint": endpoint}
}

func h08Document(entries ...map[string]any) map[string]any {
	list := make([]any, 0, len(entries))
	for _, entry := range entries {
		list = append(list, entry)
	}
	return map[string]any{"contract": modbusEndpointsContract, "endpoints": list}
}

func h08Encode(t *testing.T, document any) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func h08WriteDocument(t *testing.T, content []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "modbus-endpoints.json")
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func h08Reject(t *testing.T, content []byte) *modbusEndpointsRejection {
	t.Helper()
	cfg := ebusgateway.ModbusTCPConfig{}
	err := resolveModbusEndpointsDocument(&cfg, h08WriteDocument(t, content, 0o600), false)
	if err == nil {
		t.Fatalf("document accepted: %s", content)
	}
	rejection, ok := err.(*modbusEndpointsRejection)
	if !ok {
		t.Fatalf("rejection is not categorical: %T", err)
	}
	if !reflect.DeepEqual(cfg, ebusgateway.ModbusTCPConfig{}) {
		t.Fatalf("a rejected document changed the configuration: %+v", cfg)
	}
	return rejection
}

func TestH08ExampleDocumentResolvesDefaultsAndKeepsTheV1DriverIdentity(t *testing.T) {
	document := `{
  "contract": "helianthus.modbus-tcp-endpoints.v2",
  "endpoints": [
    {"label": "default", "family": "sunspec", "endpoint": "tcp://sunspec-a.example.invalid:502",
     "dial_timeout": "5s", "units": [1],
     "pacing": {"gap_ms": 200, "settle_ms": 1000, "cycle_s": 15, "backoff_floor_s": 5, "backoff_ceiling_s": 300}},
    {"label": "roof", "family": "sunspec", "endpoint": "tcp://sunspec-b.example.invalid:1502"},
    {"label": "site-a", "family": "huawei.emma", "endpoint": "tcp://[2001:db8::10]:502", "dial_timeout": "750ms", "units": [3, 1]}
  ]
}`
	cfg := ebusgateway.ModbusTCPConfig{}
	if err := resolveModbusEndpointsDocument(&cfg, h08WriteDocument(t, []byte(document), 0o400), false); err != nil {
		t.Fatal(err)
	}
	sunspec := ebusgateway.ModbusTCPPacing{Gap: 200 * time.Millisecond, Settle: time.Second, Cycle: 15 * time.Second, BackoffFloor: 5 * time.Second, BackoffCeiling: 300 * time.Second}
	vendor := sunspec
	vendor.Cycle = 30 * time.Second
	want := ebusgateway.ModbusTCPConfig{Enabled: true, Endpoints: []ebusgateway.ModbusTCPEndpointConfig{
		{Label: "default", Family: "sunspec", Endpoint: h08EndpointA, DialTimeout: 5 * time.Second, Units: []byte{1}, Pacing: sunspec},
		{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB, DialTimeout: 5 * time.Second, Pacing: sunspec},
		{Label: "site-a", Family: "huawei.emma", Endpoint: "tcp://[2001:db8::10]:502", DialTimeout: 750 * time.Millisecond, Units: []byte{3, 1}, Pacing: vendor},
	}}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("resolved = %+v\nwant     %+v", cfg, want)
	}
	if !modbusTCPV1SlotConfigured(cfg) {
		t.Fatal("the sunspec default entry is not the v1 slot")
	}
}

func TestH08DriverIDScheme(t *testing.T) {
	for _, tc := range []struct {
		family modbusTCPFamily
		label  string
		want   runtimecontrol.DriverID
	}{
		{modbusTCPFamilySunSpec, "default", "modbus.sunspec"},
		{modbusTCPFamilySunSpec, "roof", "modbus.sunspec.roof"},
		{modbusTCPFamilyHuaweiSmartLogger, "site-a", "modbus.huawei.smartlogger.site-a"},
		{modbusTCPFamilyHuaweiEMMA, "default", "modbus.huawei.emma"},
		{modbusTCPFamilyHuaweiSDongle, "site-b", "modbus.huawei.sdongle.site-b"},
	} {
		got, ok := modbusTCPDriverID(tc.family, tc.label)
		if !ok || got != tc.want || len(got) > 128 || !runtimestate.CanonicalControlDriverID(string(got)) {
			t.Fatalf("driver ID(%s, %s) = %q, %v; want %q", tc.family, tc.label, got, ok, tc.want)
		}
	}
	if _, ok := modbusTCPDriverID("Sunspec", "default"); ok {
		t.Fatal("a family outside the closed table derived an ID")
	}
	if _, ok := modbusTCPDriverID(modbusTCPFamilySunSpec, "a.b"); ok {
		t.Fatal("a dotted label derived an ID")
	}
}

// TestH08RejectionTableCategories produces each category of the contract's
// rejection table from its synthetic case. It fails before configuration v2
// exists: no document is read and no category is produced.
func TestH08RejectionTableCategories(t *testing.T) {
	entry := h08Entry("roof", "sunspec", h08EndpointB)
	with := func(member string, value any) []byte {
		changed := map[string]any{}
		for key, v := range entry {
			changed[key] = v
		}
		changed[member] = value
		return h08Encode(t, h08Document(h08Entry("default", "sunspec", h08EndpointA), changed))
	}
	nine := make([]map[string]any, 0, 9)
	for i := 0; i < 9; i++ {
		nine = append(nine, h08Entry("e"+string(rune('a'+i)), "sunspec", "tcp://e"+string(rune('a'+i))+".example.invalid:502"))
	}
	missingEndpoint := h08Entry("roof", "sunspec", "")
	delete(missingEndpoint, "endpoint")
	for _, tc := range []struct {
		name     string
		content  []byte
		category modbusEndpointsCategory
		entry    int
	}{
		{"duplicate label", h08Encode(t, h08Document(entry, h08Entry("roof", "sunspec", "tcp://other.example.invalid:502"))), modbusEndpointsDuplicateLabel, -1},
		{"duplicate endpoint after normalization", h08Encode(t, h08Document(entry, h08Entry("attic", "sunspec", "tcp://SUNSPEC-B.example.invalid.:01502"))), modbusEndpointsDuplicateEndpoint, -1},
		{"duplicate IPv6 spelling", h08Encode(t, h08Document(h08Entry("a", "sunspec", "tcp://[2001:db8::1]:502"), h08Entry("b", "sunspec", "tcp://[2001:0db8:0:0:0:0:0:1]:502"))), modbusEndpointsDuplicateEndpoint, -1},
		{"unknown family case", with("family", "Sunspec"), modbusEndpointsUnknownFamily, 1},
		{"unknown family vendor", with("family", "huawei"), modbusEndpointsUnknownFamily, 1},
		{"list too long", h08Encode(t, h08Document(nine...)), modbusEndpointsTooManyEntries, -1},
		{"missing endpoint", h08Encode(t, h08Document(missingEndpoint)), modbusEndpointsMissingField, 0},
		{"missing endpoints", h08Encode(t, map[string]any{"contract": modbusEndpointsContract}), modbusEndpointsMissingField, -1},
		{"label Roof_1", with("label", "Roof_1"), modbusEndpointsInvalidLabel, 1},
		{"label 1roof", with("label", "1roof"), modbusEndpointsInvalidLabel, 1},
		{"label roof-", with("label", "roof-"), modbusEndpointsInvalidLabel, 1},
		{"label a.b", with("label", "a.b"), modbusEndpointsInvalidLabel, 1},
		{"label empty", with("label", ""), modbusEndpointsInvalidLabel, 1},
		{"label too long", with("label", strings.Repeat("a", 33)), modbusEndpointsInvalidLabel, 1},
		{"empty list", h08Encode(t, h08Document()), modbusEndpointsEmptyList, -1},
		{"unknown entry member", with("enabled", true), modbusEndpointsUnknownMember, 1},
		{"unknown pacing member", with("pacing", map[string]any{"burst": 2}), modbusEndpointsUnknownMember, 1},
		{"unknown top-level member", h08Encode(t, map[string]any{"contract": modbusEndpointsContract, "endpoints": []any{entry}, "extra": 1}), modbusEndpointsUnknownMember, -1},
		{"contract value", h08Encode(t, map[string]any{"contract": "helianthus.modbus-tcp-endpoints.v3", "endpoints": []any{entry}}), modbusEndpointsInvalidDocument, -1},
		{"endpoint path", with("endpoint", "tcp://sunspec-b.example.invalid:502/path"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint userinfo", with("endpoint", "tcp://user@sunspec-b.example.invalid:502"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint missing port", with("endpoint", "tcp://sunspec-b.example.invalid"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint port zero", with("endpoint", "tcp://sunspec-b.example.invalid:0"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint scheme", with("endpoint", "udp://sunspec-b.example.invalid:502"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint whitespace", with("endpoint", " tcp://sunspec-b.example.invalid:502"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint query", with("endpoint", "tcp://sunspec-b.example.invalid:502?"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint zone", with("endpoint", "tcp://[2001:db8::1%25eth0]:502"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint too long", with("endpoint", "tcp://"+strings.Repeat("a", 60)+"."+strings.Repeat("b", 60)+"."+strings.Repeat("c", 60)+"."+strings.Repeat("d", 60)+"."+strings.Repeat("e", 40)+":502"), modbusEndpointsInvalidEndpoint, 1},
		{"endpoint wrong type", with("endpoint", 502), modbusEndpointsInvalidEndpoint, 1},
		{"timeout 50ms", with("dial_timeout", "50ms"), modbusEndpointsInvalidTimeout, 1},
		{"timeout 45s", with("dial_timeout", "45s"), modbusEndpointsInvalidTimeout, 1},
		{"timeout grammar", with("dial_timeout", "1m"), modbusEndpointsInvalidTimeout, 1},
		{"timeout wrong type", with("dial_timeout", 5), modbusEndpointsInvalidTimeout, 1},
		{"units zero", with("units", []int{0}), modbusEndpointsInvalidUnits, 1},
		{"units repeated", h08Encode(t, h08Document(h08Entry("default", "sunspec", h08EndpointA), map[string]any{"label": "emma", "family": "huawei.emma", "endpoint": h08EndpointB, "units": []int{2, 2}})), modbusEndpointsInvalidUnits, 1},
		{"units 2 for sunspec", with("units", []int{2}), modbusEndpointsInvalidUnits, 1},
		{"units empty", with("units", []int{}), modbusEndpointsInvalidUnits, 1},
		{"units wrong type", with("units", "1"), modbusEndpointsInvalidUnits, 1},
		{"pacing below bound", with("pacing", map[string]any{"gap_ms": 10}), modbusEndpointsInvalidPacing, 1},
		{"pacing above bound", with("pacing", map[string]any{"cycle_s": 3601}), modbusEndpointsInvalidPacing, 1},
		{"pacing floor above ceiling", with("pacing", map[string]any{"backoff_floor_s": 200, "backoff_ceiling_s": 100}), modbusEndpointsInvalidPacing, 1},
		{"pacing floor above default ceiling", with("pacing", map[string]any{"backoff_floor_s": 400}), modbusEndpointsInvalidPacing, 1},
		{"pacing wrong type", with("pacing", []int{1}), modbusEndpointsInvalidPacing, 1},
		{"label lone surrogate", []byte(`{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"roof\ud800","family":"sunspec","endpoint":"tcp://sunspec-b.example.invalid:502"}]}`), modbusEndpointsInvalidLabel, 0},
		{"endpoint lone surrogate", []byte(`{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"roof","family":"sunspec","endpoint":"tcp://sunspec\udc00-b.example.invalid:502"}]}`), modbusEndpointsInvalidEndpoint, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rejection := h08Reject(t, tc.content)
			if rejection.Category != tc.category || rejection.Entry != tc.entry {
				t.Fatalf("rejection = %+v; want %s entry %d", rejection, tc.category, tc.entry)
			}
			message := rejection.Error()
			for _, secret := range []string{"sunspec-a", "sunspec-b", "SUNSPEC", "roof", "Roof_1", "2001", "example.invalid"} {
				if strings.Contains(message, secret) {
					t.Fatalf("rejection echoes document material %q: %s", secret, message)
				}
			}
		})
	}
}

func TestH08DocumentStageRejections(t *testing.T) {
	valid := string(h08Encode(t, h08Document(h08Entry("default", "sunspec", h08EndpointA))))
	for name, content := range map[string]string{
		"byte-order mark":         "\xef\xbb\xbf" + valid,
		"invalid UTF-8":           strings.Replace(valid, "default", "def\xffault", 1),
		"duplicate top-level key": `{"contract":"helianthus.modbus-tcp-endpoints.v2","contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[]}`,
		"duplicate nested key":    `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"a","label":"b","family":"sunspec","endpoint":"tcp://a.example.invalid:502"}]}`,
		"null value":              `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"a","family":"sunspec","endpoint":"tcp://a.example.invalid:502","units":null}]}`,
		"fractional number":       `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"a","family":"sunspec","endpoint":"tcp://a.example.invalid:502","pacing":{"gap_ms":200.0}}]}`,
		"exponent number":         `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"a","family":"sunspec","endpoint":"tcp://a.example.invalid:502","pacing":{"gap_ms":2e2}}]}`,
		"escaped control":         `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[],"\u0001":1}`,
		"literal delete control":  `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"a` + "\x7f" + `","family":"sunspec","endpoint":"tcp://a.example.invalid:502"}]}`,
		"trailing data":           valid + `{}`,
		"trailing garbage":        valid + ` x`,
		"syntax":                  valid[:len(valid)-1],
		"top-level array":         `[` + valid + `]`,
		"endpoints not array":     `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":{}}`,
		"empty":                   ``,
		"oversized":               valid + strings.Repeat(" ", maxModbusEndpointsDocumentBytes),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "empty" {
				cfg := ebusgateway.ModbusTCPConfig{}
				path := h08WriteDocument(t, nil, 0o600)
				err := resolveModbusEndpointsDocument(&cfg, path, false)
				if rejection, ok := err.(*modbusEndpointsRejection); !ok || rejection.Category != modbusEndpointsInvalidDocument {
					t.Fatalf("empty document = %v", err)
				}
				return
			}
			if rejection := h08Reject(t, []byte(content)); rejection.Category != modbusEndpointsInvalidDocument || rejection.Entry != -1 {
				t.Fatalf("rejection = %+v", rejection)
			}
		})
	}
	t.Run("entry not an object", func(t *testing.T) {
		content := `{"contract":"helianthus.modbus-tcp-endpoints.v2","endpoints":[{"label":"a","family":"sunspec","endpoint":"tcp://a.example.invalid:502"},"x"]}`
		if rejection := h08Reject(t, []byte(content)); rejection.Category != modbusEndpointsInvalidDocument || rejection.Entry != 1 {
			t.Fatalf("rejection = %+v", rejection)
		}
	})
}

func TestH08DocumentFileSafetyMatchesTheV1File(t *testing.T) {
	content := h08Encode(t, h08Document(h08Entry("default", "sunspec", h08EndpointA)))
	for _, mode := range []os.FileMode{0o644, 0o640, 0o700, 0o604} {
		cfg := ebusgateway.ModbusTCPConfig{}
		err := resolveModbusEndpointsDocument(&cfg, h08WriteDocument(t, content, mode), false)
		if rejection, ok := err.(*modbusEndpointsRejection); !ok || rejection.Category != modbusEndpointsInvalidDocument {
			t.Fatalf("mode %v accepted: %v", mode, err)
		}
	}
	target := h08WriteDocument(t, content, 0o600)
	link := filepath.Join(t.TempDir(), "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	cfg := ebusgateway.ModbusTCPConfig{}
	if err := resolveModbusEndpointsDocument(&cfg, link, false); err == nil {
		t.Fatal("a link was accepted")
	}
	if err := resolveModbusEndpointsDocument(&cfg, filepath.Join(t.TempDir(), "absent.json"), false); err == nil {
		t.Fatal("an absent document was accepted")
	}
}

func TestH08FirstFailingCheckDecidesTheCategory(t *testing.T) {
	bad := func(label, endpoint, timeout string) map[string]any {
		entry := h08Entry(label, "sunspec", endpoint)
		if timeout != "" {
			entry["dial_timeout"] = timeout
		}
		return entry
	}
	// Entry order: entry 1's timeout fails before entry 2's endpoint.
	content := h08Encode(t, h08Document(bad("a", h08EndpointA, ""), bad("b", h08EndpointB, "50ms"), bad("c", "tcp://c.example.invalid", "")))
	if rejection := h08Reject(t, content); rejection.Category != modbusEndpointsInvalidTimeout || rejection.Entry != 1 {
		t.Fatalf("rejection = %+v", rejection)
	}
	// Within an entry: unknown member before label, label before family.
	content = h08Encode(t, h08Document(map[string]any{"label": "Bad", "family": "Sunspec", "endpoint": h08EndpointA, "x": 1}))
	if rejection := h08Reject(t, content); rejection.Category != modbusEndpointsUnknownMember {
		t.Fatalf("rejection = %+v", rejection)
	}
	// Cross-entry checks run only after every entry passed, label first.
	content = h08Encode(t, h08Document(bad("a", h08EndpointA, ""), bad("a", h08EndpointA, "")))
	if rejection := h08Reject(t, content); rejection.Category != modbusEndpointsDuplicateLabel {
		t.Fatalf("rejection = %+v", rejection)
	}
	// The entry count fails before any entry is inspected.
	many := make([]map[string]any, 0, 9)
	for i := 0; i < 9; i++ {
		many = append(many, map[string]any{"bad": true})
	}
	if rejection := h08Reject(t, h08Encode(t, h08Document(many...))); rejection.Category != modbusEndpointsTooManyEntries {
		t.Fatalf("rejection = %+v", rejection)
	}
}

// h08ParseFlags binds and parses flags exactly as the process entry point
// does; h08Resolve then makes the entry point's single resolution call.
func h08ParseFlags(t *testing.T, args ...string) (*ebusgateway.Config, *gatewayFlagInputs) {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("h08", flag.ContinueOnError)
	inputs := bindFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return &cfg, inputs
}

func h08Resolve(cfg *ebusgateway.Config, inputs *gatewayFlagInputs) error {
	return resolveModbusEndpointFile(&cfg.ModbusTCPConfig, inputs.modbusEndpointFile)
}

func h08Category(err error) modbusEndpointsCategory {
	var rejection *modbusEndpointsRejection
	if errors.As(err, &rejection) {
		return rejection.Category
	}
	return ""
}

func TestH08MixedConfigurationIsRejectedBeforeTheDocumentIsRead(t *testing.T) {
	path := h08WriteDocument(t, h08Encode(t, h08Document(h08Entry("default", "sunspec", h08EndpointA))), 0o600)
	for _, v1 := range [][]string{
		{"-modbus-tcp-enabled=true"},
		{"-modbus-tcp-enabled=false"},
		{"-modbus-tcp-endpoint", h08EndpointB},
		{"-modbus-tcp-endpoint-file", path},
		{"-modbus-tcp-dial-timeout", "5s"},
		{"-modbus-tcp-dial-timeout", "0s"},
	} {
		for _, args := range [][]string{append([]string{"-modbus-tcp-endpoints-file", path}, v1...), append(append([]string{}, v1...), "-modbus-tcp-endpoints-file", path)} {
			cfg, inputs := h08ParseFlags(t, args...)
			before := cfg.ModbusTCPConfig
			err := h08Resolve(cfg, inputs)
			if h08Category(err) != modbusEndpointsMixedConfiguration {
				t.Fatalf("%v = %v", args, err)
			}
			if !reflect.DeepEqual(before, cfg.ModbusTCPConfig) {
				t.Fatalf("mixed configuration changed the runtime configuration for %v", args)
			}
		}
	}
	// An unreadable document is still mixed when a v1 flag is present: the
	// selection check runs first.
	cfg, inputs := h08ParseFlags(t, "-modbus-tcp-endpoints-file", "/absent/endpoints.json", "-modbus-tcp-dial-timeout", "5s")
	if err := h08Resolve(cfg, inputs); h08Category(err) != modbusEndpointsMixedConfiguration {
		t.Fatalf("selection did not run first: %v", err)
	}
}

func TestH08EndpointsFlagSelectsConfigurationV2(t *testing.T) {
	path := h08WriteDocument(t, h08Encode(t, h08Document(h08Entry("default", "sunspec", h08EndpointA))), 0o600)
	cfg, inputs := h08ParseFlags(t, "-modbus-tcp-endpoints-file", path)
	if err := h08Resolve(cfg, inputs); err != nil {
		t.Fatal(err)
	}
	if !cfg.ModbusTCPConfig.Enabled || cfg.ModbusTCPConfig.Endpoint != "" || cfg.ModbusTCPConfig.DialTimeout != 0 || len(cfg.ModbusTCPConfig.Endpoints) != 1 {
		t.Fatalf("v2 configuration = %+v", cfg.ModbusTCPConfig)
	}
	cfg, inputs = h08ParseFlags(t, "-modbus-tcp-endpoints-file", "")
	if err := h08Resolve(cfg, inputs); h08Category(err) != modbusEndpointsInvalidDocument {
		t.Fatalf("an empty document path = %v", err)
	}
	usage := flag.NewFlagSet("h08-usage", flag.ContinueOnError)
	defaults := ebusgateway.DefaultConfig()
	bindFlags(usage, &defaults)
	if usage.Lookup(modbusEndpointsFlag) == nil {
		t.Fatal("the gateway does not advertise the endpoints flag")
	}
}

// TestH08SingleLineEndpointFileKeepsTodaysBehavior is the v1 characterization:
// without the endpoints flag, the entry point's resolution through bound
// flags equals the unbound single-line path of the base tree.
func TestH08SingleLineEndpointFileKeepsTodaysBehavior(t *testing.T) {
	path := writeProtectedEndpointFile(t, "tcp://192.0.2.40:502", 0o600)
	for _, args := range [][]string{
		{"-modbus-tcp-enabled=true", "-modbus-tcp-endpoint-file", path, "-modbus-tcp-dial-timeout", "3s"},
		{"-modbus-tcp-enabled=true", "-modbus-tcp-endpoint", "tcp://192.0.2.41:502", "-modbus-tcp-dial-timeout", "5s"},
		{"-modbus-tcp-enabled=false", "-modbus-tcp-endpoint-file", path},
		{"-modbus-tcp-enabled=true", "-modbus-tcp-endpoint", "tcp://192.0.2.41:502", "-modbus-tcp-endpoint-file", path},
		{},
	} {
		bound, inputs := h08ParseFlags(t, args...)
		unbound := bound.ModbusTCPConfig
		errBound := h08Resolve(bound, inputs)
		errUnbound := resolveModbusEndpointFile(&unbound, inputs.modbusEndpointFile)
		if fmt.Sprint(errBound) != fmt.Sprint(errUnbound) || !reflect.DeepEqual(bound.ModbusTCPConfig, unbound) {
			t.Fatalf("%v: v1 resolution changed: %+v / %v vs %+v / %v", args, bound.ModbusTCPConfig, errBound, unbound, errUnbound)
		}
		if len(bound.ModbusTCPConfig.Endpoints) != 0 {
			t.Fatalf("%v: v1 produced an endpoint list", args)
		}
	}
}

func TestH08RedactionCoversEveryDocumentEndpoint(t *testing.T) {
	cfg := ebusgateway.ModbusTCPConfig{Enabled: true, Endpoints: []ebusgateway.ModbusTCPEndpointConfig{
		{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
		{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB},
	}}
	err := redactFileSourcedModbusEndpoints(errors.New("dial sunspec-a.example.invalid:502 and sunspec-b.example.invalid failed: "+h08EndpointB), modbusTCPDocumentEndpoints(cfg)...)
	for _, secret := range []string{"sunspec-a", "sunspec-b"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("redacted error leaks %q: %v", secret, err)
		}
	}
	if redactFileSourcedModbusEndpoints(nil, h08EndpointA) != nil {
		t.Fatal("redaction invented an error")
	}
}
