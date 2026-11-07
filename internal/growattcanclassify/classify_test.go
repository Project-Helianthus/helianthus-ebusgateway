package growattcanclassify

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

const payload = "4142434401020304"

func classify(t *testing.T, text string, opts Options) Result {
	t.Helper()
	r, err := Classify(strings.NewReader(text), opts)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func has(f Frame, family string) bool {
	for _, c := range f.Candidates {
		if c.Family == family {
			return true
		}
	}
	return false
}
func TestCX02StandardAmbiguity(t *testing.T) {
	for _, text := range []string{
		"sim0 311#" + payload + "\nsim0 312#" + payload + "\nsim0 313#" + payload,
		"(12.25) sim0 311 [8] 41 42 43 44 01 02 03 04\nsim0 312#" + payload + "\nsim0 313#" + payload,
	} {
		r := classify(t, text, Options{})
		for _, f := range r.Frames {
			if len(f.Candidates) != 2 || !has(f, "V1.04") || !has(f, "PCS-project-code") || f.PhysicalQualified || f.RuntimeAdmission {
				t.Fatalf("ambiguity lost: %+v", f)
			}
		}
	}
}
func TestCX02StandardTupleNegativeSelectors(t *testing.T) {
	vectors := map[string]string{
		"missing-count":      "sim0 311#" + payload + "\nsim0 312#" + payload,
		"duplicate-count":    "sim0 311#" + payload + "\nsim0 311#" + payload + "\nsim0 313#" + payload,
		"separate-interface": "sim0 311#" + payload + "\nsim1 312#" + payload + "\nsim2 313#" + payload,
		"eight-digit-IDE":    "sim0 00000311#" + payload + "\nsim0 00000312#" + payload + "\nsim0 00000313#" + payload,
		"selector":           "sim0 310#" + payload + "\nsim0 312#" + payload + "\nsim0 313#" + payload,
		"dlc":                "sim0 311 [7] 41 42 43 44 01 02 03 04\nsim0 312#" + payload + "\nsim0 313#" + payload,
	}
	for name, text := range vectors {
		t.Run(name, func(t *testing.T) {
			r := classify(t, text, Options{})
			if r.Outcome != "unknown" {
				t.Fatalf("unexpected candidates: %+v", r)
			}
			for _, f := range r.Frames {
				if f.Raw == "" || f.Payload == "" {
					t.Fatal("raw lost")
				}
			}
		})
	}
}
func TestCX02LINKGeometryAndIdentity(t *testing.T) {
	r := classify(t, "(2.125) sim0 1802F407#"+payload+"\nsim0 1803F408#"+payload, Options{})
	f := r.Frames[0]
	if !has(f, "LINK") || f.ProjectCode != "ABCD" || f.ReleaseBytes != "01020304" || f.IdentityGrade != "Proven" || f.Timestamp != "(2.125)" || f.Route != "1802F407" || f.Endpoint == nil || *f.Endpoint != 7 {
		t.Fatalf("%+v", f)
	}
	if r.Frames[1].ProjectCode != "" || r.RuntimeAdmission || r.PhysicalQualified {
		t.Fatal("source-local identity leaked or admitted")
	}
	lower := classify(t, "sim0 1802F407#6162636401020304", Options{}).Frames[0]
	if lower.ProjectCode != "abcd" {
		t.Fatal("exact case lost")
	}
}
func TestCX02LINKNegativeRouteAndASCII(t *testing.T) {
	for name, id := range map[string]string{"selector": "1702F407", "direction": "180207F4", "route": "1802F307", "control-low": "1880F407", "control-high": "188FF407", "width": "1802F47"} {
		t.Run(name, func(t *testing.T) {
			f := classify(t, "sim0 "+id+"#"+payload, Options{}).Frames[0]
			if has(f, "LINK") || f.Payload != payload || f.Route != id {
				t.Fatalf("%+v", f)
			}
		})
	}
	for _, id := range []string{"187FF407", "1890F407"} {
		if !has(classify(t, "sim0 "+id+"#"+payload, Options{}).Frames[0], "LINK") {
			t.Fatal("control boundary swallowed neighbor")
		}
	}
	for name, data := range map[string]string{"nul-not-terminator": "4142430001020304", "high-byte": "4142438001020304", "short": "41424344010203", "long": "414243440102030405"} {
		t.Run(name, func(t *testing.T) {
			f := classify(t, "sim0 1802F407#"+data, Options{}).Frames[0]
			if f.ProjectCode != "" || f.Payload != data {
				t.Fatalf("%+v", f)
			}
		})
	}
}
func TestCX02PCSExtendedRoutes(t *testing.T) {
	for _, id := range []string{"1900F207", "190FF2FE", "1900DFF2", "190FDFF2", "1910DFF2", "191FDFF2"} {
		f := classify(t, "sim0 "+id+"#"+payload, Options{}).Frames[0]
		if !has(f, "PCS-extended") || f.Route != id {
			t.Fatalf("%+v", f)
		}
	}
}
func TestCX02PCSExtendedNegativeDirections(t *testing.T) {
	for name, id := range map[string]string{"source-before": "18FFF207", "source-after": "1910F207", "source-direction": "190007F2", "source-selector": "1900F307", "coordinator-before": "18FFDFF2", "coordinator-after": "1920DFF2", "coordinator-direction": "1910F2DF", "coordinator-route": "1910DEF2"} {
		t.Run(name, func(t *testing.T) {
			f := classify(t, "sim0 "+id+"#"+payload, Options{}).Frames[0]
			if has(f, "PCS-extended") || f.Payload != payload {
				t.Fatalf("%+v", f)
			}
		})
	}
}
func TestCX02ParallelGroupsAndRelease(t *testing.T) {
	for _, prefix := range []string{"50", "51", "52", "60", "61", "62", "63", "64", "65", "66", "67", "68", "6C", "6D"} {
		for _, group := range []string{"0", "7"} {
			f := classify(t, "sim0 "+prefix+group+"#"+payload, Options{}).Frames[0]
			if !has(f, "PCS-parallel") || f.Group == nil || f.IdentityGrade != "Unknown" {
				t.Fatalf("%+v", f)
			}
			if prefix == "52" && !strings.Contains(f.Candidates[0].Reason, "requires release11") {
				t.Fatal("release support invented")
			}
		}
		t.Run(prefix+"-group8-withheld", func(t *testing.T) {
			f := classify(t, "sim0 "+prefix+"8#"+payload, Options{}).Frames[0]
			if len(f.Candidates) != 0 || f.Payload != payload {
				t.Fatalf("%+v", f)
			}
		})
	}
	for _, id := range []string{"4F7", "537", "5F7", "697", "6B7", "6E7", "00000507"} {
		if len(classify(t, "sim0 "+id+"#"+payload, Options{}).Frames[0].Candidates) != 0 {
			t.Fatal("parallel range or IDE widened", id)
		}
	}
}
func TestCX02MONAssociationAndIndependentDimensions(t *testing.T) {
	for _, id := range []string{"04000000", "04FFFFFF", "08000000", "08FFFFFF", "10000000", "10FFFFFF", "14000000", "14FFFFFF"} {
		text := "sim0 " + id + "#" + payload
		f := classify(t, text, Options{MONInterfaces: []string{"sim0"}}).Frames[0]
		if !has(f, "MON") || f.Endpoint != nil || f.Slot != nil || f.Group != nil || f.RuntimeAdmission {
			t.Fatalf("dimensions or admission invented: %+v", f)
		}
		t.Run(id+"-missing-association", func(t *testing.T) {
			f := classify(t, text, Options{}).Frames[0]
			if has(f, "MON") || f.Payload != payload {
				t.Fatal("association invented")
			}
		})
		t.Run(id+"-other-interface", func(t *testing.T) {
			if has(classify(t, text, Options{MONInterfaces: []string{"sim1"}}).Frames[0], "MON") {
				t.Fatal("cross-interface association")
			}
		})
	}
	for _, id := range []string{"03FFFFFF", "05000000", "07FFFFFF", "09000000", "0FFFFFFF", "11000000", "13FFFFFF", "15000000", "1880F401"} {
		f := classify(t, "sim0 "+id+"#"+payload, Options{MONInterfaces: []string{"sim0"}}).Frames[0]
		if has(f, "MON") || f.Payload != payload {
			t.Fatalf("MON selector widened: %+v", f)
		}
	}
	r := classify(t, "sim0 1802F401#"+payload+"\nsim0 04000000#"+payload, Options{})
	if has(r.Frames[1], "MON") {
		t.Fatal("LINK identity selected MON")
	}
}
func TestCX02SharedControlOnlyUnknown(t *testing.T) {
	for _, id := range []string{"1880F401", "1881F401", "188FF401"} {
		r := classify(t, "sim0 "+id+"#"+payload, Options{MONInterfaces: []string{"sim0"}})
		if r.Outcome != "unknown" || len(r.Frames[0].Candidates) != 0 {
			t.Fatal(r)
		}
	}
}
func TestCX02InvalidAndFlagsRetention(t *testing.T) {
	for name, line := range map[string]string{
		"hex": "sim0 1802F401#GG", "odd": "sim0 1802F401#0", "overlong": "sim0 1802F401#001122334455667788",
		"FD": "sim0 1802F401##1" + payload, "FD-bracket": "sim0 1802F407 [08] 41 42 43 44 01 02 03 04", "RTR": "sim0 1802F401#R8", "RTR-bracket": "sim0 1802F401 [8] remote request",
		"error": "sim0 20000001#" + payload, "rtr-flag": "sim0 40000001#" + payload,
		"nul": "sim0 1802F401#\x00", "escape": "sim0 1802F401#\x1b", "bad-time": "(NaN) sim0 1802F401#" + payload,
		"dlc": "sim0 1802F401 [7] 41 42 43 44 01 02 03 04", "negative-dlc": "sim0 1802F401 [-1] 00", "wide-byte": "sim0 1802F401 [1] 0000",
	} {
		t.Run(name, func(t *testing.T) {
			f := classify(t, line, Options{}).Frames[0]
			if f.Raw != line || len(f.Diagnostics) == 0 || len(f.Candidates) != 0 {
				t.Fatalf("%+v", f)
			}
			if (name == "FD" || name == "FD-bracket" || name == "RTR" || name == "error" || name == "rtr-flag" || name == "RTR-bracket") && len(f.Flags) == 0 {
				t.Fatal("flags lost")
			}
		})
	}
}
func TestCX02UnknownMixedCountsAndDeterminism(t *testing.T) {
	for _, text := range []string{"", "\n", "unrecognized", "sim0 123#0000000000000000"} {
		if classify(t, text, Options{}).Outcome != "unknown" {
			t.Fatal("unknown input classified")
		}
	}
	text := "sim1 1802F401#" + payload + "\nsim0 507#" + payload + "\nsim0 123#" + payload
	a := classify(t, text, Options{})
	b := classify(t, text, Options{})
	if !reflect.DeepEqual(a, b) || a.Outcome != "mixed" || len(a.Interfaces) != 2 || a.Interfaces[0].Interface != "sim0" || a.Interfaces[0].Frames != 2 || a.Interfaces[0].Unknown != 1 {
		t.Fatalf("%+v", a)
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if !bytes.Equal(ja, jb) {
		t.Fatal("JSON nondeterminism")
	}
	var ta, tb bytes.Buffer
	if WriteText(&ta, a) != nil || WriteText(&tb, b) != nil || ta.String() != tb.String() {
		t.Fatal("text nondeterminism")
	}
}
func TestCX02BoundsAndIndependentSibling(t *testing.T) {
	line := "sim0 1802F401#" + payload
	for _, opts := range []Options{{MaxBytes: len(line)}, {MaxLine: len(line)}} {
		if _, err := Classify(strings.NewReader(line), opts); err != nil {
			t.Fatal("inclusive boundary", err)
		}
	}
	for name, opts := range map[string]Options{"bytes": {MaxBytes: len(line) - 1}, "line": {MaxLine: len(line) - 1}, "negative": {MaxLine: -1}, "ceiling": {MaxBytes: DefaultMaxBytes + 1}} {
		t.Run(name, func(t *testing.T) {
			if _, err := Classify(strings.NewReader(line), opts); err == nil {
				t.Fatal("unbounded")
			}
		})
	}
	r := classify(t, line+"\nsim0 1802F401#GG", Options{})
	if r.Frames[0].ProjectCode != "ABCD" || !has(r.Frames[0], "LINK") || len(r.Frames[1].Candidates) != 0 {
		t.Fatal("valid sibling erased")
	}
	prefix, err := Classify(strings.NewReader(line+"\n"+strings.Repeat("x", 100)), Options{MaxLine: 40})
	if err == nil || len(prefix.Frames) != 1 || !has(prefix.Frames[0], "LINK") {
		t.Fatal("resource failure lost prefix")
	}
	if _, err := Classify(failReader{}, Options{}); err == nil {
		t.Fatal("reader error ignored")
	}
	if err := WriteText(failWriter{}, r); err == nil {
		t.Fatal("writer error ignored")
	}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("synthetic read failure") }

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCX02IdentityEvidenceFieldVariation(t *testing.T) {
	base := "4142434401020304"
	for i := 0; i < 8; i++ {
		replacement := "45"
		if i >= 4 {
			replacement = "09"
		}
		mutated := base[:2*i] + replacement + base[2*i+2:]
		f := classify(t, "sim0 1802F407#"+mutated, Options{}).Frames[0]
		if f.Payload != mutated || f.ProjectCode == "" || f.IdentityGrade != "Proven" {
			t.Fatalf("byte %d lost: %+v", i, f)
		}
		if i < 4 && f.ProjectCode == "ABCD" {
			t.Fatalf("project byte %d ignored", i)
		}
		if i >= 4 && f.ReleaseBytes == "01020304" {
			t.Fatalf("release byte %d ignored", i-4)
		}
	}
	for _, tc := range []struct {
		line, iface, time, id string
		endpoint              int
	}{
		{"(0.0) simA 1802F400#" + base, "simA", "(0.0)", "1802F400", 0},
		{"(9.5) simB 1802f4ff#" + base, "simB", "(9.5)", "1802f4ff", 255},
	} {
		f := classify(t, tc.line, Options{}).Frames[0]
		if f.Interface != tc.iface || f.Timestamp != tc.time || f.Identifier != tc.id || f.Route != tc.id || f.Endpoint == nil || *f.Endpoint != tc.endpoint || f.Raw != tc.line || f.Epoch != nil || f.Sequence != nil || f.MonotonicTime != nil || f.FieldAge != nil {
			t.Fatalf("evidence changed: %+v", f)
		}
	}
	f := classify(t, "sim0 1802F407 [8] 41 42 43 44 01 02 03 04", Options{}).Frames[0]
	if f.RawDLC != "[8]" || f.DLC != 8 || f.Payload != "41 42 43 44 01 02 03 04" || !f.IDE || f.IdentityGrade != "Proven" {
		t.Fatal(f)
	}
}

func TestCX02RecordBoundAndInvalidUTF8(t *testing.T) {
	r, err := Classify(strings.NewReader(strings.Repeat("\n", MaxFrames+1)), Options{})
	if err == nil || len(r.Frames) != MaxFrames {
		t.Fatal("record limit not enforced", len(r.Frames), err)
	}
	raw := "sim0 1802F407#" + string([]byte{255})
	f := classify(t, raw, Options{}).Frames[0]
	if len(f.Candidates) != 0 || len(f.Diagnostics) == 0 || f.RawHex != hex.EncodeToString([]byte(raw)) {
		t.Fatal("invalid UTF-8 bytes not retained", f)
	}
}

func TestCX02CompactAndFDLengthMetadata(t *testing.T) {
	cases := []struct {
		line  string
		flag  string
		dlc   int
		clean bool
	}{
		{"can0 123#R8", "RTR", 8, true},
		{"can0 123#R", "RTR", 0, true},
		{"can0 123##1" + "00112233445566778899aabb", "FD", 12, true},
		{"can0 123 [12] 00 11 22 33 44 55 66 77 88 99 aa bb", "FD", 12, true},
	}
	for _, c := range cases {
		r := classify(t, c.line+"\n", Options{})
		f := r.Frames[0]
		if f.DLC != c.dlc || !hasFlag(f.Flags, c.flag) || len(f.Candidates) != 0 {
			t.Fatalf("%q: dlc=%d flags=%v candidates=%v diag=%v", c.line, f.DLC, f.Flags, f.Candidates, f.Diagnostics)
		}
		for _, d := range f.Diagnostics {
			if strings.Contains(d, "length does not match") || strings.Contains(d, "invalid") {
				t.Fatalf("%q: unexpected diagnostic %q", c.line, d)
			}
		}
	}
}

func TestCX02CompactRawDLCSuffix(t *testing.T) {
	f := classify(t, "sim0 311#1122334455667788_E", Options{}).Frames[0]
	if f.RawDLC != "E" || f.DLC != 8 || f.Payload != "1122334455667788" {
		t.Fatal(f)
	}
	for _, d := range f.Diagnostics {
		if strings.Contains(d, "payload") {
			t.Fatal(f.Diagnostics)
		}
	}
	r := classify(t, "sim0 123#R8_9", Options{}).Frames[0]
	if r.RawDLC != "9" || r.DLC != 8 || !hasFlag(r.Flags, "RTR") {
		t.Fatal(r)
	}
	for _, d := range r.Diagnostics {
		if strings.Contains(d, "invalid RTR length") {
			t.Fatal(r.Diagnostics)
		}
	}
}

func TestCX02RTRLengthAndBracketForm(t *testing.T) {
	bad := classify(t, "sim0 123#R9", Options{}).Frames[0]
	found := false
	for _, d := range bad.Diagnostics {
		found = found || d == "invalid RTR length"
	}
	if !found || bad.DLC != 0 {
		t.Fatal(bad)
	}
	ok := classify(t, "sim0 123 [8] remote request", Options{}).Frames[0]
	if !hasFlag(ok.Flags, "RTR") || ok.DLC != 8 {
		t.Fatal(ok)
	}
	for _, d := range ok.Diagnostics {
		if strings.Contains(d, "byte width") || strings.Contains(d, "payload") {
			t.Fatal(ok.Diagnostics)
		}
	}
}
