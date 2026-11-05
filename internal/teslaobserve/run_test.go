package teslaobserve

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// runHarness hands each attempt a fresh scripted source on one synthetic
// clock and counts opens.
type runHarness struct {
	clock   *syntheticClock
	scripts [][]scriptedByte
	opens   int
}

func (harness *runHarness) open() (ByteSource, error) {
	if harness.opens >= len(harness.scripts) {
		harness.opens++
		return nil, errors.New("synthetic open failure")
	}
	script := harness.scripts[harness.opens]
	harness.opens++
	return &scriptedSource{clock: harness.clock, bytes: script}, nil
}

// shifted places a script after the harness clock's current reading.
func shifted(script []scriptedByte, by time.Duration) []scriptedByte {
	out := make([]scriptedByte, len(script))
	for index, item := range script {
		out[index] = scriptedByte{value: item.value, at: item.at + by}
		for _, reading := range item.then {
			out[index].then = append(out[index].then, reading+by)
		}
	}
	return out
}

func newRunConfig(harness *runHarness, kind string, repeats int) RunConfig {
	return RunConfig{
		Endpoint: "synthetic-e", Case: kind, Repeats: repeats, Window: ms(syntheticDeadline),
		RequestedBytes: syntheticRequested, SegmentCap: syntheticSegmentCap, SegmentGap: 500 * time.Microsecond,
		Operator: OperatorRecord{PriorState: "fresh_session", PriorStateBasis: "synthetic", Condition: "synthetic fixture"},
		Open:     harness.open, Now: harness.clock.now,
	}
}

func TestTeslaObserveRunFillsExactlyTheWorksheetRows(t *testing.T) {
	harness := &runHarness{clock: &syntheticClock{base: time.Unix(0, 0)}}
	harness.scripts = [][]scriptedByte{
		segmentsAt(syntheticFragments, syntheticOffsets, 5),
		shifted(segmentsAt([]string{"TESLA\x00"}, []int{1}, 0), ms(5)),
	}
	evidence, err := Run(context.Background(), newRunConfig(harness, CaseInitial, 2), nil)
	if err != nil {
		t.Fatal(err)
	}
	if harness.opens != 2 || len(evidence.Attempts) != 2 || evidence.Attempts[0].Label != "a01" || evidence.Attempts[1].Label != "a02" {
		t.Fatalf("opens %d attempts %+v", harness.opens, evidence.Attempts)
	}
	if evidence.Attempts[0].Observation.Instance == evidence.Attempts[1].Observation.Instance {
		t.Fatal("independent attempts shared an endpoint instance")
	}
	if evidence.Schema != Schema || evidence.Mode != ModeNoSend || evidence.Family != Family || evidence.FramesSent != 0 {
		t.Fatalf("header = %+v", evidence)
	}
	assertFields(t, "preconditions", evidence.Preconditions, preconditionFields, 9)
	assertFields(t, "repeats", evidence.RepeatsAndControls, repeatFields, 7)
	assertFields(t, "review", evidence.Review, reviewFields, 5)
	for _, row := range evidence.Review {
		if row.Result != GradeUnknown || row.Grade != GradeUnknown {
			t.Fatalf("review row filled by the run: %+v", row)
		}
	}
	// a01 has four segments; a02 has one partial segment.
	if len(evidence.OrderedEvidence) != 5 || evidence.OrderedEvidence[0].SegmentOrder != "a01.s1" || evidence.OrderedEvidence[4].SegmentOrder != "a02.s1" {
		t.Fatalf("ordered evidence = %+v", evidence.OrderedEvidence)
	}
	if !strings.HasPrefix(evidence.OrderedEvidence[0].NativeBytes, "54 45; 2 bytes; window complete") ||
		!strings.Contains(evidence.OrderedEvidence[4].NativeBytes, "window incomplete (deadline)") {
		t.Fatalf("ordered evidence bytes = %q / %q", evidence.OrderedEvidence[0].NativeBytes, evidence.OrderedEvidence[4].NativeBytes)
	}
	for _, row := range evidence.OrderedEvidence {
		if row.Direction != directionText || row.Framing != framingText || strings.Contains(row.Context, "ready") {
			t.Fatalf("ordered evidence row prefilled beyond observation: %+v", row)
		}
	}
	initial := evidence.RepeatsAndControls[0]
	if !strings.Contains(initial.Result, "2 attempts") || !strings.Contains(initial.Result, "2 distinct byte windows") ||
		!strings.Contains(initial.Result, "incomplete or failed: 1") || initial.Grade != GradeUnknown {
		t.Fatalf("initial row = %+v", initial)
	}
	if evidence.RepeatsAndControls[1].Result != "Unknown: no recovery attempt recorded" ||
		evidence.RepeatsAndControls[2].Result != "Unknown: no idle attempt recorded" {
		t.Fatalf("absent cases were filled: %+v", evidence.RepeatsAndControls[1:3])
	}
	if evidence.RepeatsAndControls[3].Grade != GradeProven || evidence.RepeatsAndControls[4].Grade != GradeProven {
		t.Fatalf("offline control rows = %+v", evidence.RepeatsAndControls[3:5])
	}
	if !strings.Contains(evidence.RepeatsAndControls[6].Result, "unsuccessful attempts: a02") {
		t.Fatalf("contradiction row = %+v", evidence.RepeatsAndControls[6])
	}
	if replay := evidence.Attempts[1].Lifecycle; replay == nil || replay.Ready || !replay.AcquireRefused || replay.FramesSent != 0 {
		t.Fatalf("incomplete window replay = %+v", replay)
	}
}

func assertFields(t *testing.T, name string, rows []Row, fields []string, count int) {
	t.Helper()
	if len(rows) != count || len(fields) != count {
		t.Fatalf("%s: %d rows, %d fields, want %d", name, len(rows), len(fields), count)
	}
	for index, row := range rows {
		if row.Field != fields[index] || row.Grade == "" || row.Basis == "" {
			t.Fatalf("%s row %d = %+v", name, index, row)
		}
	}
}

func TestTeslaObserveRunAppendsIndependentAttempts(t *testing.T) {
	harness := &runHarness{clock: &syntheticClock{base: time.Unix(0, 0)}, scripts: [][]scriptedByte{nil}}
	first, err := Run(context.Background(), newRunConfig(harness, CaseIdle, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeEvidence(first)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := DecodeEvidence(encoded)
	if err != nil {
		t.Fatal(err)
	}
	second := &runHarness{clock: &syntheticClock{base: time.Unix(0, 0)}}
	second.scripts = [][]scriptedByte{segmentsAt(syntheticFragments, syntheticOffsets, 5)}
	config := newRunConfig(second, CaseRecovery, 2)
	config.Operator.ActionTimeAuthorization = "recorded"
	evidence, err := Run(context.Background(), config, &prior)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Attempts) != 3 || evidence.Attempts[0].Case != CaseIdle || evidence.Attempts[1].Label != "a02" ||
		evidence.Attempts[2].OpenError == "" || evidence.Attempts[2].Observation != nil {
		t.Fatalf("attempts = %+v", evidence.Attempts)
	}
	if !strings.Contains(evidence.Preconditions[8].Result, "a02 recorded") || !strings.Contains(evidence.Preconditions[8].Result, "a01 unknown") {
		t.Fatalf("authorization row = %+v", evidence.Preconditions[8])
	}
	if !strings.Contains(evidence.RepeatsAndControls[2].Result, "a01 deadline 0 bytes") {
		t.Fatalf("idle row = %+v", evidence.RepeatsAndControls[2])
	}
	if !strings.Contains(evidence.RepeatsAndControls[1].Result, "a03 open failed") {
		t.Fatalf("recovery row = %+v", evidence.RepeatsAndControls[1])
	}
	again, err := EncodeEvidence(evidence)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEvidence(again)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, _ := EncodeEvidence(decoded)
	if !bytes.Equal(again, reencoded) {
		t.Fatal("evidence encoding is not stable across a decode/encode round trip")
	}

	for name, mutate := range map[string]func(*Evidence){
		"schema":   func(e *Evidence) { e.Schema = "other" },
		"mode":     func(e *Evidence) { e.Mode = "SEND" },
		"endpoint": func(e *Evidence) { e.Endpoint = "synthetic-f" },
		"label":    func(e *Evidence) { e.Attempts[0].Label = "a07" },
		"operator": func(e *Evidence) { e.Attempts[0].Operator.PriorState = "guessed" },
		"observation endpoint": func(e *Evidence) {
			e.Attempts[0].Observation.Endpoint = "synthetic-f"
		},
	} {
		tampered, _ := DecodeEvidence(encoded)
		mutate(&tampered)
		if _, err := Run(context.Background(), newRunConfig(&runHarness{clock: &syntheticClock{}}, CaseIdle, 1), &tampered); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: tampered prior accepted: %v", name, err)
		}
	}

	// Attempt a02 carries real segments; malformed ones must be refused
	// before the port opens instead of panicking while rows are rebuilt.
	for name, mutate := range map[string]func(*Observation){
		"empty offsets": func(o *Observation) { o.Segments[0].Offsets = nil },
		"short offsets": func(o *Observation) {
			o.Segments[0].Offsets = o.Segments[0].Offsets[:len(o.Segments[0].Offsets)-1]
		},
		"empty segment":            func(o *Observation) { o.Segments[0].Bytes, o.Segments[0].Offsets = nil, nil },
		"overflow without offsets": func(o *Observation) { o.Coverage.Overflow = []byte{0x01} },
	} {
		tampered, _ := DecodeEvidence(again)
		mutate(tampered.Attempts[1].Observation)
		harness := &runHarness{clock: &syntheticClock{}}
		if _, err := Run(context.Background(), newRunConfig(harness, CaseIdle, 1), &tampered); !errors.Is(err, ErrInvalidRequest) || harness.opens != 0 {
			t.Fatalf("%s: malformed segments accepted: %v opens %d", name, err, harness.opens)
		}
	}
}

func TestTeslaObserveRunValidatesEveryBoundBeforeOpening(t *testing.T) {
	cases := map[string]func(*RunConfig){
		"endpoint path":     func(c *RunConfig) { c.Endpoint = "/dev/ttyUSB0" },
		"case":              func(c *RunConfig) { c.Case = "reset" },
		"zero repeats":      func(c *RunConfig) { c.Repeats = 0 },
		"too many repeats":  func(c *RunConfig) { c.Repeats = MaxRepeats + 1 },
		"zero window":       func(c *RunConfig) { c.Window = 0 },
		"long window":       func(c *RunConfig) { c.Window = MaxObservationWindow + 1 },
		"zero bytes":        func(c *RunConfig) { c.RequestedBytes = 0 },
		"too many bytes":    func(c *RunConfig) { c.RequestedBytes = MaxRequestedBytes + 1 },
		"zero segment cap":  func(c *RunConfig) { c.SegmentCap = 0 },
		"large segment cap": func(c *RunConfig) { c.SegmentCap = MaxSegmentCap + 1 },
		"zero gap":          func(c *RunConfig) { c.SegmentGap = 0 },
		"gap over window":   func(c *RunConfig) { c.SegmentGap = c.Window + 1 },
		"negative pause":    func(c *RunConfig) { c.Pause = -1 },
		"long pause":        func(c *RunConfig) { c.Pause = MaxPause + 1 },
		"no opener":         func(c *RunConfig) { c.Open = nil },
		"no clock":          func(c *RunConfig) { c.Now = nil },
		"prior state":       func(c *RunConfig) { c.Operator.PriorState = "probably_active" },
		"initiators":        func(c *RunConfig) { c.Operator.OtherInitiators = "maybe" },
		"authorization":     func(c *RunConfig) { c.Operator.ActionTimeAuthorization = "yes" },
		"pipe in text":      func(c *RunConfig) { c.Operator.Condition = "a | b" },
		"newline in text":   func(c *RunConfig) { c.Operator.PriorStateBasis = "a\nb" },
		"long text":         func(c *RunConfig) { c.Operator.OtherInitiatorsBasis = strings.Repeat("x", maxOperatorText+1) },
	}
	for name, mutate := range cases {
		harness := &runHarness{clock: &syntheticClock{}}
		config := newRunConfig(harness, CaseInitial, 1)
		mutate(&config)
		if _, err := Run(context.Background(), config, nil); !errors.Is(err, ErrInvalidRequest) || harness.opens != 0 {
			t.Fatalf("%s: err=%v opens=%d", name, err, harness.opens)
		}
	}
}

func TestTeslaObserveMarkdownCarriesTheWorksheetSections(t *testing.T) {
	harness := &runHarness{clock: &syntheticClock{base: time.Unix(0, 0)}}
	harness.scripts = [][]scriptedByte{segmentsAt(syntheticFragments, syntheticOffsets, 5)}
	evidence, err := Run(context.Background(), newRunConfig(harness, CaseInitial, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	markdown := RenderMarkdown(evidence)
	for _, want := range []string{
		"## Preconditions and observation bounds", "## Ordered native evidence",
		"## Independent repeats and negative controls", "## Review disposition",
		"| Segment order | Direction and attribution basis | Complete bounded native bytes and completeness | Relative monotonic offset/gap and uncertainty | Framing/integrity result | Context and outcome |",
		"| a01.s2 | ", "Mode: NO_SEND", "Frames sent: 0",
	} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown lacks %q", want)
		}
	}
	for _, field := range append(append(append([]string(nil), preconditionFields...), repeatFields...), reviewFields...) {
		if !strings.Contains(markdown, "| "+field+" |") {
			t.Fatalf("markdown lacks row %q", field)
		}
	}
	if cell("a|b\nc") != `a\|b c` {
		t.Fatalf("cell escaping = %q", cell("a|b\nc"))
	}
}
