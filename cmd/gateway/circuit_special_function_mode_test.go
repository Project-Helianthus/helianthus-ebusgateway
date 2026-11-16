package main

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	graphqlgo "github.com/graphql-go/graphql"
)

// The tests in this file use synthetic replies only. The circuit register
// RR001D is addressed by its literal selector so the expectation does not
// depend on the production constant.
const (
	sfmTestCircuitGroup    = byte(0x02)
	sfmTestCircuitInstance = byte(0x00)
	sfmTestRegister        = uint16(0x001D)
	sfmTestFloatRegister   = uint16(0x0010)
	sfmTestCounterName     = "semantic_b524_width_mismatch_total"
	sfmTestLogMarker       = "semantic: b524 reply width mismatch"
)

// sfmReplyBus answers B524 reads from a selector-keyed table. A selector
// without an entry gets a header-only reply, which the reader treats as no
// value (not as a width mismatch).
type sfmReplyBus struct {
	mu       sync.Mutex
	replies  map[sfmSelector][]byte
	requests int
}

type sfmSelector struct {
	opcode   byte
	group    byte
	instance byte
	addr     uint16
}

func (bus *sfmReplyBus) send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	bus.requests++
	if len(frame.Data) != 6 {
		return &protocol.Frame{Data: []byte{0x00}}, nil
	}
	key := sfmSelector{
		opcode:   frame.Data[0],
		group:    frame.Data[2],
		instance: frame.Data[3],
		addr:     uint16(frame.Data[4]) | uint16(frame.Data[5])<<8,
	}
	return testB524ResponseForSelectorPayload(frame.Data, bus.replies[key]...), nil
}

func (bus *sfmReplyBus) requestCount() int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return bus.requests
}

// sfmCircuitPoller returns a poller that refreshes one known active circuit
// (instance 0, heating type) and answers the given circuit registers.
func sfmCircuitPoller(now *time.Time, circuitReplies map[uint16][]byte) (*vaillantSemanticPoller, *graphql.LiveSemanticProvider) {
	replies := map[sfmSelector][]byte{
		{opcode: 0x02, group: sfmTestCircuitGroup, instance: sfmTestCircuitInstance, addr: 0x0002}: {0x01, 0x00},
	}
	for addr, payload := range circuitReplies {
		replies[sfmSelector{opcode: 0x02, group: sfmTestCircuitGroup, instance: sfmTestCircuitInstance, addr: addr}] = payload
	}
	bus := &sfmReplyBus{replies: replies}
	provider := graphql.NewLiveSemanticProvider()
	existingType := uint16(1)
	poller := &vaillantSemanticPoller{
		controller:                  0x15,
		source:                      0x7F,
		publication:                 newVaillantSemanticPublication(provider, nil, nil),
		scheduler:                   ebusgateway.NewSemanticReadScheduler(),
		requestTimeout:              50 * time.Millisecond,
		circuitFullScanInterval:     semanticCircuitFullScanInterval,
		lastCircuitFullScanAt:       now.Add(-5 * time.Minute),
		lastCircuitFullScanComplete: true,
		nowFn:                       func() time.Time { return *now },
		pauseFn:                     sfmNoPause,
		circuits: map[byte]*vaillantCircuitSnapshot{
			sfmTestCircuitInstance: {Instance: sfmTestCircuitInstance, Active: true, CircuitTypeRaw: &existingType},
		},
		sendFrameFn: bus.send,
	}
	return poller, provider
}

// sfmReaderPoller returns a poller whose bus answers exactly the given
// selector table.
func sfmReaderPoller(now *time.Time, replies map[sfmSelector][]byte) *vaillantSemanticPoller {
	bus := &sfmReplyBus{replies: replies}
	return &vaillantSemanticPoller{
		controller:     0x15,
		source:         0x7F,
		publication:    newVaillantSemanticPublication(graphql.NewLiveSemanticProvider(), nil, nil),
		scheduler:      ebusgateway.NewSemanticReadScheduler(),
		requestTimeout: 50 * time.Millisecond,
		nowFn:          func() time.Time { return *now },
		pauseFn:        sfmNoPause,
		sendFrameFn:    bus.send,
	}
}

// sfmNoPause skips the waits between read attempts.
func sfmNoPause(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

// sfmCounter returns the current value of one reader kind in the width
// mismatch counter map, failing the test when the map is not published.
func sfmCounter(t *testing.T, kind string) int64 {
	t.Helper()
	published, ok := expvar.Get(sfmTestCounterName).(*expvar.Map)
	if !ok {
		t.Fatalf("expvar %s is not published as a map", sfmTestCounterName)
	}
	value, ok := published.Get(kind).(*expvar.Int)
	if !ok || value == nil {
		return 0
	}
	return value.Value()
}

// sfmCaptureLog redirects the standard logger for the duration of the test
// and returns a function yielding the width mismatch lines logged so far.
// Callers must not run in parallel.
func sfmCaptureLog(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var buf bytes.Buffer
	previous := log.Writer()
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(previous) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		var lines []string
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, sfmTestLogMarker) {
				lines = append(lines, line[strings.Index(line, sfmTestLogMarker):])
			}
		}
		return lines
	}
}

type writerFunc func([]byte) (int, error)

func (fn writerFunc) Write(p []byte) (int, error) { return fn(p) }

func sfmMismatchLine(opcode, group byte, addr uint16, want, got int) string {
	return fmt.Sprintf("%s opcode=0x%02X group=0x%02X register=0x%04X want=%d got=%d", sfmTestLogMarker, opcode, group, addr, want, got)
}

// sfmPublishedConfig returns the circuit configuration of the only published
// circuit as the GraphQL, MCP and Portal surfaces encode it.
func sfmPublishedConfig(t *testing.T, provider *graphql.LiveSemanticProvider) (gql, mcpConfig, portalConfig map[string]any) {
	t.Helper()
	circuits := provider.Circuits()
	if len(circuits) != 1 {
		t.Fatalf("published circuits = %d; want 1", len(circuits))
	}

	builder := graphql.NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatal(err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ circuits { index config { circuit_special_function_mode } } }`})
	if len(result.Errors) > 0 {
		t.Fatalf("GraphQL errors: %v", result.Errors)
	}
	raw, err := json.Marshal(result.Data)
	if err != nil {
		t.Fatal(err)
	}
	var gqlDoc struct {
		Circuits []struct {
			Config map[string]any `json:"config"`
		} `json:"circuits"`
	}
	if err := json.Unmarshal(raw, &gqlDoc); err != nil || len(gqlDoc.Circuits) != 1 {
		t.Fatalf("GraphQL circuits: %s (%v)", raw, err)
	}

	decode := func(value any) map[string]any {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]any{}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	mcpCircuits := mcpSemanticProviderAdapter{provider: provider}.Circuits()
	portalCircuits := mapPortalCircuits(circuits)
	if len(mcpCircuits) != 1 || len(portalCircuits) != 1 {
		t.Fatalf("MCP circuits = %d, Portal circuits = %d; want 1 each", len(mcpCircuits), len(portalCircuits))
	}
	return gqlDoc.Circuits[0].Config, decode(mcpCircuits[0].Config), decode(portalCircuits[0].Config)
}

// TestCircuitSpecialFunctionModePublishesOneByteReply reads RR001D as one
// unsigned byte and publishes it unchanged on GraphQL, MCP and Portal.
func TestCircuitSpecialFunctionModePublishesOneByteReply(t *testing.T) {
	for _, value := range []byte{0x00, 0x03, 0x7E} {
		t.Run(fmt.Sprintf("value_%d", value), func(t *testing.T) {
			now := time.Unix(1000, 0)
			poller, provider := sfmCircuitPoller(&now, map[uint16][]byte{sfmTestRegister: {value}})
			before := sfmCounter(t, "u8")
			poller.refreshCircuits(context.Background())

			gql, mcpConfig, portalConfig := sfmPublishedConfig(t, provider)
			for surface, config := range map[string]map[string]any{"graphql": gql, "mcp": mcpConfig, "portal": portalConfig} {
				got, ok := config["circuit_special_function_mode"]
				if !ok {
					t.Fatalf("%s circuit config lacks circuit_special_function_mode: %v", surface, config)
				}
				if number, ok := got.(float64); !ok || number != float64(value) {
					t.Fatalf("%s circuit_special_function_mode = %#v; want %d", surface, got, value)
				}
			}
			if delta := sfmCounter(t, "u8") - before; delta != 0 {
				t.Fatalf("one-byte reply counted as mismatch %d times", delta)
			}
		})
	}
}

// TestCircuitSpecialFunctionModeHasNoAliasAndNoWrite checks that the old
// float field is gone from every surface and that neither the old setter
// spellings nor the new name reach the bus through set_circuit_config.
func TestCircuitSpecialFunctionModeHasNoAliasAndNoWrite(t *testing.T) {
	now := time.Unix(1000, 0)
	poller, provider := sfmCircuitPoller(&now, map[uint16][]byte{sfmTestRegister: {0x02}})
	poller.refreshCircuits(context.Background())
	gql, mcpConfig, portalConfig := sfmPublishedConfig(t, provider)
	retired := "frost" + "_prot_c"
	for surface, config := range map[string]map[string]any{"graphql": gql, "mcp": mcpConfig, "portal": portalConfig} {
		if _, ok := config[retired]; ok {
			t.Errorf("%s still publishes the retired field", surface)
		}
	}
	builder := graphql.NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatal(err)
	}
	for _, old := range []string{retired, "frost" + "ProtC"} {
		result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ circuits { config { ` + old + ` } } }`})
		if len(result.Errors) == 0 {
			t.Errorf("GraphQL accepts the retired field %s", old)
		}
	}

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV0"})
	bus := &sfmReplyBus{replies: map[sfmSelector][]byte{}}
	invoker := &dhwBoundInvoker{}
	writeBuilder := graphql.NewBuilder(reg, nil)
	writeBuilder.SetAdmittedMutationSource(0x71)
	handler, err := graphql.NewInvokeHandler(writeBuilder, reg, invoker)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"frost" + "ProtC", retired, "circuit_special_function_mode"} {
		body, err := json.Marshal(map[string]any{
			"query":     `mutation Write($field: String!, $value: String!) { set_circuit_config(index: 0, field: $field, value: $value) { success error } }`,
			"variables": map[string]string{"field": field, "value": "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var response struct {
			Data struct {
				Set graphql.ConfigMutationResult `json:"set_circuit_config"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || len(response.Errors) != 0 {
			t.Fatalf("set_circuit_config %s: err=%v body=%s", field, err, rec.Body.String())
		}
		if response.Data.Set.Success || !strings.Contains(response.Data.Set.Error, fmt.Sprintf("unknown circuit field %q", field)) {
			t.Fatalf("set_circuit_config %s = %+v; want unknown circuit field", field, response.Data.Set)
		}
	}
	if invoker.calls != 0 || bus.requestCount() != 0 {
		t.Fatalf("rejected circuit setter reached the bus: invokes=%d requests=%d", invoker.calls, bus.requestCount())
	}
}

// TestB524ReplyWidthMismatchOnCircuitRefresh covers both directions on the
// circuit refresh: a four-byte RR001D reply to the one-byte reader and a
// one-byte reply to a four-byte reader. Each is counted once under its reader
// kind, publishes no value and logs one line with selector and widths.
func TestB524ReplyWidthMismatchOnCircuitRefresh(t *testing.T) {
	cases := []struct {
		name      string
		addr      uint16
		payload   []byte
		kind      string
		want, got int
		absent    func(graphql.CircuitConfig) bool
	}{
		{
			name: "four_bytes_to_one_byte_reader", addr: sfmTestRegister, payload: []byte{0x00, 0x00, 0xA0, 0x41},
			kind: "u8", want: 1, got: 4,
		},
		{
			name: "one_byte_to_four_byte_reader", addr: sfmTestFloatRegister, payload: []byte{0x2A},
			kind: "float32le", want: 4, got: 1,
			absent: func(config graphql.CircuitConfig) bool { return config.FlowTempMaxC == nil },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := sfmCaptureLog(t)
			now := time.Unix(1000, 0)
			poller, provider := sfmCircuitPoller(&now, map[uint16][]byte{tc.addr: tc.payload})
			before := sfmCounter(t, tc.kind)
			poller.refreshCircuits(context.Background())

			if delta := sfmCounter(t, tc.kind) - before; delta != 1 {
				t.Fatalf("%s mismatch counter delta = %d; want 1", tc.kind, delta)
			}
			wantLine := sfmMismatchLine(0x02, sfmTestCircuitGroup, tc.addr, tc.want, tc.got)
			if got := lines(); len(got) != 1 || got[0] != wantLine {
				t.Fatalf("mismatch log = %q; want exactly %q", got, wantLine)
			}
			gql, mcpConfig, portalConfig := sfmPublishedConfig(t, provider)
			if tc.absent == nil {
				for surface, config := range map[string]map[string]any{"graphql": gql, "mcp": mcpConfig, "portal": portalConfig} {
					if value, ok := config["circuit_special_function_mode"]; ok && value != nil {
						t.Fatalf("%s publishes %v for a wrong-width reply", surface, value)
					}
				}
			} else if !tc.absent(provider.Circuits()[0].Config) {
				t.Fatalf("wrong-width reply published a value: %+v", provider.Circuits()[0].Config)
			}
		})
	}
}

// TestB524ReplyWidthMismatchEveryFixedWidthReader sends one reply of the
// wrong width to each fixed-width reader on a selector of its own, and one
// reply of the right width, and checks value, counter and log for both.
func TestB524ReplyWidthMismatchEveryFixedWidthReader(t *testing.T) {
	type reader func(p *vaillantSemanticPoller, sel sfmSelector) bool
	cases := []struct {
		name     string
		kind     string
		sel      sfmSelector
		wrong    []byte
		right    []byte
		want     int
		hasValue reader
	}{
		{"u8", "u8", sfmSelector{0x06, 0x0A, 0x01, 0x0019}, []byte{0x05, 0x00}, []byte{0x05}, 1,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				return p.readB524U8(context.Background(), s.opcode, s.group, s.instance, s.addr) != nil
			}},
		{"bool", "u8", sfmSelector{0x06, 0x0A, 0x01, 0x001E}, []byte{0x01, 0x00, 0x00, 0x00}, []byte{0x01}, 1,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				return p.readB524Bool(context.Background(), s.opcode, s.group, s.instance, s.addr) != nil
			}},
		{"uint16", "uint16", sfmSelector{0x02, 0x02, 0x00, 0x0015}, []byte{0x01, 0x00, 0x00}, []byte{0x01, 0x00}, 2,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				v, ok := p.readB524Uint16(context.Background(), s.opcode, s.group, s.instance, s.addr)
				return ok && v != nil
			}},
		{"u16", "uint16", sfmSelector{0x06, 0x0C, 0x02, 0x0023}, []byte{0x01, 0x02, 0x03, 0x04}, []byte{0x34, 0x12}, 2,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				return p.readB524U16(context.Background(), s.opcode, s.group, s.instance, s.addr) != nil
			}},
		{"float32le", "float32le", sfmSelector{0x02, 0x03, 0x01, 0x0014}, []byte{0x00, 0x00, 0xA0}, testB524Float32Payload(20), 4,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				_, ok := p.readB524Float32LE(context.Background(), s.opcode, s.group, s.instance, s.addr)
				return ok
			}},
		{"f32", "float32le", sfmSelector{0x06, 0x09, 0x03, 0x000F}, []byte{0x00, 0x00, 0xA0, 0x41, 0x00}, testB524Float32Payload(21), 4,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				return p.readB524F32(context.Background(), s.opcode, s.group, s.instance, s.addr) != nil
			}},
		{"uint32le", "uint32le", sfmSelector{0x02, 0x02, 0x01, 0x0024}, []byte{0x10, 0x00}, []byte{0x10, 0x00, 0x00, 0x00}, 4,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				_, ok := p.readB524Uint32LE(context.Background(), s.opcode, s.group, s.instance, s.addr)
				return ok
			}},
		{"energy_register", "uint32le", sfmSelector{vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, b524EnergyQueries[0].addr}, []byte{0x10, 0x00, 0x00, 0x00, 0x00}, []byte{0x10, 0x00, 0x00, 0x00}, 4,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				_, ok := p.readEnergyRegister(context.Background(), b524EnergyQueries[0])
				return ok
			}},
		{"date_hda3", "date_hda3", sfmSelector{0x02, 0x00, 0x00, 0x002C}, []byte{0x02, 0x01}, []byte{0x02, 0x01, 0x18}, 3,
			func(p *vaillantSemanticPoller, s sfmSelector) bool {
				_, ok := p.readB524DateHDA3(context.Background(), s.opcode, s.group, s.instance, s.addr)
				return ok
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := sfmCaptureLog(t)
			now := time.Unix(1000, 0)

			right := sfmReaderPoller(&now, map[sfmSelector][]byte{tc.sel: tc.right})
			before := sfmCounter(t, tc.kind)
			if !tc.hasValue(right, tc.sel) {
				t.Fatalf("right-width reply of %d bytes returned no value", len(tc.right))
			}
			if delta := sfmCounter(t, tc.kind) - before; delta != 0 || len(lines()) != 0 {
				t.Fatalf("right-width reply reported: counter delta %d, log %q", delta, lines())
			}

			wrong := sfmReaderPoller(&now, map[sfmSelector][]byte{tc.sel: tc.wrong})
			before = sfmCounter(t, tc.kind)
			if tc.hasValue(wrong, tc.sel) {
				t.Fatalf("wrong-width reply of %d bytes returned a value", len(tc.wrong))
			}
			if delta := sfmCounter(t, tc.kind) - before; delta != 1 {
				t.Fatalf("%s counter delta = %d; want 1", tc.kind, delta)
			}
			wantLine := sfmMismatchLine(tc.sel.opcode, tc.sel.group, tc.sel.addr, tc.want, len(tc.wrong))
			if got := lines(); len(got) != 1 || got[0] != wantLine {
				t.Fatalf("mismatch log = %q; want exactly %q", got, wantLine)
			}
		})
	}
}

// TestB524ReplyWidthMismatchCircuitRR0020StaysSilent keeps the deliberate
// rejection of the circuit RR0020 selector out of the mismatch report.
func TestB524ReplyWidthMismatchCircuitRR0020StaysSilent(t *testing.T) {
	lines := sfmCaptureLog(t)
	now := time.Unix(1000, 0)
	for _, payload := range [][]byte{{0x01}, testB524Float32Payload(46.25)} {
		sel := sfmSelector{0x02, 0x02, 0x00, 0x0020}
		poller := sfmReaderPoller(&now, map[sfmSelector][]byte{sel: payload})
		before := sfmCounter(t, "float32le")
		if _, ok := poller.readB524Float32LE(context.Background(), sel.opcode, sel.group, sel.instance, sel.addr); ok {
			t.Fatal("circuit RR0020 returned a value")
		}
		if delta := sfmCounter(t, "float32le") - before; delta != 0 || len(lines()) != 0 {
			t.Fatalf("circuit RR0020 rejection reported: counter delta %d, log %q", delta, lines())
		}
	}
}

// TestB524ReplyWidthMismatchLogIsRateLimitedPerSelector checks the ten-minute
// window per selector. Every field of the logged selector is varied on its
// own (opcode, group, register): a change of any one of them is a different
// selector and logs at once. The instance is not part of the logged selector.
func TestB524ReplyWidthMismatchLogIsRateLimitedPerSelector(t *testing.T) {
	lines := sfmCaptureLog(t)
	now := time.Unix(1000, 0)
	base := sfmSelector{0x02, 0x02, 0x00, 0x001D}
	otherOpcode := sfmSelector{0x06, 0x02, 0x00, 0x001D}
	otherGroup := sfmSelector{0x02, 0x03, 0x00, 0x001D}
	otherRegister := sfmSelector{0x02, 0x02, 0x00, 0x001E}
	otherInstance := sfmSelector{0x02, 0x02, 0x01, 0x001D}
	wrong := []byte{0x01, 0x02, 0x03, 0x04}
	poller := sfmReaderPoller(&now, map[sfmSelector][]byte{
		base: wrong, otherOpcode: wrong, otherGroup: wrong, otherRegister: wrong, otherInstance: wrong,
	})
	read := func(sel sfmSelector) {
		t.Helper()
		if poller.readB524U8(context.Background(), sel.opcode, sel.group, sel.instance, sel.addr) != nil {
			t.Fatal("wrong-width reply returned a value")
		}
	}
	line := func(sel sfmSelector) string { return sfmMismatchLine(sel.opcode, sel.group, sel.addr, 1, 4) }

	before := sfmCounter(t, "u8")
	read(base)
	now = now.Add(time.Minute)
	read(base)
	if delta := sfmCounter(t, "u8") - before; delta != 2 {
		t.Fatalf("counter delta after two mismatches = %d; want 2", delta)
	}
	if got := lines(); len(got) != 1 || got[0] != line(base) {
		t.Fatalf("log after two mismatches of one selector = %q; want one line", got)
	}

	read(otherInstance)
	if got := lines(); len(got) != 1 {
		t.Fatalf("another instance of the same logged selector logged again: %q", got)
	}
	want := []string{line(base)}
	for _, sel := range []sfmSelector{otherOpcode, otherGroup, otherRegister} {
		read(sel)
		want = append(want, line(sel))
		if got := lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("log = %q; want %q", got, want)
		}
	}

	now = time.Unix(1000, 0).Add(10*time.Minute - time.Second)
	read(base)
	if got := lines(); len(got) != len(want) {
		t.Fatalf("selector logged again inside the window: %q", got)
	}
	now = time.Unix(1000, 0).Add(10 * time.Minute)
	read(base)
	want = append(want, line(base))
	if got := lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("selector not logged again after the window: %q", got)
	}
	if delta := sfmCounter(t, "u8") - before; delta != 8 {
		t.Fatalf("counter delta = %d; want 8 (every mismatch counted)", delta)
	}
}
