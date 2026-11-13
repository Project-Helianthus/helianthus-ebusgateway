package ebusstandard_test

import (
	"errors"
	"testing"

	mcpstandard "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebusstandard"
	ebusregstandard "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebusstandard"
)

func loadCatalog(t *testing.T) ebusregstandard.Catalog {
	t.Helper()
	return ebusregstandard.MustEmbeddedCatalog()
}

func TestServer_ServicesList_IncludesKnownServices(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	out := s.ServicesList()
	if out["namespace"] != "ebus_standard" {
		t.Fatalf("namespace = %v, want ebus_standard", out["namespace"])
	}
	svcs, ok := out["services"].([]map[string]any)
	if !ok {
		t.Fatalf("services has wrong type %T", out["services"])
	}
	if len(svcs) == 0 {
		t.Fatal("catalog unexpectedly empty")
	}
}

func TestServer_ServicesList_Sorted(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	out := s.ServicesList()
	svcs := out["services"].([]map[string]any)
	for i := 1; i < len(svcs); i++ {
		prev := svcs[i-1]["pb"].(int)
		cur := svcs[i]["pb"].(int)
		if cur < prev {
			t.Fatalf("services not sorted by pb: %d before %d", prev, cur)
		}
	}
}

func TestServer_CommandsList_FilterByPB(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	pb := uint8(0x03)
	out, err := s.CommandsList(&pb)
	if err != nil {
		t.Fatalf("CommandsList err: %v", err)
	}
	cmds := out["commands"].([]map[string]any)
	for _, c := range cmds {
		if c["pb"].(int) != int(pb) {
			t.Fatalf("filter leaked pb=%v", c["pb"])
		}
		if _, ok := c["safety_class"]; !ok {
			t.Fatal("command summary missing safety_class")
		}
	}
}

func TestServer_CommandGet_ReturnsFullIdentity(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	id := "ebus_standard.service_data.start_counts"
	out, err := s.CommandGet(id)
	if err != nil {
		t.Fatalf("CommandGet(%q) err: %v", id, err)
	}
	cmd := out["command"].(map[string]any)
	identity := cmd["identity"].(map[string]any)
	want := []string{
		"namespace", "pb", "sb", "selector_path", "telegram_class",
		"direction", "request_or_response_role", "broadcast_or_addressed",
		"answer_policy", "length_prefix_mode", "selector_decoder",
		"service_variant", "transport_capability_requirements", "version",
	}
	for _, k := range want {
		if _, ok := identity[k]; !ok {
			t.Fatalf("identity missing axis %q", k)
		}
	}
}

func TestServer_CommandGet_UnknownReturnsSentinel(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	_, err := s.CommandGet("ebus_standard.nope.nope")
	if err == nil {
		t.Fatal("want error for unknown id")
	}
	if !errors.Is(err, mcpstandard.ErrUnknownCommand) {
		t.Fatalf("want errors.Is ErrUnknownCommand, got %v", err)
	}
}

func TestServer_Decode_HandlesKnownIdentity(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	// start_counts: pb=0x03 sb=0x04 direction=request telegram=addressed
	out, err := s.Decode(mcpstandard.DecodeInput{
		PB: 0x03, SB: 0x04,
		Direction:  "request",
		FrameType:  "addressed",
		PayloadHex: "0102",
	})
	if err != nil {
		t.Fatalf("Decode err: %v", err)
	}
	raw := out["raw_bytes"].([]int)
	if len(raw) != 2 || raw[0] != 1 || raw[1] != 2 {
		t.Fatalf("raw_bytes = %v, want [1,2]", raw)
	}
	if _, ok := out["validity"]; !ok {
		t.Fatal("decode missing validity")
	}
	if _, ok := out["replacement_value"]; !ok {
		t.Fatal("decode missing replacement_value")
	}
}

func TestServer_Decode_UnknownIdentityReturnsSentinel(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	_, err := s.Decode(mcpstandard.DecodeInput{
		PB:         0xAB,
		SB:         0xCD,
		Direction:  "request",
		FrameType:  "addressed",
		PayloadHex: "",
	})
	if err == nil {
		t.Fatal("want error for unknown identity")
	}
	if !errors.Is(err, mcpstandard.ErrUnknownCommand) {
		t.Fatalf("want errors.Is ErrUnknownCommand, got %v", err)
	}
}

// TestServer_Decode_RequiresDirectionAndFrameType pins that a decode
// call without direction or frame_type is rejected as INVALID_PAYLOAD
// rather than silently matching the first (pb, sb) row — which would
// return the wrong command when the catalog carries multiple variants
// on the same (pb, sb). Regression for PR #505 r3106756020.
func TestServer_Decode_RequiresDirectionAndFrameType(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	cases := []mcpstandard.DecodeInput{
		{PB: 0x03, SB: 0x04, FrameType: "addressed", PayloadHex: "0102"}, // missing direction
		{PB: 0x03, SB: 0x04, Direction: "request", PayloadHex: "0102"},   // missing frame_type
		{PB: 0x03, SB: 0x04, PayloadHex: "0102"},                         // missing both
	}
	for i, in := range cases {
		_, err := s.Decode(in)
		if err == nil {
			t.Fatalf("case %d: want error for missing selectors, got nil", i)
		}
		if !errors.Is(err, mcpstandard.ErrInvalidPayload) {
			t.Fatalf("case %d: want errors.Is ErrInvalidPayload, got %v", i, err)
		}
	}
}

func TestServer_Decode_BadHexReturnsInvalidPayload(t *testing.T) {
	s := mcpstandard.NewServer(loadCatalog(t))
	_, err := s.Decode(mcpstandard.DecodeInput{PB: 0x03, SB: 0x04, Direction: "request", FrameType: "addressed", PayloadHex: "zz"})
	if err == nil {
		t.Fatal("want error for bad hex")
	}
	if !errors.Is(err, mcpstandard.ErrInvalidPayload) {
		t.Fatalf("want errors.Is ErrInvalidPayload, got %v", err)
	}
}

func TestToolNames_Canonical(t *testing.T) {
	cases := map[string]string{
		mcpstandard.ToolServicesList: "ebus_v1_ebus_standard_services_list",
		mcpstandard.ToolCommandsList: "ebus_v1_ebus_standard_commands_list",
		mcpstandard.ToolCommandGet:   "ebus_v1_ebus_standard_command_get",
		mcpstandard.ToolDecode:       "ebus_v1_ebus_standard_decode",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("tool name got %q want %q", got, want)
		}
	}
}
