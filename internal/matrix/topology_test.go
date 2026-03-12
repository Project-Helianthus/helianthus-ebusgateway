package matrix

import "testing"

func TestGenerateTopologyCasesMatrixCounts(t *testing.T) {
	t.Parallel()

	cases := GenerateTopologyCases()
	if len(cases) != 88 {
		t.Fatalf("len(cases) = %d; want 88", len(cases))
	}

	if cases[0].ID != "T01" {
		t.Fatalf("first case id = %q; want T01", cases[0].ID)
	}
	if cases[len(cases)-1].ID != "T88" {
		t.Fatalf("last case id = %q; want T88", cases[len(cases)-1].ID)
	}

	seen := make(map[string]struct{}, len(cases))
	countByKind := make(map[TopologyKind]int)
	for _, candidate := range cases {
		if _, duplicate := seen[candidate.ID]; duplicate {
			t.Fatalf("duplicate id %q", candidate.ID)
		}
		seen[candidate.ID] = struct{}{}
		countByKind[candidate.Kind]++
	}

	if countByKind[TopologyDirectAdapter] != 4 {
		t.Fatalf(
			"direct count = %d; want 4",
			countByKind[TopologyDirectAdapter],
		)
	}
	if countByKind[TopologyViaEbusdTCP] != 4 {
		t.Fatalf(
			"ebusd count = %d; want 4",
			countByKind[TopologyViaEbusdTCP],
		)
	}
	if countByKind[TopologyProxySingle] != 16 {
		t.Fatalf(
			"proxy-single count = %d; want 16",
			countByKind[TopologyProxySingle],
		)
	}
	if countByKind[TopologyProxyDual] != 64 {
		t.Fatalf(
			"proxy-dual count = %d; want 64",
			countByKind[TopologyProxyDual],
		)
	}
}

func TestFilterCases(t *testing.T) {
	t.Parallel()

	cases := GenerateTopologyCases()
	filtered := FilterCases(cases, []string{"T02", "T09", "T88"})
	if len(filtered) != 3 {
		t.Fatalf("len(filtered) = %d; want 3", len(filtered))
	}
	if filtered[0].ID != "T02" || filtered[1].ID != "T09" || filtered[2].ID != "T88" {
		t.Fatalf("filtered ids = %#v", filtered)
	}
}

func TestGeneratePassiveSmokeCases(t *testing.T) {
	t.Parallel()

	cases := GeneratePassiveSmokeCases()
	if len(cases) != 6 {
		t.Fatalf("len(cases) = %d; want 6", len(cases))
	}

	want := []struct {
		id          string
		kind        TopologyKind
		transport   Transport
		passiveMode string
	}{
		{id: "P01", kind: TopologyDirectAdapter, transport: TransportENS, passiveMode: "unsupported_or_misconfigured"},
		{id: "P02", kind: TopologyDirectAdapter, transport: TransportENH, passiveMode: "unsupported_or_misconfigured"},
		{id: "P03", kind: TopologyProxySingle, transport: TransportENS, passiveMode: "required"},
		{id: "P04", kind: TopologyProxyDual, transport: TransportENS, passiveMode: "required"},
		{id: "P05", kind: TopologyProxyDual, transport: TransportENH, passiveMode: "required"},
		{id: "P06", kind: TopologyViaEbusdTCP, transport: TransportEbusdTCP, passiveMode: "unsupported_or_misconfigured"},
	}

	for index, expected := range want {
		candidate := cases[index]
		if candidate.ID != expected.id {
			t.Fatalf("cases[%d].ID = %q; want %q", index, candidate.ID, expected.id)
		}
		if candidate.Kind != expected.kind {
			t.Fatalf("cases[%d].Kind = %q; want %q", index, candidate.Kind, expected.kind)
		}
		if candidate.GatewayTransport != expected.transport {
			t.Fatalf("cases[%d].GatewayTransport = %q; want %q", index, candidate.GatewayTransport, expected.transport)
		}
		if candidate.PassiveMode != expected.passiveMode {
			t.Fatalf("cases[%d].PassiveMode = %q; want %q", index, candidate.PassiveMode, expected.passiveMode)
		}
	}
}

func TestCasesForSuite(t *testing.T) {
	t.Parallel()

	full, err := CasesForSuite("")
	if err != nil {
		t.Fatalf("CasesForSuite(full) error = %v", err)
	}
	if len(full) != 88 {
		t.Fatalf("len(full) = %d; want 88", len(full))
	}

	passive, err := CasesForSuite(SuitePassive)
	if err != nil {
		t.Fatalf("CasesForSuite(passive) error = %v", err)
	}
	if len(passive) != 6 {
		t.Fatalf("len(passive) = %d; want 6", len(passive))
	}

	if _, err := CasesForSuite("unknown"); err == nil {
		t.Fatalf("CasesForSuite(unknown) should fail")
	}
}
