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
	if len(cases) != 3 {
		t.Fatalf("len(cases) = %d; want 3", len(cases))
	}

	want := []TopologyCase{
		{
			ID:               "P01",
			Kind:             TopologyDirectAdapter,
			GatewayTransport: TransportENS,
			PassiveMode:      "unsupported_or_misconfigured",
		},
		{
			ID:               "P02",
			Kind:             TopologyDirectAdapter,
			GatewayTransport: TransportENH,
			PassiveMode:      "unsupported_or_misconfigured",
		},
		{
			ID:               "P06",
			Kind:             TopologyViaEbusdTCP,
			GatewayTransport: TransportEbusdTCP,
			EbusdTransport:   TransportENS,
			UsesEbusd:        true,
			PassiveMode:      "unsupported_or_misconfigured",
		},
	}

	for index, expected := range want {
		candidate := cases[index]
		if candidate.ID != expected.ID {
			t.Fatalf("cases[%d].ID = %q; want %q", index, candidate.ID, expected.ID)
		}
		if candidate.Kind != expected.Kind {
			t.Fatalf("cases[%d].Kind = %q; want %q", index, candidate.Kind, expected.Kind)
		}
		if candidate.GatewayTransport != expected.GatewayTransport {
			t.Fatalf("cases[%d].GatewayTransport = %q; want %q", index, candidate.GatewayTransport, expected.GatewayTransport)
		}
		if candidate.ProxyTransport != expected.ProxyTransport {
			t.Fatalf("cases[%d].ProxyTransport = %q; want %q", index, candidate.ProxyTransport, expected.ProxyTransport)
		}
		if candidate.EbusdTransport != expected.EbusdTransport {
			t.Fatalf("cases[%d].EbusdTransport = %q; want %q", index, candidate.EbusdTransport, expected.EbusdTransport)
		}
		if candidate.UsesProxy != expected.UsesProxy {
			t.Fatalf("cases[%d].UsesProxy = %v; want %v", index, candidate.UsesProxy, expected.UsesProxy)
		}
		if candidate.UsesEbusd != expected.UsesEbusd {
			t.Fatalf("cases[%d].UsesEbusd = %v; want %v", index, candidate.UsesEbusd, expected.UsesEbusd)
		}
		if candidate.EbusdViaProxy != expected.EbusdViaProxy {
			t.Fatalf("cases[%d].EbusdViaProxy = %v; want %v", index, candidate.EbusdViaProxy, expected.EbusdViaProxy)
		}
		if candidate.PassiveMode != expected.PassiveMode {
			t.Fatalf("cases[%d].PassiveMode = %q; want %q", index, candidate.PassiveMode, expected.PassiveMode)
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
	if len(passive) != 3 {
		t.Fatalf("len(passive) = %d; want 3", len(passive))
	}
	if passive[0].ID != "P01" || passive[1].ID != "P02" || passive[2].ID != "P06" {
		t.Fatalf("passive ids = %q %q %q; want P01 P02 P06", passive[0].ID, passive[1].ID, passive[2].ID)
	}

	if _, err := CasesForSuite("unknown"); err == nil {
		t.Fatalf("CasesForSuite(unknown) should fail")
	}
}
