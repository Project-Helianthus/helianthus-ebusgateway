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
