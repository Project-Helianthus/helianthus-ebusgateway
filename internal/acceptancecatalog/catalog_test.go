package acceptancecatalog

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func o22File(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func o22Ledger(t *testing.T) map[string]string {
	t.Helper()
	b := o22File(t, "docs/testing/hardware-acceptance-catalogue.md")
	entries := regexp.MustCompile(`(?m)^\| ([0-9]+) \| ([A-Za-z0-9+/=]+) \|$`).FindAllSubmatch(b, -1)
	if len(entries) == 0 {
		t.Fatal("missing version ledger")
	}
	ledger := map[string]string{}
	for _, e := range entries {
		if _, ok := ledger[string(e[1])]; ok {
			t.Fatal("duplicate version")
		}
		ledger[string(e[1])] = string(e[2])
	}
	return ledger
}
func o22Load(t *testing.T) O22Catalogue {
	t.Helper()
	c, err := O22ReadCatalogue(strings.NewReader(string(o22File(t, "docs/testing/hardware-acceptance-catalogue-v1.json"))), o22Ledger(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func o22Clone[T any](v T) T {
	b, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(b, &out)
	return out
}
func o22Equal(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory differs:\ngot %v\nwant %v", got, want)
	}
}
func o22Handoff(text string) []O22Row {
	var rows []O22Row
	rangeRE := regexp.MustCompile(`^([A-Z]+)([0-9]+)–[A-Z]+([0-9]+)$`)
	for _, line := range strings.Split(text, "\n") {
		p := strings.Split(line, "|")
		if len(p) != 8 {
			continue
		}
		for i := range p {
			p[i] = strings.TrimSpace(p[i])
		}
		if p[3] != "Not run" {
			continue
		}
		row := O22Row{ID: p[1], Kind: "single", Cases: []string{p[1]}, Historical: p[1] == "Prior RTU evidence applicability"}
		if m := rangeRE.FindStringSubmatch(p[1]); m != nil {
			lo, _ := strconv.Atoi(m[2])
			hi, _ := strconv.Atoi(m[3])
			row.Kind = "range"
			row.Cases = nil
			for n := lo; n <= hi; n++ {
				row.Cases = append(row.Cases, fmt.Sprintf("%s%02d", m[1], n))
			}
		}
		rows = append(rows, row)
	}
	return rows
}
func TestO22PublishedArtifacts(t *testing.T) {
	c := o22Load(t)
	if c.Version != "1" || c.ActiveForm != "full88" || len(c.HandoffRows) != 65 || len(c.Forms["full88"]) != 64 || len(c.V2Cells) != 173 || len(c.Forms["adaptermux-v2"]) != 220 {
		t.Fatal("unexpected publication inventory")
	}
	o22Equal(t, c.RetiredV2, []string{"AD11"})
	schema, err := O22ReadSchema(strings.NewReader(string(o22File(t, "docs/testing/"+c.ResultSchema))), c)
	if err != nil {
		t.Fatal(err)
	}
	if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatal("schema draft")
	}
	// Independently require all serialized record/review fields in the schema.
	defs := schema["$defs"].(map[string]any)
	for name, value := range map[string]any{"record": O22Record{}, "review": O22Review{}} {
		b, _ := json.Marshal(value)
		var keys map[string]any
		_ = json.Unmarshal(b, &keys)
		definition := defs[name].(map[string]any)
		properties := definition["properties"].(map[string]any)
		required := definition["required"].([]any)
		if definition["additionalProperties"] != false || len(properties) != len(keys) || len(required) != len(keys) {
			t.Fatal("schema/reader fields differ")
		}
		for key := range keys {
			present := false
			for _, r := range required {
				if r == key {
					present = true
				}
			}
			if !present || properties[key] == nil {
				t.Fatalf("schema omits %s.%s", name, key)
			}
		}
	}
	outcomes := defs["record"].(map[string]any)["properties"].(map[string]any)["outcome"].(map[string]any)["enum"]
	b, _ := json.Marshal(outcomes)
	var vocabulary []string
	_ = json.Unmarshal(b, &vocabulary)
	o22Equal(t, c.ResultVocabulary, vocabulary)
}
func TestO22HandoffInventory(t *testing.T) {
	c := o22Load(t)
	rows := o22Handoff(string(o22File(t, "docs/testing/consolidated-candidate-handoff.md")))
	o22Equal(t, c.HandoffRows, rows)
	var full, v2 []string
	for _, row := range rows {
		if !row.Historical {
			full = append(full, row.ID)
			if row.Kind != "range" && !regexp.MustCompile(`^AD[0-9]+$`).MatchString(row.ID) {
				v2 = append(v2, row.ID)
			}
		}
	}
	o22Equal(t, c.Forms["full88"], full)
	o22Equal(t, c.Forms["adaptermux-v2"], append(v2, c.V2Cells...))
}
func TestO22MatrixInventory(t *testing.T) {
	c := o22Load(t)
	text := string(o22File(t, "matrix/M7-adaptermux-conformance-matrix.md"))
	var transports, invariants, compat []string
	for _, line := range strings.Split(text, "\n") {
		p := strings.Split(line, "|")
		if len(p) < 3 {
			continue
		}
		id := strings.Trim(strings.TrimSpace(p[1]), "`")
		if len(p) == 7 && regexp.MustCompile(`^[A-Z]+$`).MatchString(id) {
			transports = append(transports, id)
		}
		if len(p) == 6 && regexp.MustCompile(`^(AD|PX|CX)[0-9]{2}$`).MatchString(id) {
			invariants = append(invariants, id)
		}
		if len(p) == 6 && regexp.MustCompile(`^[A-Z]+-(DS|ES)01$`).MatchString(id) {
			compat = append(compat, id)
		}
	}
	var cells []string
	for _, s := range transports {
		for _, i := range invariants {
			cells = append(cells, s+"-"+i)
		}
	}
	cells = append(cells, compat...)
	o22Equal(t, c.V2Cells, cells)
	var mappings []O22Transition
	for _, line := range strings.Split(string(o22File(t, "matrix/M7-adaptermux-conformance-cell-map.md")), "\n") {
		p := strings.Split(line, "|")
		if len(p) != 9 {
			continue
		}
		for i := range p {
			p[i] = strings.TrimSpace(p[i])
		}
		if !regexp.MustCompile(`^T[0-9]{2}$`).MatchString(p[1]) {
			continue
		}
		m := O22Transition{Old: p[1], Disposition: p[4], Code: p[5], ExpectedFailure: p[7]}
		for _, id := range regexp.MustCompile("`([A-Z]+-(?:AD|DS|ES|CX)[0-9]+)`").FindAllStringSubmatch(p[6], -1) {
			m.Keepers = append(m.Keepers, id[1])
			if !o22Contains(cells, id[1]) {
				t.Fatal("unknown keeper")
			}
		}
		mappings = append(mappings, m)
	}
	o22Equal(t, c.Transition, mappings)
	if len(mappings) != 88 {
		t.Fatal("missing old case")
	}
	for i, m := range mappings {
		if m.Old != fmt.Sprintf("T%02d", i+1) || len(m.Keepers) == 0 {
			t.Fatal("transition coverage")
		}
	}
	// Independent category membership and reason controls from the policy contract.
	reasons := map[string]string{"XF-01": "southbound plain tcp under dual-client load reports no signal (carried from proxy dual-client)", "BI-01": "adapter_no_signal", "BI-02": "uart_direct_hardware_absent", "PL-01": "mux_southbound_plain_not_implemented", "PL-02": "mux_local_responder_not_implemented", "PL-03": "mux_raw_symbol_forwarding_not_implemented"}
	o22Equal(t, c.CategoryPrecedence, []string{"BI-02", "PL-01", "PL-02", "PL-03"})
	if len(c.Categories) != len(reasons) {
		t.Fatal("category count")
	}
	for _, cat := range c.Categories {
		if cat.Reason != reasons[cat.ID] || !strings.Contains(text, "| "+cat.ID+" |") || !strings.Contains(text, cat.Reason) {
			t.Fatal("category contract drift")
		}
		var want []string
		for _, id := range cells {
			parts := strings.Split(id, "-")
			s, i := parts[0], parts[1]
			uart := strings.HasPrefix(s, "UART")
			plain := (s == "UDP" || s == "TCP") && i != "DS01"
			responder := i == "PX09" || i == "PX10" || i == "PX11"
			raw := (s == "ENS" || s == "ENH") && (i == "AD05" || i == "AD08" || i == "AD12")
			match := false
			switch cat.ID {
			case "XF-01":
				match = id == "TCP-CX01"
			case "BI-01":
				match = !uart && !plain && !responder && !raw
			case "BI-02":
				match = uart
			case "PL-01":
				match = plain
			case "PL-02":
				match = responder
			case "PL-03":
				match = raw
			}
			if match {
				want = append(want, id)
			}
		}
		o22Equal(t, cat.Cases, want)
	}
}
func TestO22CatalogueMutations(t *testing.T) {
	c := o22Load(t)
	ledger := o22Ledger(t)
	mutations := map[string]func(*O22Catalogue){
		"remove-gate": func(c *O22Catalogue) { c.Forms["full88"] = c.Forms["full88"][1:] },
		"add-gate":    func(c *O22Catalogue) { c.Forms["full88"] = append(c.Forms["full88"], "O22-extra") },
		"rename-gate": func(c *O22Catalogue) { c.HandoffRows[0].ID = "O22-renamed" },
		"remove-case": func(c *O22Catalogue) { c.HandoffRows[0].Cases = c.HandoffRows[0].Cases[1:] },
		"add-case":    func(c *O22Catalogue) { c.HandoffRows[0].Cases = append(c.HandoffRows[0].Cases, "O22-extra") },
		"rename-case": func(c *O22Catalogue) { c.HandoffRows[0].Cases[0] = "O22-renamed" },
		"historical-required": func(c *O22Catalogue) {
			c.Forms["full88"] = append(c.Forms["full88"], "Prior RTU evidence applicability")
		},
		"retired-required":        func(c *O22Catalogue) { c.Forms["adaptermux-v2"] = append(c.Forms["adaptermux-v2"], "AD11") },
		"unknown-version":         func(c *O22Catalogue) { c.Version = "O22-unknown" },
		"unreviewed-version-bump": func(c *O22Catalogue) { c.Version = "2" },
		"vocabulary":              func(c *O22Catalogue) { c.ResultVocabulary = append(c.ResultVocabulary, "O22-unknown") },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := o22Clone(c)
			mutate(&changed)
			b, _ := json.Marshal(changed)
			if _, err := O22ReadCatalogue(strings.NewReader(string(b)), ledger); err == nil {
				t.Fatal("changed identity admitted without new ledger version")
			}
		})
	}
	b := string(o22File(t, "docs/testing/"+c.ResultSchema))
	if _, err := O22ReadSchema(strings.NewReader(strings.Replace(b, "timeout", "O22-unknown", 1)), c); err == nil {
		t.Fatal("schema drift admitted")
	}
	for _, input := range []string{"{} {}", `{"unknown":true}`, strings.Repeat(" ", O22MaxBytes+1)} {
		if _, err := O22ReadCatalogue(strings.NewReader(input), ledger); err == nil {
			t.Fatal("malformed/bounded input admitted")
		}
	}
}

func o22Report(c O22Catalogue, form string) O22Report {
	// All observations and review attestations here are fabricated unit-test data.
	identity := O22Identity{Candidate: "O22-candidate", Session: "O22-session", Target: "O22-target"}
	r := O22Report{O22Identity: identity, CatalogueVersion: c.Version, Form: form, Source: "synthetic"}
	for _, gate := range c.Forms[form] {
		cases := []string{gate}
		for _, row := range c.HandoffRows {
			if row.ID == gate {
				cases = row.Cases
			}
		}
		for _, id := range cases {
			key := fmt.Sprintf("O22-record-%d", len(r.Records))
			record := O22Record{O22Identity: identity, GateID: gate, CaseID: id, RecordID: key, Source: "synthetic", Outcome: "pass", EvidenceGrade: "Proven", Qualification: "Unassessed", Admission: "Unassessed", Scope: "O22-family", Procedure: "O22-procedure", Expected: "O22-expected", Actual: "O22-synthetic-observation", EvidenceRef: "O22-public-reference#row"}
			record.Review = O22Review{O22Identity: identity, GateID: gate, CaseID: id, RecordID: key, ReviewID: "O22-review-" + key, Independent: true, Current: true}
			if form == "adaptermux-v2" {
				for _, category := range c.CategoryPrecedence {
					found := false
					for _, cat := range c.Categories {
						if cat.ID == category && o22Contains(cat.Cases, id) {
							record.Outcome = cat.Outcome
							record.Category = cat.ID
							record.Reason = cat.Reason
							record.EvidenceGrade = "Unknown"
							found = true
							break
						}
					}
					if found {
						break
					}
				}
			}
			r.Records = append(r.Records, record)
		}
	}
	return r
}
func TestO22ReportIdentityMutations(t *testing.T) {
	c := o22Load(t)
	base := o22Report(c, "full88")
	if done, err := O22ValidateReport(c, base, base.O22Identity); err != nil || done {
		t.Fatalf("synthetic control: %v %v", done, err)
	}
	b, _ := json.Marshal(base)
	parsed, err := O22ReadReport(strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	o22Equal(t, parsed, base)
	mutations := map[string]func(*O22Report){
		"gate-id": func(r *O22Report) { r.Records[0].GateID = "O22-other" }, "case-id": func(r *O22Report) { r.Records[0].CaseID = "O22-other" },
		"report-candidate": func(r *O22Report) { r.Candidate = "O22-other" }, "report-session": func(r *O22Report) { r.Session = "O22-other" }, "report-target": func(r *O22Report) { r.Target = "O22-other" },
		"row-candidate": func(r *O22Report) { r.Records[0].Candidate = "O22-other" }, "row-session": func(r *O22Report) { r.Records[0].Session = "O22-other" }, "row-target": func(r *O22Report) { r.Records[0].Target = "O22-other" },
		"review-candidate": func(r *O22Report) { r.Records[0].Review.Candidate = "O22-other" }, "review-session": func(r *O22Report) { r.Records[0].Review.Session = "O22-other" }, "review-target": func(r *O22Report) { r.Records[0].Review.Target = "O22-other" },
		"review-gate": func(r *O22Report) { r.Records[0].Review.GateID = "O22-other" }, "review-case": func(r *O22Report) { r.Records[0].Review.CaseID = "O22-other" }, "review-record": func(r *O22Report) { r.Records[0].Review.RecordID = "O22-other" },
		"review-id-empty": func(r *O22Report) { r.Records[0].Review.ReviewID = "" }, "review-id-reused": func(r *O22Report) { r.Records[0].Review.ReviewID = r.Records[1].Review.ReviewID },
		"record-id-empty": func(r *O22Report) { r.Records[0].RecordID = "" }, "record-id-reused": func(r *O22Report) { r.Records[0].RecordID = r.Records[1].RecordID },
		"review-not-independent": func(r *O22Report) { r.Records[0].Review.Independent = false }, "review-stale": func(r *O22Report) { r.Records[0].Review.Current = false },
		"source": func(r *O22Report) { r.Records[0].Source = "live" }, "report-source": func(r *O22Report) { r.Source = "O22-unknown" },
		"version": func(r *O22Report) { r.CatalogueVersion = "O22-unknown" }, "form": func(r *O22Report) { r.Form = "O22-unknown" },
		"vocabulary": func(r *O22Report) { r.Records[0].Outcome = "O22-unknown" }, "grade": func(r *O22Report) { r.Records[0].EvidenceGrade = "O22-unknown" }, "pass-omission": func(r *O22Report) { r.Records[0].Omissions = "O22-omitted" }, "pass-unproven": func(r *O22Report) { r.Records[0].EvidenceGrade = "Unknown" },
		"missing": func(r *O22Report) { r.Records = r.Records[1:] }, "duplicate": func(r *O22Report) { r.Records = append(r.Records, r.Records[0]) },
		"historical": func(r *O22Report) { r.Records[0].GateID = "Prior RTU evidence applicability" }, "planned-full88": func(r *O22Report) {
			r.Records[0].Outcome = "planned"
			r.Records[0].Category = "PL-01"
			r.Records[0].Reason = "mux_southbound_plain_not_implemented"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r := o22Clone(base)
			mutate(&r)
			if _, err := O22ValidateReport(c, r, base.O22Identity); err == nil {
				t.Fatal("identity mutation admitted")
			}
		})
	}
	for _, field := range []string{"Qualification", "Admission", "Scope", "Procedure", "Expected", "Actual", "EvidenceRef"} {
		t.Run("missing-"+field, func(t *testing.T) {
			r := o22Clone(base)
			reflect.ValueOf(&r.Records[0]).Elem().FieldByName(field).SetString("")
			if _, err := O22ValidateReport(c, r, base.O22Identity); err == nil {
				t.Fatal("missing evidence admitted")
			}
		})
	}
}
func TestO22OutcomesAndCategories(t *testing.T) {
	c := o22Load(t)
	r := o22Report(c, "adaptermux-v2")
	if complete, err := O22ValidateReport(c, r, r.O22Identity); err != nil || complete {
		t.Fatalf("v2 synthetic control: %v %v", complete, err)
	}
	for _, field := range []string{"Category", "Reason", "Outcome"} {
		t.Run("category-"+field, func(t *testing.T) {
			changed := o22Clone(r)
			for i := range changed.Records {
				if changed.Records[i].Category == "PL-03" {
					reflect.ValueOf(&changed.Records[i]).Elem().FieldByName(field).SetString("O22-other")
					break
				}
			}
			if _, err := O22ValidateReport(c, changed, r.O22Identity); err == nil {
				t.Fatal("category mutation admitted")
			}
		})
	}
	for _, id := range []string{"ENS-AD01", "ENS-DS01", "LEGACY-MUX"} {
		t.Run("planned-outside-"+id, func(t *testing.T) {
			changed := o22Clone(r)
			for i := range changed.Records {
				if changed.Records[i].CaseID == id {
					changed.Records[i].Outcome = "planned"
					changed.Records[i].Category = "PL-03"
					changed.Records[i].Reason = "mux_raw_symbol_forwarding_not_implemented"
				}
			}
			if _, err := O22ValidateReport(c, changed, r.O22Identity); err == nil {
				t.Fatal("planned outside category")
			}
		})
	}
	for _, outcome := range []string{"fail", "timeout", "not-run", "omitted-checks", "xpass"} {
		t.Run(outcome, func(t *testing.T) {
			report := o22Report(c, "full88")
			report.Source = "live"
			for i := range report.Records {
				report.Records[i].Source = "live"
			}
			report.Records[0].Outcome = outcome
			if complete, err := O22ValidateReport(c, report, report.O22Identity); err != nil || complete {
				t.Fatalf("non-pass disposition: %v %v", complete, err)
			}
		})
	}
	// Exercise the live-claim branch with fabricated attestations, never publish it as evidence.
	live := o22Report(c, "full88")
	live.Source = "live"
	for i := range live.Records {
		live.Records[i].Source = "live"
	}
	if complete, err := O22ValidateReport(c, live, live.O22Identity); err != nil || !complete {
		t.Fatalf("live-claim consistency control: %v %v", complete, err)
	}
}

func TestO22HandoffMutationControls(t *testing.T) {
	c := o22Load(t)
	source := string(o22File(t, "docs/testing/consolidated-candidate-handoff.md"))
	first := ""
	for _, line := range strings.Split(source, "\n") {
		if strings.HasPrefix(line, "| AD01 |") {
			first = line
			break
		}
	}
	if first == "" {
		t.Fatal("missing mutation control row")
	}
	for name, changed := range map[string]string{
		"delete":     strings.Replace(source, first, "", 1),
		"rename":     strings.Replace(source, "| AD01 |", "| O22-AD-renamed |", 1),
		"add":        strings.Replace(source, first, first+"\n"+strings.Replace(first, "| AD01 |", "| O22-new-gate |", 1), 1),
		"case-range": strings.Replace(source, "| T01–T04 |", "| T01–T03 |", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if reflect.DeepEqual(o22Handoff(changed), c.HandoffRows) {
				t.Fatal("handoff inventory mutation not detected")
			}
		})
	}
}
