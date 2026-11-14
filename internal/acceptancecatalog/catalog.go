// Package acceptancecatalog reads the transitional offline acceptance contract.
// It does not change a gate, authenticate evidence, or authorize hardware access.
package acceptancecatalog

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const MaxBytes = 2 << 20

type Row struct {
	ID         string   `json:"id"`
	Cases      []string `json:"cases"`
	Kind       string   `json:"kind"`
	Historical bool     `json:"historical"`
}
type Category struct {
	ID      string   `json:"id"`
	Outcome string   `json:"outcome"`
	Reason  string   `json:"reason"`
	Cases   []string `json:"cases"`
}
type Transition struct {
	Old             string   `json:"old"`
	Disposition     string   `json:"disposition"`
	Code            string   `json:"code"`
	Keepers         []string `json:"keepers"`
	ExpectedFailure string   `json:"expected_failure"`
}
type Catalog struct {
	Version                string              `json:"version"`
	ResultSchema           string              `json:"result_schema"`
	ResultSchemaSignature  string              `json:"result_schema_signature"`
	ActiveForm             string              `json:"active_form"`
	HandoffRows            []Row               `json:"handoff_rows"`
	Forms                  map[string][]string `json:"forms"`
	V2Cells                []string            `json:"v2_cells"`
	RetiredV2              []string            `json:"retired_v2"`
	Full88ExpectedFailures []Category          `json:"full88_expected_failures,omitempty"`
	Categories             []Category          `json:"categories"`
	CategoryPrecedence     []string            `json:"category_precedence"`
	Transition             []Transition        `json:"transition"`
	ResultVocabulary       []string            `json:"result_vocabulary"`
}

func decode(r io.Reader, v any) error {
	b, err := io.ReadAll(io.LimitReader(r, MaxBytes+1))
	if err != nil {
		return err
	}
	if len(b) > MaxBytes {
		return fmt.Errorf("acceptance input exceeds bound")
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err = d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}

// Signature hashes compact encoding/json output of the typed catalog.
// A consumer must supply a trusted version ledger, separate from the input.
func Signature(c Catalog) string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}
func ReadCatalog(r io.Reader, ledger map[string]string) (Catalog, error) {
	var c Catalog
	if err := decode(r, &c); err != nil {
		return c, err
	}
	signature, known := ledger[c.Version]
	if !known || signature != Signature(c) {
		return c, fmt.Errorf("unknown version or catalog signature mismatch")
	}
	return c, nil
}

type Identity struct {
	Candidate string `json:"candidate"`
	Session   string `json:"session"`
	Target    string `json:"target"`
}
type Review struct {
	Identity
	GateID      string `json:"gate_id"`
	CaseID      string `json:"case_id"`
	RecordID    string `json:"record_id"`
	ReviewID    string `json:"review_id"`
	Independent bool   `json:"independent"`
	Current     bool   `json:"current"`
}
type Record struct {
	Identity
	GateID        string `json:"gate_id"`
	CaseID        string `json:"case_id"`
	RecordID      string `json:"record_id"`
	Source        string `json:"source"`
	Outcome       string `json:"outcome"`
	Category      string `json:"category"`
	Reason        string `json:"reason"`
	EvidenceGrade string `json:"evidence_grade"`
	Qualification string `json:"qualification"`
	Admission     string `json:"admission"`
	Scope         string `json:"scope"`
	Procedure     string `json:"procedure"`
	Expected      string `json:"expected"`
	Actual        string `json:"actual"`
	EvidenceRef   string `json:"evidence_ref"`
	Omissions     string `json:"omissions"`
	Review        Review `json:"review"`
}
type Report struct {
	Identity
	CatalogVersion string   `json:"catalog_version"`
	Form           string   `json:"form"`
	Source         string   `json:"source"`
	Records        []Record `json:"records"`
}

func ReadReport(r io.Reader) (Report, error) {
	var report Report
	// A pointer distinguishes an explicit empty declaration from missing/null.
	var wire struct {
		*Report
		Records []struct {
			Record
			Omissions *string `json:"omissions"`
		} `json:"records"`
	}
	wire.Report = &report
	if err := decode(r, &wire); err != nil {
		return report, err
	}
	for i, record := range wire.Records {
		if record.Omissions == nil {
			return report, fmt.Errorf("record %d requires a non-null omissions string", i)
		}
		record.Record.Omissions = *record.Omissions
		report.Records = append(report.Records, record.Record)
	}
	return report, nil
}
func contains(values []string, s string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}
func identityValid(i Identity) bool {
	return strings.TrimSpace(i.Candidate) != "" && strings.TrimSpace(i.Session) != "" && strings.TrimSpace(i.Target) != ""
}

// ValidateReport returns complete only for current, independently reviewed,
// live passing evidence for every required case. Category-allowed non-pass
// results can be structurally valid without completing hardware acceptance.
// The caller establishes identity and review authenticity outside this package.
func ValidateReport(c Catalog, r Report, expected Identity) (bool, error) {
	if !identityValid(expected) || r.Identity != expected || r.CatalogVersion != c.Version {
		return false, fmt.Errorf("report identity/version mismatch")
	}
	gates, ok := c.Forms[r.Form]
	if !ok || (r.Source != "live" && r.Source != "synthetic") {
		return false, fmt.Errorf("unknown form/source")
	}
	required := map[string]bool{}
	for _, gate := range gates {
		cases := []string{gate}
		for _, row := range c.HandoffRows {
			if row.ID == gate {
				cases = row.Cases
			}
		}
		for _, id := range cases {
			required[gate+"\x00"+id] = true
		}
	}
	complete := r.Source == "live"
	ids := map[string]bool{}
	reviews := map[string]bool{}
	for _, record := range r.Records {
		key := record.GateID + "\x00" + record.CaseID
		if !required[key] {
			return false, fmt.Errorf("unknown or duplicate gate/case")
		}
		delete(required, key)
		if record.Identity != expected || record.Source != r.Source {
			return false, fmt.Errorf("row identity/source mismatch")
		}
		for _, field := range []string{record.RecordID, record.Qualification, record.Admission, record.Scope, record.Procedure, record.Expected, record.Actual, record.EvidenceRef} {
			if strings.TrimSpace(field) == "" {
				return false, fmt.Errorf("missing per-row evidence")
			}
		}
		review := record.Review
		if ids[record.RecordID] || reviews[review.ReviewID] || strings.TrimSpace(review.ReviewID) == "" || review.Identity != expected || review.GateID != record.GateID || review.CaseID != record.CaseID || review.RecordID != record.RecordID || !review.Independent || !review.Current {
			return false, fmt.Errorf("missing, reused, stale or mismatched per-row review")
		}
		ids[record.RecordID] = true
		reviews[review.ReviewID] = true
		if !contains([]string{"Proven", "Hypothesis", "Unknown"}, record.EvidenceGrade) || !contains(c.ResultVocabulary, record.Outcome) {
			return false, fmt.Errorf("unknown evidence grade/outcome")
		}
		if record.Outcome == "pass" && (record.EvidenceGrade != "Proven" || record.Omissions != "") {
			return false, fmt.Errorf("pass lacks proven complete evidence")
		}
		if record.Outcome == "planned" || record.Outcome == "xfail" || record.Outcome == "blocked-infra" {
			categories := c.Categories
			if r.Form == "full88" {
				categories = c.Full88ExpectedFailures
			}
			allowed := false
			for _, cat := range categories {
				if cat.ID == record.Category && cat.Outcome == record.Outcome && cat.Reason == record.Reason && contains(cat.Cases, record.CaseID) {
					allowed = true
				}
			}
			// Reserved/planned categories take precedence even over pass/xfail.
			if !allowed {
				return false, fmt.Errorf("outcome outside category")
			}
		} else if record.Category != "" || record.Reason != "" {
			return false, fmt.Errorf("unexpected category/reason")
		}
		if r.Form == "adaptermux-v2" {
			for _, category := range c.CategoryPrecedence {
				found := false
				for _, cat := range c.Categories {
					if cat.ID == category && contains(cat.Cases, record.CaseID) {
						if record.Category != cat.ID || record.Outcome != cat.Outcome || record.Reason != cat.Reason {
							return false, fmt.Errorf("reserved cell category mismatch")
						}
						found = true
						break
					}
				}
				if found {
					break
				}
			}
		}
		if record.Outcome != "pass" {
			complete = false
		}
	}
	if len(required) != 0 {
		return false, fmt.Errorf("missing gate/case records")
	}
	return complete, nil
}

// ReadSchema parses and verifies the companion schema using only the standard library.
func ReadSchema(r io.Reader, c Catalog) (map[string]any, error) {
	var schema map[string]any
	if err := decode(r, &schema); err != nil {
		return nil, err
	}
	b, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	if base64.StdEncoding.EncodeToString(sum[:]) != c.ResultSchemaSignature {
		return nil, fmt.Errorf("result schema signature mismatch")
	}
	return schema, nil
}
