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

const O22MaxBytes = 2 << 20

type O22Row struct {
	ID         string   `json:"id"`
	Cases      []string `json:"cases"`
	Kind       string   `json:"kind"`
	Historical bool     `json:"historical"`
}
type O22Category struct {
	ID      string   `json:"id"`
	Outcome string   `json:"outcome"`
	Reason  string   `json:"reason"`
	Cases   []string `json:"cases"`
}
type O22Transition struct {
	Old             string   `json:"old"`
	Disposition     string   `json:"disposition"`
	Code            string   `json:"code"`
	Keepers         []string `json:"keepers"`
	ExpectedFailure string   `json:"expected_failure"`
}
type O22Catalogue struct {
	Version               string              `json:"version"`
	ResultSchema          string              `json:"result_schema"`
	ResultSchemaSignature string              `json:"result_schema_signature"`
	ActiveForm            string              `json:"active_form"`
	HandoffRows           []O22Row            `json:"handoff_rows"`
	Forms                 map[string][]string `json:"forms"`
	V2Cells               []string            `json:"v2_cells"`
	RetiredV2             []string            `json:"retired_v2"`
	Categories            []O22Category       `json:"categories"`
	CategoryPrecedence    []string            `json:"category_precedence"`
	Transition            []O22Transition     `json:"transition"`
	ResultVocabulary      []string            `json:"result_vocabulary"`
}

func o22Decode(r io.Reader, v any) error {
	b, err := io.ReadAll(io.LimitReader(r, O22MaxBytes+1))
	if err != nil {
		return err
	}
	if len(b) > O22MaxBytes {
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

// O22Signature hashes compact encoding/json output of the typed catalogue.
// A consumer must supply a trusted version ledger, separate from the input.
func O22Signature(c O22Catalogue) string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}
func O22ReadCatalogue(r io.Reader, ledger map[string]string) (O22Catalogue, error) {
	var c O22Catalogue
	if err := o22Decode(r, &c); err != nil {
		return c, err
	}
	signature, known := ledger[c.Version]
	if !known || signature != O22Signature(c) {
		return c, fmt.Errorf("unknown version or catalogue signature mismatch")
	}
	return c, nil
}

type O22Identity struct {
	Candidate string `json:"candidate"`
	Session   string `json:"session"`
	Target    string `json:"target"`
}
type O22Review struct {
	O22Identity
	GateID      string `json:"gate_id"`
	CaseID      string `json:"case_id"`
	RecordID    string `json:"record_id"`
	ReviewID    string `json:"review_id"`
	Independent bool   `json:"independent"`
	Current     bool   `json:"current"`
}
type O22Record struct {
	O22Identity
	GateID        string    `json:"gate_id"`
	CaseID        string    `json:"case_id"`
	RecordID      string    `json:"record_id"`
	Source        string    `json:"source"`
	Outcome       string    `json:"outcome"`
	Category      string    `json:"category"`
	Reason        string    `json:"reason"`
	EvidenceGrade string    `json:"evidence_grade"`
	Qualification string    `json:"qualification"`
	Admission     string    `json:"admission"`
	Scope         string    `json:"scope"`
	Procedure     string    `json:"procedure"`
	Expected      string    `json:"expected"`
	Actual        string    `json:"actual"`
	EvidenceRef   string    `json:"evidence_ref"`
	Omissions     string    `json:"omissions"`
	Review        O22Review `json:"review"`
}
type O22Report struct {
	O22Identity
	CatalogueVersion string      `json:"catalogue_version"`
	Form             string      `json:"form"`
	Source           string      `json:"source"`
	Records          []O22Record `json:"records"`
}

func O22ReadReport(r io.Reader) (O22Report, error) {
	var report O22Report
	err := o22Decode(r, &report)
	return report, err
}
func o22Contains(values []string, s string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}
func o22IdentityValid(i O22Identity) bool {
	return strings.TrimSpace(i.Candidate) != "" && strings.TrimSpace(i.Session) != "" && strings.TrimSpace(i.Target) != ""
}

// O22ValidateReport returns complete only for current, independently reviewed,
// live passing evidence for every required case. Category-allowed non-pass
// results can be structurally valid without completing hardware acceptance.
// The caller establishes identity and review authenticity outside this package.
func O22ValidateReport(c O22Catalogue, r O22Report, expected O22Identity) (bool, error) {
	if !o22IdentityValid(expected) || r.O22Identity != expected || r.CatalogueVersion != c.Version {
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
		if record.O22Identity != expected || record.Source != r.Source {
			return false, fmt.Errorf("row identity/source mismatch")
		}
		for _, field := range []string{record.RecordID, record.Qualification, record.Admission, record.Scope, record.Procedure, record.Expected, record.Actual, record.EvidenceRef} {
			if strings.TrimSpace(field) == "" {
				return false, fmt.Errorf("missing per-row evidence")
			}
		}
		review := record.Review
		if ids[record.RecordID] || reviews[review.ReviewID] || strings.TrimSpace(review.ReviewID) == "" || review.O22Identity != expected || review.GateID != record.GateID || review.CaseID != record.CaseID || review.RecordID != record.RecordID || !review.Independent || !review.Current {
			return false, fmt.Errorf("missing, reused, stale or mismatched per-row review")
		}
		ids[record.RecordID] = true
		reviews[review.ReviewID] = true
		if !o22Contains([]string{"Proven", "Hypothesis", "Unknown"}, record.EvidenceGrade) || !o22Contains(c.ResultVocabulary, record.Outcome) {
			return false, fmt.Errorf("unknown evidence grade/outcome")
		}
		if record.Outcome == "pass" && (record.EvidenceGrade != "Proven" || record.Omissions != "") {
			return false, fmt.Errorf("pass lacks proven complete evidence")
		}
		if record.Outcome == "planned" || record.Outcome == "xfail" || record.Outcome == "blocked-infra" {
			if r.Form != "adaptermux-v2" {
				return false, fmt.Errorf("category result outside v2")
			}
			allowed := false
			for _, cat := range c.Categories {
				if cat.ID == record.Category && cat.Outcome == record.Outcome && cat.Reason == record.Reason && o22Contains(cat.Cases, record.CaseID) {
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
					if cat.ID == category && o22Contains(cat.Cases, record.CaseID) {
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

// O22ReadSchema parses and verifies the companion schema using only the standard library.
func O22ReadSchema(r io.Reader, c O22Catalogue) (map[string]any, error) {
	var schema map[string]any
	if err := o22Decode(r, &schema); err != nil {
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
