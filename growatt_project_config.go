package ebusgateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
)

// GrowattPCSConfig is inert; listen-only confirmation records an external
// precondition and never establishes physical qualification.
// Native contract: helianthus-docs-canbus, PCS Profile selection and
// Data validity and consumer obligations.
type GrowattPCSConfig struct {
	Enabled             bool
	ListenOnlyConfirmed bool
	StaleAfter          time.Duration
	Native              canbusreg.SelectionSpec
}

// GrowattLINKConfig selects each admitted source explicitly. No omitted source
// inherits the default family. Native contract: helianthus-docs-canbus, LINK
// Transport, addressing and data validity; Identify the peer before interpreting
// version-dependent fields.
type GrowattLINKConfig struct {
	Enabled             bool
	ListenOnlyConfirmed bool
	StaleAfter          time.Duration
	Native              canbusreg.SelectionSpec
	SourceSelectors     string
}

type GrowattLINKSourceSelector struct {
	Source                         uint8
	ProjectCode, Release, Contract string
}

// ParseGrowattLINKSourceSelectors accepts one bounded array with strict keys.
func ParseGrowattLINKSourceSelectors(value, project, release string) ([]GrowattLINKSourceSelector, error) {
	if len(value) > 16384 {
		return nil, errors.New("source selectors exceed input bound")
	}
	d := json.NewDecoder(strings.NewReader(value))
	t, err := d.Token()
	if err != nil || t != json.Delim('[') {
		return nil, errors.New("source selectors require an array")
	}
	var result []GrowattLINKSourceSelector
	seen := map[int]bool{}
	for d.More() {
		if len(result) == 32 {
			return nil, errors.New("source selector bound is 32")
		}
		t, err = d.Token()
		if err != nil || t != json.Delim('{') {
			return nil, errors.New("source selector requires an object")
		}
		keys := map[string]json.RawMessage{}
		for d.More() {
			t, err = d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := t.(string)
			if !ok {
				return nil, errors.New("invalid key")
			}
			if _, duplicate := keys[key]; duplicate {
				return nil, errors.New("duplicate source selector key")
			}
			switch key {
			case "source", "project_code", "release", "contract", "use_default":
			default:
				return nil, fmt.Errorf("unknown source selector key %q", key)
			}
			var raw json.RawMessage
			if err = d.Decode(&raw); err != nil {
				return nil, err
			}
			keys[key] = raw
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		var source int
		if err = json.Unmarshal(keys["source"], &source); err != nil || source < 1 || source > 254 || source == 244 || source == 245 || seen[source] {
			return nil, errors.New("invalid or duplicate source")
		}
		seen[source] = true
		var s GrowattLINKSourceSelector
		s.Source = uint8(source)
		if json.Unmarshal(keys["contract"], &s.Contract) != nil || (s.Contract != "legacy19" && s.Contract != "release11") {
			return nil, errors.New("explicit native contract required")
		}
		if raw, defaults := keys["use_default"]; defaults {
			var use bool
			if json.Unmarshal(raw, &use) != nil || !use || len(keys) != 3 {
				return nil, errors.New("default and explicit selectors cannot mix")
			}
			s.ProjectCode, s.Release = project, release
		} else {
			if len(keys) != 4 || json.Unmarshal(keys["project_code"], &s.ProjectCode) != nil || json.Unmarshal(keys["release"], &s.Release) != nil {
				return nil, errors.New("explicit project and release required")
			}
		}
		if len(s.ProjectCode) != 4 || len(s.Release) != 4 {
			return nil, errors.New("exact project and release required")
		}
		result = append(result, s)
	}
	if _, err = d.Token(); err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("trailing source selector JSON")
	}
	if len(result) == 0 {
		return nil, errors.New("at least one source selector required")
	}
	return result, nil
}

func validateGrowattProjectNative(n canbusreg.SelectionSpec) (canbusreg.CXSelection, error) {
	s, err := canbusreg.NewCXSelection(n)
	if err != nil {
		return s, err
	}
	if s.SelectionGroup() == "" {
		return s, errors.New("unsupported explicit native selection")
	}
	if n.Flavor == "link" {
		if n.Direction != "module-to-coordinator" || n.Scope != "local" || !n.RouteSet || (n.Destination != 244 && n.Destination != 245) {
			return s, errors.New("invalid LINK context")
		}
	} else {
		if n.Direction != "bms-to-pcs" || (n.Flavor != "pcs-standard" && n.Flavor != "pcs-extended19" && n.Flavor != "pcs-parallel") {
			return s, errors.New("invalid PCS context")
		}
		if (n.Flavor == "pcs-parallel" && n.Bank != "parallel") || (n.Flavor != "pcs-parallel" && n.Bank != "ordinary") {
			return s, errors.New("explicit matching PCS bank required")
		}
		if n.Flavor == "pcs-extended19" && !n.RouteSet {
			return s, errors.New("extended PCS route required")
		}
	}
	return s, nil
}

func (c GrowattPCSConfig) Selection() (canbusreg.CXSelection, error) {
	if !c.Enabled {
		if c != (GrowattPCSConfig{}) {
			return canbusreg.CXSelection{}, errors.New("disabled PCS has active fields")
		}
		return canbusreg.CXSelection{}, nil
	}
	if !c.ListenOnlyConfirmed || c.StaleAfter <= 0 || c.Native.Flavor == "link" {
		return canbusreg.CXSelection{}, errors.New("PCS preconditions required")
	}
	return validateGrowattProjectNative(c.Native)
}

func (c GrowattLINKConfig) Selections() ([]canbusreg.CXSelection, error) {
	if !c.Enabled {
		if c != (GrowattLINKConfig{}) {
			return nil, errors.New("disabled LINK has active fields")
		}
		return nil, nil
	}
	if !c.ListenOnlyConfirmed || c.StaleAfter <= 0 || c.Native.Flavor != "link" || c.Native.Source != 0 {
		return nil, errors.New("LINK preconditions and explicit source map required")
	}
	entries, err := ParseGrowattLINKSourceSelectors(c.SourceSelectors, c.Native.ProjectCode, c.Native.Release)
	if err != nil {
		return nil, err
	}
	result := make([]canbusreg.CXSelection, 0, len(entries))
	for _, entry := range entries {
		n := c.Native
		n.Source, n.ProjectCode, n.Release, n.Choice = entry.Source, entry.ProjectCode, entry.Release, entry.Contract
		s, err := validateGrowattProjectNative(n)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, nil
}
