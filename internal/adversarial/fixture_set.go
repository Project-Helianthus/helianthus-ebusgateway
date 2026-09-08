package adversarial

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"sync"
)

//go:embed fixtures/v1/fixture-input-manifest.json fixtures/v1/inputs/* fixtures/v1/positive/* fixtures/v1/negative-cases.json
var fixtureFiles embed.FS

type manifestV1 struct {
	FixtureContract string             `json:"fixture_contract"`
	Suite           Suite              `json:"suite"`
	Purpose         string             `json:"purpose"`
	Artifacts       []manifestArtifact `json:"artifacts"`
	Cases           []manifestCase     `json:"cases"`
}
type manifestArtifact struct {
	ArtifactID string `json:"artifact_id"`
	Role       string `json:"role"`
	Path       string `json:"path"`
	MediaType  string `json:"media_type"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
}
type manifestCase struct {
	CaseID              string   `json:"case_id"`
	DriverArtifactID    string   `json:"driver_artifact_id"`
	ResourceArtifactIDs []string `json:"resource_artifact_ids"`
}
type fixtureSetV1 struct {
	manifest    manifestV1
	rawManifest []byte
	artifacts   map[string][]byte
	byID        map[string]manifestArtifact
	cases       map[string]manifestCase
	drivers     map[string]FixtureV1
}

var fixtureOnce sync.Once
var fixtureSet *fixtureSetV1
var fixtureSetErr error

type BoundFixture struct {
	driver       FixtureV1
	caseInfo     manifestCase
	driverDigest string
	resources    map[string][]byte
}

func (b BoundFixture) CaseID() string    { return b.caseInfo.CaseID }
func (b BoundFixture) RunID() string     { return b.driver.RunID }
func (b BoundFixture) Driver() FixtureV1 { return cloneFixtureV1(b.driver) }

type FixtureContext struct{ bound BoundFixture }

func (c FixtureContext) CaseID() string { return c.bound.CaseID() }
func (c FixtureContext) Resource(id string) ([]byte, bool) {
	v, ok := c.bound.resources[id]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func loadFixtureSetV1() (*fixtureSetV1, error) {
	fixtureOnce.Do(func() { fixtureSet, fixtureSetErr = buildFixtureSetV1() })
	return fixtureSet, fixtureSetErr
}

func buildFixtureSetV1() (*fixtureSetV1, error) {
	raw, err := fixtureFiles.ReadFile("fixtures/v1/fixture-input-manifest.json")
	if err != nil {
		return nil, errors.New("manifest unavailable")
	}
	return buildFixtureSetData(raw, func(p string) ([]byte, error) { return fixtureFiles.ReadFile("fixtures/v1/" + p) }, true)
}

func buildFixtureSetData(raw []byte, readFile func(string) ([]byte, error), requirePinned bool) (*fixtureSetV1, error) {
	sum := sha256.Sum256(raw)
	if requirePinned && hex.EncodeToString(sum[:]) != fixtureSetDigest {
		return nil, errors.New("manifest digest")
	}
	if err := scanJSON(raw, "manifest"); err != nil {
		return nil, errors.New("manifest JSON")
	}
	generic, err := decodeRaw(raw)
	if err != nil {
		return nil, errors.New("manifest JSON")
	}
	if err := manifestShape(generic); err != nil {
		return nil, errors.New("manifest shape")
	}
	var m manifestV1
	if err := strictUnmarshal(raw, &m); err != nil {
		return nil, errors.New("manifest shape")
	}
	if m.FixtureContract != "helianthus.adversarial-runtime.fixture-set/v1" || m.Suite.ID != SuiteID || m.Suite.Version != 1 || m.Purpose != "deterministic synthetic offline executor inputs; not live evidence" {
		return nil, errors.New("manifest identity")
	}
	s := &fixtureSetV1{manifest: m, rawManifest: append([]byte(nil), raw...), artifacts: map[string][]byte{}, byID: map[string]manifestArtifact{}, cases: map[string]manifestCase{}, drivers: map[string]FixtureV1{}}
	last := ""
	total := int64(0)
	for _, a := range m.Artifacts {
		if a.ArtifactID <= last || a.ArtifactID == "" {
			return nil, errors.New("artifact order")
		}
		last = a.ArtifactID
		if _, ok := s.byID[a.ArtifactID]; ok {
			return nil, errors.New("artifact duplicate")
		}
		if !safeManifestPath(a.Path) || !oneOf(a.Role, "scenario-driver", "cache-image") || (a.Role == "scenario-driver" && a.MediaType != "application/json") || (a.Role == "cache-image" && a.MediaType != "application/octet-stream") || a.SizeBytes < 0 || a.SizeBytes > maximumJSONBytes || !hex64.MatchString(a.SHA256) {
			return nil, errors.New("artifact metadata")
		}
		data, err := readFile(a.Path)
		if err != nil {
			return nil, errors.New("artifact unavailable")
		}
		total += int64(len(data))
		if total > 4*maximumJSONBytes || int64(len(data)) != a.SizeBytes || digest(data) != a.SHA256 {
			return nil, errors.New("artifact binding")
		}
		s.byID[a.ArtifactID] = a
		s.artifacts[a.ArtifactID] = append([]byte(nil), data...)
	}
	used := map[string]int{}
	runs := map[string]bool{}
	last = ""
	for _, c := range m.Cases {
		if c.CaseID <= last || !caseIDPattern.MatchString(c.CaseID) {
			return nil, errors.New("case order")
		}
		last = c.CaseID
		if _, ok := s.cases[c.CaseID]; ok {
			return nil, errors.New("case duplicate")
		}
		driverMeta, ok := s.byID[c.DriverArtifactID]
		if !ok || driverMeta.Role != "scenario-driver" || c.DriverArtifactID != "driver-"+c.CaseID {
			return nil, errors.New("case driver")
		}
		used[c.DriverArtifactID]++
		driver, err := ParseFixtureV1(s.artifacts[c.DriverArtifactID])
		if err != nil || driver.FixtureCaseID != c.CaseID || runs[driver.RunID] {
			return nil, errors.New("driver contract")
		}
		runs[driver.RunID] = true
		s.drivers[c.CaseID] = driver
		prev := ""
		for _, id := range c.ResourceArtifactIDs {
			if id <= prev {
				return nil, errors.New("resource order")
			}
			prev = id
			a, ok := s.byID[id]
			if !ok || a.Role != "cache-image" {
				return nil, errors.New("resource role")
			}
			used[id]++
		}
		if c.CaseID == "offline-all-pass" || c.CaseID == "evaluated-fail" || c.CaseID == "execution-error" || c.CaseID == "infrastructure-block" {
			if len(c.ResourceArtifactIDs) != 1 {
				return nil, errors.New("ADV-04 resource")
			}
		}
		adv4 := driver.Scenarios[3].ResourceArtifactIDs
		if !reflect.DeepEqual(adv4, c.ResourceArtifactIDs) {
			return nil, errors.New("resource binding")
		}
		for i := 0; i < 3; i++ {
			if len(driver.Scenarios[i].ResourceArtifactIDs) != 0 {
				return nil, errors.New("resource scenario")
			}
		}
		s.cases[c.CaseID] = c
	}
	if len(s.cases) != 4 {
		return nil, errors.New("case count")
	}
	for id := range s.byID {
		if used[id] != 1 {
			return nil, errors.New("artifact reference")
		}
	}
	return s, nil
}

func manifestShape(v map[string]any) error {
	r, e := exactObject(v, "fixture_contract", "suite", "purpose", "artifacts", "cases")
	if e != nil {
		return e
	}
	if e = nonNull(r, "fixture_contract", "suite", "purpose", "artifacts", "cases"); e != nil {
		return e
	}
	s, e := exactObject(r["suite"], "id", "version")
	if e != nil {
		return e
	}
	if e = nonNull(s, "id", "version"); e != nil {
		return e
	}
	aa, e := array(r["artifacts"])
	if e != nil {
		return e
	}
	for _, x := range aa {
		o, e := exactObject(x, "artifact_id", "role", "path", "media_type", "size_bytes", "sha256")
		if e != nil {
			return e
		}
		if e = nonNull(o, "artifact_id", "role", "path", "media_type", "size_bytes", "sha256"); e != nil {
			return e
		}
	}
	cc, e := array(r["cases"])
	if e != nil {
		return e
	}
	for _, x := range cc {
		o, e := exactObject(x, "case_id", "driver_artifact_id", "resource_artifact_ids")
		if e != nil {
			return e
		}
		if e = nonNull(o, "case_id", "driver_artifact_id", "resource_artifact_ids"); e != nil {
			return e
		}
		if _, e = array(o["resource_artifact_ids"]); e != nil {
			return e
		}
	}
	return nil
}
func safeManifestPath(p string) bool {
	return p != "" && path.Clean(p) == p && !strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "../") && !strings.Contains(p, "/../")
}
func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func BindFixtureDriverV1(raw []byte) (BoundFixture, error) {
	if len(raw) > maximumJSONBytes {
		return BoundFixture{}, fmt.Errorf("%w: artifact_size", ErrInvalidFixture)
	}
	s, err := loadFixtureSetV1()
	if err != nil {
		return BoundFixture{}, fmt.Errorf("%w: manifest", ErrInvalidFixture)
	}
	sha := digest(raw)
	var meta manifestArtifact
	found := false
	for _, a := range s.byID {
		if a.Role == "scenario-driver" && a.SizeBytes == int64(len(raw)) && a.SHA256 == sha {
			meta = a
			found = true
			break
		}
	}
	if !found {
		return BoundFixture{}, fmt.Errorf("%w: raw_driver_binding", ErrInvalidFixture)
	}
	driver, err := ParseFixtureV1(raw)
	if err != nil {
		return BoundFixture{}, err
	}
	c, ok := s.cases[driver.FixtureCaseID]
	if !ok || c.DriverArtifactID != meta.ArtifactID {
		return BoundFixture{}, fmt.Errorf("%w: case_binding", ErrInvalidFixture)
	}
	resources := map[string][]byte{}
	for _, id := range c.ResourceArtifactIDs {
		resources[id] = append([]byte(nil), s.artifacts[id]...)
	}
	return BoundFixture{driver: driver, caseInfo: c, driverDigest: sha, resources: resources}, nil
}

func ValidateFixtureProjectionV1(r ReportV1, b BoundFixture) error {
	if err := ValidateRuntimeReportV1(r); err != nil {
		return err
	}
	raw, err := fixtureFiles.ReadFile("fixtures/v1/positive/" + b.caseInfo.CaseID + ".json")
	if err != nil {
		return fmt.Errorf("%w: projection_fixture", ErrInvalidReport)
	}
	expected, err := ParseReportV1(raw)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(r, expected) {
		return fmt.Errorf("%w: fixture_projection", ErrInvalidReport)
	}
	return nil
}

func cloneFixtureV1(v FixtureV1) FixtureV1 {
	out := v
	out.Scenarios = append([]FixtureScenario(nil), v.Scenarios...)
	for i := range out.Scenarios {
		out.Scenarios[i].Events = append([]FixtureEvent(nil), v.Scenarios[i].Events...)
		out.Scenarios[i].ResourceArtifactIDs = make([]string, len(v.Scenarios[i].ResourceArtifactIDs))
		copy(out.Scenarios[i].ResourceArtifactIDs, v.Scenarios[i].ResourceArtifactIDs)
		if v.Scenarios[i].Precondition.UnavailableReason != nil {
			out.Scenarios[i].Precondition.UnavailableReason = cloneString(v.Scenarios[i].Precondition.UnavailableReason)
		}
		if v.Scenarios[i].Observations.Baseline != nil {
			x := *v.Scenarios[i].Observations.Baseline
			out.Scenarios[i].Observations.Baseline = &x
		}
		if v.Scenarios[i].Observations.End != nil {
			x := *v.Scenarios[i].Observations.End
			out.Scenarios[i].Observations.End = &x
		}
		if v.Scenarios[i].TerminalError != nil {
			x := *v.Scenarios[i].TerminalError
			out.Scenarios[i].TerminalError = &x
		}
	}
	return out
}
