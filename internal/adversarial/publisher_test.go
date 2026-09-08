package adversarial

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

func fakeBuildInfo(revision, modified string) buildInfoReader {
	return func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: revision},
			{Key: "vcs.modified", Value: modified},
		}}, true
	}
}

func fakeExecutableDigest(digest string, err error) executableDigest {
	return func() (string, error) { return digest, err }
}

func TestPublisherBindsResolvedSubjectAndProducer(t *testing.T) {
	publisher, err := newPublisher(fakeBuildInfo(testProducerCommit, "false"), fakeExecutableDigest(testProducerSHA256, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			driver := fixtureBytes(t, "inputs", name)
			report, err := publisher.NewExecutor(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), nil, nil).Run(driver)
			if err != nil {
				t.Fatal(err)
			}
			if report.Provenance.Subject.Commit != testProducerCommit || report.Provenance.Producer.Commit != testProducerCommit || report.Provenance.Producer.BuildSHA256 != testProducerSHA256 {
				t.Fatalf("provenance: %#v", report.Provenance)
			}
			if report.Provenance.Subject.ArtifactSHA256 != fixtureSetDigest || report.Provenance.FixtureSetSHA256 != fixtureSetDigest {
				t.Fatalf("fixture digest: %#v", report.Provenance)
			}
			raw, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateReportBytes(raw); err != nil {
				t.Fatal(err)
			}
			if err := publisher.WriteReport(driver, report, filepath.Join(t.TempDir(), name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublisherResolutionFailsBeforeWriting(t *testing.T) {
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	startedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	missingBuildInfo := func() (*debug.BuildInfo, bool) { return nil, false }
	emptyBuildInfo := func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{}, true }
	duplicateRevision := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: testProducerCommit},
			{Key: "vcs.revision", Value: testProducerCommit},
			{Key: "vcs.modified", Value: "false"},
		}}, true
	}
	cases := []struct {
		name string
		info buildInfoReader
		hash executableDigest
	}{
		{"missing_build_info", missingBuildInfo, fakeExecutableDigest(testProducerSHA256, nil)},
		{"missing_vcs_metadata", emptyBuildInfo, fakeExecutableDigest(testProducerSHA256, nil)},
		{"dirty", fakeBuildInfo(testProducerCommit, "true"), fakeExecutableDigest(testProducerSHA256, nil)},
		{"malformed_revision", fakeBuildInfo(strings.ToUpper(testProducerCommit), "false"), fakeExecutableDigest(testProducerSHA256, nil)},
		{"malformed_modified", fakeBuildInfo(testProducerCommit, "FALSE"), fakeExecutableDigest(testProducerSHA256, nil)},
		{"duplicate_revision", duplicateRevision, fakeExecutableDigest(testProducerSHA256, nil)},
		{"hash_failure", fakeBuildInfo(testProducerCommit, "false"), fakeExecutableDigest("", errors.New("hash failed"))},
		{"malformed_hash", fakeBuildInfo(testProducerCommit, "false"), fakeExecutableDigest(strings.ToUpper(testProducerSHA256), nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			report, err := publishReportWithResolver(tc.info, tc.hash, driver, startedAt, path)
			if !errors.Is(err, ErrInvalidReport) || !reflect.DeepEqual(report, ReportV1{}) {
				t.Fatalf("report=%#v err=%v", report, err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination created: %v", err)
			}
		})
	}
}

func TestPublisherRejectsTamperedProvenanceBeforeWriting(t *testing.T) {
	publisher := canonicalPublisher()
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	base, err := publisher.NewExecutor(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), nil, nil).Run(driver)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ReportV1){
		func(report *ReportV1) { report.Provenance.Subject.Commit = strings.Repeat("c", 40) },
		func(report *ReportV1) { report.Provenance.Producer.Commit = strings.Repeat("c", 40) },
		func(report *ReportV1) { report.Provenance.Producer.BuildSHA256 = strings.Repeat("c", 64) },
	} {
		report := cloneReport(base)
		mutate(&report)
		path := filepath.Join(t.TempDir(), "tampered.json")
		if err := publisher.WriteReport(driver, report, path); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("tampered provenance accepted: %v", err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("destination created: %v", err)
		}
	}
}

func TestPublisherRejectsTamperedFixtureProjectionBeforeWriting(t *testing.T) {
	publisher := canonicalPublisher()
	driver := fixtureBytes(t, "inputs", "evaluated-fail")
	report, err := publisher.NewExecutor(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), nil, nil).Run(driver)
	if err != nil {
		t.Fatal(err)
	}
	scenario := &report.Scenarios[0]
	scenario.Metrics.End.SemanticBusCollisionsTotal--
	scenario.Metrics.Delta.SemanticBusCollisionsTotal--
	scenario.Evaluation.Collisions.Observed--
	scenario.Evaluation.Collisions.Passed = true
	scenario.Outcome = "pass"
	report.Summary.Passed++
	report.Summary.Failed--
	report.Summary.Verdict = "pass"
	if err := ValidateReport(report); err != nil {
		t.Fatalf("mutation must remain structurally valid: %v", err)
	}
	for name, raw := range map[string][]byte{
		"matching_driver": driver,
		"wrong_driver":    fixtureBytes(t, "inputs", "offline-all-pass"),
		"unbound_driver":  []byte(`{"schema_version":1}`),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "forged-pass.json")
			if err := publisher.WriteReport(raw, report, path); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("tampered fixture projection accepted: %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination created: %v", err)
			}
		})
	}
}
