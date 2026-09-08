package adversarial

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

const (
	publishedProducerCommit = "3cbae8e2c46e2a819c9d4caaf57ebdd54c4b518f"
	publishedProducerSHA256 = "e66f701ccf5b57611bf9cd54c5f3e00c6988fd9e07d766e4821eb4f3c61ccadf"
)

func fakeBuildInfo(revision, modified string) buildInfoReader {
	return func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: revision}, {Key: "vcs.modified", Value: modified}}}, true
	}
}

func fakeExecutableDigest(digest string, err error) executableDigest {
	return func() (string, error) { return digest, err }
}

func TestPublishedReportsCarryExactSubjectAndProducer(t *testing.T) {
	publisher, err := newPublisher(fakeBuildInfo(publishedProducerCommit, "false"), fakeExecutableDigest(publishedProducerSHA256, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			raw := fixtureBytes(t, "positive", name)
			report, err := ParseReportV1(raw)
			if err != nil {
				t.Fatal(err)
			}
			if report.Provenance.Subject.Commit != subjectCommit || report.Provenance.Subject.ArtifactSHA256 != fixtureSetDigest || report.Provenance.FixtureSetSHA256 != fixtureSetDigest {
				t.Fatalf("subject provenance: %#v", report.Provenance.Subject)
			}
			if report.Provenance.Producer.Commit != publishedProducerCommit || report.Provenance.Producer.BuildSHA256 != publishedProducerSHA256 {
				t.Fatalf("producer provenance: %#v", report.Provenance.Producer)
			}
			if err := ValidateReport(report); err != nil {
				t.Fatalf("A report invalid under B: %v", err)
			}
			if err := ValidateReportBytes(raw); err != nil {
				t.Fatal(err)
			}
			if err := publisher.WriteReport(report, filepath.Join(t.TempDir(), name+".json")); err != nil {
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
		return &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: publishedProducerCommit}, {Key: "vcs.revision", Value: publishedProducerCommit}, {Key: "vcs.modified", Value: "false"}}}, true
	}
	cases := []struct {
		name string
		info buildInfoReader
		hash executableDigest
	}{
		{"missing_build_info", missingBuildInfo, fakeExecutableDigest(publishedProducerSHA256, nil)},
		{"missing_vcs_metadata", emptyBuildInfo, fakeExecutableDigest(publishedProducerSHA256, nil)},
		{"dirty", fakeBuildInfo(publishedProducerCommit, "true"), fakeExecutableDigest(publishedProducerSHA256, nil)},
		{"malformed_revision", fakeBuildInfo(strings.ToUpper(publishedProducerCommit), "false"), fakeExecutableDigest(publishedProducerSHA256, nil)},
		{"malformed_modified", fakeBuildInfo(publishedProducerCommit, "FALSE"), fakeExecutableDigest(publishedProducerSHA256, nil)},
		{"duplicate_revision", duplicateRevision, fakeExecutableDigest(publishedProducerSHA256, nil)},
		{"hash_failure", fakeBuildInfo(publishedProducerCommit, "false"), fakeExecutableDigest("", errors.New("hash failed"))},
		{"malformed_hash", fakeBuildInfo(publishedProducerCommit, "false"), fakeExecutableDigest(strings.ToUpper(publishedProducerSHA256), nil)},
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

func TestPublisherRejectsTamperedProducerBeforeWriting(t *testing.T) {
	publisher, err := newPublisher(fakeBuildInfo(publishedProducerCommit, "false"), fakeExecutableDigest(publishedProducerSHA256, nil))
	if err != nil {
		t.Fatal(err)
	}
	base, err := publisher.NewExecutor(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), nil, nil).Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ReportV1){
		func(report *ReportV1) { report.Provenance.Producer.Commit = strings.Repeat("b", 40) },
		func(report *ReportV1) { report.Provenance.Producer.BuildSHA256 = strings.Repeat("c", 64) },
	} {
		report := cloneReport(base)
		mutate(&report)
		path := filepath.Join(t.TempDir(), "tampered.json")
		if err := publisher.WriteReport(report, path); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("tampered producer accepted: %v", err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("destination created: %v", err)
		}
	}
}
