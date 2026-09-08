package adversarial

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"testing"
	"time"
)

const (
	publishedFixtureSubjectCommit  = "936edbe873f35a8bad3763223dba9566154574d6"
	publishedFixtureProducerSHA256 = "fc8993b6b0532a219534e557dfd8c7ee0974fe1402e14c8e7787f9cdb488d4d1"
	publishedProducerRecordSHA256  = "a711159a0c0b63523d2bcc65896049b8d0a63b539bdcc15effc2dce3cc26850a"
)

func TestPublishedPositiveArtifactsMatchProjectionAndBuildRecord(t *testing.T) {
	rawRecord, err := fixtureFiles.ReadFile("fixtures/v1/producer-build-evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	recordDigest := sha256.Sum256(rawRecord)
	if hex.EncodeToString(recordDigest[:]) != publishedProducerRecordSHA256 {
		t.Fatal("producer build evidence bytes changed")
	}
	var record struct {
		Schema string `json:"schema"`
		Source struct {
			Commit string `json:"commit"`
			Tree   string `json:"tree"`
			State  string `json:"state"`
		} `json:"source"`
		Build struct {
			SHA256      string `json:"sha256"`
			VCSRevision string `json:"vcs_revision"`
			VCSModified bool   `json:"vcs_modified"`
		} `json:"build"`
		Generation struct {
			Cases []struct {
				CaseID       string `json:"case_id"`
				OutputSHA256 string `json:"output_sha256"`
			} `json:"cases"`
		} `json:"generation"`
		Reports map[string]string `json:"reports"`
	}
	if err := json.Unmarshal(rawRecord, &record); err != nil {
		t.Fatal(err)
	}
	if record.Schema != "helianthus.gateway.adversarial-producer-build/v1" || record.Source.Commit != publishedFixtureSubjectCommit || record.Source.Tree != "d59bdfe02b9ad1168fe8d7ba2aff275f87ef06fd" || record.Source.State != "clean" || record.Build.SHA256 != publishedFixtureProducerSHA256 || record.Build.VCSRevision != publishedFixtureSubjectCommit || record.Build.VCSModified {
		t.Fatalf("build record identity: %#v", record)
	}
	if len(record.Generation.Cases) != len(fixtureCases) || len(record.Reports) != len(fixtureCases) {
		t.Fatalf("artifact counts: generation=%d reports=%d", len(record.Generation.Cases), len(record.Reports))
	}
	producer := producerIdentity{Repository: subjectRepository, Commit: publishedFixtureSubjectCommit, Component: "internal/adversarial", BuildKind: "go-test-binary", BuildSHA256: publishedFixtureProducerSHA256}
	for index, name := range fixtureCases {
		raw, err := fixtureFiles.ReadFile("fixtures/v1/positive/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		if got := hex.EncodeToString(digest[:]); record.Reports[name+".json"] != got || record.Generation.Cases[index].CaseID != name || record.Generation.Cases[index].OutputSHA256 != got {
			t.Fatalf("%s record digest mismatch: %s", name, got)
		}
		expected, err := ParseReportV1(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateRuntimeReportV1(expected); err != nil {
			t.Fatalf("%s validation: %v", name, err)
		}
		driver := fixtureBytes(t, "inputs", name)
		bound, err := BindFixtureDriverV1(driver)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateFixtureProjectionV1(expected, bound); err != nil {
			t.Fatalf("%s projection: %v", name, err)
		}
		actual, err := (Executor{StartedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), producer: producer, subject: publishedFixtureSubjectCommit}).Run(driver)
		if err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%s exact projection: err=%v equal=%v", name, err, reflect.DeepEqual(actual, expected))
		}
	}
}

func TestPublisherUsesStableFixtureSubjectWithAuthenticatedProducer(t *testing.T) {
	publisher, err := newPublisher(func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: testProducerCommit}, {Key: "vcs.modified", Value: "false"}}}, true
	}, func() (string, error) { return testProducerSHA256, nil })
	if err != nil {
		t.Fatal(err)
	}
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	report, err := publisher.NewExecutor(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), nil, nil).Run(driver)
	if err != nil {
		t.Fatal(err)
	}
	if report.Provenance.Subject.Commit != publishedFixtureSubjectCommit || report.Provenance.Producer.Commit != testProducerCommit {
		t.Fatalf("provenance: %#v", report.Provenance)
	}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := publisher.WriteReport(driver, report, path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
