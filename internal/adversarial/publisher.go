package adversarial

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"
)

type Publisher struct {
	producer producerIdentity
}

type buildInfoReader func() (*debug.BuildInfo, bool)
type executableDigest func() (string, error)

func NewPublisher() (Publisher, error) {
	return newPublisher(debug.ReadBuildInfo, currentExecutableSHA256)
}

func PublishReport(rawDriver []byte, startedAt time.Time, path string) (ReportV1, error) {
	return publishReportWithResolver(debug.ReadBuildInfo, currentExecutableSHA256, rawDriver, startedAt, path)
}

func publishReportWithResolver(readBuildInfo buildInfoReader, hashExecutable executableDigest, rawDriver []byte, startedAt time.Time, path string) (ReportV1, error) {
	publisher, err := newPublisher(readBuildInfo, hashExecutable)
	if err != nil {
		return ReportV1{}, err
	}
	report, err := publisher.NewExecutor(startedAt, nil, nil).Run(rawDriver)
	if err != nil {
		return ReportV1{}, err
	}
	if err := publisher.WriteReport(report, path); err != nil {
		return ReportV1{}, err
	}
	return report, nil
}

func newPublisher(readBuildInfo buildInfoReader, hashExecutable executableDigest) (Publisher, error) {
	identity, err := resolveProducerIdentity(readBuildInfo, hashExecutable)
	if err != nil {
		return Publisher{}, err
	}
	return Publisher{producer: identity}, nil
}

func resolveProducerIdentity(readBuildInfo buildInfoReader, hashExecutable executableDigest) (producerIdentity, error) {
	info, ok := readBuildInfo()
	if !ok || info == nil {
		return producerIdentity{}, fmt.Errorf("%w: producer_build_info", ErrInvalidReport)
	}
	revision, modified := "", ""
	revisionCount, modifiedCount := 0, 0
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision, revisionCount = setting.Value, revisionCount+1
		case "vcs.modified":
			modified, modifiedCount = setting.Value, modifiedCount+1
		}
	}
	if revisionCount != 1 || modifiedCount != 1 || !hex40.MatchString(revision) || modified != "false" {
		return producerIdentity{}, fmt.Errorf("%w: producer_build_info", ErrInvalidReport)
	}
	digest, err := hashExecutable()
	if err != nil || !hex64.MatchString(digest) {
		return producerIdentity{}, fmt.Errorf("%w: producer_executable", ErrInvalidReport)
	}
	return producerIdentity{Repository: subjectRepository, Commit: revision, Component: "internal/adversarial", BuildKind: "go-test-binary", BuildSHA256: digest}, nil
}

func currentExecutableSHA256() (string, error) {
	name, err := os.Executable()
	if err != nil {
		return "", err
	}
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (p Publisher) NewExecutor(startedAt time.Time, action FixtureAction, observer FixtureObserver) Executor {
	return Executor{StartedAt: startedAt, Action: action, Observer: observer, producer: p.producer}
}

func (p Publisher) WriteReport(report ReportV1, path string) error {
	if p.producer != (producerIdentity{}) && report.Provenance.Producer == p.producer.wire() {
		return WriteReport(report, path)
	}
	return fmt.Errorf("%w: publisher_identity", ErrInvalidReport)
}
