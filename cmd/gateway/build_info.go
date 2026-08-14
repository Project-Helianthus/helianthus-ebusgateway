package main

import (
	"errors"
	"runtime/debug"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

type gatewayBuildInfo struct {
	ReleaseVersion string
	BuildID        string
}

var gatewayBuildRevisionResolver = readGatewayBuildRevision

func newGatewayBuildInfo(releaseVersion, buildIdentity string) (gatewayBuildInfo, error) {
	releaseVersion = strings.TrimSpace(releaseVersion)
	buildIdentity = strings.TrimSpace(buildIdentity)
	if releaseVersion == "" {
		return gatewayBuildInfo{}, errors.New("gateway release version is required")
	}
	if buildIdentity == "" {
		return gatewayBuildInfo{}, errors.New("gateway build identity is required")
	}
	return gatewayBuildInfo{ReleaseVersion: releaseVersion, BuildID: buildIdentity}, nil
}

// resolveGatewayBuildInfo establishes the process-wide build identity once at
// startup. An explicitly injected identity is authoritative; ordinary
// -buildvcs binaries resolve their full revision exactly once here.
func resolveGatewayBuildInfo(releaseVersion, buildIdentity string) (gatewayBuildInfo, error) {
	if strings.TrimSpace(buildIdentity) != "unknown" {
		return newGatewayBuildInfo(releaseVersion, buildIdentity)
	}
	revision, err := gatewayBuildRevisionResolver()
	if err == nil && isFullGitRevision(revision) {
		return newGatewayBuildInfo(releaseVersion, revision)
	}
	return newGatewayBuildInfo(releaseVersion, "unknown")
}

func (info gatewayBuildInfo) EvidenceVersion() string {
	if isFullGitRevision(info.BuildID) {
		return info.ReleaseVersion + "+git." + info.BuildID
	}
	return info.ReleaseVersion + "+build." + info.BuildID
}

func (info gatewayBuildInfo) OneShotEvidenceIdentity() syncevidence.OneShotBuildIdentity {
	operationVersion := "build:" + info.BuildID
	if isFullGitRevision(info.BuildID) {
		operationVersion = "git:" + info.BuildID
	}
	return syncevidence.OneShotBuildIdentity{
		RecorderVersion:  info.EvidenceVersion(),
		ReplayVersion:    info.EvidenceVersion(),
		OperationVersion: operationVersion,
	}
}

func gatewayBuildString(info gatewayBuildInfo) string {
	return info.ReleaseVersion + "+" + info.BuildID
}

func readGatewayBuildRevision() (string, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", errors.New("read build information")
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && isFullGitRevision(setting.Value) {
			return setting.Value, nil
		}
	}
	return "", errors.New("full build revision unavailable")
}

func isFullGitRevision(value string) bool {
	return len(value) == 40 && strings.Trim(value, "0123456789abcdef") == ""
}
