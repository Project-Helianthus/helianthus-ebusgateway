package main

import (
	"errors"
	"strings"
)

type gatewayBuildInfo struct {
	ReleaseVersion string
	BuildID        string
}

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
