//go:build !linux

package mcp

import "os"

func eebusV1PlatformPeerUIDResolver() eebusV1PeerUIDResolver {
	return nil
}

func eebusV1OwnedByEffectiveUID(os.FileInfo) bool {
	return true
}
