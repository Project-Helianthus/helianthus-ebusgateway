//go:build !linux

package mcp

func eebusV1PlatformPeerUIDResolver() eebusV1PeerUIDResolver {
	return nil
}
