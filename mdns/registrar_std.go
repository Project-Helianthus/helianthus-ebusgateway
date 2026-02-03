//go:build !tinygo

package mdns

import (
	"fmt"

	"github.com/grandcat/zeroconf"
)

type zeroconfRegistrar struct{}

func defaultRegistrar() registrar {
	return zeroconfRegistrar{}
}

func (zeroconfRegistrar) Register(service Service) (Advertiser, error) {
	server, err := zeroconf.Register(service.Instance, service.Service, service.Domain, service.Port, service.Text, nil)
	if err != nil {
		return nil, fmt.Errorf("mdns register failed: %w", err)
	}
	return &zeroconfAdvertiser{server: server}, nil
}

type zeroconfAdvertiser struct {
	server *zeroconf.Server
}

func (a *zeroconfAdvertiser) Close() error {
	if a == nil || a.server == nil {
		return nil
	}
	a.server.Shutdown()
	return nil
}

