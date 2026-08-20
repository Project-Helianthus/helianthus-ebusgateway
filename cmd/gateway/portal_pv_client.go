package main

import (
	"context"
	"errors"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func newPortalPVClient(config ebusgateway.PortalPVConfig) (func(context.Context) (portal.ForwardedResponse, error), error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if !config.SemanticEnabled {
		return nil, nil
	}
	client, err := m2mgraphql.NewClient(m2mgraphql.ClientConfig{
		URL: config.M2MURL, ServerName: config.M2MServerName, CAFile: config.M2MCAFile,
		ClientCertFile: config.M2MClientCert, ClientKeyFile: config.M2MClientKey, AssetRef: config.AssetRef,
	})
	if err != nil {
		return nil, errors.New("portal PV M2M client is invalid")
	}
	return func(ctx context.Context) (portal.ForwardedResponse, error) {
		response, err := client.Current(ctx)
		if err != nil {
			return portal.ForwardedResponse{}, err
		}
		return portal.ForwardedResponse{Status: response.Status, ContentType: response.ContentType, Body: response.Body}, nil
	}, nil
}
