package main

import (
	"context"
	"errors"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func newPortalPVClient(config ebusgateway.PortalPVConfig) (func(context.Context) (portal.ForwardedResponse, error), error) {
	return newPortalSemanticClient(config, portalSemanticPV)
}

func newPortalStorageClient(config ebusgateway.PortalStorageConfig) (func(context.Context) (portal.ForwardedResponse, error), error) {
	return newPortalSemanticClient(config, portalSemanticStorage)
}

func newPortalEVSEClient(config ebusgateway.PortalEVSEConfig) (func(context.Context) (portal.ForwardedResponse, error), error) {
	return newPortalSemanticClient(config, portalSemanticEVSE)
}

type portalSemanticKind uint8

const (
	portalSemanticPV portalSemanticKind = iota
	portalSemanticStorage
	portalSemanticEVSE
)

func newPortalSemanticClient(config ebusgateway.PortalPVConfig, kind portalSemanticKind) (func(context.Context) (portal.ForwardedResponse, error), error) {
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
		var response m2mgraphql.Response
		var err error
		switch kind {
		case portalSemanticPV:
			response, err = client.Current(ctx)
		case portalSemanticStorage:
			response, err = client.StorageCurrent(ctx)
		case portalSemanticEVSE:
			response, err = client.EVSECurrent(ctx)
		default:
			return portal.ForwardedResponse{}, errors.New("portal semantic kind is invalid")
		}
		if err != nil {
			return portal.ForwardedResponse{}, err
		}
		return portal.ForwardedResponse{Status: response.Status, ContentType: response.ContentType, Body: response.Body}, nil
	}, nil
}
