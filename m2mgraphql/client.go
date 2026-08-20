package m2mgraphql

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const clientTimeout = 5 * time.Second

type ClientConfig struct {
	URL            string
	ServerName     string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	AssetRef       string
}

type Client struct {
	url   string
	asset string
	http  *http.Client
}

type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

func NewClient(config ClientConfig) (*Client, error) {
	for _, value := range []string{config.URL, config.ServerName, config.CAFile, config.ClientCertFile, config.ClientKeyFile, config.AssetRef} {
		if strings.TrimSpace(value) == "" {
			return nil, errors.New("M2M GraphQL client configuration is incomplete")
		}
	}
	parsedURL, err := url.Parse(config.URL)
	if err != nil || !parsedURL.IsAbs() || parsedURL.Scheme != "https" || parsedURL.Host == "" ||
		parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" || parsedURL.Opaque != "" ||
		parsedURL.ForceQuery || parsedURL.Path != route || parsedURL.RawPath != "" {
		return nil, errors.New("M2M GraphQL client URL is invalid")
	}
	certificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
	if err != nil {
		return nil, errors.New("M2M GraphQL client certificate is invalid")
	}
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, errors.New("M2M GraphQL client CA is unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("M2M GraphQL client CA is invalid")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: config.ServerName, Certificates: []tls.Certificate{certificate}}, ResponseHeaderTimeout: clientTimeout}
	return &Client{url: config.URL, asset: config.AssetRef, http: &http.Client{Transport: transport, Timeout: clientTimeout}}, nil
}

func (client *Client) Current(ctx context.Context) (Response, error) {
	if client == nil || client.http == nil {
		return Response{}, errors.New("M2M GraphQL client unavailable")
	}
	body, err := json.Marshal(map[string]any{
		"operationName": "M2MCurrentSnapshot", "query": fixedQuery,
		"variables": map[string]any{"request": map[string]string{"contractId": contractID, "assetRef": client.asset}},
	})
	if err != nil {
		return Response{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.url, bytes.NewReader(body))
	if err != nil {
		return Response{}, errors.New("M2M GraphQL request is invalid")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return Response{}, errors.New("M2M GraphQL request failed")
	}
	defer func() { _ = response.Body.Close() }()
	mediaType, _, contentTypeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || contentTypeErr != nil || mediaType != "application/json" {
		return Response{}, errors.New("M2M GraphQL response is invalid")
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(encoded) > maxResponseBytes || !json.Valid(encoded) {
		return Response{}, errors.New("M2M GraphQL response is invalid")
	}
	return Response{Status: response.StatusCode, ContentType: response.Header.Get("Content-Type"), Body: encoded}, nil
}
