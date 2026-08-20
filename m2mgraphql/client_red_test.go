package m2mgraphql

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortalClientUsesTheEmbeddedClosedCanonicalQuery(t *testing.T) {
	if _, ok := any(fixedQuery).(string); !ok {
		t.Fatal("fixed query is not available to the Portal client")
	}
	if len(fixedQuery) == 0 {
		t.Fatal("fixed canonical query is empty")
	}
}

func TestPortalClientRejectsNonCanonicalUpstreamURLs(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	config := portalClientTestConfig(t, server)
	for _, rawURL := range []string{
		"http://example.test/graphql/m2m/v1",
		"https://user:secret@example.test/graphql/m2m/v1",
		"https://example.test/graphql",
		"https://example.test/graphql/m2m/v1?query=1",
		"https://example.test/graphql/m2m/v1#fragment",
		"/graphql/m2m/v1",
	} {
		t.Run(rawURL, func(t *testing.T) {
			candidate := config
			candidate.URL = rawURL
			if _, err := NewClient(candidate); err == nil {
				t.Fatalf("NewClient accepted non-canonical URL %q", rawURL)
			}
		})
	}
}

func TestPortalClientRejectsNonContractHTTPResponses(t *testing.T) {
	for _, test := range []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{name: "wrong_status", status: http.StatusBadGateway, contentType: "application/json", body: `{}`},
		{name: "wrong_content_type", status: http.StatusOK, contentType: "text/plain", body: `{}`},
		{name: "oversized", status: http.StatusOK, contentType: "application/json", body: strings.Repeat("x", maxResponseBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			client, err := NewClient(portalClientTestConfig(t, server))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Current(context.Background()); err == nil {
				t.Fatalf("Current accepted status=%d content-type=%q body-bytes=%d", test.status, test.contentType, len(test.body))
			}
		})
	}
}

func portalClientTestConfig(t *testing.T, server *httptest.Server) ClientConfig {
	t.Helper()
	dir := t.TempDir()
	certificate := server.TLS.Certificates[0]
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile := filepath.Join(dir, "client.pem")
	keyFile := filepath.Join(dir, "client-key.pem")
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	serverName := "example.com"
	if len(leaf.DNSNames) > 0 {
		serverName = leaf.DNSNames[0]
	}
	return ClientConfig{
		URL: server.URL + "/graphql/m2m/v1", ServerName: serverName, CAFile: caFile,
		ClientCertFile: certFile, ClientKeyFile: keyFile, AssetRef: "pv-asset-fixture",
	}
}
