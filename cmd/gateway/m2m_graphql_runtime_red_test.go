package main

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestM2MGraphQLRuntime_IsDisabledByDefaultAndSeparateFromGenericHTTP(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	runtime, err := newM2MGraphQLRuntime(cfg, nil)
	if err != nil {
		t.Fatalf("newM2MGraphQLRuntime: %v", err)
	}
	if runtime != nil {
		t.Fatalf("default config exposed M2M runtime: %#v", runtime)
	}
}

func TestM2MGraphQLRuntime_RequiresMutualTLSAndClosesListener(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	cfg := ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"pv-asset-fixture"}, DeniedPrincipalFingerprints: []string{certs.deniedFingerprint},
	}}
	runtime, err := newM2MGraphQLRuntime(cfg, nil)
	if err != nil {
		t.Fatalf("newM2MGraphQLRuntime: %v", err)
	}
	if runtime == nil || !runtime.RequiresVerifiedClientCertificate() {
		t.Fatalf("runtime=%#v; want dedicated verified-mTLS listener", runtime)
	}
	if runtime.server.ReadHeaderTimeout <= 0 || runtime.server.ReadTimeout <= 0 || runtime.server.WriteTimeout <= 0 || runtime.server.IdleTimeout <= 0 {
		t.Fatalf("M2M listener has unbounded network deadlines: %#v", runtime.server)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "m2m.gateway.test", certs.goodClient, true)
	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "wrong.m2m.gateway.test", certs.goodClient, false)
	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "m2m.gateway.test", tls.Certificate{}, false)
	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "m2m.gateway.test", certs.untrustedClient, false)
	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "m2m.gateway.test", certs.deniedClient, false)
	if err := runtime.Close(); err != nil {
		t.Fatalf("close dedicated M2M listener: %v", err)
	}
	if _, err := net.DialTimeout("tcp", runtime.Addr(), 100*time.Millisecond); err == nil {
		t.Fatal("closed dedicated M2M listener still accepts TCP")
	}
}

func TestM2MGraphQLRuntime_IncompleteEnabledConfigFailsClosed(t *testing.T) {
	_, err := newM2MGraphQLRuntime(ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{ListenAddr: "127.0.0.1:0", AllowedAssets: []string{"pv-asset-fixture"}}}, nil)
	if err == nil {
		t.Fatal("incomplete enabled M2M configuration started a listener")
	}
}

func TestM2MGraphQLRuntime_StalledHandshakeDoesNotBlockOtherPrincipals(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	cfg := ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"pv-asset-fixture"},
	}}
	runtime, err := newM2MGraphQLRuntime(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	stalled, err := net.Dial("tcp", runtime.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stalled.Close() })
	time.Sleep(50 * time.Millisecond)
	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "m2m.gateway.test", certs.goodClient, true)
}

func TestM2MGraphQLRuntime_RejectsServerCertificateForDifferentConfiguredIdentity(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	_, err := newM2MGraphQLRuntime(ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "other.gateway.test",
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"pv-asset-fixture"},
	}}, nil)
	if err == nil {
		t.Fatal("server certificate identity mismatch was accepted")
	}
}

func TestM2MGraphQLFlags_WireDedicatedListenerConfiguration(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("m2m-graphql", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	err := fs.Parse([]string{
		"-m2m-graphql-listen=127.0.0.1:8443",
		"-m2m-graphql-server-name=m2m.gateway.test",
		"-m2m-graphql-client-ca=/run/secrets/client-ca.pem",
		"-m2m-graphql-server-cert=/run/secrets/server.pem",
		"-m2m-graphql-server-key=/run/secrets/server-key.pem",
		"-m2m-graphql-allowed-assets=pv-b,pv-a",
		"-m2m-graphql-known-assets=pv-a",
		"-m2m-graphql-denied-principals=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	})
	if err != nil {
		t.Fatalf("parse M2M GraphQL flags: %v", err)
	}
	got := cfg.M2MGraphQL
	if got.ListenAddr != "127.0.0.1:8443" || got.ServerName != "m2m.gateway.test" ||
		got.ClientCAFile != "/run/secrets/client-ca.pem" || got.ServerCertFile != "/run/secrets/server.pem" ||
		got.ServerKeyFile != "/run/secrets/server-key.pem" || len(got.AllowedAssets) != 2 || got.AllowedAssets[0] != "pv-a" || len(got.KnownAssets) != 1 || got.KnownAssets[0] != "pv-a" ||
		len(got.DeniedPrincipalFingerprints) != 1 || got.DeniedPrincipalFingerprints[0] != strings.Repeat("a", 64) {
		t.Fatalf("M2M GraphQL flag config=%+v", got)
	}
}

func TestM2MGraphQLRuntime_KnownAssetWithoutSnapshotReturnsSourceUnavailable(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	cfg := ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"pv-asset-known"}, KnownAssets: []string{"pv-asset-known"},
	}}
	runtime, err := newM2MGraphQLRuntime(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	query, err := os.ReadFile("../../m2mgraphql/testdata/public-graphql-m2m-v1.query.graphql")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"operationName": "M2MCurrentSnapshot", "query": string(query),
		"variables": map[string]any{"request": map[string]string{"contractId": "PUBLIC_GRAPHQL_M2M_V1", "assetRef": "pv-asset-known"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: certs.pool, ServerName: "m2m.gateway.test", MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certs.goodClient},
	}}}
	response, err := client.Post("https://"+runtime.Addr()+"/graphql/m2m/v1", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		Errors []struct {
			Extensions struct {
				Code string `json:"code"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Errors) != 1 || envelope.Errors[0].Extensions.Code != "SOURCE_UNAVAILABLE" {
		t.Fatalf("known asset without snapshot response=%+v", envelope)
	}
}

func TestM2MGraphQLRuntime_BoundsPreMTLSConnectionsAndReleasesSlots(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	cfg := ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"pv-asset-fixture"},
	}}
	runtime, err := newM2MGraphQLRuntime(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	stalled := make([]net.Conn, 0, m2mMaxPreTLSConnections-1)
	for range m2mMaxPreTLSConnections - 1 {
		connection, err := net.Dial("tcp", runtime.Addr())
		if err != nil {
			t.Fatal(err)
		}
		stalled = append(stalled, connection)
	}
	t.Cleanup(func() {
		for _, connection := range stalled {
			_ = connection.Close()
		}
	})
	time.Sleep(50 * time.Millisecond)
	valid, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", runtime.Addr(), &tls.Config{
		RootCAs: certs.pool, ServerName: "m2m.gateway.test", MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certs.goodClient},
	})
	if err != nil {
		t.Fatalf("admitted client failed while bounded slots remained: %v", err)
	}
	excess, err := net.Dial("tcp", runtime.Addr())
	if err != nil {
		t.Fatal(err)
	}
	_ = excess.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := excess.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection beyond pre-mTLS capacity was not rejected")
	}
	_ = excess.Close()
	_ = valid.Close()
	assertM2MTLSDial(t, runtime.Addr(), certs.pool, "m2m.gateway.test", certs.goodClient, true)
}

type m2mTLSCertificates struct {
	pool                                      *x509.CertPool
	caFile, serverCertFile, serverKeyFile     string
	goodClient, untrustedClient, deniedClient tls.Certificate
	deniedFingerprint                         string
}

func newM2MTLSCertificates(t *testing.T) m2mTLSCertificates {
	t.Helper()
	dir := t.TempDir()
	caKey, ca := newM2MCertificateAuthority(t)
	server := newM2MLeafCertificate(t, ca, caKey, "m2m.gateway.test", false)
	good := newM2MLeafCertificate(t, ca, caKey, "principal-good", true)
	denied := newM2MLeafCertificate(t, ca, caKey, "principal-denied", true)
	otherKey, otherCA := newM2MCertificateAuthority(t)
	untrusted := newM2MLeafCertificate(t, otherCA, otherKey, "principal-untrusted", true)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return m2mTLSCertificates{pool: pool, caFile: writeM2MPEM(t, dir, "client-ca.pem", "CERTIFICATE", ca.Raw), serverCertFile: writeM2MPEM(t, dir, "server.pem", "CERTIFICATE", server.Certificate[0]), serverKeyFile: writeM2MPEM(t, dir, "server-key.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(server.PrivateKey.(*rsa.PrivateKey))), goodClient: good, untrustedClient: untrusted, deniedClient: denied, deniedFingerprint: m2mCertificateFingerprint(denied.Certificate[0])}
}

func newM2MCertificateAuthority(t *testing.T) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "m2m test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return key, cert
}

func newM2MLeafCertificate(t *testing.T, ca *x509.Certificate, caKey crypto.Signer, name string, client bool) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
	if client {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writeM2MPEM(t *testing.T, dir, name, kind string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func m2mCertificateFingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}
func assertM2MTLSDial(t *testing.T, addr string, roots *x509.CertPool, name string, certificate tls.Certificate, want bool) {
	t.Helper()
	config := &tls.Config{RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS13}
	if len(certificate.Certificate) != 0 {
		config.Certificates = []tls.Certificate{certificate}
	}
	dialer := &net.Dialer{Timeout: time.Second}
	connection, err := tls.DialWithDialer(dialer, "tcp", addr, config)
	// TLS 1.3 allows the client to finish locally before it observes the
	// server's fatal client-certificate alert. Force one application read so
	// this assertion observes the peer's final authentication outcome.
	if err == nil && !want {
		_ = connection.SetDeadline(time.Now().Add(250 * time.Millisecond))
		_, err = connection.Read(make([]byte, 1))
	}
	if connection != nil {
		_ = connection.Close()
	}
	if (err == nil) != want {
		t.Fatalf("TLS dial serverName=%q certificate=%t err=%v want success=%t", name, len(certificate.Certificate) != 0, err, want)
	}
}
