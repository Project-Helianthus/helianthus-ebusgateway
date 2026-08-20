package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

type m2mGraphQLRuntime struct {
	listener net.Listener
	server   *http.Server
}

const (
	m2mHTTPHeaderTimeout    = 5 * time.Second
	m2mHTTPBodyTimeout      = 10 * time.Second
	m2mMaxPreTLSConnections = 16
)

type boundedM2MListener struct {
	net.Listener
	slots chan struct{}
}

type boundedM2MConnection struct {
	net.Conn
	release func()
	once    sync.Once
}

func newBoundedM2MListener(listener net.Listener, capacity int) *boundedM2MListener {
	return &boundedM2MListener{Listener: listener, slots: make(chan struct{}, capacity)}
}

func (listener *boundedM2MListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case listener.slots <- struct{}{}:
			return &boundedM2MConnection{Conn: connection, release: func() { <-listener.slots }}, nil
		default:
			_ = connection.Close()
		}
	}
}

func (connection *boundedM2MConnection) Close() error {
	err := connection.Conn.Close()
	connection.once.Do(connection.release)
	return err
}

func newM2MGraphQLRuntime(config ebusgateway.Config, adapter *modbusadapter.Adapter) (*m2mGraphQLRuntime, error) {
	if config.M2MGraphQL.Disabled() {
		return nil, nil
	}
	if err := validateM2MGraphQLConfig(config.M2MGraphQL); err != nil {
		return nil, err
	}
	tlsConfig, err := newM2MTLSConfig(config.M2MGraphQL)
	if err != nil {
		return nil, errors.New("M2M GraphQL TLS configuration is invalid")
	}
	allowed := make(map[string]struct{}, len(config.M2MGraphQL.AllowedAssets))
	for _, asset := range config.M2MGraphQL.AllowedAssets {
		allowed[asset] = struct{}{}
	}
	known := make(map[string]struct{}, len(config.M2MGraphQL.KnownAssets))
	for _, asset := range config.M2MGraphQL.KnownAssets {
		known[asset] = struct{}{}
	}
	handler, err := m2mgraphql.NewHandler(m2mgraphql.Config{
		AllowedAssets: allowed,
		AssetExists: func(asset string) bool {
			if _, ok := known[asset]; ok {
				return true
			}
			if adapter == nil {
				return false
			}
			_, _, ok := adapter.CanonicalPVSnapshotByAsset(asset)
			return ok
		},
		SnapshotByAssetAt: func(_ context.Context, asset string) (pv.Snapshot, time.Time, bool) {
			if adapter == nil {
				return pv.Snapshot{}, time.Time{}, false
			}
			return adapter.CanonicalPVSnapshotByAsset(asset)
		},
	})
	if err != nil {
		return nil, errors.New("M2M GraphQL handler configuration is invalid")
	}
	rawListener, err := net.Listen("tcp", config.M2MGraphQL.ListenAddr)
	if err != nil {
		return nil, errors.New("M2M GraphQL listener could not start")
	}
	listener := newBoundedM2MListener(rawListener, m2mMaxPreTLSConnections)
	runtime := &m2mGraphQLRuntime{listener: listener}
	runtime.server = &http.Server{TLSConfig: tlsConfig, ErrorLog: log.New(io.Discard, "", 0), ReadHeaderTimeout: m2mHTTPHeaderTimeout, ReadTimeout: m2mHTTPBodyTimeout, WriteTimeout: m2mHTTPBodyTimeout, IdleTimeout: m2mHTTPBodyTimeout, Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(response, request.WithContext(m2mgraphql.WithMTLSPrincipal(request.Context(), m2mFingerprint(request.TLS.PeerCertificates[0].Raw))))
	})}
	go func() { _ = runtime.server.Serve(tls.NewListener(listener, tlsConfig)) }()
	return runtime, nil
}

func newM2MTLSConfig(config ebusgateway.M2MGraphQLConfig) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(config.ServerCertFile, config.ServerKeyFile)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil || leaf.VerifyHostname(config.ServerName) != nil {
		return nil, errors.New("server certificate identity mismatch")
	}
	certificate.Leaf = leaf
	caPEM, err := os.ReadFile(config.ClientCAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("invalid client CA")
	}
	denied := make(map[string]struct{}, len(config.DeniedPrincipalFingerprints))
	for _, fingerprint := range config.DeniedPrincipalFingerprints {
		denied[strings.ToLower(fingerprint)] = struct{}{}
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if len(verifiedChains) == 0 || len(rawCerts) == 0 {
			return errors.New("unverified client certificate")
		}
		if _, blocked := denied[m2mFingerprint(rawCerts[0])]; blocked {
			return errors.New("denied client certificate")
		}
		return nil
	}, SessionTicketsDisabled: true}, nil
}

func m2mFingerprint(der []byte) string { sum := sha256.Sum256(der); return hex.EncodeToString(sum[:]) }
func (runtime *m2mGraphQLRuntime) Addr() string {
	if runtime == nil || runtime.listener == nil {
		return ""
	}
	return runtime.listener.Addr().String()
}
func (runtime *m2mGraphQLRuntime) RequiresVerifiedClientCertificate() bool {
	return runtime != nil && runtime.server != nil
}
func (runtime *m2mGraphQLRuntime) Close() error {
	if runtime == nil || runtime.server == nil {
		return nil
	}
	return runtime.server.Close()
}
