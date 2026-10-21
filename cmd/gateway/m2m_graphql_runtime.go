package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/feedv1"
)

type m2mGraphQLRuntime struct {
	listener net.Listener
	server   *http.Server
}

const (
	m2mHTTPHeaderTimeout     = 5 * time.Second
	m2mHTTPBodyTimeout       = 10 * time.Second
	m2mResponseHeadroom      = 500 * time.Millisecond
	m2mProcessingHeadroom    = 250 * time.Millisecond
	m2mRequestTimeout        = m2mHTTPBodyTimeout - m2mResponseHeadroom
	m2mNativeOperationBudget = m2mRequestTimeout - m2mProcessingHeadroom
	m2mMaxPreTLSConnections  = 16
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

func newM2MGraphQLRuntime(config ebusgateway.Config, adapter *modbusadapter.Adapter, growatt ...*growattBMSRS485ProductionProvider) (*m2mGraphQLRuntime, error) {
	var storage *growattBMSRS485ProductionProvider
	if len(growatt) == 1 {
		storage = growatt[0]
	}
	return newM2MGraphQLRuntimeWithTesla(config, adapter, storage, nil)
}

func newM2MGraphQLRuntimeWithTesla(config ebusgateway.Config, adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, tesla *teslaHSCRetainedOwner) (*m2mGraphQLRuntime, error) {
	return newM2MGraphQLRuntimeWithMatter(config, adapter, growatt, tesla, nil)
}

func newM2MGraphQLRuntimeWithMatter(config ebusgateway.Config, adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, tesla *teslaHSCRetainedOwner, matterProvider feedv1.Provider, managed ...*managedSidecarBindings) (*m2mGraphQLRuntime, error) {
	if config.M2MGraphQL.Disabled() {
		return nil, nil
	}
	if err := validateM2MGraphQLConfig(config.M2MGraphQL); err != nil {
		return nil, err
	}
	if err := config.ValidatePortalStorage(); err != nil {
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
	var semanticEVSECurrent func(context.Context, string) (json.RawMessage, bool)
	if tesla != nil {
		semanticEVSECurrent = func(ctx context.Context, asset string) (json.RawMessage, bool) {
			current, err := currentTeslaEVSEPublic(ctx, asset, tesla)
			return current, err == nil
		}
	}
	pvCurrent := func(_ context.Context, asset string) (json.RawMessage, bool) {
		if adapter == nil {
			return nil, false
		}
		current, ok := adapter.SemanticPVCurrentByAsset(asset)
		if !ok {
			return nil, false
		}
		encoded, err := json.Marshal(map[string]any{"snapshot": current.Snapshot, "evaluation": current.Evaluation, "selections": current.Selections, "projection": current.Projection})
		return encoded, err == nil
	}
	storageCurrent := newGrowattStorageGraphQLProvider(growatt)
	if len(managed) == 1 && managed[0] != nil {
		bindings := managed[0]
		pvCurrent = func(ctx context.Context, asset string) (out json.RawMessage, ok bool) {
			err := bindings.modbus(ctx, func(adapter *modbusadapter.Adapter) error {
				current, available := adapter.SemanticPVCurrentByAsset(asset)
				if !available {
					return errors.New("PV publication unavailable")
				}
				var err error
				out, err = json.Marshal(map[string]any{"snapshot": current.Snapshot, "evaluation": current.Evaluation, "selections": current.Selections, "projection": current.Projection})
				return err
			})
			return out, err == nil
		}
		storageCurrent = func(ctx context.Context, asset string) (out json.RawMessage, ok bool) {
			err := bindings.growatt(ctx, func(provider *growattBMSRS485ProductionProvider) error {
				var err error
				out, err = currentGrowattStoragePublic(ctx, asset, provider)
				return err
			})
			return out, err == nil
		}
	}
	graphqlHandler, err := m2mgraphql.NewHandler(m2mgraphql.Config{
		AllowedAssets:          allowed,
		SemanticPVCurrent:      pvCurrent,
		SemanticStorageCurrent: storageCurrent,
		SemanticEVSECurrent:    semanticEVSECurrent,
	})
	if err != nil {
		return nil, errors.New("M2M GraphQL handler configuration is invalid")
	}
	handler := graphqlHandler
	if matterProvider != nil {
		instance, err := newMatterBindingFeedInstanceID()
		if err != nil {
			return nil, errors.New("matter binding feed instance identity is unavailable")
		}
		matterHandler, err := feedv1.New(matterProvider, feedv1.Options{InstanceID: instance, ReplayLimit: 64})
		if err != nil {
			return nil, errors.New("matter binding feed handler configuration is invalid")
		}
		// The Matter routes spend the same per-host request budget as GraphQL
		// before the serialized capture; a refusal is the feed's own error.
		budgetedMatter, err := m2mgraphql.RequestBudget(graphqlHandler, http.StripPrefix("/matter-binding", matterHandler), feedv1.WriteUnavailable)
		if err != nil {
			return nil, errors.New("matter binding feed request budget is unavailable")
		}
		mux := http.NewServeMux()
		mux.Handle("/graphql/m2m/v1", graphqlHandler)
		mux.Handle("/matter-binding/", budgetedMatter)
		handler = mux
	}
	rawListener, err := net.Listen("tcp", config.M2MGraphQL.ListenAddr)
	if err != nil {
		return nil, errors.New("M2M GraphQL listener could not start")
	}
	listener := newBoundedM2MListener(rawListener, m2mMaxPreTLSConnections)
	runtime := &m2mGraphQLRuntime{listener: listener}
	// The listener provides server-authenticated TLS 1.3 transport integrity
	// only. No client principal is derived: client certificates are optional
	// and publication is filtered by the configured asset allow-list.
	runtime.server = &http.Server{TLSConfig: tlsConfig, ErrorLog: log.New(io.Discard, "", 0), ReadHeaderTimeout: m2mHTTPHeaderTimeout, ReadTimeout: m2mHTTPBodyTimeout, WriteTimeout: m2mHTTPBodyTimeout, IdleTimeout: m2mHTTPBodyTimeout, Handler: newM2MRequestDeadlineHandler(handler, m2mRequestTimeout)}
	go func() { _ = runtime.server.Serve(tls.NewListener(listener, tlsConfig)) }()
	return runtime, nil
}

func newMatterBindingFeedInstanceID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "matter-" + hex.EncodeToString(value[:]), nil
}

func currentTeslaEVSEPublic(ctx context.Context, asset string, tesla *teslaHSCRetainedOwner) (json.RawMessage, error) {
	if tesla == nil || asset != tesla.cfg.AssetID {
		return nil, errors.New("tesla HSC EVSE asset unavailable")
	}
	current, err := tesla.TeslaGen3EVSESemanticCurrent(ctx)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(current)
	if err != nil || !json.Valid(encoded) {
		return nil, errors.New("tesla HSC EVSE publication is invalid")
	}
	return json.RawMessage(encoded), nil
}

func newM2MRequestDeadlineHandler(next http.Handler, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), timeout)
		defer cancel()
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}

// newGrowattStorageGraphQLProvider deliberately refreshes through the same
// serialized native-observation -> SemReg publication transaction as MCP.
// A GraphQL or Portal reader therefore neither depends on MCP priming nor
// observes an unevaluated cache entry.
func newGrowattStorageGraphQLProvider(growatt ...*growattBMSRS485ProductionProvider) func(context.Context, string) (json.RawMessage, bool) {
	return func(ctx context.Context, asset string) (json.RawMessage, bool) {
		published, err := currentGrowattStoragePublic(ctx, asset, growatt...)
		return published, err == nil
	}
}

func currentGrowattStoragePublic(ctx context.Context, asset string, growatt ...*growattBMSRS485ProductionProvider) (json.RawMessage, error) {
	if len(growatt) != 1 || growatt[0] == nil || growatt[0].storage == nil || asset != string(growatt[0].storage.assetID) {
		return nil, errors.New("growatt BMS storage asset unavailable")
	}
	published, err := growatt[0].GrowattStorageSemanticCurrent(ctx)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(published)
	if err != nil || !json.Valid(encoded) {
		return nil, errors.New("growatt BMS storage publication is invalid")
	}
	return json.RawMessage(encoded), nil
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
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.NoClientCert, SessionTicketsDisabled: true}
	return tlsConfig, nil
}

func (runtime *m2mGraphQLRuntime) Addr() string {
	if runtime == nil || runtime.listener == nil {
		return ""
	}
	return runtime.listener.Addr().String()
}
func (runtime *m2mGraphQLRuntime) Close() error {
	if runtime == nil || runtime.server == nil {
		return nil
	}
	return runtime.server.Close()
}
