package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestPUBLIC05SunSpecSemRegSurvivesGatewayProvidersAndPortal(t *testing.T) {
	words := sunspectest.ObservedFroniusFloatControlsWords()
	sunspectest.SetFloat(words, 20, 1_234.5)
	sunspectest.SetFloat(words, 22, 50)
	listener, _, err := sunspectest.ServeSunSpecChain(words)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{
		Enabled: true, Endpoint: "tcp://" + listener.Addr().String(), DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := modbusadapter.Start(context.Background(), config, (&net.Dialer{}).DialContext, func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return modbus.NewTCPEndpoint(config)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := modbusadapter.NewSunSpecProducer(adapter, modbusadapter.SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "test:public-05-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := producer.Qualify(context.Background(), modbusadapter.SunSpecPollIdentity{PollGeneration: 501, DeadlineIdentity: 601})
	if err != nil || initial.Outcome != modbusadapter.SunSpecQualificationGO {
		t.Fatalf("initial qualification=%+v err=%v", initial, err)
	}

	// Use the production 30-second fast-telemetry policy. The next healthy
	// refresh keeps power current while the invalid frequency field can retain
	// only the now-stale prior 50 Hz evidence.
	time.Sleep(31 * time.Second)
	sunspectest.SetFloat(words, 20, 4_321.5)
	sunspectest.SetFloat(words, 22, 2_000)
	refresh, err := producer.Refresh(context.Background(), modbusadapter.SunSpecPollIdentity{PollGeneration: 502, DeadlineIdentity: 602})
	if err != nil || refresh.Outcome != modbusadapter.SunSpecQualificationGO {
		t.Fatalf("invalid-frequency refresh=%+v err=%v", refresh, err)
	}
	direct, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("native refresh did not publish a SemReg PV view")
	}
	assertPUBLIC05ComposedPV(t, direct)

	provider := newGatewayModbusMCPProvider(adapter).(*gatewayModbusMCPProvider)
	mcpResult, err := provider.SemanticPVCurrent(context.Background(), initial.CapabilityID, initial.SampleID)
	if err != nil {
		t.Fatal(err)
	}
	mcpData, ok := mcpResult.Data.(map[string]any)
	if !ok {
		t.Fatalf("Gateway MCP provider changed the native SemReg view: %#v", mcpResult.Data)
	}
	providerView, ok := public05ProviderView(mcpData)
	if !ok || !reflect.DeepEqual(providerView.Snapshot, direct.Snapshot) || !reflect.DeepEqual(providerView.Projection, direct.Projection) {
		t.Fatalf("Gateway MCP provider changed snapshot/projection identity: %#v", mcpResult.Data)
	}
	assertPUBLIC05ComposedPV(t, providerView)

	certs := newM2MTLSCertificates(t)
	asset := string(direct.Snapshot.AssetID)
	runtime, err := newM2MGraphQLRuntime(ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{asset},
	}}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	dir := t.TempDir()
	clientCert := writeM2MPEM(t, dir, "portal-client.pem", "CERTIFICATE", certs.goodClient.Certificate[0])
	clientKey := writeM2MPEM(t, dir, "portal-client-key.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(certs.goodClient.PrivateKey.(*rsa.PrivateKey)))
	forward, err := newPortalPVClient(ebusgateway.PortalPVConfig{
		SemanticEnabled: true,
		M2MURL:          "https://localhost:" + runtime.Addr()[len("127.0.0.1:"):] + "/graphql/m2m/v1",
		M2MServerName:   "m2m.gateway.test",
		M2MCAFile:       certs.caFile,
		M2MClientCert:   clientCert,
		M2MClientKey:    clientKey,
		AssetRef:        asset,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := portal.NewHandler(portal.Options{SemanticPVEnabled: true, SemanticPV: forward})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/semantic/pv/current", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("Portal composed response=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	portalPV := envelope.Data["semanticPVCurrent"]
	var portalView modbusadapter.SemanticPVCurrent
	if len(portalPV) == 0 || json.Unmarshal(portalPV, &portalView) != nil || !reflect.DeepEqual(portalView.Snapshot, direct.Snapshot) || !reflect.DeepEqual(portalView.Projection, direct.Projection) {
		t.Fatalf("Portal/GraphQL changed the native SemReg snapshot/projection: %s", portalPV)
	}
	assertPUBLIC05ComposedPV(t, portalView)
}

func public05ProviderView(data map[string]any) (modbusadapter.SemanticPVCurrent, bool) {
	snapshot, snapshotOK := data["snapshot"].(semreg.Snapshot)
	evaluation, evaluationOK := data["evaluation"].(semreg.EvaluationView)
	selections, selectionsOK := data["selections"].([]semreg.Selection)
	report, projectionOK := data["projection"].(projection.ProjectionReport)
	return modbusadapter.SemanticPVCurrent{Snapshot: snapshot, Evaluation: evaluation, Selections: selections, Projection: report}, snapshotOK && evaluationOK && selectionsOK && projectionOK
}

func assertPUBLIC05ComposedPV(t *testing.T, current modbusadapter.SemanticPVCurrent) {
	t.Helper()
	var power, frequency *semreg.FactEnvelope
	for index := range current.Snapshot.Facts {
		switch current.Snapshot.Facts[index].Key.FactID {
		case "pv.ac.aggregate_active_power":
			power = &current.Snapshot.Facts[index]
		case "pv.ac.frequency":
			frequency = &current.Snapshot.Facts[index]
		}
	}
	if power == nil || len(power.Candidates) != 1 || power.Candidates[0].Value == nil || power.Candidates[0].Value.Quantity == nil ||
		power.Candidates[0].Value.Quantity.Number.Coefficient != "43215" || power.Candidates[0].Value.Quantity.Number.Exponent10 != -1 {
		t.Fatalf("composed active power=%+v; want exact 4321.5 W", power)
	}
	if frequency == nil || len(frequency.Candidates) != 1 || frequency.Candidates[0].Value == nil || frequency.Candidates[0].Value.Quantity == nil ||
		frequency.Candidates[0].Value.Quantity.Number.Coefficient != "5" || frequency.Candidates[0].Value.Quantity.Number.Exponent10 != 1 {
		t.Fatalf("composed retained frequency=%+v; want exact prior 50 Hz", frequency)
	}
	var powerFresh, frequencyStale bool
	for _, evaluated := range current.Evaluation.Facts {
		switch evaluated.CandidateID {
		case power.Candidates[0].CandidateID:
			powerFresh = evaluated.Freshness == semreg.FreshnessFresh && evaluated.EffectiveAvailability == semreg.AvailabilityAvailable
		case frequency.Candidates[0].CandidateID:
			frequencyStale = evaluated.Freshness == semreg.FreshnessStale && evaluated.EffectiveAvailability == semreg.AvailabilityDegraded
		}
	}
	var powerSelected, frequencySelected bool
	for _, selection := range current.Selections {
		switch selection.Key.FactID {
		case power.Key.FactID:
			powerSelected = selection.SelectedCandidate == power.Candidates[0].CandidateID
		case frequency.Key.FactID:
			frequencySelected = true
		}
	}
	if !powerFresh || !frequencyStale || !powerSelected || frequencySelected {
		t.Fatalf("composed evaluation/selection lost field isolation: evaluation=%+v selections=%+v", current.Evaluation.Facts, current.Selections)
	}
	var powerExact, frequencyWithheld bool
	for _, disposition := range current.Projection.Dispositions {
		switch disposition.ItemID {
		case "projection.gateway.pv.inverter.ac.power.active":
			powerExact = disposition.Outcome == projection.ProjectionExact && len(disposition.Loss) == 0
		case "projection.gateway.pv.inverter.ac.frequency":
			frequencyWithheld = disposition.Outcome == projection.ProjectionWithheld && disposition.Reason != nil && *disposition.Reason == "mapping.field_invalid"
		}
	}
	if !powerExact || !frequencyWithheld {
		t.Fatalf("composed projection lost field isolation: %+v", current.Projection.Dispositions)
	}
}
