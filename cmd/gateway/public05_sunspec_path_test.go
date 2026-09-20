package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestPUBLIC05SunSpecSemRegSurvivesGatewayProvidersAndPortal(t *testing.T) {
	source := &public05SunSpecSource{words: public05SunSpecWords()}
	source.setFloat(20, 1_234.5)
	source.setFloat(22, 50)
	listener := source.serve(t)

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

	source.setFloat(20, 4_321.5)
	source.setFloat(22, 2_000)
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

type public05SunSpecSource struct {
	mu    sync.RWMutex
	words []uint16
}

func (source *public05SunSpecSource) setFloat(payloadOffset int, value float32) {
	const payloadStart = 2 + 67 + 2
	bits := math.Float32bits(value)
	source.mu.Lock()
	defer source.mu.Unlock()
	source.words[payloadStart+payloadOffset] = uint16(bits >> 16)
	source.words[payloadStart+payloadOffset+1] = uint16(bits)
}

func (source *public05SunSpecSource) serve(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		for {
			header := make([]byte, 7)
			if _, err := io.ReadFull(connection, header); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(header[4:6]))
			body := make([]byte, length-1)
			if _, err := io.ReadFull(connection, body); err != nil || len(body) != 5 {
				return
			}
			offset := binary.BigEndian.Uint16(body[1:3])
			count := binary.BigEndian.Uint16(body[3:5])
			start, end := int(offset)-40000, int(offset)-40000+int(count)
			source.mu.RLock()
			if start < 0 || end > len(source.words) {
				source.mu.RUnlock()
				return
			}
			response := make([]byte, 9+2*int(count))
			copy(response[:2], header[:2])
			binary.BigEndian.PutUint16(response[4:6], uint16(3+2*int(count)))
			response[6], response[7], response[8] = header[6], body[0], byte(2*count)
			for index, word := range source.words[start:end] {
				binary.BigEndian.PutUint16(response[9+2*index:], word)
			}
			source.mu.RUnlock()
			if _, err := connection.Write(response); err != nil {
				return
			}
		}
	}()
	return listener
}

type public05SunSpecModel struct {
	id, length uint16
	payload    []uint16
}

func public05SunSpecWords() []uint16 {
	common := make([]uint16, 65)
	public05PutSunSpecString(common[0:16], "Fronius")
	public05PutSunSpecString(common[16:32], "Symo GEN24 10.0")
	public05PutSunSpecString(common[40:48], "1.41.11-1")
	public05PutSunSpecString(common[48:64], "synthetic")
	inverter := make([]uint16, 60)
	inverter[46] = 4
	mppt := make([]uint16, 88)
	mppt[6] = 4
	models := []public05SunSpecModel{
		{1, 65, common}, {113, 60, inverter}, {120, 26, make([]uint16, 26)}, {121, 30, make([]uint16, 30)},
		{122, 44, make([]uint16, 44)}, {123, 24, make([]uint16, 24)}, {160, 88, mppt}, {124, 24, make([]uint16, 24)},
	}
	words := []uint16{0x5375, 0x6e53}
	for _, model := range models {
		words = append(words, model.id, model.length)
		words = append(words, model.payload...)
	}
	return append(words, 0xffff, 0)
}

func public05PutSunSpecString(words []uint16, value string) {
	data := []byte(value)
	for index := range words {
		var high, low byte
		if 2*index < len(data) {
			high = data[2*index]
		}
		if 2*index+1 < len(data) {
			low = data[2*index+1]
		}
		words[index] = uint16(high)<<8 | uint16(low)
	}
}
