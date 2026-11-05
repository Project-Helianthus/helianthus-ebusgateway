package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type h10Step struct {
	Unit     byte   `json:"unit"`
	Request  []byte `json:"request"`
	Response []byte `json:"response"`
}
type h10Cycle struct {
	Name  string    `json:"name"`
	Steps []h10Step `json:"steps"`
}
type h10Replay struct {
	Family       string     `json:"family"`
	AllowedUnits []byte     `json:"allowed_units"`
	Cycles       []h10Cycle `json:"cycles"`
}

func h10Load(t *testing.T, name string) h10Replay {
	t.Helper()
	data, err := os.ReadFile("testdata/huawei/" + name + "_replay.json")
	if err != nil {
		t.Fatal(err)
	}
	var out h10Replay
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// This replay serves a test-only local TCP stream. Every
// emitted PDU is compared with the independent public-fixture replay order.
type h10Conn struct {
	mu       sync.Mutex
	steps    []h10Step
	recorded [][]byte
	response []byte
	timeout  bool
	clock    *h10Clock
	closed   bool
}

func (c *h10Conn) Write(raw []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if len(raw) < 8 || len(c.steps) == 0 {
		return 0, errors.New("unexpected fake TCP request")
	}
	step := c.steps[0]
	c.steps = c.steps[1:]
	pdu := append([]byte(nil), raw[7:]...)
	c.recorded = append(c.recorded, pdu)
	if raw[6] != step.Unit || !bytes.Equal(pdu, step.Request) {
		return 0, errors.New("fake TCP PDU mismatch")
	}
	if pdu[0] != 3 && (pdu[0] != 43 || len(pdu) != 4 || pdu[1] != 14) {
		return 0, errors.New("unsafe function")
	}
	if len(step.Response) == 0 {
		c.timeout = true
		return len(raw), nil
	}
	c.response = append([]byte(nil), raw[:7]...)
	binary.BigEndian.PutUint16(c.response[4:6], uint16(len(step.Response)+1))
	c.response = append(c.response, step.Response...)
	return len(raw), nil
}
func (c *h10Conn) Read(dst []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if c.timeout {
		c.timeout = false
		c.mu.Unlock()
		_ = c.clock.Wait(context.Background(), 5*time.Second)
		c.mu.Lock()
		return 0, os.ErrDeadlineExceeded
	}
	if len(c.response) == 0 {
		return 0, io.EOF
	}
	n := copy(dst, c.response)
	c.response = c.response[n:]
	return n, nil
}
func (c *h10Conn) Close() error                     { c.mu.Lock(); defer c.mu.Unlock(); c.closed = true; return nil }
func (c *h10Conn) LocalAddr() net.Addr              { return nil }
func (c *h10Conn) RemoteAddr() net.Addr             { return nil }
func (c *h10Conn) SetDeadline(time.Time) error      { return nil }
func (c *h10Conn) SetReadDeadline(time.Time) error  { return nil }
func (c *h10Conn) SetWriteDeadline(time.Time) error { return nil }

type h10Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *h10Clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *h10Clock) Wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return nil
}

func h10Loopback(t *testing.T, replay *h10Conn) string {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		peer, err := listener.AcceptTCP()
		if err != nil {
			return
		}
		defer func() { _ = peer.Close() }()
		t.Cleanup(func() { _ = peer.Close() })
		for {
			header := make([]byte, 7)
			if _, err := io.ReadFull(peer, header); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint16(header[4:6])) - 1
			if size < 1 || size > modbus.MaxTolerantPDUSize {
				t.Error("invalid replay request size")
				return
			}
			raw := append(header, make([]byte, size)...)
			if _, err := io.ReadFull(peer, raw[7:]); err != nil {
				return
			}
			if _, err := replay.Write(raw); err != nil {
				t.Error(err)
				return
			}
			response := make([]byte, 7+modbus.MaxTolerantPDUSize)
			n, err := replay.Read(response)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := peer.Write(response[:n]); err != nil {
				return
			}
		}
	}()
	return "tcp://" + listener.Addr().String()
}

func h10Native(t *testing.T, fixture h10Replay) (huaweiRegistryProvider, *modbusadapter.Adapter, *h10Conn, *h10Clock) {
	t.Helper()
	clock := &h10Clock{now: time.Now().Add(time.Second)}
	conn := &h10Conn{clock: clock}
	for _, cycle := range fixture.Cycles {
		conn.steps = append(conn.steps, cycle.Steps...)
	}
	// The native endpoint requires a real TCP connection; only the local replay listener is dialed.
	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: h10Loopback(t, conn), DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	config.Clock = clock.Now
	config.SemanticPV, err = modbusadapter.NewSemanticPVClass(newTestSemanticHub(t))
	if err != nil {
		t.Fatal(err)
	}
	config.Endpoint.Framing = modbus.TCPFramingTolerant
	config.Endpoint.MaxBufferedBytes = 7 + modbus.MaxTolerantPDUSize
	adapter, err := modbusadapter.Start(context.Background(), config, func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}, func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return modbus.NewTCPEndpoint(config)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	session, err := modbusadapter.NewReadOnlySession(adapter)
	if err != nil {
		t.Fatal(err)
	}
	typ := reflect.TypeOf(session)
	for i := 0; i < typ.NumMethod(); i++ {
		if typ.Method(i).Name == "Write" || typ.Method(i).Name == "WriteHolding" {
			t.Fatal("session exposes write")
		}
	}
	family := modbusTCPFamilyHuaweiSmartLogger
	if fixture.Family == "S-Dongle" {
		family = modbusTCPFamilyHuaweiSDongle
	}
	provider, err := newHuaweiRegistryProvider(family, modbusreg.HuaweiProviderConfig{Source: "alpha", AllowedUnits: fixture.AllowedUnits, Clock: clock.Now, Wait: clock.Wait})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Start(session); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider, adapter, conn, clock
}
func h10Refresh(t *testing.T, provider huaweiRegistryProvider, clock *h10Clock) modbusreg.HuaweiProviderCycle {
	t.Helper()
	if next := provider.NextRefreshAt(); next.After(clock.Now()) {
		_ = clock.Wait(context.Background(), next.Sub(clock.Now()))
	}
	cycle, err := provider.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return cycle
}
func TestH10TCPProviderPVSnapshotParity(t *testing.T) {
	for _, name := range []string{"smartlogger", "sdongle"} {
		t.Run(name, func(t *testing.T) {
			fixture := h10Load(t, name)
			provider, _, conn, clock := h10Native(t, fixture)
			hub := newTestSemanticHub(t)
			publication, err := newHuaweiPVPublication(hub, "huawei-test", "source:huawei-alpha", "alpha", nil, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			cycle := h10Refresh(t, provider, clock)
			if cycle.Outcome != modbusreg.HuaweiProviderCycleComplete {
				t.Fatalf("identity: %+v", cycle)
			}
			if err := publication.Publish(cycle); err != nil {
				t.Fatal(err)
			}
			if len(publication.published) == 0 {
				t.Fatal("provider published no PV assets")
			}
			snapshotProvider := newGatewaySemanticSnapshotProvider(&gatewayRuntimeControl{hub: hub})
			snapshotProvider.now = clock.Now
			gql, mcp := m11Handlers(t, snapshotProvider)
			for _, asset := range publication.published {
				selection := m11Selection(reflect.TypeOf(semanticview.View{}))
				query := fmt.Sprintf("{a:semanticSnapshot(assetIds:[%q])%s b:semanticSnapshot(assetIds:[%q])%s}", asset, selection, asset, selection)
				response := mcpRPC(t, mcp, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"semantic.v1.snapshot.get","arguments":{"asset_ids":[%q]}}}`, asset))
				if response.Error != nil {
					t.Fatal(response.Error)
				}
				var envelope map[string]any
				if err := json.Unmarshal([]byte(response.Result["content"].([]any)[0].(map[string]any)["text"].(string)), &envelope); err != nil {
					t.Fatal(err)
				}
				m11AssertParity(t, m11GraphQL(t, gql, query), envelope)
			}
			baselineReceipts := map[semreg.CandidateID]semreg.TimePoint{}
			for unit, asset := range publication.published {
				view, err := hub.Inspect(asset)
				if err != nil {
					t.Fatal(err)
				}
				if len(view.Snapshot.Facts) == 0 {
					t.Fatal("empty PV facts")
				}
				for _, envelope := range view.Snapshot.Facts {
					for _, candidate := range envelope.Candidates {
						baselineReceipts[candidate.CandidateID] = candidate.Times.ReceivedAt
					}
					if envelope.Key.PackID != huaweiPVPack {
						t.Fatal("foreign pack")
					}
				}
				if asset != huaweiAssetID("alpha", "pv", unit) {
					t.Fatal("asset identity drift")
				}
			}
			second := h10Refresh(t, provider, clock)
			last := second
			if err := publication.Publish(second); err != nil {
				t.Fatal(err)
			}
			if name == "smartlogger" {
				stale, fresh := false, false
				for _, child := range second.Children {
					asset, ok := publication.published[child.UnitID]
					if !ok {
						continue
					}
					view, _ := hub.Inspect(asset)
					for _, env := range view.Snapshot.Facts {
						for _, fact := range env.Candidates {
							if child.Evidence == modbusreg.HuaweiProviderEvidenceStale {
								stale = stale || fact.Quality.Freshness == semreg.FreshnessStale
								if fact.Times.ReceivedAt != baselineReceipts[fact.CandidateID] {
									t.Fatal("retained child fact received a new receipt time")
								}
							} else {
								fresh = fresh || fact.Quality.Freshness == semreg.FreshnessFresh
							}
						}
					}
				}
				if !stale || !fresh {
					t.Fatal("child timeout did not preserve isolated stale/fresh facts")
				}
			}
			for _, raw := range conn.recorded {
				if raw[0] != 3 && (raw[0] != 43 || raw[1] != 14) {
					t.Fatal("unsafe PDU recorded")
				}
			}
			if name == "sdongle" {
				third := h10Refresh(t, provider, clock)
				last = third
				if err := publication.Publish(third); err != nil {
					t.Fatal(err)
				}
			}
			conn.mu.Lock()
			remaining := append([]h10Step(nil), conn.steps...)
			conn.mu.Unlock()
			if len(remaining) != 0 {
				// A caller deadline can stop an attempt before a socket write, or
				// fence the native generation. All four failed native attempts are
				// still required; only an identical timeout-only wire suffix may
				// remain. A successful response can never be skipped.
				if len(remaining) > 3 || len(last.Exchanges) < 4 {
					t.Fatalf("unreplayed PDUs without bounded failed attempts: %d", len(remaining))
				}
				for i, exchange := range last.Exchanges[len(last.Exchanges)-4:] {
					if exchange.Attempt != i+1 || exchange.UnitID != remaining[0].Unit || exchange.Outcome == modbusreg.HuaweiProviderExchangeAnswered || exchange.Outcome == modbusreg.HuaweiProviderExchangeException {
						t.Fatal("missing wire requests lack native failure evidence")
					}
				}
				for _, step := range remaining {
					if len(step.Response) != 0 || step.Unit != remaining[0].Unit || !bytes.Equal(step.Request, remaining[0].Request) {
						t.Fatal("non-timeout replay step omitted")
					}
				}
			}
		})
	}
}

func TestH10FenceIsolationSuccessorAndLatePublication(t *testing.T) {
	fixture := h10Load(t, "smartlogger")
	provider, _, _, clock := h10Native(t, fixture)
	cycle := h10Refresh(t, provider, clock)
	hub := newTestSemanticHub(t)
	active := true
	first, err := newHuaweiPVPublication(hub, "huawei-first", "source:huawei-first", "alpha", nil, func() bool { return active })
	if err != nil {
		t.Fatal(err)
	}
	other, err := newHuaweiPVPublication(hub, "huawei-other", "source:huawei-other", "beta", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	otherCycle := cycle
	otherCycle.Source = "beta"
	if err := other.Publish(otherCycle); err != nil {
		t.Fatal(err)
	}
	active = false
	if err := first.Fence(); err != nil {
		t.Fatal(err)
	}
	for _, asset := range first.published {
		h10AssertWithdrawn(t, hub, asset)
	}
	for _, asset := range other.published {
		if !hub.Readable(asset) {
			t.Fatal("other instance withdrawn")
		}
	}
	successor, err := newHuaweiPVPublication(hub, "huawei-first", "source:huawei-first", "alpha", first.handle, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	// A restart re-identifies through a fresh native session and connection.
	freshProvider, _, _, freshClock := h10Native(t, fixture)
	if !freshClock.Now().After(cycle.CompletedAt) {
		_ = freshClock.Wait(context.Background(), cycle.CompletedAt.Sub(freshClock.Now())+time.Second)
	}
	next := h10Refresh(t, freshProvider, freshClock)
	if err := successor.Publish(next); err != nil {
		t.Fatal(err)
	}
	late := next
	late.Cycle++
	if err := first.Publish(late); err == nil {
		t.Fatal("late predecessor publication admitted")
	}
	for _, asset := range successor.published {
		if !hub.Readable(asset) {
			t.Fatal("successor not readable")
		}
	}
}

func TestH10MappingValuesAndWithheldFacts(t *testing.T) {
	for _, name := range []string{"smartlogger", "sdongle"} {
		t.Run(name, func(t *testing.T) {
			provider, _, _, clock := h10Native(t, h10Load(t, name))
			cycle := h10Refresh(t, provider, clock)
			hub := newTestSemanticHub(t)
			publication, err := newHuaweiPVPublication(hub, "mapping-test", "source:synthetic", "alpha", nil, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			if err := publication.Publish(cycle); err != nil {
				t.Fatal(err)
			}
			asset := huaweiAssetID("alpha", "pv", 1)
			view, err := hub.Inspect(asset)
			if err != nil {
				t.Fatal(err)
			}
			quantities := map[semreg.DefinitionID]map[string]semreg.Decimal{}
			for _, envelope := range view.Snapshot.Facts {
				for _, candidate := range envelope.Candidates {
					if candidate.Value == nil || candidate.Value.Quantity == nil {
						continue
					}
					if quantities[envelope.Key.FactID] == nil {
						quantities[envelope.Key.FactID] = map[string]semreg.Decimal{}
					}
					dimension := *envelope.Key.Dimensions[0].Value.Text
					quantities[envelope.Key.FactID][dimension] = candidate.Value.Quantity.Number
				}
			}
			suffix := string(asset)[len("pv-asset-"):]
			wantVoltage := semreg.Decimal{Coefficient: "6128", Exponent10: -1}
			wantEnergy := semreg.Decimal{Coefficient: "42", Exponent10: 3}
			if name == "sdongle" {
				wantVoltage = semreg.Decimal{Coefficient: "655"}
				wantEnergy = semreg.Decimal{Coefficient: "15", Exponent10: 3}
			}
			if got := quantities["pv.dc.voltage"]["input:1"]; got != wantVoltage {
				t.Fatalf("voltage=%+v", got)
			}
			if got := quantities["pv.energy.generated"]["system:"+suffix]; got != wantEnergy {
				t.Fatalf("counter=%+v", got)
			}
			if name == "sdongle" {
				if got := quantities["pv.ac.aggregate_active_power"]["inverter:"+suffix]; got != (semreg.Decimal{Coefficient: "466"}) {
					t.Fatalf("power conversion=%+v", got)
				}
			} else if len(quantities["pv.ac.aggregate_active_power"]) != 0 {
				t.Fatal("refused power fabricated")
			}
			for _, id := range []semreg.DefinitionID{"pv.ac.voltage", "pv.ac.current", "pv.ac.frequency", "pv.temperature.inverter"} {
				if len(quantities[id]) != 0 {
					t.Fatal("unconsumed native word promoted")
				}
			}
		})
	}
}

func TestH10LostChildIdentityWithdrawsOnlyThatChild(t *testing.T) {
	provider, _, _, clock := h10Native(t, h10Load(t, "smartlogger"))
	cycle := h10Refresh(t, provider, clock)
	hub := newTestSemanticHub(t)
	publication, err := newHuaweiPVPublication(hub, "withdraw-test", "source:synthetic", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if err := publication.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	var victim byte
	for unit := range publication.published {
		victim = unit
		break
	}
	if len(publication.published) < 2 {
		t.Fatal("fixture needs sibling")
	}
	originalCycle := cycle
	cycle.Cycle++
	cycle.CompletedAt = cycle.CompletedAt.Add(time.Second)
	cycle.Children = append([]modbusreg.HuaweiProviderChildObservation(nil), cycle.Children...)
	for i := range cycle.Children {
		if cycle.Children[i].UnitID == victim {
			cycle.Children[i].Evidence = modbusreg.HuaweiProviderEvidenceWithheld
			cycle.Children[i].Reason = modbusreg.HuaweiProviderReasonIdentityMismatch
		}
	}
	if err := publication.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	if _, ok := publication.published[victim]; ok {
		t.Fatal("lost identity remains published")
	}
	view, err := hub.Inspect(huaweiAssetID("alpha", "pv", victim))
	if err != nil {
		t.Fatal(err)
	}
	for _, link := range view.Snapshot.IdentityLinks {
		if link.State == semreg.LinkQualified {
			t.Fatal("lost identity still qualified")
		}
	}
	for _, asset := range publication.published {
		if !hub.Readable(asset) {
			t.Fatal("sibling lost")
		}
	}
	// An incomplete listing is not evidence of an empty inventory.
	cycle.Cycle++
	cycle.CompletedAt = cycle.CompletedAt.Add(time.Second)
	cycle.Outcome = modbusreg.HuaweiProviderCycleIncomplete
	cycle.Children = nil
	count := len(publication.published)
	if err := publication.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	if len(publication.published) != count {
		t.Fatal("incomplete listing withdrew valid sibling")
	}
	// A later completed cycle can restore the same admitted child.
	originalCycle.Cycle = cycle.Cycle + 1
	originalCycle.CompletedAt = cycle.CompletedAt.Add(time.Second)
	if err := publication.Publish(originalCycle); err != nil {
		t.Fatal(err)
	}
	if _, ok := publication.published[victim]; !ok {
		t.Fatal("admitted child not restored")
	}
}

type h10SingleCycleProvider struct{ cycle modbusreg.HuaweiProviderCycle }

func (p *h10SingleCycleProvider) Start(modbusreg.HuaweiProviderSession) error     { return nil }
func (p *h10SingleCycleProvider) Reconnect(modbusreg.HuaweiProviderSession) error { return nil }
func (p *h10SingleCycleProvider) Close() error                                    { return nil }
func (p *h10SingleCycleProvider) NextRefreshAt() time.Time                        { return time.Now().Add(time.Hour) }
func (p *h10SingleCycleProvider) Refresh(ctx context.Context) (modbusreg.HuaweiProviderCycle, error) {
	return p.cycle, ctx.Err()
}

func TestH10ManagedDriverStopRestartFencesHuaweiWork(t *testing.T) {
	native, _, _, clock := h10Native(t, h10Load(t, "smartlogger"))
	cycle := h10Refresh(t, native, clock)
	hub := newTestSemanticHub(t)
	var previous *semhub.Handle
	var publications []*huaweiPVPublication
	var endpoints []*m03Endpoint
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		endpoint := &m03Endpoint{}
		endpoints = append(endpoints, endpoint)
		return endpoint, nil
	}, func(ctx context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
		run, cancel := context.WithCancel(ctx)
		publication, err := newHuaweiPVPublication(hub, "pv.primary", "pv.configured", "alpha", previous, adapter.PublicationActive)
		if err == nil {
			previous = publication.handle
			publications = append(publications, publication)
		}
		return &huaweiManagedWork{ctx: run, cancel: cancel, adapter: adapter, instance: modbusTCPInstance{label: "alpha", family: modbusTCPFamilyHuaweiSmartLogger, cycle: time.Hour}, publication: publication, provider: &h10SingleCycleProvider{cycle: cycle}, err: err, done: make(chan struct{})}
	})
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	asset := huaweiAssetID("alpha", "pv", 1)
	m03Eventually(t, "all Huawei publications", func() bool {
		for _, child := range cycle.Children {
			if child.Evidence == modbusreg.HuaweiProviderEvidenceWithheld {
				continue
			}
			view, err := hub.Inspect(huaweiAssetID("alpha", "pv", child.UnitID))
			if err != nil || len(view.Snapshot.Facts) == 0 {
				return false
			}
		}
		return true
	})
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	retainedAcrossAssets := 0
	for _, child := range cycle.Children {
		if child.Evidence == modbusreg.HuaweiProviderEvidenceWithheld {
			continue
		}
		childAsset := huaweiAssetID("alpha", "pv", child.UnitID)
		h10AssertWithdrawn(t, hub, childAsset)
		view, err := hub.Inspect(childAsset)
		if err != nil {
			t.Fatal(err)
		}
		if len(view.Snapshot.Retained) <= 16 || len(view.Snapshot.Retained) > 32 {
			t.Fatal("fixture must establish a per-asset second-generation retention overflow")
		}
		retainedAcrossAssets += len(view.Snapshot.Retained)
	}
	if retainedAcrossAssets <= 32 {
		t.Fatal("first generation must retain more than 32 observations across separate inverter assets")
	}
	if endpoints[0].closes.Load() != 1 {
		t.Fatal("STOP failed to withdraw assets and close once")
	}
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	m03Eventually(t, "all Huawei successor publications", func() bool {
		for _, child := range cycle.Children {
			if child.Evidence == modbusreg.HuaweiProviderEvidenceWithheld {
				continue
			}
			view, err := hub.Inspect(huaweiAssetID("alpha", "pv", child.UnitID))
			if err != nil || len(view.Snapshot.Facts) == 0 {
				return false
			}
		}
		return true
	})
	if len(publications) != 2 || publications[1].handle.Generation() <= publications[0].handle.Generation() {
		t.Fatal("successor generation not advanced")
	}
	late := cycle
	late.Cycle++
	if err := publications[0].Publish(late); err == nil {
		t.Fatal("late predecessor published after RESTART")
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatalf("second STOP must retain both generations within each inverter snapshot: %v", err)
	}
	h10AssertWithdrawn(t, hub, asset)
	if endpoints[1].closes.Load() != 1 {
		t.Fatal("successor STOP failed")
	}
}

func h10AssertWithdrawn(t *testing.T, hub *semhub.Hub, asset semreg.AssetID) {
	t.Helper()
	view, err := hub.Inspect(asset)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot.Facts) != 0 || len(view.Snapshot.Retained) == 0 {
		t.Fatal("stopped asset did not withdraw live facts and retain evidence")
	}
	for _, binding := range view.Snapshot.Bindings {
		if binding.State == semreg.BindingCurrent {
			t.Fatal("stopped asset kept a live binding")
		}
	}
}

func TestH10PartialGroupFailurePreservesFactFreshness(t *testing.T) {
	fixture := h10Load(t, "smartlogger")
	groups := map[uint16]h10Step{}
	for _, step := range fixture.Cycles[0].Steps {
		if step.Unit == 2 && len(step.Request) == 5 && step.Request[0] == 3 {
			groups[binary.BigEndian.Uint16(step.Request[1:3])] = step
		}
	}
	for _, offset := range []uint16{32000, 32016, 32106} {
		if len(groups[offset].Response) == 0 {
			t.Fatal("missing synthetic monitoring group")
		}
	}
	// Refresh the child's status and input groups, then fail its counter read.
	// The real native provider must retain only the failed group's old words.
	var steps []h10Step
	inserted := false
	for _, step := range fixture.Cycles[1].Steps {
		if step.Unit != 2 {
			steps = append(steps, step)
			continue
		}
		if inserted {
			continue
		}
		steps = append(steps, groups[32000], groups[32016])
		failed := groups[32106]
		failed.Response = nil
		for i := 0; i < 4; i++ {
			steps = append(steps, failed)
		}
		inserted = true
	}
	if !inserted {
		t.Fatal("missing child timeout fixture")
	}
	fixture.Cycles[1].Steps = steps
	provider, _, _, clock := h10Native(t, fixture)
	hub := newTestSemanticHub(t)
	publication, err := newHuaweiPVPublication(hub, "group-freshness", "source:group-freshness", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	first := h10Refresh(t, provider, clock)
	if err := publication.Publish(first); err != nil {
		t.Fatal(err)
	}
	asset := huaweiAssetID("alpha", "pv", 2)
	before, err := hub.Inspect(asset)
	if err != nil {
		t.Fatal(err)
	}
	receipts := map[semreg.CandidateID]semreg.TimePoint{}
	for _, envelope := range before.Snapshot.Facts {
		for _, fact := range envelope.Candidates {
			receipts[fact.CandidateID] = fact.Times.ReceivedAt
		}
	}
	second := h10Refresh(t, provider, clock)
	childStale := false
	for _, child := range second.Children {
		if child.UnitID == 2 {
			childStale = child.Evidence == modbusreg.HuaweiProviderEvidenceStale
		}
	}
	if !childStale {
		t.Fatal("native partial failure did not mark the child stale")
	}
	if err := publication.Publish(second); err != nil {
		t.Fatal(err)
	}
	after, err := hub.Inspect(asset)
	if err != nil {
		t.Fatal(err)
	}
	fresh, retained := 0, 0
	for _, envelope := range after.Snapshot.Facts {
		for _, fact := range envelope.Candidates {
			switch envelope.Key.FactID {
			case "pv.dc.voltage", "pv.dc.current":
				fresh++
				if fact.Quality.Freshness != semreg.FreshnessFresh || fact.Times.ReceivedAt == receipts[fact.CandidateID] {
					t.Fatal("newly acquired input fact was made stale by a later group failure")
				}
			case "pv.energy.generated":
				retained++
				if fact.Quality.Freshness != semreg.FreshnessStale || fact.Times.ReceivedAt != receipts[fact.CandidateID] {
					t.Fatal("failed counter group lost its stale original receipt")
				}
			}
		}
	}
	if fresh == 0 || retained == 0 {
		t.Fatal("mixed current and retained group facts missing")
	}
}
