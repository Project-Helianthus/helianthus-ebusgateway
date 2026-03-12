package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
	graphqlclient "github.com/machinebox/graphql"
)

var canonicalQueryRootFields = []string{
	"zones",
	"dhw",
	"energyTotals",
	"circuits",
	"radioDevices",
	"fm5SemanticMode",
	"solar",
	"cylinders",
	"boilerStatus",
	"system",
	"schedules",
	"busSummary",
	"busMessages",
	"busPeriodicity",
}

type testBusObservabilityProvider struct {
	snapshot BusObservabilitySnapshot
}

func (provider testBusObservabilityProvider) Snapshot() BusObservabilitySnapshot {
	return cloneBusObservabilitySnapshot(provider.snapshot)
}

type driftingBusObservabilityProvider struct {
	mu    sync.Mutex
	calls int
}

func (provider *driftingBusObservabilityProvider) Snapshot() BusObservabilitySnapshot {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	provider.calls++
	sequence := provider.calls
	family := fmt.Sprintf("snapshot-%d", sequence)

	return BusObservabilitySnapshot{
		Summary: &BusSummary{
			Messages:    BusBoundedListSummary{Count: sequence, Capacity: 16},
			Periodicity: BusBoundedListSummary{Count: sequence, Capacity: 8},
		},
		Messages: []BusMessage{
			{
				Family: family,
			},
		},
		Periodicity: []BusPeriodicityEntry{
			{
				Family: family,
			},
		},
	}
}

func (provider *driftingBusObservabilityProvider) CallCount() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

func TestQueryResolvers_Integration(t *testing.T) {
	provider := mockProvider{
		planes: []registry.Plane{
			mockPlane{
				name: "heating",
				methods: []registry.Method{
					mockMethod{
						name:     "get_status",
						readOnly: true,
						template: mockTemplate{primary: 0xB5, secondary: 0x04},
						response: schemaSelector("status"),
					},
				},
			},
		},
	}

	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
		Providers: []registry.PlaneProvider{provider},
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	defer func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	}()

	gateway.Registry.Register(registry.DeviceInfo{
		Address:         0x10,
		Manufacturer:    "vaillant",
		DeviceID:        "device-a",
		SerialNumber:    "serial-a",
		SoftwareVersion: "1.0",
		HardwareVersion: "7603",
	})
	gateway.Registry.Register(registry.DeviceInfo{
		Address:         0x11,
		Manufacturer:    "vaillant",
		DeviceID:        "device-a",
		SerialNumber:    "serial-a",
		SoftwareVersion: "1.0",
		HardwareVersion: "7603",
	})

	builder := NewBuilder(gateway.Registry, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start error = %v", err)
	}

	handler, err := NewHandler(builder)
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	client := graphqlclient.NewClient(server.URL)
	semantic := NewLiveSemanticProvider()
	builder.SetSemanticProvider(semantic)
	builder.SetBusObservabilityProvider(testBusObservabilityProvider{
		snapshot: BusObservabilitySnapshot{
			Summary: &BusSummary{
				Status: &BusObservabilityStatus{
					TransportClass: "ens",
					Capability: BusObservabilityCapability{
						ActiveSupported:    true,
						PassiveSupported:   true,
						BroadcastSupported: true,
						PassiveAvailable:   false,
						PassiveState:       "warming_up",
						PassiveReason:      "unsupported_or_misconfigured",
						EndpointState:      "temporarily_disconnected",
						TapConnected:       false,
					},
					Warmup: BusObservabilityWarmup{
						State:                 "warming_up",
						Blocker:               "completed_transactions",
						ElapsedSeconds:        12.5,
						CompletedTransactions: 2,
						RequiredTransactions:  5,
						CompletionMode:        "thresholds_met",
					},
					TimingQuality: BusObservabilityTimingQuality{
						Active:      "wire_estimated",
						Passive:     "wire_estimated",
						Busy:        "wire_estimated",
						Periodicity: "wire_estimated",
					},
					Degraded: BusObservabilityDegraded{
						Active:  true,
						Reasons: []string{"unsupported_or_misconfigured", "dedup_degraded"},
					},
				},
				Messages:    BusBoundedListSummary{Count: 2, Capacity: 1024},
				Periodicity: BusBoundedListSummary{Count: 2, Capacity: 256},
				Counters: BusObservabilityCounters{
					SeriesBudgetOverflowTotal:      1,
					PeriodicityBudgetOverflowTotal: 2,
				},
			},
			Messages: []BusMessage{
				{
					Scope:         "active",
					Family:        "B509",
					FrameType:     "initiator_target",
					Outcome:       "success",
					ObservedAt:    "2026-03-12T18:00:00Z",
					SourceAddress: 8,
					TargetAddress: 21,
					RequestLen:    1,
					ResponseLen:   2,
				},
				{
					Scope:         "passive",
					Family:        "B524",
					FrameType:     "initiator_target",
					Outcome:       "success",
					ObservedAt:    "2026-03-12T18:00:05Z",
					SourceAddress: 21,
					TargetAddress: 38,
					RequestLen:    2,
					ResponseLen:   3,
				},
			},
			Periodicity: []BusPeriodicityEntry{
				{
					SourceBucket: "controller",
					TargetBucket: "boiler",
					Primary:      0xB5,
					Secondary:    0x09,
					Family:       "B509",
					State:        "available",
					LastSeen:     "2026-03-12T18:00:02Z",
					SampleCount:  2,
					LastInterval: "10s",
					MeanInterval: "10s",
					MinInterval:  "10s",
					MaxInterval:  "10s",
				},
				{
					SourceBucket: "controller",
					TargetBucket: "module",
					Primary:      0xB5,
					Secondary:    0x16,
					Family:       "B524",
					State:        "warming_up",
					LastSeen:     "2026-03-12T18:00:06Z",
					SampleCount:  3,
					LastInterval: "5s",
					MeanInterval: "5s",
					MinInterval:  "5s",
					MaxInterval:  "5s",
				},
			},
		},
	})

	t.Run("devices", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				devices {
					address
					addresses
					manufacturer
					deviceId
					planes {
						name
						methods {
							name
							readOnly
							primary
							secondary
							response {
								fields {
									name
									type
									size
								}
							}
						}
					}
					projections {
						plane
						nodes {
							id
							path
							canonicalPath
						}
						edges {
							id
							from
							to
						}
					}
				}
			}
		`)

		var response struct {
			Devices []struct {
				Address      int    `json:"address"`
				Addresses    []int  `json:"addresses"`
				Manufacturer string `json:"manufacturer"`
				DeviceID     string `json:"deviceId"`
				Planes       []struct {
					Name    string `json:"name"`
					Methods []struct {
						Name      string `json:"name"`
						ReadOnly  bool   `json:"readOnly"`
						Primary   int    `json:"primary"`
						Secondary int    `json:"secondary"`
						Response  struct {
							Fields []struct {
								Name string `json:"name"`
								Type string `json:"type"`
								Size int    `json:"size"`
							} `json:"fields"`
						} `json:"response"`
					} `json:"methods"`
				} `json:"planes"`
				Projections []struct {
					Plane string `json:"plane"`
					Nodes []struct {
						ID            string `json:"id"`
						Path          string `json:"path"`
						CanonicalPath string `json:"canonicalPath"`
					} `json:"nodes"`
					Edges []struct {
						ID   string `json:"id"`
						From string `json:"from"`
						To   string `json:"to"`
					} `json:"edges"`
				} `json:"projections"`
			} `json:"devices"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("devices query error = %v", err)
		}
		if len(response.Devices) != 1 {
			t.Fatalf("devices = %d; want 1", len(response.Devices))
		}
		if response.Devices[0].Address != 16 || response.Devices[0].DeviceID != "device-a" {
			t.Fatalf("device payload = %+v; want address=16 deviceId=device-a", response.Devices[0])
		}
		if len(response.Devices[0].Addresses) != 2 || response.Devices[0].Addresses[0] != 16 || response.Devices[0].Addresses[1] != 17 {
			t.Fatalf("addresses = %+v; want [16 17]", response.Devices[0].Addresses)
		}
		if len(response.Devices[0].Planes) != 1 {
			t.Fatalf("planes = %d; want 1", len(response.Devices[0].Planes))
		}
		method := response.Devices[0].Planes[0].Methods[0]
		if method.Name != "get_status" || method.Primary != 0xB5 || method.Secondary != 0x04 {
			t.Fatalf("method = %+v; want get_status 0xB5 0x04", method)
		}
		if len(method.Response.Fields) != 1 || method.Response.Fields[0].Name != "status" {
			t.Fatalf("method response fields = %+v; want status", method.Response.Fields)
		}

		if len(response.Devices[0].Projections) != 1 {
			t.Fatalf("projections = %d; want 1", len(response.Devices[0].Projections))
		}
		projection := response.Devices[0].Projections[0]
		if projection.Plane != "Observability" {
			t.Fatalf("projection plane = %s; want Observability", projection.Plane)
		}
		if len(projection.Nodes) != 2 {
			t.Fatalf("projection nodes = %d; want 2", len(projection.Nodes))
		}
		expectedRoot := fmt.Sprintf("Service:/ebus/addr@%02X/device@%s", 0x10, "device-a")
		expectedChild := fmt.Sprintf("%s/method@get_status", expectedRoot)
		if projection.Nodes[0].ID != expectedRoot || projection.Nodes[0].CanonicalPath != expectedRoot {
			t.Fatalf("projection root node = %+v; want id=%s", projection.Nodes[0], expectedRoot)
		}
		if projection.Nodes[1].ID != expectedChild || projection.Nodes[1].CanonicalPath != expectedChild {
			t.Fatalf("projection child node = %+v; want id=%s", projection.Nodes[1], expectedChild)
		}
		if len(projection.Edges) != 1 {
			t.Fatalf("projection edges = %d; want 1", len(projection.Edges))
		}
		if projection.Edges[0].From != expectedRoot || projection.Edges[0].To != expectedChild {
			t.Fatalf("projection edge = %+v; want from=%s to=%s", projection.Edges[0], expectedRoot, expectedChild)
		}
	})

	t.Run("device", func(t *testing.T) {
		for _, address := range []int{16, 17} {
			request := graphqlclient.NewRequest(`
				query($address: Int!) {
					device(address: $address) {
						address
						addresses
						deviceId
					}
				}
			`)
			request.Var("address", address)

			var response struct {
				Device struct {
					Address   int    `json:"address"`
					Addresses []int  `json:"addresses"`
					DeviceID  string `json:"deviceId"`
				} `json:"device"`
			}

			if err := client.Run(context.Background(), request, &response); err != nil {
				t.Fatalf("device query error = %v", err)
			}
			if response.Device.Address != 16 || response.Device.DeviceID != "device-a" {
				t.Fatalf("address %d device = %+v; want address=16 deviceId=device-a", address, response.Device)
			}
			if len(response.Device.Addresses) != 2 || response.Device.Addresses[0] != 16 || response.Device.Addresses[1] != 17 {
				t.Fatalf("address %d addresses = %+v; want [16 17]", address, response.Device.Addresses)
			}
		}
	})

	t.Run("service_status", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
				query {
					daemonStatus {
						status
						firmwareVersion
						updatesAvailable
						initiatorAddress
					}
					adapterStatus {
						status
						firmwareVersion
						updatesAvailable
				}
			}
		`)

		var response struct {
			DaemonStatus struct {
				Status           string `json:"status"`
				FirmwareVersion  string `json:"firmwareVersion"`
				UpdatesAvailable bool   `json:"updatesAvailable"`
				InitiatorAddress string `json:"initiatorAddress"`
			} `json:"daemonStatus"`
			AdapterStatus struct {
				Status           string `json:"status"`
				FirmwareVersion  string `json:"firmwareVersion"`
				UpdatesAvailable bool   `json:"updatesAvailable"`
			} `json:"adapterStatus"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("status query error = %v", err)
		}

		if response.DaemonStatus.Status == "" {
			t.Fatalf("daemon status empty")
		}
		if response.DaemonStatus.InitiatorAddress != "" {
			t.Fatalf("daemon initiatorAddress = %q; want empty for static provider", response.DaemonStatus.InitiatorAddress)
		}
		if response.AdapterStatus.Status == "" {
			t.Fatalf("adapter status empty")
		}
	})

	t.Run("query_root_inventory", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				__schema {
					queryType {
						fields {
							name
						}
					}
				}
			}
		`)

		var response struct {
			Schema struct {
				QueryType struct {
					Fields []struct {
						Name string `json:"name"`
					} `json:"fields"`
				} `json:"queryType"`
			} `json:"__schema"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("query root introspection error = %v", err)
		}

		got := make(map[string]bool, len(response.Schema.QueryType.Fields))
		for _, field := range response.Schema.QueryType.Fields {
			got[field.Name] = true
		}

		for _, name := range canonicalQueryRootFields {
			if !got[name] {
				t.Fatalf("query root missing %q in introspection: %#v", name, got)
			}
		}
	})

	t.Run("bus_observability", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				busSummary {
					status {
						transportClass
						capability {
							activeSupported
							passiveSupported
							broadcastSupported
							passiveAvailable
							passiveState
							passiveReason
							endpointState
							tapConnected
						}
						warmup {
							state
							blocker
							elapsedSeconds
							completedTransactions
							requiredTransactions
							completionMode
						}
						timingQuality {
							active
							passive
							busy
							periodicity
						}
						degraded {
							active
							reasons
						}
					}
					messages {
						count
						capacity
					}
					periodicity {
						count
						capacity
					}
					counters {
						seriesBudgetOverflowTotal
						periodicityBudgetOverflowTotal
					}
				}
				busMessages(limit: 1) {
					status {
						capability {
							passiveState
							passiveReason
						}
						timingQuality {
							passive
							periodicity
						}
					}
					count
					capacity
					items {
						scope
						family
						frameType
						outcome
						observedAt
						sourceAddress
						targetAddress
						requestLen
						responseLen
					}
				}
				busPeriodicity(limit: 1) {
					status {
						warmup {
							state
						}
						degraded {
							active
						}
					}
					count
					capacity
					items {
						sourceBucket
						targetBucket
						primary
						secondary
						family
						state
						lastSeen
						sampleCount
						lastInterval
						meanInterval
						minInterval
						maxInterval
					}
				}
			}
		`)

		var response struct {
			BusSummary struct {
				Status struct {
					TransportClass string `json:"transportClass"`
					Capability     struct {
						PassiveState  string `json:"passiveState"`
						PassiveReason string `json:"passiveReason"`
						EndpointState string `json:"endpointState"`
					} `json:"capability"`
					Warmup struct {
						State                 string  `json:"state"`
						Blocker               string  `json:"blocker"`
						ElapsedSeconds        float64 `json:"elapsedSeconds"`
						CompletedTransactions int     `json:"completedTransactions"`
						RequiredTransactions  int     `json:"requiredTransactions"`
					} `json:"warmup"`
					TimingQuality struct {
						Passive     string `json:"passive"`
						Periodicity string `json:"periodicity"`
					} `json:"timingQuality"`
					Degraded struct {
						Active  bool     `json:"active"`
						Reasons []string `json:"reasons"`
					} `json:"degraded"`
				} `json:"status"`
				Messages struct {
					Count    int `json:"count"`
					Capacity int `json:"capacity"`
				} `json:"messages"`
				Periodicity struct {
					Count    int `json:"count"`
					Capacity int `json:"capacity"`
				} `json:"periodicity"`
				Counters struct {
					SeriesBudgetOverflowTotal      string `json:"seriesBudgetOverflowTotal"`
					PeriodicityBudgetOverflowTotal string `json:"periodicityBudgetOverflowTotal"`
				} `json:"counters"`
			} `json:"busSummary"`
			BusMessages struct {
				Status struct {
					Capability struct {
						PassiveState  string `json:"passiveState"`
						PassiveReason string `json:"passiveReason"`
					} `json:"capability"`
					TimingQuality struct {
						Passive     string `json:"passive"`
						Periodicity string `json:"periodicity"`
					} `json:"timingQuality"`
				} `json:"status"`
				Count    int `json:"count"`
				Capacity int `json:"capacity"`
				Items    []struct {
					Family        string `json:"family"`
					ObservedAt    string `json:"observedAt"`
					SourceAddress int    `json:"sourceAddress"`
					TargetAddress int    `json:"targetAddress"`
				} `json:"items"`
			} `json:"busMessages"`
			BusPeriodicity struct {
				Status struct {
					Warmup struct {
						State string `json:"state"`
					} `json:"warmup"`
				} `json:"status"`
				Count    int `json:"count"`
				Capacity int `json:"capacity"`
				Items    []struct {
					Family      string `json:"family"`
					Primary     int    `json:"primary"`
					Secondary   int    `json:"secondary"`
					LastSeen    string `json:"lastSeen"`
					SampleCount int    `json:"sampleCount"`
				} `json:"items"`
			} `json:"busPeriodicity"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("bus observability query error = %v", err)
		}
		if response.BusSummary.Status.TransportClass != "ens" {
			t.Fatalf("transportClass = %q; want ens", response.BusSummary.Status.TransportClass)
		}
		if response.BusSummary.Status.Capability.PassiveState != "warming_up" {
			t.Fatalf("passiveState = %q; want warming_up", response.BusSummary.Status.Capability.PassiveState)
		}
		if response.BusSummary.Status.Warmup.Blocker != "completed_transactions" {
			t.Fatalf("warmup.blocker = %q; want completed_transactions", response.BusSummary.Status.Warmup.Blocker)
		}
		if response.BusSummary.Status.TimingQuality.Passive != "wire_estimated" {
			t.Fatalf("timingQuality.passive = %q; want wire_estimated", response.BusSummary.Status.TimingQuality.Passive)
		}
		if !response.BusSummary.Status.Degraded.Active || len(response.BusSummary.Status.Degraded.Reasons) != 2 {
			t.Fatalf("degraded = %+v; want active with 2 reasons", response.BusSummary.Status.Degraded)
		}
		if response.BusSummary.Messages.Count != 2 || response.BusSummary.Messages.Capacity != 1024 {
			t.Fatalf("messages summary = %+v; want count=2 capacity=1024", response.BusSummary.Messages)
		}
		if response.BusSummary.Periodicity.Count != 2 || response.BusSummary.Periodicity.Capacity != 256 {
			t.Fatalf("periodicity summary = %+v; want count=2 capacity=256", response.BusSummary.Periodicity)
		}
		if response.BusSummary.Counters.SeriesBudgetOverflowTotal != "1" || response.BusSummary.Counters.PeriodicityBudgetOverflowTotal != "2" {
			t.Fatalf("counters = %+v; want 1/2 as strings", response.BusSummary.Counters)
		}

		if response.BusMessages.Count != 2 || response.BusMessages.Capacity != 1024 {
			t.Fatalf("busMessages wrapper = %+v; want count=2 capacity=1024", response.BusMessages)
		}
		if len(response.BusMessages.Items) != 1 || response.BusMessages.Items[0].Family != "B524" {
			t.Fatalf("busMessages items = %+v; want trimmed B524 item", response.BusMessages.Items)
		}
		if response.BusMessages.Items[0].ObservedAt != "2026-03-12T18:00:05Z" {
			t.Fatalf("busMessages observedAt = %q; want 2026-03-12T18:00:05Z", response.BusMessages.Items[0].ObservedAt)
		}
		if response.BusMessages.Status.Capability.PassiveReason != "unsupported_or_misconfigured" {
			t.Fatalf("busMessages passiveReason = %q; want unsupported_or_misconfigured", response.BusMessages.Status.Capability.PassiveReason)
		}

		if response.BusPeriodicity.Count != 2 || response.BusPeriodicity.Capacity != 256 {
			t.Fatalf("busPeriodicity wrapper = %+v; want count=2 capacity=256", response.BusPeriodicity)
		}
		if len(response.BusPeriodicity.Items) != 1 || response.BusPeriodicity.Items[0].Family != "B524" {
			t.Fatalf("busPeriodicity items = %+v; want trimmed B524 item", response.BusPeriodicity.Items)
		}
		if response.BusPeriodicity.Items[0].LastSeen != "2026-03-12T18:00:06Z" {
			t.Fatalf("busPeriodicity lastSeen = %q; want 2026-03-12T18:00:06Z", response.BusPeriodicity.Items[0].LastSeen)
		}
		if response.BusPeriodicity.Status.Warmup.State != "warming_up" {
			t.Fatalf("busPeriodicity warmup.state = %q; want warming_up", response.BusPeriodicity.Status.Warmup.State)
		}
	})

	t.Run("bus_observability_shared_snapshot_per_operation", func(t *testing.T) {
		builder := NewBuilder(mockRegistry{}, nil)
		provider := &driftingBusObservabilityProvider{}
		builder.SetBusObservabilityProvider(provider)

		handler, err := NewInvokeHandler(builder, nil, nil)
		if err != nil {
			t.Fatalf("NewInvokeHandler error = %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ busSummary { messages { count } periodicity { count } } busMessages { count items { family } } busPeriodicity { count items { family } } }"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response struct {
			Data struct {
				BusSummary struct {
					Messages struct {
						Count int `json:"count"`
					} `json:"messages"`
					Periodicity struct {
						Count int `json:"count"`
					} `json:"periodicity"`
				} `json:"busSummary"`
				BusMessages struct {
					Count int `json:"count"`
					Items []struct {
						Family string `json:"family"`
					} `json:"items"`
				} `json:"busMessages"`
				BusPeriodicity struct {
					Count int `json:"count"`
					Items []struct {
						Family string `json:"family"`
					} `json:"items"`
				} `json:"busPeriodicity"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("json.Unmarshal response: %v body=%s", err, rec.Body.String())
		}
		if len(response.Errors) != 0 {
			t.Fatalf("errors = %#v; want none", response.Errors)
		}
		if provider.CallCount() != 1 {
			t.Fatalf("Snapshot() calls = %d; want 1 shared snapshot per operation", provider.CallCount())
		}
		if response.Data.BusSummary.Messages.Count != 1 || response.Data.BusSummary.Periodicity.Count != 1 {
			t.Fatalf("busSummary = %+v; want snapshot-1 counts", response.Data.BusSummary)
		}
		if response.Data.BusMessages.Count != 1 || len(response.Data.BusMessages.Items) != 1 || response.Data.BusMessages.Items[0].Family != "snapshot-1" {
			t.Fatalf("busMessages = %+v; want count=1 family=snapshot-1", response.Data.BusMessages)
		}
		if response.Data.BusPeriodicity.Count != 1 || len(response.Data.BusPeriodicity.Items) != 1 || response.Data.BusPeriodicity.Items[0].Family != "snapshot-1" {
			t.Fatalf("busPeriodicity = %+v; want count=1 family=snapshot-1", response.Data.BusPeriodicity)
		}
	})

	t.Run("bus_observability_counters_allow_uint64_values", func(t *testing.T) {
		builder := NewBuilder(mockRegistry{}, nil)
		builder.SetBusObservabilityProvider(testBusObservabilityProvider{
			snapshot: BusObservabilitySnapshot{
				Summary: &BusSummary{
					Counters: BusObservabilityCounters{
						SeriesBudgetOverflowTotal:      1<<31 + 7,
						PeriodicityBudgetOverflowTotal: 1<<40 + 9,
					},
				},
			},
		})

		handler, err := NewHandler(builder)
		if err != nil {
			t.Fatalf("NewHandler error = %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ busSummary { counters { seriesBudgetOverflowTotal periodicityBudgetOverflowTotal } } }"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response struct {
			Data struct {
				BusSummary struct {
					Counters struct {
						SeriesBudgetOverflowTotal      string `json:"seriesBudgetOverflowTotal"`
						PeriodicityBudgetOverflowTotal string `json:"periodicityBudgetOverflowTotal"`
					} `json:"counters"`
				} `json:"busSummary"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("json.Unmarshal response: %v body=%s", err, rec.Body.String())
		}
		if len(response.Errors) != 0 {
			t.Fatalf("errors = %#v; want none", response.Errors)
		}
		if response.Data.BusSummary.Counters.SeriesBudgetOverflowTotal != "2147483655" {
			t.Fatalf("seriesBudgetOverflowTotal = %q; want 2147483655", response.Data.BusSummary.Counters.SeriesBudgetOverflowTotal)
		}
		if response.Data.BusSummary.Counters.PeriodicityBudgetOverflowTotal != "1099511627785" {
			t.Fatalf("periodicityBudgetOverflowTotal = %q; want 1099511627785", response.Data.BusSummary.Counters.PeriodicityBudgetOverflowTotal)
		}
	})

	t.Run("zones_dhw", func(t *testing.T) {
		associatedCircuit := 1
		roomTemperatureZoneMapping := 2
		semantic.SetZones([]Zone{
			{
				ID:   "zone-2",
				Name: "Etaj",
				Config: ZoneConfig{
					OperatingMode:              "heat",
					Preset:                     "manual",
					AllowedModes:               []string{"off", "auto", "heat"},
					CircuitType:                "underfloor",
					AssociatedCircuit:          &associatedCircuit,
					RoomTemperatureZoneMapping: &roomTemperatureZoneMapping,
				},
			},
		})

		request := graphqlclient.NewRequest(`
			query {
				zones {
					id
					name
					state {
						currentTempC
						currentHumidityPct
						hvacAction
						specialFunction
						heatingDemandPct
						valvePositionPct
					}
					config {
						operatingMode
						preset
						targetTempC
						allowedModes
						circuitType
						associatedCircuit
						roomTemperatureZoneMapping
					}
				}
				dhw {
					state {
						currentTempC
						specialFunction
						heatingDemandPct
					}
					config {
						operatingMode
						preset
						targetTempC
					}
				}
				circuits {
					index
					circuitType
					hasMixer
					state {
						pumpActive
						mixerPositionPct
						flowTemperatureC
						flowSetpointC
						calcFlowTempC
						circuitState
						humidity
						dewPoint
						pumpHours
						pumpStarts
					}
					config {
						heatingCurve
						flowTempMaxC
						flowTempMinC
						summerLimitC
						frostProtC
						roomTempControl
						coolingEnabled
					}
					managingDevice {
						role
						deviceId
						address
					}
				}
					radioDevices {
						group
						instance
						slotMode
					deviceConnected
					deviceClassAddress
					deviceModel
					firmwareVersion
					hardwareIdentifier
					remoteControlAddress
					devicePaired
					receptionStrength
					zoneAssignment
						roomTemperatureC
						roomHumidityPct
					}
					fm5SemanticMode
					solar {
						collectorTemperatureC
						returnTemperatureC
						pumpActive
						currentYield
						pumpHours
						solarEnabled
						functionMode
					}
					cylinders {
						index
						temperatureC
						maxSetpointC
						chargeHysteresisC
						chargeOffsetC
					}
					system {
						state {
							systemOff
						systemWaterPressure
						systemFlowTemperature
						outdoorTemperature
						outdoorTemperatureAvg24h
						maintenanceDue
						hwcCylinderTemperatureTop
						hwcCylinderTemperatureBottom
					}
					config {
						adaptiveHeatingCurve
						alternativePoint
						heatingCircuitBivalencePoint
						dhwBivalencePoint
						hcEmergencyTemperature
						hwcMaxFlowTempDesired
						maxRoomHumidity
					}
					properties {
						systemScheme
						moduleConfigurationVR71
					}
				}
				schedules {
					programs {
						zone
						hc
						config {
							maxSlots
							timeResolution
							minDuration
							hasTemperature
							tempSlots
							minTempC
							maxTempC
						}
						slotsUsed
						days {
							weekday
							slots {
								startHour
								startMinute
								endHour
								endMinute
								temperatureC
								temperatureRaw
							}
						}
					}
				}
			}
		`)

		var response struct {
			Zones []struct {
				ID    string `json:"id"`
				Name  string `json:"name"`
				State struct {
					CurrentTempC       *float64 `json:"currentTempC"`
					CurrentHumidityPct *float64 `json:"currentHumidityPct"`
					HvacAction         *string  `json:"hvacAction"`
					SpecialFunction    *string  `json:"specialFunction"`
					HeatingDemandPct   *float64 `json:"heatingDemandPct"`
					ValvePositionPct   *float64 `json:"valvePositionPct"`
				} `json:"state"`
				Config struct {
					OperatingMode              *string  `json:"operatingMode"`
					Preset                     *string  `json:"preset"`
					TargetTempC                *float64 `json:"targetTempC"`
					AllowedModes               []string `json:"allowedModes"`
					CircuitType                *string  `json:"circuitType"`
					AssociatedCircuit          *int     `json:"associatedCircuit"`
					RoomTemperatureZoneMapping *int     `json:"roomTemperatureZoneMapping"`
				} `json:"config"`
			} `json:"zones"`
			DHW *struct {
				State struct {
					CurrentTempC     *float64 `json:"currentTempC"`
					SpecialFunction  *string  `json:"specialFunction"`
					HeatingDemandPct *float64 `json:"heatingDemandPct"`
				} `json:"state"`
				Config struct {
					OperatingMode *string  `json:"operatingMode"`
					Preset        *string  `json:"preset"`
					TargetTempC   *float64 `json:"targetTempC"`
				} `json:"config"`
			} `json:"dhw"`
			Circuits []struct {
				Index       int    `json:"index"`
				CircuitType string `json:"circuitType"`
				HasMixer    bool   `json:"hasMixer"`
				State       struct {
					PumpActive       *bool    `json:"pumpActive"`
					MixerPositionPct *float64 `json:"mixerPositionPct"`
					FlowTemperatureC *float64 `json:"flowTemperatureC"`
					FlowSetpointC    *float64 `json:"flowSetpointC"`
					CalcFlowTempC    *float64 `json:"calcFlowTempC"`
					CircuitState     *string  `json:"circuitState"`
					Humidity         *float64 `json:"humidity"`
					DewPoint         *float64 `json:"dewPoint"`
					PumpHours        *float64 `json:"pumpHours"`
					PumpStarts       *int     `json:"pumpStarts"`
				} `json:"state"`
				Config struct {
					HeatingCurve    *float64 `json:"heatingCurve"`
					FlowTempMaxC    *float64 `json:"flowTempMaxC"`
					FlowTempMinC    *float64 `json:"flowTempMinC"`
					SummerLimitC    *float64 `json:"summerLimitC"`
					FrostProtC      *float64 `json:"frostProtC"`
					RoomTempControl *string  `json:"roomTempControl"`
					CoolingEnabled  *bool    `json:"coolingEnabled"`
				} `json:"config"`
				ManagingDevice struct {
					Role     string  `json:"role"`
					DeviceID *string `json:"deviceId"`
					Address  *int    `json:"address"`
				} `json:"managingDevice"`
			} `json:"circuits"`
			RadioDevices []struct {
				Group                int      `json:"group"`
				Instance             int      `json:"instance"`
				SlotMode             string   `json:"slotMode"`
				DeviceConnected      *bool    `json:"deviceConnected"`
				DeviceClassAddress   *int     `json:"deviceClassAddress"`
				DeviceModel          *string  `json:"deviceModel"`
				FirmwareVersion      *string  `json:"firmwareVersion"`
				HardwareIdentifier   *int     `json:"hardwareIdentifier"`
				RemoteControlAddress *int     `json:"remoteControlAddress"`
				DevicePaired         *bool    `json:"devicePaired"`
				ReceptionStrength    *int     `json:"receptionStrength"`
				ZoneAssignment       *int     `json:"zoneAssignment"`
				RoomTemperatureC     *float64 `json:"roomTemperatureC"`
				RoomHumidityPct      *float64 `json:"roomHumidityPct"`
			} `json:"radioDevices"`
			FM5SemanticMode string `json:"fm5SemanticMode"`
			Solar           *struct {
				CollectorTemperatureC *float64 `json:"collectorTemperatureC"`
				ReturnTemperatureC    *float64 `json:"returnTemperatureC"`
				PumpActive            *bool    `json:"pumpActive"`
				CurrentYield          *float64 `json:"currentYield"`
				PumpHours             *float64 `json:"pumpHours"`
				SolarEnabled          *bool    `json:"solarEnabled"`
				FunctionMode          *bool    `json:"functionMode"`
			} `json:"solar"`
			Cylinders []struct {
				Index             int      `json:"index"`
				TemperatureC      *float64 `json:"temperatureC"`
				MaxSetpointC      *float64 `json:"maxSetpointC"`
				ChargeHysteresisC *float64 `json:"chargeHysteresisC"`
				ChargeOffsetC     *float64 `json:"chargeOffsetC"`
			} `json:"cylinders"`
			System *struct {
				State struct {
					SystemOff                    *bool    `json:"systemOff"`
					SystemWaterPressure          *float64 `json:"systemWaterPressure"`
					SystemFlowTemperature        *float64 `json:"systemFlowTemperature"`
					OutdoorTemperature           *float64 `json:"outdoorTemperature"`
					OutdoorTemperatureAvg24h     *float64 `json:"outdoorTemperatureAvg24h"`
					MaintenanceDue               *bool    `json:"maintenanceDue"`
					HwcCylinderTemperatureTop    *float64 `json:"hwcCylinderTemperatureTop"`
					HwcCylinderTemperatureBottom *float64 `json:"hwcCylinderTemperatureBottom"`
				} `json:"state"`
				Config struct {
					AdaptiveHeatingCurve         *bool    `json:"adaptiveHeatingCurve"`
					AlternativePoint             *float64 `json:"alternativePoint"`
					HeatingCircuitBivalencePoint *float64 `json:"heatingCircuitBivalencePoint"`
					DhwBivalencePoint            *float64 `json:"dhwBivalencePoint"`
					HcEmergencyTemperature       *float64 `json:"hcEmergencyTemperature"`
					HwcMaxFlowTempDesired        *float64 `json:"hwcMaxFlowTempDesired"`
					MaxRoomHumidity              *int     `json:"maxRoomHumidity"`
				} `json:"config"`
				Properties struct {
					SystemScheme            *int `json:"systemScheme"`
					ModuleConfigurationVR71 *int `json:"moduleConfigurationVR71"`
				} `json:"properties"`
			} `json:"system"`
			Schedules *struct {
				Programs []struct {
					Zone int    `json:"zone"`
					HC   string `json:"hc"`
				} `json:"programs"`
			} `json:"schedules"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("zones/dhw query error = %v", err)
		}
		if len(response.Zones) != 1 {
			t.Fatalf("zones = %d; want 1", len(response.Zones))
		}
		if response.Zones[0].Config.AssociatedCircuit == nil || *response.Zones[0].Config.AssociatedCircuit != 1 {
			t.Fatalf("associatedCircuit = %#v; want 1", response.Zones[0].Config.AssociatedCircuit)
		}
		if response.Zones[0].Config.RoomTemperatureZoneMapping == nil || *response.Zones[0].Config.RoomTemperatureZoneMapping != 2 {
			t.Fatalf("roomTemperatureZoneMapping = %#v; want 2", response.Zones[0].Config.RoomTemperatureZoneMapping)
		}
		if response.DHW != nil {
			t.Fatalf("dhw expected nil with static provider")
		}
		if len(response.Circuits) != 0 {
			t.Fatalf("circuits = %d; want 0", len(response.Circuits))
		}
		if len(response.RadioDevices) != 0 {
			t.Fatalf("radioDevices = %d; want 0", len(response.RadioDevices))
		}
		if response.FM5SemanticMode != "ABSENT" {
			t.Fatalf("fm5SemanticMode = %q; want ABSENT", response.FM5SemanticMode)
		}
		if response.Solar != nil {
			t.Fatalf("solar expected nil with static provider")
		}
		if len(response.Cylinders) != 0 {
			t.Fatalf("cylinders = %d; want 0", len(response.Cylinders))
		}
		if response.System != nil {
			t.Fatalf("system expected nil with static provider")
		}
		if response.Schedules != nil {
			t.Fatalf("schedules expected nil with static provider")
		}
	})

	t.Run("circuits expose explicit managing device", func(t *testing.T) {
		deviceID := "VR_71"
		semantic.SetCircuits([]CircuitStatus{{
			Index:       0,
			CircuitType: "heating",
			ManagingDevice: ManagingDevice{
				Role:     ManagingDeviceRoleFunctionModule,
				DeviceID: &deviceID,
				Address:  intPtr(0x26),
			},
		}})
		defer semantic.SetCircuits(nil)

		request := graphqlclient.NewRequest(`
			query {
				circuits {
					index
					managingDevice {
						role
						deviceId
						address
					}
				}
			}
		`)

		var response struct {
			Circuits []struct {
				Index          int `json:"index"`
				ManagingDevice struct {
					Role     string  `json:"role"`
					DeviceID *string `json:"deviceId"`
					Address  *int    `json:"address"`
				} `json:"managingDevice"`
			} `json:"circuits"`
		}
		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("circuits query error = %v", err)
		}
		if len(response.Circuits) != 1 {
			t.Fatalf("circuits = %d; want 1", len(response.Circuits))
		}
		if response.Circuits[0].ManagingDevice.Role != string(ManagingDeviceRoleFunctionModule) {
			t.Fatalf("managingDevice.role = %q; want %q", response.Circuits[0].ManagingDevice.Role, ManagingDeviceRoleFunctionModule)
		}
		if response.Circuits[0].ManagingDevice.DeviceID == nil || *response.Circuits[0].ManagingDevice.DeviceID != "VR_71" {
			t.Fatalf("managingDevice.deviceId = %#v; want VR_71", response.Circuits[0].ManagingDevice.DeviceID)
		}
		if response.Circuits[0].ManagingDevice.Address == nil || *response.Circuits[0].ManagingDevice.Address != 0x26 {
			t.Fatalf("managingDevice.address = %#v; want 0x26", response.Circuits[0].ManagingDevice.Address)
		}
	})

	t.Run("removed vr71CircuitStartIndex field errors", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				system {
					properties {
						vr71CircuitStartIndex
					}
				}
			}
		`)

		var response any
		err := client.Run(context.Background(), request, &response)
		if err == nil {
			t.Fatal("query error = nil; want missing-field error")
		}
		if !strings.Contains(err.Error(), "vr71CircuitStartIndex") {
			t.Fatalf("query error = %v; want mention of vr71CircuitStartIndex", err)
		}
	})

	t.Run("energy_totals_root", func(t *testing.T) {
		semantic.ApplyEnergyFromRegister(EnergyMergeKey{
			Channel: "gas",
			Usage:   "hot_water",
			Period:  "day",
		}, 3.5)
		semantic.ApplyEnergyFromRegister(EnergyMergeKey{
			Channel:  "gas",
			Usage:    "hot_water",
			Period:   "year",
			YearKind: "previous",
		}, 120.0)
		semantic.ApplyEnergyFromRegister(EnergyMergeKey{
			Channel:  "gas",
			Usage:    "hot_water",
			Period:   "year",
			YearKind: "current",
		}, 240.0)
		semantic.ApplyEnergyFromRegister(EnergyMergeKey{
			Channel: "electricity",
			Usage:   "heating",
			Period:  "day",
		}, 1.25)
		semantic.ApplyEnergyFromRegister(EnergyMergeKey{
			Channel: "solar",
			Usage:   "cooling",
			Period:  "day",
		}, 2.75)

		request := graphqlclient.NewRequest(`
			query {
				energyTotals {
					gas { dhw { today yearly } climate { today yearly } }
					electric { dhw { today yearly } climate { today yearly } }
					solar { dhw { today yearly } climate { today yearly } }
				}
			}
		`)

		var response struct {
			EnergyTotals *struct {
				Gas struct {
					DHW struct {
						Today  float64   `json:"today"`
						Yearly []float64 `json:"yearly"`
					} `json:"dhw"`
					Climate struct {
						Today  float64   `json:"today"`
						Yearly []float64 `json:"yearly"`
					} `json:"climate"`
				} `json:"gas"`
				Electric struct {
					DHW struct {
						Today  float64   `json:"today"`
						Yearly []float64 `json:"yearly"`
					} `json:"dhw"`
					Climate struct {
						Today  float64   `json:"today"`
						Yearly []float64 `json:"yearly"`
					} `json:"climate"`
				} `json:"electric"`
				Solar struct {
					DHW struct {
						Today  float64   `json:"today"`
						Yearly []float64 `json:"yearly"`
					} `json:"dhw"`
					Climate struct {
						Today  float64   `json:"today"`
						Yearly []float64 `json:"yearly"`
					} `json:"climate"`
				} `json:"solar"`
			} `json:"energyTotals"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("energyTotals root query error = %v", err)
		}
		if response.EnergyTotals == nil {
			t.Fatal("energyTotals = nil; want non-nil")
		}
		if response.EnergyTotals.Gas.DHW.Today != 3.5 {
			t.Fatalf("gas.dhw.today = %v; want 3.5", response.EnergyTotals.Gas.DHW.Today)
		}
		if len(response.EnergyTotals.Gas.DHW.Yearly) != 2 || response.EnergyTotals.Gas.DHW.Yearly[0] != 120.0 || response.EnergyTotals.Gas.DHW.Yearly[1] != 240.0 {
			t.Fatalf("gas.dhw.yearly = %#v; want [120 240]", response.EnergyTotals.Gas.DHW.Yearly)
		}
		if response.EnergyTotals.Electric.Climate.Today != 1.25 {
			t.Fatalf("electric.climate.today = %v; want 1.25", response.EnergyTotals.Electric.Climate.Today)
		}
		if response.EnergyTotals.Solar.Climate.Today != 2.75 {
			t.Fatalf("solar.climate.today = %v; want 2.75", response.EnergyTotals.Solar.Climate.Today)
		}
	})

	t.Run("energy_totals_on_device", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				devices {
					address
					role
					energyTotals {
						gas { dhw { today yearly } climate { today yearly } }
						electric { dhw { today yearly } climate { today yearly } }
						solar { dhw { today yearly } climate { today yearly } }
					}
				}
			}
		`)

		var response struct {
			Devices []struct {
				Address      int     `json:"address"`
				Role         *string `json:"role"`
				EnergyTotals *struct {
					Gas struct {
						DHW struct {
							Today  float64   `json:"today"`
							Yearly []float64 `json:"yearly"`
						} `json:"dhw"`
						Climate struct {
							Today  float64   `json:"today"`
							Yearly []float64 `json:"yearly"`
						} `json:"climate"`
					} `json:"gas"`
				} `json:"energyTotals"`
			} `json:"devices"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("energyTotals on device query error = %v", err)
		}
		for _, dev := range response.Devices {
			if dev.EnergyTotals != nil {
				t.Fatalf("device %d energyTotals expected nil with no semantic provider", dev.Address)
			}
		}
	})

	t.Run("planes", func(t *testing.T) {
		for _, address := range []int{16, 17} {
			request := graphqlclient.NewRequest(`
				query($address: Int!) {
					planes(address: $address) {
						name
					}
				}
			`)
			request.Var("address", address)

			var response struct {
				Planes []struct {
					Name string `json:"name"`
				} `json:"planes"`
			}

			if err := client.Run(context.Background(), request, &response); err != nil {
				t.Fatalf("planes query error = %v", err)
			}
			if len(response.Planes) != 1 || response.Planes[0].Name != "heating" {
				t.Fatalf("address %d planes = %+v; want [heating]", address, response.Planes)
			}
		}
	})

	t.Run("methods", func(t *testing.T) {
		for _, address := range []int{16, 17} {
			request := graphqlclient.NewRequest(`
				query($address: Int!, $plane: String!) {
					methods(address: $address, plane: $plane) {
						name
					}
				}
			`)
			request.Var("address", address)
			request.Var("plane", "heating")

			var response struct {
				Methods []struct {
					Name string `json:"name"`
				} `json:"methods"`
			}

			if err := client.Run(context.Background(), request, &response); err != nil {
				t.Fatalf("methods query error = %v", err)
			}
			if len(response.Methods) != 1 || response.Methods[0].Name != "get_status" {
				t.Fatalf("address %d methods = %+v; want [get_status]", address, response.Methods)
			}
		}
	})

	t.Run("schedules_populated", func(t *testing.T) {
		tempC := 22.5
		tempRaw := 225
		minTemp := 5.0
		maxTemp := 30.0
		semantic.SetSchedules(&ScheduleStatus{
			Programs: []ScheduleProgram{
				{
					Zone: 0,
					HC:   "heating",
					Config: &ScheduleConfig{
						MaxSlots:       12,
						TimeResolution: 10,
						MinDuration:    5,
						HasTemperature: true,
						TempSlots:      12,
						MinTempC:       &minTemp,
						MaxTempC:       &maxTemp,
					},
					SlotsUsed: []int{1, 0, 1, 1, 1, 1, 1},
					Days: []ScheduleDayProgram{
						{
							Weekday: "monday",
							Slots: []ScheduleTimerSlot{
								{
									StartHour:      0,
									StartMinute:    0,
									EndHour:        24,
									EndMinute:      0,
									TemperatureC:   &tempC,
									TemperatureRaw: &tempRaw,
								},
							},
						},
					},
				},
			},
		})
		defer semantic.SetSchedules(nil)

		request := graphqlclient.NewRequest(`
			query {
				schedules {
					programs {
						zone
						hc
						config {
							maxSlots
							timeResolution
							minDuration
							hasTemperature
							tempSlots
							minTempC
							maxTempC
						}
						slotsUsed
						days {
							weekday
							slots {
								startHour
								startMinute
								endHour
								endMinute
								temperatureC
								temperatureRaw
							}
						}
					}
				}
			}
		`)

		var response struct {
			Schedules *struct {
				Programs []struct {
					Zone   int    `json:"zone"`
					HC     string `json:"hc"`
					Config *struct {
						MaxSlots       int      `json:"maxSlots"`
						TimeResolution int      `json:"timeResolution"`
						MinDuration    int      `json:"minDuration"`
						HasTemperature bool     `json:"hasTemperature"`
						TempSlots      int      `json:"tempSlots"`
						MinTempC       *float64 `json:"minTempC"`
						MaxTempC       *float64 `json:"maxTempC"`
					} `json:"config"`
					SlotsUsed []int `json:"slotsUsed"`
					Days      []struct {
						Weekday string `json:"weekday"`
						Slots   []struct {
							StartHour      int      `json:"startHour"`
							StartMinute    int      `json:"startMinute"`
							EndHour        int      `json:"endHour"`
							EndMinute      int      `json:"endMinute"`
							TemperatureC   *float64 `json:"temperatureC"`
							TemperatureRaw *int     `json:"temperatureRaw"`
						} `json:"slots"`
					} `json:"days"`
				} `json:"programs"`
			} `json:"schedules"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("schedules query error = %v", err)
		}
		if response.Schedules == nil {
			t.Fatalf("schedules = nil; want non-nil")
		}
		if len(response.Schedules.Programs) != 1 {
			t.Fatalf("programs = %d; want 1", len(response.Schedules.Programs))
		}
		prog := response.Schedules.Programs[0]
		if prog.Zone != 0 || prog.HC != "heating" {
			t.Fatalf("program zone=%d hc=%q; want 0/heating", prog.Zone, prog.HC)
		}
		if prog.Config == nil || prog.Config.MaxSlots != 12 {
			t.Fatalf("config maxSlots = %v; want 12", prog.Config)
		}
		if prog.Config.MinTempC == nil || *prog.Config.MinTempC != 5.0 {
			t.Fatalf("config minTempC = %v; want 5.0", prog.Config.MinTempC)
		}
		if len(prog.SlotsUsed) != 7 || prog.SlotsUsed[0] != 1 || prog.SlotsUsed[1] != 0 {
			t.Fatalf("slotsUsed = %v; want [1 0 1 1 1 1 1]", prog.SlotsUsed)
		}
		if len(prog.Days) != 1 || prog.Days[0].Weekday != "monday" {
			t.Fatalf("days = %+v; want [monday]", prog.Days)
		}
		if len(prog.Days[0].Slots) != 1 {
			t.Fatalf("monday slots = %d; want 1", len(prog.Days[0].Slots))
		}
		slot := prog.Days[0].Slots[0]
		if slot.StartHour != 0 || slot.EndHour != 24 {
			t.Fatalf("slot hours = %d-%d; want 0-24", slot.StartHour, slot.EndHour)
		}
		if slot.TemperatureC == nil || *slot.TemperatureC != 22.5 {
			t.Fatalf("slot temperatureC = %v; want 22.5", slot.TemperatureC)
		}
		if slot.TemperatureRaw == nil || *slot.TemperatureRaw != 225 {
			t.Fatalf("slot temperatureRaw = %v; want 225", slot.TemperatureRaw)
		}
	})
}

type mockProvider struct {
	planes []registry.Plane
}

func (provider mockProvider) Name() string {
	return "mock"
}

func (provider mockProvider) Match(registry.DeviceInfo) bool {
	return true
}

func (provider mockProvider) CreatePlanes(registry.DeviceInfo) []registry.Plane {
	return provider.planes
}

func (provider mockProvider) CreateProjections(info registry.DeviceInfo, planes []registry.Plane) []registry.Projection {
	projection, ok := mockProjection(info)
	if !ok {
		return nil
	}
	return []registry.Projection{projection}
}

func mockProjection(info registry.DeviceInfo) (registry.Projection, bool) {
	base := []registry.PathSegment{
		{Name: "ebus"},
		{Name: fmt.Sprintf("addr@%02X", info.Address)},
		{Name: fmt.Sprintf("device@%s", info.DeviceID)},
	}
	canonicalRoot := registry.ProjectionPath{Plane: registry.ServicePlane, Segments: base}
	rootPath := registry.ProjectionPath{Plane: "Observability", Segments: base}
	root, err := registry.NewNode(rootPath, canonicalRoot)
	if err != nil {
		return registry.Projection{}, false
	}

	childSegments := append(append([]registry.PathSegment(nil), base...), registry.PathSegment{Name: "method@get_status"})
	canonicalChild := registry.ProjectionPath{Plane: registry.ServicePlane, Segments: childSegments}
	childPath := registry.ProjectionPath{Plane: "Observability", Segments: childSegments}
	child, err := registry.NewNode(childPath, canonicalChild)
	if err != nil {
		return registry.Projection{}, false
	}
	edge, err := registry.NewEdge("Observability", root.ID, child.ID)
	if err != nil {
		return registry.Projection{}, false
	}
	projection, err := registry.NewProjection("Observability", []registry.Node{root, child}, []registry.Edge{edge})
	if err != nil {
		return registry.Projection{}, false
	}
	return projection, true
}

func schemaSelector(name string) schema.SchemaSelector {
	return schema.SchemaSelector{
		Default: schema.Schema{
			Fields: []schema.SchemaField{
				{Name: name, Type: types.DATA1b{}},
			},
		},
	}
}

func intPtr(value int) *int {
	v := value
	return &v
}

func TestQueryResolvers_DevicesReflectLateRegistryPopulation(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	defer func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	}()

	builder := NewBuilder(gateway.Registry, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start error = %v", err)
	}

	handler, err := NewHandler(builder)
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	gateway.Registry.Register(registry.DeviceInfo{
		Address:         0x15,
		Manufacturer:    "Vaillant",
		DeviceID:        "BASV2",
		SoftwareVersion: "0507",
		HardwareVersion: "1704",
	})

	client := graphqlclient.NewClient(server.URL)
	request := graphqlclient.NewRequest(`
		query {
			devices {
				address
				deviceId
			}
		}
	`)

	var response struct {
		Devices []struct {
			Address  int    `json:"address"`
			DeviceID string `json:"deviceId"`
		} `json:"devices"`
	}

	if err := client.Run(context.Background(), request, &response); err != nil {
		t.Fatalf("devices query error = %v", err)
	}
	if len(response.Devices) != 1 {
		t.Fatalf("devices = %d; want 1", len(response.Devices))
	}
	if response.Devices[0].Address != 21 || response.Devices[0].DeviceID != "BASV2" {
		t.Fatalf("device payload = %+v; want address=21 deviceId=BASV2", response.Devices[0])
	}
}
