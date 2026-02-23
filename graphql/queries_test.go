package graphql

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgo/transport"
	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/schema"
	graphqlclient "github.com/machinebox/graphql"
)

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

	t.Run("devices", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				devices {
					address
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
		request := graphqlclient.NewRequest(`
			query($address: Int!) {
				device(address: $address) {
					address
					deviceId
				}
			}
		`)
		request.Var("address", 16)

		var response struct {
			Device struct {
				Address  int    `json:"address"`
				DeviceID string `json:"deviceId"`
			} `json:"device"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("device query error = %v", err)
		}
		if response.Device.Address != 16 || response.Device.DeviceID != "device-a" {
			t.Fatalf("device = %+v; want address=16 deviceId=device-a", response.Device)
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

	t.Run("zones_dhw", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query {
				zones {
					id
					name
					operatingMode
					preset
					currentTempC
					targetTempC
					heatingDemand
				}
				dhw {
					operatingMode
					preset
					currentTempC
					targetTempC
					heatingDemand
				}
			}
		`)

		var response struct {
			Zones []struct {
				ID            string   `json:"id"`
				Name          string   `json:"name"`
				OperatingMode *string  `json:"operatingMode"`
				Preset        *string  `json:"preset"`
				CurrentTempC  *float64 `json:"currentTempC"`
				TargetTempC   *float64 `json:"targetTempC"`
				HeatingDemand *float64 `json:"heatingDemand"`
			} `json:"zones"`
			DHW *struct {
				OperatingMode *string  `json:"operatingMode"`
				Preset        *string  `json:"preset"`
				CurrentTempC  *float64 `json:"currentTempC"`
				TargetTempC   *float64 `json:"targetTempC"`
				HeatingDemand *float64 `json:"heatingDemand"`
			} `json:"dhw"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("zones/dhw query error = %v", err)
		}
		if len(response.Zones) != 0 {
			t.Fatalf("zones = %d; want 0", len(response.Zones))
		}
		if response.DHW != nil {
			t.Fatalf("dhw expected nil with static provider")
		}
	})

	t.Run("energy_totals", func(t *testing.T) {
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
			t.Fatalf("energyTotals query error = %v", err)
		}
		if response.EnergyTotals != nil {
			t.Fatalf("energyTotals expected nil with static provider")
		}
	})

	t.Run("planes", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query($address: Int!) {
				planes(address: $address) {
					name
				}
			}
		`)
		request.Var("address", 16)

		var response struct {
			Planes []struct {
				Name string `json:"name"`
			} `json:"planes"`
		}

		if err := client.Run(context.Background(), request, &response); err != nil {
			t.Fatalf("planes query error = %v", err)
		}
		if len(response.Planes) != 1 || response.Planes[0].Name != "heating" {
			t.Fatalf("planes = %+v; want [heating]", response.Planes)
		}
	})

	t.Run("methods", func(t *testing.T) {
		request := graphqlclient.NewRequest(`
			query($address: Int!, $plane: String!) {
				methods(address: $address, plane: $plane) {
					name
				}
			}
		`)
		request.Var("address", 16)
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
			t.Fatalf("methods = %+v; want [get_status]", response.Methods)
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
