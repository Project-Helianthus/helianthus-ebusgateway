package matrix

import "fmt"

type Transport string

const (
	TransportENS      Transport = "ens"
	TransportENH      Transport = "enh"
	TransportUDPPlain Transport = "udp"
	TransportEbusdTCP Transport = "ebusd-tcp"
)

var StandardTransports = []Transport{
	TransportENS,
	TransportENH,
	TransportUDPPlain,
}

type TopologyKind string

const (
	TopologyDirectAdapter TopologyKind = "direct-adapter"
	TopologyViaEbusdTCP   TopologyKind = "via-ebusd-tcp"
	TopologyProxySingle   TopologyKind = "proxy-single-client"
	TopologyProxyDual     TopologyKind = "proxy-dual-client"
)

type TopologyCase struct {
	ID               string       `json:"id"`
	Kind             TopologyKind `json:"kind"`
	GatewayTransport Transport    `json:"gateway_transport"`
	ProxyTransport   Transport    `json:"proxy_transport,omitempty"`
	EbusdTransport   Transport    `json:"ebusd_transport,omitempty"`
	UsesProxy        bool         `json:"uses_proxy"`
	UsesEbusd        bool         `json:"uses_ebusd"`
	EbusdViaProxy    bool         `json:"ebusd_via_proxy"`
}

func GenerateTopologyCases() []TopologyCase {
	nextID := 1
	buildID := func() string {
		value := fmt.Sprintf("T%02d", nextID)
		nextID++
		return value
	}

	cases := make([]TopologyCase, 0, 42)

	for _, transport := range StandardTransports {
		cases = append(cases, TopologyCase{
			ID:               buildID(),
			Kind:             TopologyDirectAdapter,
			GatewayTransport: transport,
		})
	}

	for _, transport := range StandardTransports {
		cases = append(cases, TopologyCase{
			ID:               buildID(),
			Kind:             TopologyViaEbusdTCP,
			GatewayTransport: TransportEbusdTCP,
			EbusdTransport:   transport,
			UsesEbusd:        true,
		})
	}

	for _, gatewayTransport := range StandardTransports {
		for _, proxyTransport := range StandardTransports {
			cases = append(cases, TopologyCase{
				ID:               buildID(),
				Kind:             TopologyProxySingle,
				GatewayTransport: gatewayTransport,
				ProxyTransport:   proxyTransport,
				UsesProxy:        true,
			})
		}
	}

	for _, gatewayTransport := range StandardTransports {
		for _, proxyTransport := range StandardTransports {
			for _, ebusdTransport := range StandardTransports {
				cases = append(cases, TopologyCase{
					ID:               buildID(),
					Kind:             TopologyProxyDual,
					GatewayTransport: gatewayTransport,
					ProxyTransport:   proxyTransport,
					EbusdTransport:   ebusdTransport,
					UsesProxy:        true,
					UsesEbusd:        true,
					EbusdViaProxy:    true,
				})
			}
		}
	}

	return cases
}

func FilterCases(cases []TopologyCase, includeIDs []string) []TopologyCase {
	if len(includeIDs) == 0 {
		return append([]TopologyCase(nil), cases...)
	}
	allowed := make(map[string]struct{}, len(includeIDs))
	for _, id := range includeIDs {
		allowed[id] = struct{}{}
	}
	filtered := make([]TopologyCase, 0, len(cases))
	for _, candidate := range cases {
		if _, ok := allowed[candidate.ID]; ok {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}
