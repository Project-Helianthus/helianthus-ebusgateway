package graphql

import (
	"fmt"
	"net/http"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	graphqlgo "github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

type graphqlSchemaTypes struct {
	fieldType          *graphqlgo.Object
	responseType       *graphqlgo.Object
	methodType         *graphqlgo.Object
	projectionNodeType *graphqlgo.Object
	projectionEdgeType *graphqlgo.Object
	projectionType     *graphqlgo.Object
	planeType          *graphqlgo.Object
	deviceType         *graphqlgo.Object
	broadcastType      *graphqlgo.Object
	statusType         *graphqlgo.Object
	zoneType           *graphqlgo.Object
	dhwType            *graphqlgo.Object
	energyTotals       *graphqlgo.Object
}

func buildSchemaTypes() graphqlSchemaTypes {
	zoneType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Zone",
		Fields: graphqlgo.Fields{
			"id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					return zone.ID, nil
				},
			},
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					return zone.Name, nil
				},
			},
			"operatingMode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.OperatingMode == "" {
						return nil, nil
					}
					return zone.OperatingMode, nil
				},
			},
			"preset": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.Preset == "" {
						return nil, nil
					}
					return zone.Preset, nil
				},
			},
			"hvacAction": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.HvacAction == "" {
						return nil, nil
					}
					return zone.HvacAction, nil
				},
			},
			"allowedModes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return []string{}, nil
					}
					if len(zone.AllowedModes) == 0 {
						return []string{"off", "auto", "heat"}, nil
					}
					return zone.AllowedModes, nil
				},
			},
			"currentTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.CurrentTempC == nil {
						return nil, nil
					}
					return *zone.CurrentTempC, nil
				},
			},
			"targetTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.TargetTempC == nil {
						return nil, nil
					}
					return *zone.TargetTempC, nil
				},
			},
			"currentHumidityPct": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.CurrentHumidityPct == nil {
						return nil, nil
					}
					return *zone.CurrentHumidityPct, nil
				},
			},
			"heatingDemand": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.HeatingDemand == nil {
						return nil, nil
					}
					return *zone.HeatingDemand, nil
				},
			},
			"specialFunction": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.SpecialFunction == "" {
						return nil, nil
					}
					return zone.SpecialFunction, nil
				},
			},
			"circuitTypeRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.CircuitTypeRaw == "" {
						return nil, nil
					}
					return zone.CircuitTypeRaw, nil
				},
			},
			"zoneCircuitIndexRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.ZoneCircuitIndexRaw == "" {
						return nil, nil
					}
					return zone.ZoneCircuitIndexRaw, nil
				},
			},
			"zoneOperationModeRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.ZoneOperationModeRaw == "" {
						return nil, nil
					}
					return zone.ZoneOperationModeRaw, nil
				},
			},
			"zoneValveStatusRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.ZoneValveStatusRaw == "" {
						return nil, nil
					}
					return zone.ZoneValveStatusRaw, nil
				},
			},
			"zoneSpecialFunctionRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					zone, ok := params.Source.(Zone)
					if !ok {
						return nil, nil
					}
					if zone.ZoneSpecialFunctionRaw == "" {
						return nil, nil
					}
					return zone.ZoneSpecialFunctionRaw, nil
				},
			},
		},
	})

	energySeriesType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "EnergySeries",
		Fields: graphqlgo.Fields{
			"today": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Float),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.Today, nil
				},
			},
			"yearly": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Float))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					series, ok := params.Source.(EnergySeries)
					if !ok {
						return nil, nil
					}
					return series.Yearly, nil
				},
			},
		},
	})

	energyChannelType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "EnergyChannel",
		Fields: graphqlgo.Fields{
			"dhw": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energySeriesType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					channel, ok := params.Source.(EnergyChannel)
					if !ok {
						return nil, nil
					}
					return channel.DHW, nil
				},
			},
			"climate": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energySeriesType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					channel, ok := params.Source.(EnergyChannel)
					if !ok {
						return nil, nil
					}
					return channel.Climate, nil
				},
			},
		},
	})

	energyTotalsType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "EnergyTotals",
		Fields: graphqlgo.Fields{
			"gas": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyChannelType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					totals, ok := params.Source.(*EnergyTotals)
					if !ok || totals == nil {
						return nil, nil
					}
					return totals.Gas, nil
				},
			},
			"electric": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyChannelType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					totals, ok := params.Source.(*EnergyTotals)
					if !ok || totals == nil {
						return nil, nil
					}
					return totals.Electric, nil
				},
			},
			"solar": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(energyChannelType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					totals, ok := params.Source.(*EnergyTotals)
					if !ok || totals == nil {
						return nil, nil
					}
					return totals.Solar, nil
				},
			},
		},
	})

	dhwType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "DhwStatus",
		Fields: graphqlgo.Fields{
			"operatingMode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.OperatingMode == "" {
						return nil, nil
					}
					return status.OperatingMode, nil
				},
			},
			"preset": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.Preset == "" {
						return nil, nil
					}
					return status.Preset, nil
				},
			},
			"currentTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.CurrentTempC == nil {
						return nil, nil
					}
					return *status.CurrentTempC, nil
				},
			},
			"targetTempC": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.TargetTempC == nil {
						return nil, nil
					}
					return *status.TargetTempC, nil
				},
			},
			"heatingDemand": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.HeatingDemand == nil {
						return nil, nil
					}
					return *status.HeatingDemand, nil
				},
			},
			"specialFunction": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.SpecialFunction == "" {
						return nil, nil
					}
					return status.SpecialFunction, nil
				},
			},
			"dhwOperationModeRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.DhwOperationModeRaw == "" {
						return nil, nil
					}
					return status.DhwOperationModeRaw, nil
				},
			},
			"dhwSpecialFunctionRaw": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*DhwStatus)
					if !ok {
						return nil, nil
					}
					if status.DhwSpecialFunctionRaw == "" {
						return nil, nil
					}
					return status.DhwSpecialFunctionRaw, nil
				},
			},
		},
	})

	statusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ServiceStatus",
		Fields: graphqlgo.Fields{
			"status": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.Status, nil
				},
			},
			"firmwareVersion": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.FirmwareVersion, nil
				},
			},
			"updatesAvailable": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.UpdatesAvailable, nil
				},
			},
			"initiatorAddress": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(ServiceStatus)
					if !ok {
						return nil, nil
					}
					return status.InitiatorAddress, nil
				},
			},
		},
	})

	fieldType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Field",
		Fields: graphqlgo.Fields{
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					field, ok := fieldFromSource(params)
					if !ok {
						return nil, nil
					}
					return field.Name, nil
				},
			},
			"type": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					field, ok := fieldFromSource(params)
					if !ok {
						return nil, nil
					}
					return field.Type, nil
				},
			},
			"size": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					field, ok := fieldFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(field.Size), nil
				},
			},
		},
	})

	responseType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ResponseSchema",
		Fields: graphqlgo.Fields{
			"fields": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(fieldType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					response, ok := responseFromSource(params)
					if !ok {
						return nil, nil
					}
					return response.Fields, nil
				},
			},
		},
	})

	methodType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Method",
		Fields: graphqlgo.Fields{
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return method.Name, nil
				},
			},
			"readOnly": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return method.ReadOnly, nil
				},
			},
			"primary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(method.Primary), nil
				},
			},
			"secondary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(method.Secondary), nil
				},
			},
			"response": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(responseType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					method, ok := methodFromSource(params)
					if !ok {
						return nil, nil
					}
					return method.Response, nil
				},
			},
		},
	})

	projectionNodeType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ProjectionNode",
		Fields: graphqlgo.Fields{
			"id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					node, ok := projectionNodeFromSource(params)
					if !ok {
						return nil, nil
					}
					return node.ID, nil
				},
			},
			"path": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					node, ok := projectionNodeFromSource(params)
					if !ok {
						return nil, nil
					}
					return node.Path, nil
				},
			},
			"canonicalPath": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					node, ok := projectionNodeFromSource(params)
					if !ok {
						return nil, nil
					}
					return node.CanonicalPath, nil
				},
			},
		},
	})

	projectionEdgeType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ProjectionEdge",
		Fields: graphqlgo.Fields{
			"id": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					edge, ok := projectionEdgeFromSource(params)
					if !ok {
						return nil, nil
					}
					return edge.ID, nil
				},
			},
			"from": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					edge, ok := projectionEdgeFromSource(params)
					if !ok {
						return nil, nil
					}
					return edge.From, nil
				},
			},
			"to": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					edge, ok := projectionEdgeFromSource(params)
					if !ok {
						return nil, nil
					}
					return edge.To, nil
				},
			},
		},
	})

	projectionType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Projection",
		Fields: graphqlgo.Fields{
			"plane": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					projection, ok := projectionFromSource(params)
					if !ok {
						return nil, nil
					}
					return projection.Plane, nil
				},
			},
			"nodes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(projectionNodeType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					projection, ok := projectionFromSource(params)
					if !ok {
						return nil, nil
					}
					return projection.Nodes, nil
				},
			},
			"edges": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(projectionEdgeType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					projection, ok := projectionFromSource(params)
					if !ok {
						return nil, nil
					}
					return projection.Edges, nil
				},
			},
		},
	})

	planeType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Plane",
		Fields: graphqlgo.Fields{
			"name": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					plane, ok := planeFromSource(params)
					if !ok {
						return nil, nil
					}
					return plane.Name, nil
				},
			},
			"methods": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(methodType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					plane, ok := planeFromSource(params)
					if !ok {
						return nil, nil
					}
					return plane.Methods, nil
				},
			},
		},
	})

	deviceType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Device",
		Fields: graphqlgo.Fields{
			"address": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return int(device.Address), nil
				},
			},
			"addresses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Int))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					addresses := normalizeDeviceAddresses(device.Address, device.Addresses)
					values := make([]int, len(addresses))
					for index, address := range addresses {
						values[index] = int(address)
					}
					return values, nil
				},
			},
			"manufacturer": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.Manufacturer, nil
				},
			},
			"deviceId": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.DeviceID, nil
				},
			},
			"serialNumber": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.SerialNumber, nil
				},
			},
			"macAddress": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.MacAddress, nil
				},
			},
			"softwareVersion": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.SoftwareVersion, nil
				},
			},
			"hardwareVersion": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.HardwareVersion, nil
				},
			},
			"displayName": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.DisplayName == "" {
						return nil, nil
					}
					return device.DisplayName, nil
				},
			},
			"productFamily": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.ProductFamily == "" {
						return nil, nil
					}
					return device.ProductFamily, nil
				},
			},
			"productModel": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.ProductModel == "" {
						return nil, nil
					}
					return device.ProductModel, nil
				},
			},
			"partNumber": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.PartNumber == "" {
						return nil, nil
					}
					return device.PartNumber, nil
				},
			},
			"role": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					if device.Role == "" {
						return nil, nil
					}
					return device.Role, nil
				},
			},
			"planes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(planeType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.Planes, nil
				},
			},
			"projections": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(projectionType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					device, ok := deviceFromSource(params)
					if !ok {
						return nil, nil
					}
					return device.Projections, nil
				},
			},
		},
	})

	return graphqlSchemaTypes{
		fieldType:          fieldType,
		responseType:       responseType,
		methodType:         methodType,
		projectionNodeType: projectionNodeType,
		projectionEdgeType: projectionEdgeType,
		projectionType:     projectionType,
		planeType:          planeType,
		deviceType:         deviceType,
		broadcastType:      buildBroadcastType(),
		statusType:         statusType,
		zoneType:           zoneType,
		dhwType:            dhwType,
		energyTotals:       energyTotalsType,
	}
}

func buildQueryType(builder *Builder, types graphqlSchemaTypes) *graphqlgo.Object {
	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "Query",
		Fields: graphqlgo.Fields{
			"daemonStatus": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.statusType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.statusProvider().DaemonStatus(), nil
				},
			},
			"adapterStatus": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(types.statusType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.statusProvider().AdapterStatus(), nil
				},
			},
			"zones": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.zoneType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().Zones(), nil
				},
			},
			"dhw": &graphqlgo.Field{
				Type: types.dhwType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().DHW(), nil
				},
			},
			"energyTotals": &graphqlgo.Field{
				Type: types.energyTotals,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					return builder.semanticProvider().EnergyTotals(), nil
				},
			},
			"devices": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.deviceType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					snapshot := builder.Schema()
					return snapshot.Devices, nil
				},
			},
			"device": &graphqlgo.Field{
				Type: types.deviceType,
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					address, err := parseAddress(params.Args["address"])
					if err != nil {
						return nil, err
					}
					snapshot := builder.Schema()
					device, ok := findDevice(snapshot.Devices, address)
					if !ok {
						return nil, nil
					}
					return device, nil
				},
			},
			"planes": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.planeType))),
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					address, err := parseAddress(params.Args["address"])
					if err != nil {
						return nil, err
					}
					snapshot := builder.Schema()
					device, ok := findDevice(snapshot.Devices, address)
					if !ok {
						return []Plane{}, nil
					}
					return device.Planes, nil
				},
			},
			"methods": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(types.methodType))),
				Args: graphqlgo.FieldConfigArgument{
					"address": &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
					"plane":   &graphqlgo.ArgumentConfig{Type: graphqlgo.NewNonNull(graphqlgo.String)},
				},
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					address, err := parseAddress(params.Args["address"])
					if err != nil {
						return nil, err
					}
					planeName, _ := params.Args["plane"].(string)
					snapshot := builder.Schema()
					device, ok := findDevice(snapshot.Devices, address)
					if !ok {
						return []Method{}, nil
					}
					plane, ok := findPlane(device.Planes, planeName)
					if !ok {
						return []Method{}, nil
					}
					return plane.Methods, nil
				},
			},
		},
	})
}

func NewQuerySchema(builder *Builder) (graphqlgo.Schema, error) {
	if builder == nil {
		return graphqlgo.Schema{}, fmt.Errorf("graphql query schema missing builder: %w", ebuserrors.ErrInvalidPayload)
	}

	types := buildSchemaTypes()
	queryType := buildQueryType(builder, types)

	return graphqlgo.NewSchema(graphqlgo.SchemaConfig{Query: queryType})
}

func NewHandler(builder *Builder) (http.Handler, error) {
	schema, err := NewSchema(builder, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	return handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false,
	}), nil
}

func parseAddress(raw any) (byte, error) {
	switch value := raw.(type) {
	case int:
		return toAddress(value)
	case int64:
		return toAddress(int(value))
	case float64:
		return toAddress(int(value))
	default:
		return 0, fmt.Errorf("graphql query invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
}

func toAddress(value int) (byte, error) {
	if value < 0 || value > 0xFF {
		return 0, fmt.Errorf("graphql query invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
	return byte(value), nil
}

func findDevice(devices []Device, address byte) (Device, bool) {
	for _, device := range devices {
		if deviceHasAddress(device, address) {
			return device, true
		}
	}
	return Device{}, false
}

func deviceHasAddress(device Device, address byte) bool {
	for _, candidate := range normalizeDeviceAddresses(device.Address, device.Addresses) {
		if candidate == address {
			return true
		}
	}
	return false
}

func findPlane(planes []Plane, name string) (Plane, bool) {
	for _, plane := range planes {
		if plane.Name == name {
			return plane, true
		}
	}
	return Plane{}, false
}

func deviceFromSource(params graphqlgo.ResolveParams) (Device, bool) {
	switch value := params.Source.(type) {
	case Device:
		return value, true
	case *Device:
		if value == nil {
			return Device{}, false
		}
		return *value, true
	default:
		return Device{}, false
	}
}

func planeFromSource(params graphqlgo.ResolveParams) (Plane, bool) {
	switch value := params.Source.(type) {
	case Plane:
		return value, true
	case *Plane:
		if value == nil {
			return Plane{}, false
		}
		return *value, true
	default:
		return Plane{}, false
	}
}

func projectionFromSource(params graphqlgo.ResolveParams) (Projection, bool) {
	switch value := params.Source.(type) {
	case Projection:
		return value, true
	case *Projection:
		if value == nil {
			return Projection{}, false
		}
		return *value, true
	default:
		return Projection{}, false
	}
}

func projectionNodeFromSource(params graphqlgo.ResolveParams) (ProjectionNode, bool) {
	switch value := params.Source.(type) {
	case ProjectionNode:
		return value, true
	case *ProjectionNode:
		if value == nil {
			return ProjectionNode{}, false
		}
		return *value, true
	default:
		return ProjectionNode{}, false
	}
}

func projectionEdgeFromSource(params graphqlgo.ResolveParams) (ProjectionEdge, bool) {
	switch value := params.Source.(type) {
	case ProjectionEdge:
		return value, true
	case *ProjectionEdge:
		if value == nil {
			return ProjectionEdge{}, false
		}
		return *value, true
	default:
		return ProjectionEdge{}, false
	}
}

func methodFromSource(params graphqlgo.ResolveParams) (Method, bool) {
	switch value := params.Source.(type) {
	case Method:
		return value, true
	case *Method:
		if value == nil {
			return Method{}, false
		}
		return *value, true
	default:
		return Method{}, false
	}
}

func responseFromSource(params graphqlgo.ResolveParams) (ResponseSchema, bool) {
	switch value := params.Source.(type) {
	case ResponseSchema:
		return value, true
	case *ResponseSchema:
		if value == nil {
			return ResponseSchema{}, false
		}
		return *value, true
	default:
		return ResponseSchema{}, false
	}
}

func fieldFromSource(params graphqlgo.ResolveParams) (Field, bool) {
	switch value := params.Source.(type) {
	case Field:
		return value, true
	case *Field:
		if value == nil {
			return Field{}, false
		}
		return *value, true
	default:
		return Field{}, false
	}
}
