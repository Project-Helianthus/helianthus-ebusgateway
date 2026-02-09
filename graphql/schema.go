package graphql

import (
	"fmt"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/schema"
)

type Schema struct {
	Devices []Device
}

type Device struct {
	Address         byte
	Manufacturer    string
	DeviceID        string
	SerialNumber    string
	MacAddress      string
	SoftwareVersion string
	HardwareVersion string
	Planes          []Plane
	Projections     []Projection
}

type Plane struct {
	Name    string
	Methods []Method
}

type Method struct {
	Name      string
	ReadOnly  bool
	Primary   byte
	Secondary byte
	Response  ResponseSchema
}

type ResponseSchema struct {
	Fields []Field
}

type Field struct {
	Name string
	Type string
	Size int
}

type Projection struct {
	Plane string
	Nodes []ProjectionNode
	Edges []ProjectionEdge
}

type ProjectionNode struct {
	ID            string
	Path          string
	CanonicalPath string
}

type ProjectionEdge struct {
	ID   string
	From string
	To   string
}

type Registry interface {
	Iterate(func(registry.DeviceEntry) bool)
}

func BuildSchema(reg Registry) (Schema, error) {
	if reg == nil {
		return Schema{}, fmt.Errorf("graphql schema build missing registry: %w", ebuserrors.ErrInvalidPayload)
	}

	out := Schema{
		Devices: make([]Device, 0),
	}
	var buildErr error

	reg.Iterate(func(entry registry.DeviceEntry) bool {
		device := Device{
			Address:         entry.Address(),
			Manufacturer:    entry.Manufacturer(),
			DeviceID:        entry.DeviceID(),
			SerialNumber:    entry.SerialNumber(),
			MacAddress:      entry.MacAddress(),
			SoftwareVersion: entry.SoftwareVersion(),
			HardwareVersion: entry.HardwareVersion(),
			Planes:          make([]Plane, 0),
			Projections:     make([]Projection, 0),
		}

		for _, plane := range entry.Planes() {
			methods := make([]Method, 0, len(plane.Methods()))
			for _, method := range plane.Methods() {
				template := method.Template()
				if template == nil {
					buildErr = fmt.Errorf("graphql schema build method %s missing template: %w", method.Name(), ebuserrors.ErrInvalidPayload)
					return false
				}

				response, err := selectResponseSchema(entry, method.ResponseSchema())
				if err != nil {
					buildErr = err
					return false
				}

				methods = append(methods, Method{
					Name:      method.Name(),
					ReadOnly:  method.ReadOnly(),
					Primary:   template.Primary(),
					Secondary: template.Secondary(),
					Response:  response,
				})
			}

			device.Planes = append(device.Planes, Plane{
				Name:    plane.Name(),
				Methods: methods,
			})
		}

		for _, projection := range entry.Projections() {
			if err := projection.Validate(); err != nil {
				buildErr = fmt.Errorf("graphql schema build projection %q invalid: %w", projection.Plane, err)
				return false
			}
			nodes := make([]ProjectionNode, len(projection.Nodes))
			for index, node := range projection.Nodes {
				nodes[index] = ProjectionNode{
					ID:            string(node.ID),
					Path:          node.Path.String(),
					CanonicalPath: node.CanonicalPath.String(),
				}
			}
			edges := make([]ProjectionEdge, len(projection.Edges))
			for index, edge := range projection.Edges {
				edges[index] = ProjectionEdge{
					ID:   string(edge.ID),
					From: string(edge.From),
					To:   string(edge.To),
				}
			}
			device.Projections = append(device.Projections, Projection{
				Plane: projection.Plane,
				Nodes: nodes,
				Edges: edges,
			})
		}

		out.Devices = append(out.Devices, device)
		return true
	})

	if buildErr != nil {
		return Schema{}, buildErr
	}

	return out, nil
}

func selectResponseSchema(entry registry.DeviceEntry, selector schema.SchemaSelector) (ResponseSchema, error) {
	if entry == nil {
		return ResponseSchema{}, fmt.Errorf("graphql schema build missing device: %w", ebuserrors.ErrInvalidPayload)
	}

	selected := selector.Select(entry.Address(), entry.HardwareVersion())
	fields := make([]Field, 0, len(selected.Fields))
	for _, field := range selected.Fields {
		if field.Type == nil {
			return ResponseSchema{}, fmt.Errorf("graphql schema build field %q missing type: %w", field.Name, ebuserrors.ErrInvalidPayload)
		}
		fields = append(fields, Field{
			Name: field.Name,
			Type: dataTypeName(field.Type),
			Size: field.Type.Size(),
		})
	}

	return ResponseSchema{Fields: fields}, nil
}

func dataTypeName(dataType types.DataType) string {
	switch dataType.(type) {
	case types.DATA1b:
		return "DATA1b"
	case types.DATA2b:
		return "DATA2b"
	case types.DATA2c:
		return "DATA2c"
	case types.EXP:
		return "EXP"
	case types.WORD:
		return "WORD"
	case types.BCD:
		return "BCD"
	case types.BITFIELD:
		return "BITFIELD"
	default:
		return fmt.Sprintf("%T", dataType)
	}
}

func cloneSchema(schema Schema) Schema {
	if len(schema.Devices) == 0 {
		return Schema{}
	}
	devices := make([]Device, len(schema.Devices))
	for i, device := range schema.Devices {
		planes := make([]Plane, len(device.Planes))
		for j, plane := range device.Planes {
			methods := make([]Method, len(plane.Methods))
			for k, method := range plane.Methods {
				fields := make([]Field, len(method.Response.Fields))
				copy(fields, method.Response.Fields)
				methods[k] = Method{
					Name:      method.Name,
					ReadOnly:  method.ReadOnly,
					Primary:   method.Primary,
					Secondary: method.Secondary,
					Response: ResponseSchema{
						Fields: fields,
					},
				}
			}
			planes[j] = Plane{
				Name:    plane.Name,
				Methods: methods,
			}
		}
		projections := make([]Projection, len(device.Projections))
		for j, projection := range device.Projections {
			nodes := make([]ProjectionNode, len(projection.Nodes))
			copy(nodes, projection.Nodes)
			edges := make([]ProjectionEdge, len(projection.Edges))
			copy(edges, projection.Edges)
			projections[j] = Projection{
				Plane: projection.Plane,
				Nodes: nodes,
				Edges: edges,
			}
		}
		devices[i] = Device{
			Address:         device.Address,
			Manufacturer:    device.Manufacturer,
			DeviceID:        device.DeviceID,
			SerialNumber:    device.SerialNumber,
			MacAddress:      device.MacAddress,
			SoftwareVersion: device.SoftwareVersion,
			HardwareVersion: device.HardwareVersion,
			Planes:          planes,
			Projections:     projections,
		}
	}
	return Schema{Devices: devices}
}
