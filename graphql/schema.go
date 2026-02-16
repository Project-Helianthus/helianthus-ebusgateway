package graphql

import (
	"fmt"
	"strings"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/schema"
	"github.com/d3vi1/helianthus-ebusreg/vaillant/productids"
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
	DisplayName     string
	ProductFamily   string
	ProductModel    string
	PartNumber      string
	Role            string
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
	catalog, catalogErr := productids.LoadCatalog()

	reg.Iterate(func(entry registry.DeviceEntry) bool {
		partNumber := ""
		displayName := ""
		family := ""
		model := ""
		role := ""
		if strings.EqualFold(entry.Manufacturer(), "Vaillant") {
			partNumber = extractVaillantPartNumber(entry.SerialNumber())
			displayName, family, model, role = resolveVaillantProduct(partNumber, catalog, catalogErr)
		}

		device := Device{
			Address:         entry.Address(),
			Manufacturer:    entry.Manufacturer(),
			DeviceID:        entry.DeviceID(),
			SerialNumber:    entry.SerialNumber(),
			MacAddress:      entry.MacAddress(),
			SoftwareVersion: entry.SoftwareVersion(),
			HardwareVersion: entry.HardwareVersion(),
			DisplayName:     displayName,
			ProductFamily:   family,
			ProductModel:    model,
			PartNumber:      partNumber,
			Role:            role,
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
			canonicalByID := make(map[registry.NodeID]string, len(projection.Nodes))
			for index, node := range projection.Nodes {
				canonicalPath := node.CanonicalPath.String()
				canonicalByID[node.ID] = canonicalPath
				nodes[index] = ProjectionNode{
					ID:            canonicalPath,
					Path:          node.Path.String(),
					CanonicalPath: canonicalPath,
				}
			}
			edges := make([]ProjectionEdge, len(projection.Edges))
			for index, edge := range projection.Edges {
				from := canonicalByID[edge.From]
				to := canonicalByID[edge.To]
				if from == "" {
					from = string(edge.From)
				}
				if to == "" {
					to = string(edge.To)
				}
				edgeID := edge.ID
				if from != "" && to != "" {
					stableID, err := registry.StableEdgeID(projection.Plane, registry.NodeID(from), registry.NodeID(to))
					if err == nil {
						edgeID = stableID
					}
				}
				edges[index] = ProjectionEdge{
					ID:   string(edgeID),
					From: from,
					To:   to,
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

func extractVaillantPartNumber(serial string) string {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return ""
	}

	parts := strings.Split(serial, "-")
	if len(parts) >= 4 {
		partNumber := strings.TrimSpace(parts[3])
		if len(partNumber) == 10 && isDigits(partNumber) {
			return partNumber
		}
	}

	compact := make([]byte, 0, len(serial))
	for i := 0; i < len(serial); i++ {
		ch := serial[i]
		if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
			compact = append(compact, ch)
		}
	}
	if len(compact) >= 16 {
		candidate := string(compact[6:16])
		if len(candidate) == 10 && isDigits(candidate) {
			return candidate
		}
	}

	return ""
}

func resolveVaillantProduct(partNumber string, catalog productids.Catalog, catalogErr error) (displayName string, family string, model string, role string) {
	if partNumber == "" || catalogErr != nil {
		return "", "", "", ""
	}
	record, ok := catalog.ByPartNumber[partNumber]
	if !ok {
		return "", "", "", ""
	}

	family = strings.TrimSpace(record.Family)
	model = strings.TrimSpace(record.ProductModel)
	role = strings.TrimSpace(record.Role)
	displayName = formatVaillantDisplayName(family, role)
	return displayName, family, model, role
}

func formatVaillantDisplayName(family string, role string) string {
	if family == "" {
		return role
	}
	parts := strings.Fields(family)
	if len(parts) > 0 && looksLikeVaillantFunctionalModule(parts[0]) && role != "" {
		return parts[0] + " " + role
	}
	return family
}

func looksLikeVaillantFunctionalModule(prefix string) bool {
	if len(prefix) < 3 || !strings.HasPrefix(prefix, "FM") {
		return false
	}
	for i := 2; i < len(prefix); i++ {
		if prefix[i] < '0' || prefix[i] > '9' {
			return false
		}
	}
	return true
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
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
			DisplayName:     device.DisplayName,
			ProductFamily:   device.ProductFamily,
			ProductModel:    device.ProductModel,
			PartNumber:      device.PartNumber,
			Role:            device.Role,
			Planes:          planes,
			Projections:     projections,
		}
	}
	return Schema{Devices: devices}
}
