package graphql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/schema"
)

func TestBuildSchema_ReflectsRegistry(t *testing.T) {
	entryA := mockEntry{
		info: registry.DeviceInfo{
			Address:         0x10,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{
			mockPlane{
				name: "heating",
				methods: []registry.Method{
					mockMethod{
						name:     "get_status",
						readOnly: true,
						template: mockTemplate{primary: 0xB5, secondary: 0x04},
						response: schema.SchemaSelector{
							Default: schema.Schema{Fields: []schema.SchemaField{{Name: "base", Type: types.DATA1b{}}}},
							Conditions: []schema.SchemaCondition{
								{
									HasMinHW: true,
									MinHW:    7600,
									Schema:   schema.Schema{Fields: []schema.SchemaField{{Name: "hw", Type: types.WORD{}}}},
								},
							},
						},
					},
				},
			},
		},
	}

	entryB := mockEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-b",
			SoftwareVersion: "2.0",
			HardwareVersion: "7500",
		},
		planes: []registry.Plane{
			mockPlane{
				name: "dhw",
				methods: []registry.Method{
					mockMethod{
						name:     "get_parameters",
						readOnly: true,
						template: mockTemplate{primary: 0xB5, secondary: 0x09},
						response: schema.SchemaSelector{
							Default: schema.Schema{Fields: []schema.SchemaField{{Name: "base", Type: types.DATA1b{}}}},
						},
					},
				},
			},
		},
	}

	reg := mockRegistry{entries: []registry.DeviceEntry{entryA, entryB}}
	got, err := BuildSchema(reg)
	if err != nil {
		t.Fatalf("BuildSchema error = %v", err)
	}
	if len(got.Devices) != 2 {
		t.Fatalf("Devices = %d; want 2", len(got.Devices))
	}

	methodA := got.Devices[0].Planes[0].Methods[0]
	if methodA.Name != "get_status" || methodA.Primary != 0xB5 || methodA.Secondary != 0x04 {
		t.Fatalf("MethodA = %+v; want name=get_status primary=0xB5 secondary=0x04", methodA)
	}
	if len(methodA.Response.Fields) != 1 || methodA.Response.Fields[0].Name != "hw" || methodA.Response.Fields[0].Type != "WORD" {
		t.Fatalf("MethodA response = %+v; want field hw WORD", methodA.Response.Fields)
	}

	methodB := got.Devices[1].Planes[0].Methods[0]
	if methodB.Name != "get_parameters" || methodB.Primary != 0xB5 || methodB.Secondary != 0x09 {
		t.Fatalf("MethodB = %+v; want name=get_parameters primary=0xB5 secondary=0x09", methodB)
	}
	if len(methodB.Response.Fields) != 1 || methodB.Response.Fields[0].Name != "base" || methodB.Response.Fields[0].Type != "DATA1b" {
		t.Fatalf("MethodB response = %+v; want field base DATA1b", methodB.Response.Fields)
	}
}

func TestBuilder_RebuildsOnChange(t *testing.T) {
	entryA := mockEntry{
		info: registry.DeviceInfo{
			Address:         0x10,
			Manufacturer:    "vaillant",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{
			mockPlane{
				name: "heating",
				methods: []registry.Method{
					mockMethod{
						name:     "get_status",
						readOnly: true,
						template: mockTemplate{primary: 0xB5, secondary: 0x04},
						response: schema.SchemaSelector{
							Default: schema.Schema{Fields: []schema.SchemaField{{Name: "status", Type: types.DATA1b{}}}},
						},
					},
				},
			},
		},
	}

	entryB := mockEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			HardwareVersion: "7500",
		},
		planes: []registry.Plane{
			mockPlane{
				name: "dhw",
				methods: []registry.Method{
					mockMethod{
						name:     "get_parameters",
						readOnly: true,
						template: mockTemplate{primary: 0xB5, secondary: 0x09},
						response: schema.SchemaSelector{
							Default: schema.Schema{Fields: []schema.SchemaField{{Name: "base", Type: types.DATA1b{}}}},
						},
					},
				},
			},
		},
	}

	reg := &mutableRegistry{entries: []registry.DeviceEntry{entryA}}
	changes := make(chan struct{}, 1)

	builder := NewBuilder(reg, changes)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := builder.Start(ctx); err != nil {
		t.Fatalf("Start error = %v", err)
	}

	rev := builder.Revision()
	if len(builder.Schema().Devices) != 1 {
		t.Fatalf("Devices = %d; want 1", len(builder.Schema().Devices))
	}

	reg.SetEntries([]registry.DeviceEntry{entryA, entryB})
	changes <- struct{}{}

	waitForRevision(t, builder, rev)

	if len(builder.Schema().Devices) != 2 {
		t.Fatalf("Devices = %d; want 2", len(builder.Schema().Devices))
	}
}

func waitForRevision(t *testing.T, builder *Builder, last uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if builder.Revision() > last {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("revision did not advance from %d", last)
}

type mockRegistry struct {
	entries []registry.DeviceEntry
}

func (reg mockRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	for _, entry := range reg.entries {
		if !fn(entry) {
			return
		}
	}
}

type mutableRegistry struct {
	mu      sync.RWMutex
	entries []registry.DeviceEntry
}

func (reg *mutableRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	reg.mu.RLock()
	entries := append([]registry.DeviceEntry(nil), reg.entries...)
	reg.mu.RUnlock()
	for _, entry := range entries {
		if !fn(entry) {
			return
		}
	}
}

func (reg *mutableRegistry) SetEntries(entries []registry.DeviceEntry) {
	reg.mu.Lock()
	reg.entries = entries
	reg.mu.Unlock()
}

type mockEntry struct {
	info   registry.DeviceInfo
	planes []registry.Plane
}

func (entry mockEntry) Address() byte {
	return entry.info.Address
}

func (entry mockEntry) Manufacturer() string {
	return entry.info.Manufacturer
}

func (entry mockEntry) DeviceID() string {
	return entry.info.DeviceID
}

func (entry mockEntry) SoftwareVersion() string {
	return entry.info.SoftwareVersion
}

func (entry mockEntry) HardwareVersion() string {
	return entry.info.HardwareVersion
}

func (entry mockEntry) Planes() []registry.Plane {
	return entry.planes
}

type mockPlane struct {
	name    string
	methods []registry.Method
}

func (plane mockPlane) Name() string {
	return plane.name
}

func (plane mockPlane) Methods() []registry.Method {
	return plane.methods
}

type mockMethod struct {
	name     string
	readOnly bool
	template registry.FrameTemplate
	response schema.SchemaSelector
}

func (method mockMethod) Name() string {
	return method.name
}

func (method mockMethod) ReadOnly() bool {
	return method.readOnly
}

func (method mockMethod) Template() registry.FrameTemplate {
	return method.template
}

func (method mockMethod) ResponseSchema() schema.SchemaSelector {
	return method.response
}

type mockTemplate struct {
	primary   byte
	secondary byte
}

func (template mockTemplate) Primary() byte {
	return template.primary
}

func (template mockTemplate) Secondary() byte {
	return template.secondary
}
