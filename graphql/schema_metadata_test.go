package graphql

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestBuildSchema_VaillantMatchIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	upper := mockEntry{
		info: registry.DeviceInfo{
			Address:      0x10,
			Manufacturer: "Vaillant",
			DeviceID:     "dev-upper",
			SerialNumber: "21-22-09-0020184848-0082-005409-N4",
		},
	}
	lower := mockEntry{
		info: registry.DeviceInfo{
			Address:      0x11,
			Manufacturer: "vaillant",
			DeviceID:     "dev-lower",
			SerialNumber: "21-22-09-0020184848-0082-005409-N4",
		},
	}

	got, err := BuildSchema(mockRegistry{entries: []registry.DeviceEntry{upper, lower}})
	if err != nil {
		t.Fatalf("BuildSchema error = %v", err)
	}
	if len(got.Devices) != 2 {
		t.Fatalf("devices = %d; want 2", len(got.Devices))
	}

	if got.Devices[0].PartNumber == "" {
		t.Fatalf("upper PartNumber empty; want extracted part number")
	}
	if got.Devices[1].PartNumber != got.Devices[0].PartNumber {
		t.Fatalf("lower PartNumber = %q; want %q", got.Devices[1].PartNumber, got.Devices[0].PartNumber)
	}
}

func TestBuildSchema_UsesDeviceIDFallbackWhenVaillantPartNumberMissing(t *testing.T) {
	t.Parallel()

	netx := mockEntry{
		info: registry.DeviceInfo{
			Address:      0x04,
			Manufacturer: "Vaillant",
			DeviceID:     "NETX3",
		},
	}
	vr71 := mockEntry{
		info: registry.DeviceInfo{
			Address:      0x26,
			Manufacturer: "Vaillant",
			DeviceID:     "VR_71",
		},
	}

	got, err := BuildSchema(mockRegistry{entries: []registry.DeviceEntry{netx, vr71}})
	if err != nil {
		t.Fatalf("BuildSchema error = %v", err)
	}
	if len(got.Devices) != 2 {
		t.Fatalf("devices = %d; want 2", len(got.Devices))
	}

	if got.Devices[0].DisplayName != "myVaillant Connect" || got.Devices[0].ProductModel != "VR940f" {
		t.Fatalf("NETX3 fallback metadata = %+v; want myVaillant Connect / VR940f", got.Devices[0])
	}
	if got.Devices[1].DisplayName != "FM5 Control Centre" || got.Devices[1].ProductModel != "VR 71" {
		t.Fatalf("VR_71 fallback metadata = %+v; want FM5 Control Centre / VR 71", got.Devices[1])
	}
}

func TestCloneSchema_PreservesDeviceMetadataFields(t *testing.T) {
	t.Parallel()

	original := Schema{
		Devices: []Device{
			{
				Address:       0x08,
				Addresses:     []byte{0x08, 0x09},
				Manufacturer:  "Vaillant",
				DeviceID:      "dev-a",
				DisplayName:   "FM5 Control Centre",
				ProductFamily: "sensoCOMFORT",
				ProductModel:  "VR 71",
				PartNumber:    "0020184848",
				Role:          "controller",
				Planes:        []Plane{{Name: "system"}},
				Projections:   []Projection{{Plane: "Service"}},
			},
		},
	}

	cloned := cloneSchema(original)
	if len(cloned.Devices) != 1 {
		t.Fatalf("devices = %d; want 1", len(cloned.Devices))
	}
	got := cloned.Devices[0]
	if got.DisplayName != "FM5 Control Centre" ||
		got.ProductFamily != "sensoCOMFORT" ||
		got.ProductModel != "VR 71" ||
		got.PartNumber != "0020184848" ||
		got.Role != "controller" {
		t.Fatalf("metadata clone mismatch: %+v", got)
	}
	if len(got.Addresses) != 2 || got.Addresses[0] != 0x08 || got.Addresses[1] != 0x09 {
		t.Fatalf("addresses clone mismatch: %v", got.Addresses)
	}
}
