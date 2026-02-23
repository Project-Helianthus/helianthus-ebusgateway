package main

import "testing"

func TestBuildB524ReadSelector(t *testing.T) {
	t.Parallel()

	got := buildB524ReadSelector(0x02, 0x03, 0x01, 0x001C)
	want := []byte{0x06, 0x02, 0x00, 0x03, 0x01, 0x1C, 0x00}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d; want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=0x%02x; want 0x%02x", i, got[i], want[i])
		}
	}
}

func TestParseB524ReadPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		payload  []byte
		opcode   byte
		group    byte
		instance byte
		addr     uint16
		want     []byte
		ok       bool
	}{
		{
			name:     "valid payload with float value",
			payload:  []byte{0x01, 0x03, 0x22, 0x00, 0x00, 0x00, 0x30, 0x41},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			want:     []byte{0x00, 0x00, 0x30, 0x41},
			ok:       true,
		},
		{
			name:     "header only has no value",
			payload:  []byte{0x01, 0x03, 0x22, 0x00},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "short payload",
			payload:  []byte{0x01, 0x03, 0x22},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "single 0x00 has no value",
			payload:  []byte{0x00},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "rejects mismatched group",
			payload:  []byte{0x01, 0x02, 0x22, 0x00, 0x01},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "rejects mismatched addr",
			payload:  []byte{0x01, 0x03, 0x21, 0x00, 0x01},
			opcode:   0x02,
			group:    0x03,
			instance: 0x01,
			addr:     0x0022,
			ok:       false,
		},
		{
			name:     "allows kind 0x00 with value bytes",
			payload:  []byte{0x00, 0x03, 0x1c, 0x00, 0xff},
			opcode:   0x02,
			group:    0x03,
			instance: 0x02,
			addr:     0x001c,
			want:     []byte{0xff},
			ok:       true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := parseB524ReadPayload(test.payload, test.opcode, test.group, test.instance, test.addr)
			if ok != test.ok {
				t.Fatalf("ok = %v; want %v", ok, test.ok)
			}
			if !test.ok {
				return
			}
			if len(got) != len(test.want) {
				t.Fatalf("len(got) = %d; want %d", len(got), len(test.want))
			}
			for i := range got {
				if got[i] != test.want[i] {
					t.Fatalf("got[%d] = 0x%02x; want 0x%02x", i, got[i], test.want[i])
				}
			}
		})
	}
}
