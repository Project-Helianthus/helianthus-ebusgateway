package main

import (
	"bytes"
	"math"
	"testing"
)

func TestDecodeDATA2c(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want float64
		ok   bool
	}{
		{name: "normal", in: []byte{0x20, 0x03}, want: 50.0, ok: true},
		{name: "negative", in: []byte{0xF0, 0xFF}, want: -1.0, ok: true},
		{name: "short", in: []byte{0x20}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeDATA2c(tc.in)
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v want %v", tc.name, ok, tc.ok)
		}
		if tc.ok && math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("%s: got=%v want=%v", tc.name, got, tc.want)
		}
	}
}

func TestDecodeDATA2b(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want float64
		ok   bool
	}{
		{name: "normal", in: []byte{0x00, 0x01}, want: 1.0, ok: true},
		{name: "boundary", in: []byte{0xFF, 0xFF}, want: 65535.0 / 256.0, ok: true},
		{name: "short", in: []byte{}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeDATA2b(tc.in)
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v want %v", tc.name, ok, tc.ok)
		}
		if tc.ok && math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("%s: got=%v want=%v", tc.name, got, tc.want)
		}
	}
}

func TestDecodeUCH(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want uint8
		ok   bool
	}{
		{name: "normal", in: []byte{0x7F}, want: 0x7F, ok: true},
		{name: "zero", in: []byte{0x00}, want: 0x00, ok: true},
		{name: "short", in: []byte{}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeUCH(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s: got=(%v,%v) want=(%v,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDecodeUIN(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want uint16
		ok   bool
	}{
		{name: "normal", in: []byte{0x34, 0x12}, want: 0x1234, ok: true},
		{name: "zero", in: []byte{0x00, 0x00}, want: 0x0000, ok: true},
		{name: "short", in: []byte{0x34}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeUIN(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s: got=(%v,%v) want=(%v,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDecodeSIN(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want int16
		ok   bool
	}{
		{name: "positive", in: []byte{0x34, 0x12}, want: 0x1234, ok: true},
		{name: "negative", in: []byte{0xCC, 0xFF}, want: -52, ok: true},
		{name: "short", in: []byte{0xCC}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeSIN(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s: got=(%v,%v) want=(%v,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDecodeOnOff(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want bool
		ok   bool
	}{
		{name: "off", in: []byte{0xF0}, want: false, ok: true},
		{name: "on", in: []byte{0x00}, want: true, ok: true},
		{name: "short", in: []byte{}, want: false, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeOnOff(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s: got=(%v,%v) want=(%v,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDecodeHoursum2(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want float64
		ok   bool
	}{
		{name: "normal", in: []byte{0x05, 0x00}, want: 10.0, ok: true},
		{name: "boundary", in: []byte{0xFF, 0xFF}, want: 131070.0, ok: true},
		{name: "short", in: []byte{0x05}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeHoursum2(tc.in)
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v want %v", tc.name, ok, tc.ok)
		}
		if tc.ok && math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("%s: got=%v want=%v", tc.name, got, tc.want)
		}
	}
}

func TestDecodeUIN100(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want float64
		ok   bool
	}{
		{name: "normal", in: []byte{0x10, 0x27}, want: 100.0, ok: true},
		{name: "zero", in: []byte{0x00, 0x00}, want: 0.0, ok: true},
		{name: "short", in: []byte{0x10}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodeUIN100(tc.in)
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v want %v", tc.name, ok, tc.ok)
		}
		if tc.ok && math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("%s: got=%v want=%v", tc.name, got, tc.want)
		}
	}
}

func TestDecodePercent0(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want uint8
		ok   bool
	}{
		{name: "normal", in: []byte{50}, want: 50, ok: true},
		{name: "boundary", in: []byte{100}, want: 100, ok: true},
		{name: "short", in: []byte{}, want: 0, ok: false},
	}
	for _, tc := range tests {
		got, ok := decodePercent0(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s: got=(%v,%v) want=(%v,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestEncodeTempDATA2c(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want []byte
	}{
		{name: "positive", in: 50.0, want: []byte{0x20, 0x03}},
		{name: "negative", in: -1.0, want: []byte{0xF0, 0xFF}},
		{name: "zero", in: 0.0, want: []byte{0x00, 0x00}},
	}
	for _, tc := range tests {
		got := encodeTempDATA2c(tc.in)
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("%s: got=%v want=%v", tc.name, got, tc.want)
		}
	}
}
