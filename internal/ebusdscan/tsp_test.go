package ebusdscan

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTSPWithIncludes(t *testing.T) {
	dir := t.TempDir()
	child := filepath.Join(dir, "child.tsp")
	parent := filepath.Join(dir, "parent.tsp")

	if err := os.WriteFile(child, []byte(`@base(B524, 0x24, 0x02, 0x03, 0x04, 0x05)
@ext(0xAA, 0xBB)
@ext(0x10, 0x11, 0x12, 0x13)
`), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.WriteFile(parent, []byte(`@zz(0x15)
@base(B509, 09, 00, 0x02, 0x01, 0x00)
@ext(0x12, 0x34)
@include("child.tsp")
`), 0o644); err != nil {
		t.Fatalf("write parent: %v", err)
	}

	result, err := LoadTSP(parent, Options{})
	if err != nil {
		t.Fatalf("LoadTSP error: %v", err)
	}
	if result.Target != 0x15 {
		t.Fatalf("target = 0x%02x; want 0x15", result.Target)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("entries = %d; want 3", len(result.Entries))
	}

	first := result.Entries[0]
	if first.Method != methodGetRegister || first.Addr != 0x1234 {
		t.Fatalf("first entry = %+v; want get_register addr=0x1234", first)
	}

	second := result.Entries[1]
	if second.Method != methodGetExtRegister || second.Addr != 0xAABB || second.Group != 0x03 || second.Instance != 0x04 || second.Opcode != 0x02 {
		t.Fatalf("second entry = %+v; want ext addr=0xAABB group=0x03 instance=0x04 opcode=0x02", second)
	}

	third := result.Entries[2]
	if third.Method != methodGetExtRegister || third.Addr != 0x1213 || third.Group != 0x10 || third.Instance != 0x11 || third.Opcode != 0x02 {
		t.Fatalf("third entry = %+v; want ext addr=0x1213 group=0x10 instance=0x11 opcode=0x02", third)
	}
}

func TestBuildHex(t *testing.T) {
	entry := Entry{Method: methodGetRegister, Addr: 0xF600}
	data, err := BuildHex(0x15, entry)
	if err != nil {
		t.Fatalf("BuildHex error: %v", err)
	}
	want := "15B509030DF600"
	if got := HexString(data); got != want {
		t.Fatalf("hex = %s; want %s", got, want)
	}
}
