package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/growattcanclassify"
)

func TestCX02CLIStdinAndFile(t *testing.T) {
	text := "(1.25) sim0 1802F407#4142434401020304\n"
	path := filepath.Join(t.TempDir(), "invented.txt")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	var previous string
	for _, args := range [][]string{nil, {"-input", path}} {
		var out, errs bytes.Buffer
		if code := run(args, strings.NewReader(text), &out, &errs); code != 0 {
			t.Fatalf("exit=%d %s", code, errs.String())
		}
		var result growattcanclassify.Result
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Outcome != "candidate" || result.RuntimeAdmission || result.PhysicalQualified || result.Frames[0].Identifier != "1802F407" {
			t.Fatal(result)
		}
		if previous != "" && out.String() != previous {
			t.Fatal("file/stdin differ")
		}
		previous = out.String()
	}
}
func TestCX02CLITextAssociationAndErrors(t *testing.T) {
	for _, tc := range []struct {
		args     []string
		input    string
		code     int
		contains string
	}{
		{[]string{"-format", "text", "-mon-association", "sim0"}, "sim0 04000000#0000000000000000", 0, "MON"},
		{[]string{"-format", "xml"}, "", 2, ""}, {[]string{"-bogus"}, "", 2, ""}, {[]string{"extra"}, "", 2, ""},
		{[]string{"-max-bytes", "2"}, "long input", 1, ""}, {[]string{"-max-line", "2"}, "long input", 1, ""},
		{[]string{"-input", "nonexistent-invented.txt"}, "", 1, ""}, {[]string{"-input", "."}, "", 1, ""},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var out, errs bytes.Buffer
			code := run(tc.args, strings.NewReader(tc.input), &out, &errs)
			if code != tc.code || !strings.Contains(out.String(), tc.contains) {
				t.Fatalf("exit=%d out=%s err=%s", code, &out, &errs)
			}
			if code != 0 && errs.Len() == 0 {
				t.Fatal("missing diagnostic")
			}
		})
	}
}
