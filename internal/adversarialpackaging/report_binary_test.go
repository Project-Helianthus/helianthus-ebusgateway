package adversarialpackaging

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompiledReportWriterRunsOutsideCheckoutWithoutPython(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	imports := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, "./internal/adversarial")
	imports.Dir = repo
	out, err := imports.CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v: %s", err, out)
	}
	if strings.Contains(string(out), "os/exec") {
		t.Fatal("runtime package imports os/exec")
	}
	for _, trimpath := range []bool{false, true} {
		name := "normal"
		if trimpath {
			name = "trimpath"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			module := filepath.Join(root, "module")
			run := filepath.Join(root, "run")
			if err := os.MkdirAll(module, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(run, 0o755); err != nil {
				t.Fatal(err)
			}
			goMod := "module github.com/Project-Helianthus/helianthus-ebusgateway/packagingprobe\n\ngo 1.24\n\nrequire github.com/Project-Helianthus/helianthus-ebusgateway v0.0.0\nreplace github.com/Project-Helianthus/helianthus-ebusgateway => " + repo + "\n"
			source := `package main
import("os"; "github.com/Project-Helianthus/helianthus-ebusgateway/internal/adversarial")
func main(){b,e:=os.ReadFile(os.Args[1]);if e!=nil{panic(e)};r,e:=adversarial.ParseReportV1(b);if e!=nil{panic(e)};if e=adversarial.WriteReport(r,os.Args[2]);e!=nil{panic(e)}}
`
			if err := os.WriteFile(filepath.Join(module, "go.mod"), []byte(goMod), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(module, "main.go"), []byte(source), 0o644); err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(root, "writer")
			args := []string{"build", "-o", binary}
			if trimpath {
				args = append(args, "-trimpath")
			}
			args = append(args, ".")
			build := exec.Command("go", args...)
			build.Dir = module
			if out, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build: %v: %s", err, out)
			}
			input, err := os.ReadFile(filepath.Join(repo, "internal", "adversarial", "fixtures", "v1", "positive", "offline-all-pass.json"))
			if err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(run, "input.json")
			outputPath := filepath.Join(run, "output.json")
			if err := os.WriteFile(inputPath, input, 0o644); err != nil {
				t.Fatal(err)
			}
			emptyPath := filepath.Join(root, "empty-path")
			if err := os.Mkdir(emptyPath, 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, inputPath, outputPath)
			cmd.Dir = run
			cmd.Env = append(os.Environ(), "PATH="+emptyPath)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("outside-checkout run: %v: %s", err, out)
			}
			if written, err := os.ReadFile(outputPath); err != nil || len(written) == 0 {
				t.Fatalf("written=%d err=%v", len(written), err)
			}
		})
	}
}
