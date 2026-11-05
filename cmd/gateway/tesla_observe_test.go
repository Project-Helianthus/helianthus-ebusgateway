package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/teslaobserve"
)

const teslaObserveTestPath = "/dev/synthetic-observe-port"

func TestTeslaObservePublicationSyncsParentDirectory(t *testing.T) {
	for _, replace := range []bool{false, true} {
		for _, failSync := range []bool{false, true} {
			t.Run(fmt.Sprintf("replace=%t/failSync=%t", replace, failSync), func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "evidence.json")
				if replace {
					if err := os.WriteFile(path, []byte("prior evidence"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				original := teslaObserveSyncDirectory
				t.Cleanup(func() { teslaObserveSyncDirectory = original })
				syncFailure := errors.New("synthetic directory sync failure")
				calls := 0
				data := []byte("complete evidence")
				teslaObserveSyncDirectory = func(parent string) error {
					calls++
					if parent != dir {
						t.Fatalf("sync directory %q, want %q", parent, dir)
					}
					got, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(got, data) {
						t.Fatalf("sync before publication: data=%q error=%v", got, err)
					}
					if failSync {
						return syncFailure
					}
					return original(parent)
				}
				err := writeTeslaObserveFile(path, data, replace)
				if calls != 1 {
					t.Fatalf("directory sync calls = %d, want 1", calls)
				}
				if (failSync && !errors.Is(err, syncFailure)) || (!failSync && err != nil) {
					t.Fatalf("publication error = %v, failSync=%t", err, failSync)
				}
				got, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(got, data) {
					t.Fatalf("published evidence lost after sync: data=%q error=%v", got, readErr)
				}
			})
		}
	}
}

func TestTeslaObserveDirectorySyncFailureFailsRun(t *testing.T) {
	original := teslaObserveSyncDirectory
	t.Cleanup(func() { teslaObserveSyncDirectory = original })
	teslaObserveSyncDirectory = func(string) error { return syscall.EIO }
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(t.TempDir()), ports.build()); code != teslaObserveExitRun {
		t.Fatalf("directory sync failure exit = %d: %s", code, ports.stderr.String())
	}
}

// teslaObserveTestSource delivers fixed bytes, then blocks until ctx ends.
type teslaObserveTestSource struct {
	mu       sync.Mutex
	values   []byte
	onClose  func()
	closeErr error
	readErr  error
}

func (source *teslaObserveTestSource) ReceiveByte(ctx context.Context) (byte, error) {
	source.mu.Lock()
	if len(source.values) == 0 && source.readErr != nil {
		err := source.readErr
		source.mu.Unlock()
		return 0, err
	}
	if len(source.values) == 0 {
		source.mu.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}
	value := source.values[0]
	source.values = source.values[1:]
	source.mu.Unlock()
	return value, nil
}

func (source *teslaObserveTestSource) Close() error {
	if source.onClose != nil {
		source.onClose()
	}
	return source.closeErr
}

type teslaObserveTestPorts struct {
	opened []string
	closed int
	stderr bytes.Buffer
	// openErr and closeErr make the next sources fail to open or to close.
	openErr  error
	closeErr error
}

func (ports *teslaObserveTestPorts) build() teslaObservePorts {
	return teslaObservePorts{
		OpenSerial: func(path string) (teslaobserve.ByteSource, error) {
			ports.opened = append(ports.opened, path)
			if ports.openErr != nil {
				return nil, ports.openErr
			}
			return &teslaObserveTestSource{
				values: []byte{0x01, 0x02, 0x03}, onClose: func() { ports.closed++ }, closeErr: ports.closeErr,
			}, nil
		},
		Now:     time.Now,
		Stderr:  &ports.stderr,
		Context: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
	}
}

func teslaObserveArgs(dir string, extra ...string) []string {
	return append([]string{
		"-serial-path", teslaObserveTestPath, "-case", "initial", "-window", "200ms", "-window-bytes", "3",
		"-out", filepath.Join(dir, "evidence.json"), "-worksheet-out", filepath.Join(dir, "worksheet.md"),
	}, extra...)
}

func TestTeslaObserveDispatchSelectsOnlyItsSubcommand(t *testing.T) {
	ports := &teslaObserveTestPorts{}
	for _, argv := range [][]string{{"gateway"}, {"gateway", "-scan"}, {"gateway", "tesla-observe-x"}, {"gateway", "-tesla-observe"}} {
		if _, selected := dispatchTeslaObserve(argv, ports.build); selected {
			t.Fatalf("%v selected the observation run", argv)
		}
	}
	code, selected := dispatchTeslaObserve([]string{"gateway", teslaObserveCommand}, ports.build)
	if !selected || code != teslaObserveExitRefused || len(ports.opened) != 0 {
		t.Fatalf("bare subcommand code=%d selected=%t opened=%v", code, selected, ports.opened)
	}
}

func TestTeslaObserveWritesOwnerOnlyEvidenceWithoutTheLocalPath(t *testing.T) {
	dir := t.TempDir()
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir, "-repeats", "2", "-prior-state", "fresh_session"), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("exit %d: %s", code, ports.stderr.String())
	}
	if len(ports.opened) != 2 || ports.opened[0] != teslaObserveTestPath {
		t.Fatalf("opened %v", ports.opened)
	}
	for _, name := range []string{"evidence.json", "worksheet.md"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode %v", name, info.Mode().Perm())
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("synthetic-observe-port")) || bytes.Contains(data, []byte("/dev/")) {
			t.Fatalf("%s exposes the local path", name)
		}
	}
	data, _ := os.ReadFile(filepath.Join(dir, "evidence.json"))
	evidence, err := teslaobserve.DecodeEvidence(data)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Mode != teslaobserve.ModeNoSend || evidence.FramesSent != 0 || len(evidence.Attempts) != 2 ||
		evidence.Endpoint != "tesla-wc3-rs485" || evidence.Attempts[0].Observation.Termination != teslaobserve.TerminationCaptured {
		t.Fatalf("evidence = %+v", evidence)
	}
	if replay := evidence.Attempts[0].Lifecycle; replay == nil || replay.Ready || replay.FramesSent != 0 {
		t.Fatalf("lifecycle replay = %+v", replay)
	}
}

func TestTeslaObserveRefusesOverwriteAndAppendsOnRequest(t *testing.T) {
	dir := t.TempDir()
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("first run exit %d: %s", code, ports.stderr.String())
	}
	before, _ := os.ReadFile(filepath.Join(dir, "evidence.json"))
	opened := len(ports.opened)
	if code := runTeslaObserve(teslaObserveArgs(dir), ports.build()); code != teslaObserveExitRefused || len(ports.opened) != opened {
		t.Fatalf("overwrite exit %d opened %d", code, len(ports.opened))
	}
	after, _ := os.ReadFile(filepath.Join(dir, "evidence.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("refused run changed the evidence file")
	}
	if code := runTeslaObserve(teslaObserveArgs(dir, "-append", "-case", "idle"), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("append exit %d: %s", code, ports.stderr.String())
	}
	data, _ := os.ReadFile(filepath.Join(dir, "evidence.json"))
	evidence, err := teslaobserve.DecodeEvidence(data)
	if err != nil || len(evidence.Attempts) != 2 || evidence.Attempts[1].Label != "a02" || evidence.Attempts[1].Case != "idle" {
		t.Fatalf("appended evidence err=%v attempts=%+v", err, evidence.Attempts)
	}
	info, _ := os.Stat(filepath.Join(dir, "evidence.json"))
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("appended mode %v", info.Mode().Perm())
	}
}

func TestTeslaObserveRefusesToReplaceAnExistingWorksheetWithoutAppend(t *testing.T) {
	dir := t.TempDir()
	sheet := filepath.Join(dir, "worksheet.md")
	if err := os.WriteFile(sheet, []byte("prior evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir), ports.build()); code != teslaObserveExitRefused || len(ports.opened) != 0 {
		t.Fatalf("exit %d opened %v", code, ports.opened)
	}
	data, _ := os.ReadFile(sheet)
	if string(data) != "prior evidence" {
		t.Fatalf("worksheet changed: %q", data)
	}
	if _, err := os.Stat(filepath.Join(dir, "evidence.json")); err == nil {
		t.Fatal("refused run wrote evidence")
	}
}

func TestTeslaObserveRefusesBeforeOpeningThePort(t *testing.T) {
	dir := t.TempDir()
	cases := map[string][]string{
		"no serial path":  {"-case", "initial", "-out", filepath.Join(dir, "a.json")},
		"no out":          {"-serial-path", teslaObserveTestPath, "-case", "initial"},
		"no case":         {"-serial-path", teslaObserveTestPath, "-out", filepath.Join(dir, "b.json")},
		"path label":      teslaObserveArgs(dir, "-endpoint-id", "/dev/ttyUSB0"),
		"append missing":  teslaObserveArgs(dir, "-append"),
		"same outputs":    {"-serial-path", teslaObserveTestPath, "-case", "initial", "-out", filepath.Join(dir, "c.json"), "-worksheet-out", filepath.Join(dir, "c.json")},
		"positional":      append(teslaObserveArgs(dir), "extra"),
		"unknown flag":    teslaObserveArgs(dir, "-transmit"),
		"window too long": teslaObserveArgs(dir, "-window", "61s"),
		"bad prior state": teslaObserveArgs(dir, "-prior-state", "guess"),
	}
	for name, args := range cases {
		ports := &teslaObserveTestPorts{}
		if code := runTeslaObserve(args, ports.build()); code != teslaObserveExitRefused || len(ports.opened) != 0 {
			t.Fatalf("%s: exit %d opened %v", name, code, ports.opened)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("refusals wrote files: %v", entries)
	}
}

func TestTeslaObserveSerialErrorsDropTheLocalPath(t *testing.T) {
	err := redactTeslaSerialError(&fs.PathError{Op: "read", Path: teslaObserveTestPath, Err: syscall.EIO})
	if strings.Contains(err.Error(), "synthetic-observe-port") || !errors.Is(err, syscall.EIO) {
		t.Fatalf("redacted error = %v", err)
	}
	if !errors.Is(redactTeslaSerialError(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("context error is no longer matchable")
	}
	if redactTeslaSerialError(nil) != nil {
		t.Fatal("nil error changed")
	}
}

// The gateway flag set keeps no Tesla serial, activation or send flag; the
// observation run has its own flag set behind its subcommand.
func TestTeslaObserveAddsNoGatewayFlags(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	flags := flag.NewFlagSet("gateway", flag.ContinueOnError)
	bindFlags(flags, &cfg)
	flags.VisitAll(func(f *flag.Flag) {
		name := strings.ToLower(f.Name)
		if strings.Contains(name, "tesla") && (strings.Contains(name, "serial") || strings.Contains(name, "activation") ||
			strings.Contains(name, "observe") || strings.Contains(name, "write")) {
			t.Fatalf("gateway flag %s", f.Name)
		}
	})
}

func TestTeslaObserveRemovesAPartialNewEvidenceFileAfterWriteFailure(t *testing.T) {
	original := teslaObserveWriteData
	defer func() { teslaObserveWriteData = original }()
	teslaObserveWriteData = func(file *os.File, data []byte) error {
		_, _ = file.Write(data[:len(data)/2])
		return errors.New("synthetic disk full")
	}
	path := filepath.Join(t.TempDir(), "evidence.json")
	if err := writeTeslaObserveFile(path, []byte("{\"complete\":true}\n"), false); err == nil {
		t.Fatal("write failure was not reported")
	}
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("partial evidence file left behind: %v", err)
	}
}

func TestTeslaObserveRefusesOutputAliasesBeforeOpening(t *testing.T) {
	for _, alias := range []string{"relative", "symlink", "hardlink", "directory symlink"} {
		t.Run(alias, func(t *testing.T) {
			dir := t.TempDir()
			ports := &teslaObserveTestPorts{}
			if code := runTeslaObserve(teslaObserveArgs(dir), ports.build()); code != teslaObserveExitOK {
				t.Fatalf("seed exit %d: %s", code, ports.stderr.String())
			}
			out := filepath.Join(dir, "evidence.json")
			before, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			sheet := filepath.Join(dir, "alias")
			switch alias {
			case "relative":
				cwd, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				sheet, err = filepath.Rel(cwd, out)
				if err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(out, sheet); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(out, sheet); err != nil {
					t.Fatal(err)
				}
			case "directory symlink":
				if err := os.Symlink(dir, sheet); err != nil {
					t.Fatal(err)
				}
				sheet = filepath.Join(sheet, "evidence.json")
			}
			opened := len(ports.opened)
			args := teslaObserveArgs(dir, "-append", "-worksheet-out", sheet)
			if code := runTeslaObserve(args, ports.build()); code != teslaObserveExitRefused || len(ports.opened) != opened {
				t.Fatalf("alias exit %d opened %v: %s", code, ports.opened, ports.stderr.String())
			}
			after, err := os.ReadFile(out)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("alias changed evidence: %v", err)
			}
		})
	}
}

// teslaObserveDirEntries lists a directory so a test can show it unchanged.
func teslaObserveDirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	return names
}

func TestTeslaObservePrepareOpensAndClosesWithoutRecordingAnything(t *testing.T) {
	original := teslaObserveWriteData
	defer func() { teslaObserveWriteData = original }()
	teslaObserveWriteData = func(*os.File, []byte) error {
		t.Error("prepare wrote a file")
		return nil
	}
	before := teslaObserveDirEntries(t, ".")
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve([]string{"-prepare", "-serial-path", teslaObserveTestPath}, ports.build()); code != teslaObserveExitOK {
		t.Fatalf("exit %d: %s", code, ports.stderr.String())
	}
	if len(ports.opened) != 1 || ports.opened[0] != teslaObserveTestPath || ports.closed != 1 {
		t.Fatalf("opened %v closed %d", ports.opened, ports.closed)
	}
	if after := teslaObserveDirEntries(t, "."); strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("prepare changed the working directory: %v -> %v", before, after)
	}
	message := ports.stderr.String()
	if !strings.Contains(message, "nothing observed or recorded") || strings.Contains(message, "synthetic-observe-port") {
		t.Fatalf("prepare message = %q", message)
	}
	// An explicit -prepare=false is the ordinary observation run, which does
	// write its evidence.
	teslaObserveWriteData = original
	dir := t.TempDir()
	ports = &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir, "-prepare=false"), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("-prepare=false exit %d: %s", code, ports.stderr.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "evidence.json")); err != nil {
		t.Fatalf("-prepare=false wrote no evidence: %v", err)
	}
}

// -prepare accepts only -serial-path. Every flag that belongs to an
// observation or its evidence output is refused before the port opens.
func TestTeslaObservePrepareRefusesInvalidCombinationsBeforeOpening(t *testing.T) {
	dir := t.TempDir()
	cases := map[string][]string{
		"no serial path":     {"-prepare"},
		"empty serial path":  {"-prepare", "-serial-path", ""},
		"positional":         {"-prepare", "-serial-path", teslaObserveTestPath, "extra"},
		"unknown flag":       {"-prepare", "-serial-path", teslaObserveTestPath, "-transmit"},
		"out":                {"-prepare", "-serial-path", teslaObserveTestPath, "-out", filepath.Join(dir, "a.json")},
		"append":             {"-prepare", "-serial-path", teslaObserveTestPath, "-append"},
		"worksheet out":      {"-prepare", "-serial-path", teslaObserveTestPath, "-worksheet-out", filepath.Join(dir, "a.md")},
		"case":               {"-prepare", "-serial-path", teslaObserveTestPath, "-case", "initial"},
		"repeats":            {"-prepare", "-serial-path", teslaObserveTestPath, "-repeats", "2"},
		"window":             {"-prepare", "-serial-path", teslaObserveTestPath, "-window", "5s"},
		"window bytes":       {"-prepare", "-serial-path", teslaObserveTestPath, "-window-bytes", "3"},
		"segment cap":        {"-prepare", "-serial-path", teslaObserveTestPath, "-segment-cap", "3"},
		"segment gap":        {"-prepare", "-serial-path", teslaObserveTestPath, "-segment-gap", "1ms"},
		"pause":              {"-prepare", "-serial-path", teslaObserveTestPath, "-pause", "1s"},
		"endpoint id":        {"-prepare", "-serial-path", teslaObserveTestPath, "-endpoint-id", "synthetic-e"},
		"prior state":        {"-prepare", "-serial-path", teslaObserveTestPath, "-prior-state", "fresh_session"},
		"other initiators":   {"-prepare", "-serial-path", teslaObserveTestPath, "-other-initiators", "excluded"},
		"condition":          {"-prepare", "-serial-path", teslaObserveTestPath, "-condition", "synthetic"},
		"authorization":      {"-prepare", "-serial-path", teslaObserveTestPath, "-action-time-authorization", "recorded"},
		"observation setup":  teslaObserveArgs(dir, "-prepare"),
		"flag despite false": {"-prepare", "-serial-path", teslaObserveTestPath, "-append=false"},
	}
	for name, args := range cases {
		ports := &teslaObserveTestPorts{}
		if code := runTeslaObserve(args, ports.build()); code != teslaObserveExitRefused || len(ports.opened) != 0 || ports.closed != 0 {
			t.Fatalf("%s: exit %d opened %v closed %d: %s", name, code, ports.opened, ports.closed, ports.stderr.String())
		}
	}
	if entries := teslaObserveDirEntries(t, dir); len(entries) != 0 {
		t.Fatalf("refusals wrote files: %v", entries)
	}
	ports := &teslaObserveTestPorts{}
	runTeslaObserve([]string{"-prepare", "-serial-path", teslaObserveTestPath, "-out", filepath.Join(dir, "a.json")}, ports.build())
	if !strings.Contains(ports.stderr.String(), "-prepare accepts only -serial-path; remove -out") {
		t.Fatalf("refusal message = %q", ports.stderr.String())
	}
}

func TestTeslaObservePrepareReportsOpenAndCloseFailuresWithoutThePath(t *testing.T) {
	failures := map[string]*teslaObserveTestPorts{
		"open":      {openErr: &fs.PathError{Op: "open", Path: teslaObserveTestPath, Err: syscall.EIO}},
		"close":     {closeErr: &fs.PathError{Op: "close", Path: teslaObserveTestPath, Err: syscall.EIO}},
		"no source": nil,
	}
	for name, ports := range failures {
		var build func() teslaObservePorts
		var stderr *bytes.Buffer
		if ports == nil {
			stderr = &bytes.Buffer{}
			build = func() teslaObservePorts {
				return teslaObservePorts{
					OpenSerial: func(string) (teslaobserve.ByteSource, error) { return nil, nil },
					Now:        time.Now, Stderr: stderr,
					Context: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
				}
			}
		} else {
			stderr = &ports.stderr
			build = ports.build
		}
		if code := runTeslaObserve([]string{"-prepare", "-serial-path", teslaObserveTestPath}, build()); code != teslaObserveExitRun {
			t.Fatalf("%s: exit %d: %s", name, code, stderr.String())
		}
		message := stderr.String()
		if !strings.Contains(message, "prepare failed") || strings.Contains(message, "synthetic-observe-port") || strings.Contains(message, "/dev/") {
			t.Fatalf("%s: message = %q", name, message)
		}
	}
}

// An evidence file whose segment direction was edited after the run is
// refused on append before the port opens, and stays byte-identical.
func TestTeslaObserveAppendRefusesAnEditedDirectionBeforeOpening(t *testing.T) {
	dir := t.TempDir()
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("seed exit %d: %s", code, ports.stderr.String())
	}
	out := filepath.Join(dir, "evidence.json")
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	edited := bytes.Replace(data, []byte(`"direction": "receive"`), []byte(`"direction": "transmit"`), 1)
	if bytes.Equal(edited, data) {
		t.Fatal("seed evidence has no receive segment to edit")
	}
	if err := os.WriteFile(out, edited, 0o600); err != nil {
		t.Fatal(err)
	}
	opened := len(ports.opened)
	if code := runTeslaObserve(teslaObserveArgs(dir, "-append"), ports.build()); code != teslaObserveExitRefused || len(ports.opened) != opened {
		t.Fatalf("edited prior exit %d opened %v: %s", code, ports.opened, ports.stderr.String())
	}
	if !strings.Contains(ports.stderr.String(), "inconsistent observation metadata") {
		t.Fatalf("refusal message = %q", ports.stderr.String())
	}
	after, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(after, edited) {
		t.Fatalf("refused append changed the evidence file: %v", err)
	}
}

func TestTeslaObserveFailedAttemptsPersistWithRunExit(t *testing.T) {
	for _, kind := range []string{"open", "read", "mixed"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			ports := (&teslaObserveTestPorts{}).build()
			opens := 0
			ports.OpenSerial = func(string) (teslaobserve.ByteSource, error) {
				opens++
				if kind == "open" || (kind == "mixed" && opens == 1) {
					return nil, syscall.EIO
				}
				return &teslaObserveTestSource{values: []byte{0x54}, readErr: syscall.EIO}, nil
			}
			if code := runTeslaObserve(teslaObserveArgs(dir, "-repeats", "2"), ports); code != teslaObserveExitRun {
				t.Fatalf("exit %d, want run failure", code)
			}
			data, err := os.ReadFile(filepath.Join(dir, "evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := teslaobserve.DecodeEvidence(data)
			if err != nil || len(evidence.Attempts) != 2 || evidence.FramesSent != 0 {
				t.Fatalf("evidence=%+v err=%v", evidence, err)
			}
			for _, attempt := range evidence.Attempts {
				if attempt.OpenError != "" {
					continue
				}
				if attempt.Observation == nil || attempt.Observation.Termination != teslaobserve.TerminationReadError || !bytes.Equal(attempt.Observation.Window(), []byte{0x54}) {
					t.Fatalf("lost failed read evidence: %+v", attempt)
				}
			}
			if _, err := os.Stat(filepath.Join(dir, "worksheet.md")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTeslaObserveConcurrentAppendRefusesBeforeOpening(t *testing.T) {
	dir := t.TempDir()
	seed := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir), seed.build()); code != teslaObserveExitOK {
		t.Fatalf("seed exit %d", code)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	first := (&teslaObserveTestPorts{}).build()
	open := first.OpenSerial
	first.OpenSerial = func(path string) (teslaobserve.ByteSource, error) {
		close(entered)
		<-release
		return open(path)
	}
	go func() { done <- runTeslaObserve(teslaObserveArgs(dir, "-append"), first) }()
	<-entered
	second := &teslaObserveTestPorts{}
	code := runTeslaObserve(teslaObserveArgs(dir, "-append"), second.build())
	close(release)
	firstCode := <-done
	if code != teslaObserveExitRefused || len(second.opened) != 0 {
		t.Fatalf("concurrent exit %d opens %v", code, second.opened)
	}
	if firstCode != teslaObserveExitOK {
		t.Fatalf("first exit %d", firstCode)
	}
	if code := runTeslaObserve(teslaObserveArgs(dir, "-append"), second.build()); code != teslaObserveExitOK {
		t.Fatalf("retry exit %d", code)
	}
	data, err := os.ReadFile(filepath.Join(dir, "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := teslaobserve.DecodeEvidence(data)
	if err != nil || len(evidence.Attempts) != 3 {
		t.Fatalf("attempts=%+v err=%v", evidence.Attempts, err)
	}
}

func TestTeslaObserveAppendPreservesSymlinkTargets(t *testing.T) {
	dir := t.TempDir()
	ports := &teslaObserveTestPorts{}
	if code := runTeslaObserve(teslaObserveArgs(dir), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("seed exit %d", code)
	}
	out, sheet := filepath.Join(dir, "evidence.json"), filepath.Join(dir, "worksheet.md")
	outLink, sheetLink := filepath.Join(dir, "evidence-link"), filepath.Join(dir, "worksheet-link")
	for link, target := range map[string]string{outLink: out, sheetLink: sheet} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	if code := runTeslaObserve(teslaObserveArgs(dir, "-append", "-out", outLink, "-worksheet-out", sheetLink), ports.build()); code != teslaObserveExitOK {
		t.Fatalf("append exit %d", code)
	}
	for _, link := range []string{outLink, sheetLink} {
		info, err := os.Lstat(link)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("link replaced: %s %v", link, err)
		}
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := teslaobserve.DecodeEvidence(data)
	if err != nil || len(evidence.Attempts) != 2 {
		t.Fatalf("canonical attempts=%+v err=%v", evidence.Attempts, err)
	}
	markdown, err := os.ReadFile(sheet)
	if err != nil || string(markdown) != teslaobserve.RenderMarkdown(evidence) {
		t.Fatalf("canonical worksheet stale: %v", err)
	}
}

func TestTeslaObserveAppendRejectsHardLinkedTargetsBeforeOpening(t *testing.T) {
	for _, output := range []string{"evidence.json", "worksheet.md"} {
		t.Run(output, func(t *testing.T) {
			dir := t.TempDir()
			seed := &teslaObserveTestPorts{}
			if code := runTeslaObserve(teslaObserveArgs(dir), seed.build()); code != teslaObserveExitOK {
				t.Fatalf("seed exit %d", code)
			}
			target, alias := filepath.Join(dir, output), filepath.Join(dir, "alias")
			before, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Link(target, alias); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{target, alias} {
				flag := "-out"
				if output == "worksheet.md" {
					flag = "-worksheet-out"
				}
				ports := &teslaObserveTestPorts{}
				if code := runTeslaObserve(teslaObserveArgs(dir, "-append", flag, path), ports.build()); code != teslaObserveExitRefused || len(ports.opened) != 0 {
					t.Fatalf("exit %d opened %d: %s", code, len(ports.opened), ports.stderr.String())
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("refused append changed file: %v", err)
				}
			}
			first, _ := os.Stat(target)
			second, _ := os.Stat(alias)
			if !os.SameFile(first, second) {
				t.Fatal("refused append split hard links")
			}
		})
	}
}

func TestTeslaObserveWorksheetFailurePreservesOutputPair(t *testing.T) {
	for _, replace := range []bool{false, true} {
		for _, failure := range []string{"create", "write", "sync", "publish"} {
			t.Run(fmt.Sprintf("append=%t/%s", replace, failure), func(t *testing.T) {
				dir := t.TempDir()
				out, worksheet := filepath.Join(dir, "evidence.json"), filepath.Join(dir, "worksheet.md")
				var prior, priorWorksheet []byte
				if replace {
					if code := runTeslaObserve(teslaObserveArgs(dir), (&teslaObserveTestPorts{}).build()); code != teslaObserveExitOK {
						t.Fatalf("seed exit %d", code)
					}
					var err error
					prior, err = os.ReadFile(out)
					if err != nil {
						t.Fatal(err)
					}
					priorWorksheet, err = os.ReadFile(worksheet)
					if err != nil {
						t.Fatal(err)
					}
				}
				write, publish := teslaObserveWriteData, teslaObservePublishFile
				t.Cleanup(func() { teslaObserveWriteData, teslaObservePublishFile = write, publish })
				calls := 0
				teslaObserveWriteData = func(file *os.File, data []byte) error {
					calls++
					if calls == 2 && (failure == "write" || failure == "sync") {
						if failure == "sync" {
							if _, err := file.Write(data); err != nil {
								return err
							}
						}
						return syscall.EIO
					}
					return write(file, data)
				}
				teslaObservePublishFile = func(staged, path string, replace bool) error {
					if filepath.Base(path) == "worksheet.md" && failure == "publish" {
						return syscall.EIO
					}
					return publish(staged, path, replace)
				}
				extra := []string{}
				if replace {
					extra = append(extra, "-append")
				}
				if failure == "create" {
					extra = append(extra, "-worksheet-out", filepath.Join(dir, "missing", "worksheet.md"))
				}
				ports := &teslaObserveTestPorts{}
				wantCode, wantOpens := teslaObserveExitRun, 1
				if failure == "create" {
					// The worksheet's missing directory now refuses its output
					// lock before collection, preserving both outputs earlier.
					wantCode, wantOpens = teslaObserveExitRefused, 0
				}
				if code := runTeslaObserve(teslaObserveArgs(dir, extra...), ports.build()); code != wantCode || len(ports.opened) != wantOpens {
					t.Fatalf("exit %d opened %d: %s", code, len(ports.opened), ports.stderr.String())
				}
				for path, want := range map[string][]byte{out: prior, worksheet: priorWorksheet} {
					got, err := os.ReadFile(path)
					if replace {
						if err != nil || !bytes.Equal(got, want) {
							t.Fatalf("output changed after worksheet failure: %v", err)
						}
					} else if !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("new output published after worksheet failure: %v", err)
					}
				}
				staged, err := filepath.Glob(filepath.Join(dir, ".tesla-observe-*"))
				if err != nil || len(staged) != 0 {
					t.Fatalf("staging leftovers %v: %v", staged, err)
				}
				teslaObserveWriteData, teslaObservePublishFile = write, publish
				if failure == "create" {
					extra = extra[:len(extra)-2]
				}
				if code := runTeslaObserve(teslaObserveArgs(dir, extra...), (&teslaObserveTestPorts{}).build()); code != teslaObserveExitOK {
					t.Fatalf("retry exit %d", code)
				}
				data, err := os.ReadFile(out)
				if err != nil {
					t.Fatal(err)
				}
				evidence, err := teslaobserve.DecodeEvidence(data)
				if err != nil {
					t.Fatal(err)
				}
				markdown, err := os.ReadFile(worksheet)
				if err != nil || string(markdown) != teslaobserve.RenderMarkdown(evidence) {
					t.Fatalf("retry left inconsistent worksheet: %v", err)
				}
			})
		}
	}
}

func TestTeslaObserveConcurrentAppendWithSharedWorksheetRefusesBeforeOpening(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(fmt.Sprintf("worksheetSymlink=%t", alias), func(t *testing.T) {
			dir := t.TempDir()
			firstOut, secondOut := filepath.Join(dir, "a.json"), filepath.Join(dir, "b.json")
			worksheet := filepath.Join(dir, "z-worksheet.md")
			firstArgs := teslaObserveArgs(dir, "-out", firstOut, "-worksheet-out", worksheet)
			if code := runTeslaObserve(firstArgs, (&teslaObserveTestPorts{}).build()); code != teslaObserveExitOK {
				t.Fatalf("seed exit %d", code)
			}
			prior, err := os.ReadFile(firstOut)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(secondOut, prior, 0o600); err != nil {
				t.Fatal(err)
			}
			secondWorksheet := worksheet
			if alias {
				secondWorksheet = filepath.Join(dir, "worksheet-alias")
				if err := os.Symlink(worksheet, secondWorksheet); err != nil {
					t.Fatal(err)
				}
			}
			secondArgs := teslaObserveArgs(dir, "-append", "-out", secondOut, "-worksheet-out", secondWorksheet)
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan int, 1)
			first := (&teslaObserveTestPorts{}).build()
			open := first.OpenSerial
			first.OpenSerial = func(path string) (teslaobserve.ByteSource, error) {
				close(entered)
				<-release
				return open(path)
			}
			go func() { done <- runTeslaObserve(append(firstArgs, "-append"), first) }()
			<-entered
			second := &teslaObserveTestPorts{}
			code := runTeslaObserve(secondArgs, second.build())
			close(release)
			firstCode := <-done
			if code != teslaObserveExitRefused || len(second.opened) != 0 {
				t.Fatalf("shared worksheet exit %d opened %d", code, len(second.opened))
			}
			if firstCode != teslaObserveExitOK {
				t.Fatalf("first exit %d", firstCode)
			}
			secondData, err := os.ReadFile(secondOut)
			if err != nil || !bytes.Equal(secondData, prior) {
				t.Fatalf("refused append changed second evidence: %v", err)
			}
			firstData, err := os.ReadFile(firstOut)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := teslaobserve.DecodeEvidence(firstData)
			if err != nil || len(evidence.Attempts) != 2 {
				t.Fatalf("first evidence attempts=%d error=%v", len(evidence.Attempts), err)
			}
			markdown, err := os.ReadFile(worksheet)
			if err != nil || string(markdown) != teslaobserve.RenderMarkdown(evidence) {
				t.Fatalf("shared worksheet does not match successful append: %v", err)
			}
			for _, path := range []string{firstOut, secondOut, worksheet} {
				if _, err := os.Stat(path + ".lock"); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("lock retained after completion or refusal: %v", err)
				}
			}
			if code := runTeslaObserve(secondArgs, second.build()); code != teslaObserveExitOK {
				t.Fatalf("retry after release exit %d", code)
			}
		})
	}
}

func TestTeslaObserveFirstRunAndAppendShareWorksheetLocks(t *testing.T) {
	for _, firstAppend := range []bool{false, true} {
		t.Run(fmt.Sprintf("firstAppend=%t", firstAppend), func(t *testing.T) {
			dir := t.TempDir()
			firstOut, secondOut := filepath.Join(dir, "a.json"), filepath.Join(dir, "b.json")
			worksheet := filepath.Join(dir, "z-worksheet.md")
			seedOut := secondOut
			if firstAppend {
				seedOut = firstOut
			}
			seedArgs := teslaObserveArgs(dir, "-out", seedOut, "-worksheet-out", "")
			if code := runTeslaObserve(seedArgs, (&teslaObserveTestPorts{}).build()); code != teslaObserveExitOK {
				t.Fatalf("seed exit %d", code)
			}
			prior, err := os.ReadFile(seedOut)
			if err != nil {
				t.Fatal(err)
			}
			firstArgs := teslaObserveArgs(dir, "-out", firstOut, "-worksheet-out", worksheet)
			secondArgs := teslaObserveArgs(dir, "-out", secondOut, "-worksheet-out", worksheet)
			if firstAppend {
				firstArgs = append(firstArgs, "-append")
			} else {
				secondArgs = append(secondArgs, "-append")
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan int, 1)
			first := (&teslaObserveTestPorts{}).build()
			open := first.OpenSerial
			first.OpenSerial = func(path string) (teslaobserve.ByteSource, error) {
				close(entered)
				<-release
				return open(path)
			}
			go func() { done <- runTeslaObserve(firstArgs, first) }()
			<-entered
			second := &teslaObserveTestPorts{}
			code := runTeslaObserve(secondArgs, second.build())
			close(release)
			firstCode := <-done
			if code != teslaObserveExitRefused || len(second.opened) != 0 || firstCode != teslaObserveExitOK {
				t.Fatalf("mixed runs first=%d second=%d second opens=%d", firstCode, code, len(second.opened))
			}
			secondData, err := os.ReadFile(secondOut)
			if firstAppend {
				if !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("refused first run published evidence: %v", err)
				}
			} else if err != nil || !bytes.Equal(secondData, prior) {
				t.Fatalf("refused append changed evidence: %v", err)
			}
			firstData, err := os.ReadFile(firstOut)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := teslaobserve.DecodeEvidence(firstData)
			if err != nil {
				t.Fatal(err)
			}
			markdown, err := os.ReadFile(worksheet)
			if err != nil || string(markdown) != teslaobserve.RenderMarkdown(evidence) {
				t.Fatalf("worksheet does not match successful mixed-mode run: %v", err)
			}
			for _, path := range []string{firstOut, secondOut, worksheet} {
				if _, err := os.Stat(path + ".lock"); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("mixed-mode lock retained: %v", err)
				}
			}
		})
	}
}

func TestTeslaObserveRefusesDanglingOutputSymlinksBeforeOpening(t *testing.T) {
	for _, appendTo := range []bool{false, true} {
		for _, flag := range []string{"-out", "-worksheet-out"} {
			for _, parentLink := range []bool{false, true} {
				t.Run(fmt.Sprintf("append=%t/%s/parent=%t", appendTo, flag, parentLink), func(t *testing.T) {
					dir := t.TempDir()
					before := map[string][]byte{}
					if appendTo {
						if code := runTeslaObserve(teslaObserveArgs(dir), (&teslaObserveTestPorts{}).build()); code != teslaObserveExitOK {
							t.Fatalf("seed exit %d", code)
						}
						for _, name := range []string{"evidence.json", "worksheet.md"} {
							data, err := os.ReadFile(filepath.Join(dir, name))
							if err != nil {
								t.Fatal(err)
							}
							before[name] = data
						}
					}
					link, target := filepath.Join(dir, "dangling"), filepath.Join(dir, "missing")
					if err := os.Symlink(target, link); err != nil {
						t.Fatal(err)
					}
					output := link
					if parentLink {
						output = filepath.Join(link, "output")
					}
					extra := []string{flag, output}
					if appendTo {
						extra = append(extra, "-append")
					}
					ports := &teslaObserveTestPorts{}
					if code := runTeslaObserve(teslaObserveArgs(dir, extra...), ports.build()); code != teslaObserveExitRefused || len(ports.opened) != 0 {
						t.Fatalf("dangling output exit %d opened %d: %s", code, len(ports.opened), ports.stderr.String())
					}
					if !strings.Contains(ports.stderr.String(), "cannot resolve") {
						t.Fatalf("not refused during path resolution: %s", ports.stderr.String())
					}
					got, err := os.Readlink(link)
					if err != nil || got != target {
						t.Fatalf("refusal changed dangling symlink: %v", err)
					}
					for _, name := range []string{"evidence.json", "worksheet.md"} {
						data, err := os.ReadFile(filepath.Join(dir, name))
						if appendTo {
							if err != nil || !bytes.Equal(data, before[name]) {
								t.Fatalf("refusal changed prior output: %v", err)
							}
						} else if !errors.Is(err, fs.ErrNotExist) {
							t.Fatalf("refusal published output: %v", err)
						}
					}
					if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("refusal created dangling target: %v", err)
					}
				})
			}
		}
	}
}

func TestTeslaObserveSingleFileInterruptedStaging(t *testing.T) {
	const childPath = "HELIANTHUS_TEST_OBSERVE_INTERRUPTED_PATH"
	const childReplace = "HELIANTHUS_TEST_OBSERVE_INTERRUPTED_REPLACE"
	const interrupted = 23
	data := []byte("{\"complete\":true}\n")
	if path := os.Getenv(childPath); path != "" {
		teslaObserveWriteData = func(file *os.File, data []byte) error {
			if _, err := file.Write(data[:len(data)/2]); err != nil {
				return err
			}
			// Exit without unwinding defers, as with abrupt process loss.
			os.Exit(interrupted)
			return nil
		}
		err := writeTeslaObserveFile(path, data, os.Getenv(childReplace) == "true")
		t.Fatalf("writer did not interrupt during staging: %v", err)
	}
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replace=%t", replace), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "evidence.json")
			prior := []byte("{\"prior\":true}\n")
			if replace {
				if err := os.WriteFile(path, prior, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command(os.Args[0], "-test.run=^TestTeslaObserveSingleFileInterruptedStaging$")
			command.Env = append(os.Environ(), childPath+"="+path, fmt.Sprintf("%s=%t", childReplace, replace))
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != interrupted {
				t.Fatalf("child exit %v: %s", err, output)
			}
			got, err := os.ReadFile(path)
			if replace {
				if err != nil || !bytes.Equal(got, prior) {
					t.Fatalf("interruption changed prior evidence: %v", err)
				}
			} else if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("interruption exposed partial evidence: %v", err)
			}
			if err := writeTeslaObserveFile(path, data, replace); err != nil {
				t.Fatalf("retry after interrupted staging: %v", err)
			}
			got, err = os.ReadFile(path)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("retry did not publish complete evidence: %v", err)
			}
			if err := writeTeslaObserveFile(path, []byte("must not replace"), false); !errors.Is(err, fs.ErrExist) {
				t.Fatalf("exclusive publication did not refuse existing output: %v", err)
			}
			got, err = os.ReadFile(path)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("exclusive refusal changed complete evidence: %v", err)
			}
		})
	}
}
