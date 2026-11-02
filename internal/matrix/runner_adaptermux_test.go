package matrix

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeExplorerEnv turns the test binary into a fake VRC Explorer: it parses
// the scan arguments, connects to --host/--port, sends one admitted read and
// one write-class telegram, prints the answers and exits 0.
const fakeExplorerEnv = "MATRIX_FAKE_EXPLORER"

func TestMain(m *testing.M) {
	if os.Getenv(fakeExplorerEnv) == "1" {
		os.Exit(runFakeExplorer(os.Args[1:]))
	}
	os.Exit(m.Run())
}

func runFakeExplorer(args []string) int {
	if len(args) == 0 || args[0] != "scan" {
		fmt.Fprintf(os.Stderr, "fake explorer: unexpected args %q\n", args)
		return 2
	}
	values := map[string]string{}
	for index := 1; index+1 < len(args); index += 2 {
		values[args[index]] = args[index+1]
	}
	conn, err := net.Dial("tcp", net.JoinHostPort(values["--host"], values["--port"]))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fake explorer: %v\n", err)
		return 3
	}
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	for _, command := range []string{"info", "hex -s f7 15b52406020003001600", "hex -s f7 15b5240702010300160001"} {
		if _, err := io.WriteString(conn, command+"\n"); err != nil {
			return 4
		}
		lines, err := readEbusdAnswer(reader)
		if err != nil {
			return 5
		}
		fmt.Printf("%s -> %s\n", command, strings.Join(lines, " / "))
	}
	return 0
}

type v2Invocation struct {
	transports []string
	cases      []string
	session    string
	candidate  string
	target     string
	setup      string
}

func runV2DryRun(t *testing.T, outputDir string, invocation v2Invocation) ([]CaseVerdict, error) {
	t.Helper()
	runner, err := NewRunner(RunnerOptions{
		OutputDir:     outputDir,
		Suite:         SuiteAdapterMuxV2,
		Target:        invocation.target,
		Transports:    invocation.transports,
		IncludeIDs:    invocation.cases,
		SessionID:     invocation.session,
		Candidate:     invocation.candidate,
		HardwareSetup: invocation.setup,
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}
	return runner.Run(context.Background())
}

type v2Index struct {
	Target                 string        `json:"target"`
	Suite                  string        `json:"suite"`
	MatrixInventoryVersion string        `json:"matrix_inventory_version"`
	SessionID              string        `json:"session_id"`
	Candidate              string        `json:"candidate"`
	Cases                  []CaseVerdict `json:"cases"`
}

func readV2Index(t *testing.T, outputDir string) v2Index {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outputDir, "index.json"))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	var index v2Index
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatalf("decode index: %v", err)
	}
	return index
}

func TestAdapterMuxV2DryRunWritesDeclaredInventoryWithoutCommands(t *testing.T) {
	t.Parallel()

	outputDir := filepath.Join(t.TempDir(), "results")
	marker := filepath.Join(t.TempDir(), "executed")
	runner, err := NewRunner(RunnerOptions{
		OutputDir:    outputDir,
		Suite:        SuiteAdapterMuxV2,
		StartGateway: "touch " + marker,
		StopGateway:  "touch " + marker,
		SmokeCommand: "touch " + marker,
		StartEbusd:   "touch " + marker,
		StopEbusd:    "touch " + marker,
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}
	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a dry run executed a command (marker stat err %v)", err)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	dirs, files := 0, []string{}
	for _, entry := range entries {
		if entry.IsDir() {
			dirs++
		} else {
			files = append(files, entry.Name())
		}
	}
	if dirs != 173 || len(files) != 1 || files[0] != "index.json" {
		t.Fatalf("output = %d dirs, files %v; want 173 dirs and index.json", dirs, files)
	}

	index := readV2Index(t, outputDir)
	if index.Suite != SuiteAdapterMuxV2 || index.MatrixInventoryVersion != AdapterMuxV2InventoryVersion || len(index.Cases) != 173 {
		t.Fatalf("index = suite %q version %q cases %d", index.Suite, index.MatrixInventoryVersion, len(index.Cases))
	}
	inventory := GenerateAdapterMuxV2Cases("")
	for position, verdict := range index.Cases {
		cell := inventory[position].AdapterMux
		if verdict.CaseID != inventory[position].ID {
			t.Fatalf("index case %d = %s; want %s", position, verdict.CaseID, inventory[position].ID)
		}
		if len(verdict.Commands) != 0 {
			t.Fatalf("%s ran commands in a dry run", verdict.CaseID)
		}
		switch {
		case cell.Reserved == nil:
			if verdict.Outcome != caseOutcomePlanned || verdict.PlannedReason != "" || verdict.InfraReason != "" {
				t.Fatalf("%s dry-run verdict = %+v", verdict.CaseID, verdict)
			}
		case cell.Reserved.Outcome == caseOutcomeBlocked:
			if verdict.Outcome != caseOutcomeBlocked || verdict.InfraReason != cell.Reserved.Reason || verdict.PlannedReason != "" {
				t.Fatalf("%s reserved verdict = %+v", verdict.CaseID, verdict)
			}
		default:
			if verdict.Outcome != caseOutcomePlanned || verdict.PlannedReason != cell.Reserved.Reason || verdict.InfraReason != "" {
				t.Fatalf("%s reserved verdict = %+v", verdict.CaseID, verdict)
			}
		}
	}
	if len(verdicts) != 173 {
		t.Fatalf("len(verdicts) = %d", len(verdicts))
	}

	// Every schema field is present in a v2 verdict, empty or not.
	data, err := os.ReadFile(filepath.Join(outputDir, "UARTRAW-CX01", "verdict.json"))
	if err != nil {
		t.Fatalf("read verdict: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("decode verdict: %v", err)
	}
	for _, field := range []string{"southbound_transport", "invariant", "invariant_family", "composition", "northbound_client",
		"hardware_setup", "session_id", "candidate", "planned_reason", "infra_reason", "expectation"} {
		if _, ok := raw[field]; !ok {
			t.Fatalf("verdict misses field %q", field)
		}
	}
	if raw["southbound_transport"] != "uart-raw" || raw["infra_reason"] != "uart_direct_hardware_absent" {
		t.Fatalf("UARTRAW-CX01 verdict = %v", raw)
	}

	var tcpCX01 CaseVerdict
	data, _ = os.ReadFile(filepath.Join(outputDir, "TCP-CX01", "verdict.json"))
	if err := json.Unmarshal(data, &tcpCX01); err != nil {
		t.Fatalf("decode TCP-CX01: %v", err)
	}
	if tcpCX01.Expected != expectedStatusFail || tcpCX01.Expectation != AdapterMuxV2ExpectedFailures[0].Reason || tcpCX01.PlannedReason != CategoryPL01.Reason {
		t.Fatalf("TCP-CX01 = %+v", tcpCX01)
	}
}

func TestAdapterMuxV2AggregatesFilteredInvocations(t *testing.T) {
	t.Parallel()

	outputDir := filepath.Join(t.TempDir(), "results")
	invocations := []v2Invocation{
		{transports: []string{"ENS"}, setup: "setup-ens"},
		{transports: []string{"ENH"}, setup: "setup-enh"},
		{transports: []string{"UDP"}, setup: "setup-udp"},
		{transports: []string{"TCP"}, setup: "setup-tcp"},
		{transports: []string{"UARTENS", "UARTENH", "UARTRAW"}, setup: "setup-none"},
	}
	wantPartial := []int{25, 51, 76, 101, 173}
	for step, invocation := range invocations {
		invocation.session, invocation.candidate, invocation.target = "session-a", "candidate-a", "ha-addon"
		if _, err := runV2DryRun(t, outputDir, invocation); err != nil {
			t.Fatalf("invocation %d: %v", step, err)
		}
		index := readV2Index(t, outputDir)
		if len(index.Cases) != wantPartial[step] {
			t.Fatalf("after invocation %d the index holds %d cases; want %d", step, len(index.Cases), wantPartial[step])
		}
		if index.SessionID != "session-a" || index.Candidate != "candidate-a" || index.Target != "ha-addon" {
			t.Fatalf("index identity = %q %q %q", index.SessionID, index.Candidate, index.Target)
		}
	}

	index := readV2Index(t, outputDir)
	inventory := GenerateAdapterMuxV2Cases("")
	wantSetup := map[string]string{"ENS": "setup-ens", "ENH": "setup-enh", "UDP": "setup-udp", "TCP": "setup-tcp"}
	for position, verdict := range index.Cases {
		if verdict.CaseID != inventory[position].ID {
			t.Fatalf("index case %d = %s; want %s", position, verdict.CaseID, inventory[position].ID)
		}
		if verdict.SessionID != "session-a" || verdict.Candidate != "candidate-a" || verdict.Target != "ha-addon" {
			t.Fatalf("%s identity = %q %q %q", verdict.CaseID, verdict.SessionID, verdict.Candidate, verdict.Target)
		}
		if got, want := verdict.HardwareSetup, wantSetup[inventory[position].AdapterMux.Row]; got != want {
			t.Fatalf("%s hardware_setup = %q; want %q", verdict.CaseID, got, want)
		}
	}
	for _, verdict := range index.Cases {
		if verdict.CaseID == "EBUSD-ES01" && verdict.HardwareSetup != "setup-enh" {
			t.Fatalf("EBUSD-ES01 hardware_setup = %q; want the ENH setup", verdict.HardwareSetup)
		}
	}
}

// Each test below varies one field of the aggregation identity:
//   - session_id: catches a rebuild that does not compare SessionID;
//   - candidate: catches a rebuild that does not compare Candidate;
//   - target: catches a rebuild that does not compare Target;
//   - hardware_setup per transport: catches a rebuild that does not group
//     labels by transport row.
func TestAdapterMuxV2RejectsForeignSessionCandidateTarget(t *testing.T) {
	t.Parallel()

	base := v2Invocation{transports: []string{"ENS"}, session: "session-a", candidate: "candidate-a", target: "local", setup: "setup-ens"}
	foreign := []struct {
		name   string
		mutate func(*v2Invocation)
		want   string
	}{
		{"session", func(invocation *v2Invocation) { invocation.session = "session-b" }, "session_id"},
		{"candidate", func(invocation *v2Invocation) { invocation.candidate = "candidate-b" }, "candidate"},
		{"target", func(invocation *v2Invocation) { invocation.target = "ha-addon" }, "target"},
	}
	for _, tc := range foreign {
		outputDir := filepath.Join(t.TempDir(), "results")
		if _, err := runV2DryRun(t, outputDir, base); err != nil {
			t.Fatalf("%s: first invocation: %v", tc.name, err)
		}
		second := base
		second.transports = []string{"ENH"}
		second.setup = "setup-enh"
		tc.mutate(&second)
		_, err := runV2DryRun(t, outputDir, second)
		if !errors.Is(err, ErrForeignVerdict) || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v; want a foreign %s rejection", tc.name, err, tc.want)
		}
		if _, statErr := os.Stat(filepath.Join(outputDir, "ENH-AD01")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("%s: the rejected invocation wrote a case directory", tc.name)
		}
		if index := readV2Index(t, outputDir); len(index.Cases) != 25 || index.SessionID != base.session ||
			index.Candidate != base.candidate || index.Target != base.target {
			t.Fatalf("%s: index changed after the rejected invocation", tc.name)
		}
	}
}

func TestAdapterMuxV2RejectsSecondHardwareSetupWithinTransport(t *testing.T) {
	t.Parallel()

	outputDir := filepath.Join(t.TempDir(), "results")
	base := v2Invocation{transports: []string{"ENH"}, session: "session-a", candidate: "candidate-a", target: "local", setup: "setup-enh"}
	if _, err := runV2DryRun(t, outputDir, base); err != nil {
		t.Fatalf("first invocation: %v", err)
	}

	partial := base
	partial.cases = []string{"ENH-AD01"}
	partial.setup = "setup-other"
	if _, err := runV2DryRun(t, outputDir, partial); err == nil || !strings.Contains(err.Error(), "second hardware setup") {
		t.Fatalf("err = %v; want a second-setup rejection", err)
	}

	// EBUSD-ES01 runs on the ENH setup: a different label for it alone is a
	// second setup for ENH.
	esOnly := base
	esOnly.cases = []string{"EBUSD-ES01"}
	esOnly.setup = "setup-other"
	if _, err := runV2DryRun(t, outputDir, esOnly); err == nil || !strings.Contains(err.Error(), "second hardware setup") {
		t.Fatalf("EBUSD-ES01 err = %v; want a second-setup rejection", err)
	}

	// A different transport may use a different setup.
	other := base
	other.transports = []string{"ENS"}
	other.setup = "setup-ens"
	if _, err := runV2DryRun(t, outputDir, other); err != nil {
		t.Fatalf("ENS on its own setup: %v", err)
	}

	// Re-running every cell of the transport replaces them all.
	rerun := base
	rerun.setup = "setup-enh-2"
	if _, err := runV2DryRun(t, outputDir, rerun); err != nil {
		t.Fatalf("full ENH re-run on a new setup: %v", err)
	}
}

func TestAdapterMuxV2RejectsTamperedOrForeignContent(t *testing.T) {
	t.Parallel()

	base := v2Invocation{transports: []string{"ENS"}, session: "session-a", candidate: "candidate-a", target: "local", setup: "setup-ens"}

	tampers := []struct{ from, to string }{
		{`"invariant": "AD02"`, `"invariant": "AD03"`},
		{`"southbound_transport": "ens"`, `"southbound_transport": "enh"`},
		{`"case_id": "ENS-AD02"`, `"case_id": "ENS-AD03"`},
		{`"suite": "adaptermux-v2"`, `"suite": "full88"`},
	}
	for _, tamper := range tampers {
		outputDir := filepath.Join(t.TempDir(), "results")
		if _, err := runV2DryRun(t, outputDir, base); err != nil {
			t.Fatalf("first invocation: %v", err)
		}
		path := filepath.Join(outputDir, "ENS-AD02", "verdict.json")
		data, _ := os.ReadFile(path)
		tampered := strings.Replace(string(data), tamper.from, tamper.to, 1)
		if tampered == string(data) {
			t.Fatalf("tamper %q did not apply", tamper.from)
		}
		if err := os.WriteFile(path, []byte(tampered), 0o644); err != nil {
			t.Fatalf("tamper: %v", err)
		}
		second := base
		second.transports, second.setup = []string{"ENH"}, "setup-enh"
		if _, err := runV2DryRun(t, outputDir, second); !errors.Is(err, ErrForeignVerdict) {
			t.Fatalf("tamper %q err = %v; want a rejection", tamper.to, err)
		}
	}

	fullDir := filepath.Join(t.TempDir(), "results")
	full, err := NewRunner(RunnerOptions{OutputDir: fullDir, IncludeIDs: []string{"T01"}})
	if err != nil {
		t.Fatalf("NewRunner full88: %v", err)
	}
	if _, err := full.Run(context.Background()); err != nil {
		t.Fatalf("full88 run: %v", err)
	}
	if _, err := runV2DryRun(t, fullDir, base); !errors.Is(err, ErrForeignVerdict) {
		t.Fatalf("full88 directory err = %v; want a rejection", err)
	}
}

func TestAdapterMuxV2RerunReplacesCaseDirectory(t *testing.T) {
	t.Parallel()

	outputDir := filepath.Join(t.TempDir(), "results")
	base := v2Invocation{transports: []string{"ENS"}, session: "session-a", candidate: "candidate-a", target: "local", setup: "setup-ens"}
	if _, err := runV2DryRun(t, outputDir, base); err != nil {
		t.Fatalf("first invocation: %v", err)
	}
	stale := filepath.Join(outputDir, "ENS-AD01", "logs", "stale.log")
	if err := os.WriteFile(stale, []byte("old run\n"), 0o644); err != nil {
		t.Fatalf("write stale: %v", err)
	}
	rerun := base
	rerun.cases = []string{"ENS-AD01"}
	if _, err := runV2DryRun(t, outputDir, rerun); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the re-run kept a stale file (stat err %v)", err)
	}
	if index := readV2Index(t, outputDir); len(index.Cases) != 25 {
		t.Fatalf("index holds %d cases after the re-run; want 25", len(index.Cases))
	}
}

func TestAdapterMuxV2OptionValidation(t *testing.T) {
	t.Parallel()

	valid := RunnerOptions{
		OutputDir:     t.TempDir(),
		Suite:         SuiteAdapterMuxV2,
		Transports:    []string{"ENS"},
		SessionID:     "session-a",
		Candidate:     "candidate-a",
		HardwareSetup: "setup-ens",
	}
	if _, err := NewRunner(valid); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	invalid := []struct {
		name   string
		mutate func(*RunnerOptions)
	}{
		{"empty setup", func(options *RunnerOptions) { options.HardwareSetup = "" }},
		{"blank setup", func(options *RunnerOptions) { options.HardwareSetup = "   " }},
		{"empty session", func(options *RunnerOptions) { options.SessionID = "" }},
		{"empty candidate", func(options *RunnerOptions) { options.Candidate = "" }},
		{"setup with a path", func(options *RunnerOptions) { options.HardwareSetup = "../setup" }},
		{"candidate with a coordinate", func(options *RunnerOptions) { options.Candidate = "host:8888" }},
		{"unknown transport", func(options *RunnerOptions) { options.Transports = []string{"EBUSD"} }},
		{"expected failures", func(options *RunnerOptions) { options.ExpectedFailures = map[string]string{"ENS-AD01": "x"} }},
		{"proxy", func(options *RunnerOptions) { options.StartProxy = "start-proxy" }},
		{"external client", func(options *RunnerOptions) { options.ExternalClient = "telnet" }},
		{"workload target broadcast", func(options *RunnerOptions) { options.WorkloadTarget = "fe" }},
		{"workload target initiator", func(options *RunnerOptions) { options.WorkloadTarget = "10" }},
		{"workload initiator non-initiator", func(options *RunnerOptions) { options.WorkloadInitiator = "08" }},
		{"workload initiator not hex", func(options *RunnerOptions) { options.WorkloadInitiator = "zz" }},
	}
	for _, tc := range invalid {
		options := valid
		tc.mutate(&options)
		if _, err := NewRunner(options); err == nil {
			t.Fatalf("%s: NewRunner accepted invalid options", tc.name)
		}
	}

	// Execute without --transports still needs the session labels.
	execute := RunnerOptions{OutputDir: t.TempDir(), Suite: SuiteAdapterMuxV2, Execute: true}
	if _, err := NewRunner(execute); err == nil {
		t.Fatalf("execute without session labels must be rejected")
	}
	// The v2 options are rejected for the other suites.
	for _, suite := range []string{"", SuitePassive} {
		if _, err := NewRunner(RunnerOptions{OutputDir: t.TempDir(), Suite: suite, SessionID: "session-a"}); err == nil {
			t.Fatalf("suite %q accepted an adaptermux-v2 option", suite)
		}
	}
}

type executeFixture struct {
	runner  *Runner
	output  string
	marker  string
	ebusd   *fakeEbusd
	enhPeer *fakeENHPeer
}

func newExecuteFixture(t *testing.T, cases []string, mutate func(*RunnerOptions)) (*executeFixture, error) {
	t.Helper()
	fixture := &executeFixture{
		output:  filepath.Join(t.TempDir(), "results"),
		marker:  filepath.Join(t.TempDir(), "commands.log"),
		ebusd:   &fakeEbusd{},
		enhPeer: &fakeENHPeer{response: []byte{0xB5, 0x01}},
	}
	record := func(name string) string { return fmt.Sprintf("echo %s >> '%s'", name, fixture.marker) }
	options := RunnerOptions{
		OutputDir:         fixture.output,
		Suite:             SuiteAdapterMuxV2,
		Target:            "local",
		IncludeIDs:        cases,
		Execute:           true,
		SessionID:         "session-a",
		Candidate:         "candidate-a",
		HardwareSetup:     "setup-a",
		StartGateway:      record("gateway-start"),
		StopGateway:       record("gateway-stop"),
		StartEbusd:        record("ebusd-start"),
		StopEbusd:         record("ebusd-stop"),
		SmokeCommand:      record("smoke-$MATRIX_INVARIANT-$MATRIX_NORTHBOUND_CLIENT-$MATRIX_HARDWARE_SETUP"),
		MuxENHAddr:        "mux.invalid:1",
		EbusdCommandAddr:  "ebusd.invalid:1",
		WorkloadInitiator: "f7",
		WorkloadTarget:    "15",
		CaseTimeout:       10 * time.Second,
	}
	if mutate != nil {
		mutate(&options)
	}
	runner, err := NewRunner(options)
	if err != nil {
		return nil, err
	}
	runner.dial = func(ctx context.Context, address string) (net.Conn, error) {
		switch address {
		case "mux.invalid:1":
			client, server := net.Pipe()
			go fixture.enhPeer.serve(server)
			return client, nil
		case "ebusd.invalid:1":
			return fixture.ebusd.dial(ctx, address)
		default:
			return nil, fmt.Errorf("unexpected dial %q", address)
		}
	}
	fixture.runner = runner
	return fixture, nil
}

func (fixture *executeFixture) commandsRun(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(fixture.marker)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	return strings.Fields(string(data))
}

func TestAdapterMuxV2ExecuteDrivesGuardedClients(t *testing.T) {
	t.Parallel()

	fixture, err := newExecuteFixture(t, []string{"ENH-PX01", "ENH-AD06", "ENH-AD01", "ENH-AD05"}, nil)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	verdicts, err := fixture.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	byID := map[string]CaseVerdict{}
	for _, verdict := range verdicts {
		byID[verdict.CaseID] = verdict
	}
	for _, id := range []string{"ENH-PX01", "ENH-AD06", "ENH-AD01"} {
		verdict := byID[id]
		if verdict.Outcome != caseOutcomePass || verdict.HardwareSetup != "setup-a" || verdict.SessionID != "session-a" {
			t.Fatalf("%s verdict = %+v", id, verdict)
		}
	}
	clientOf := func(verdict CaseVerdict) string {
		for _, command := range verdict.Commands {
			if strings.HasPrefix(command.Name, "client-") {
				return command.Name + ":" + command.Status
			}
		}
		return ""
	}
	if got := clientOf(byID["ENH-PX01"]); got != "client-synthetic-enh:passed" {
		t.Fatalf("ENH-PX01 client = %q", got)
	}
	if got := clientOf(byID["ENH-AD06"]); got != "client-ebusd:passed" {
		t.Fatalf("ENH-AD06 client = %q", got)
	}
	if got := clientOf(byID["ENH-AD01"]); got != "" {
		t.Fatalf("ENH-AD01 drove a client %q", got)
	}

	reserved := byID["ENH-AD05"]
	if reserved.Status != actualStatusReserved || reserved.Outcome != caseOutcomePlanned ||
		reserved.PlannedReason != CategoryPL03.Reason || len(reserved.Commands) != 0 {
		t.Fatalf("ENH-AD05 verdict = %+v", reserved)
	}

	// Only allowlisted requests reached the fake ENH listener and ebusd.
	for _, logical := range fixture.enhPeer.observed() {
		onWire := GuardTelegram{Target: logical[1], Primary: logical[2], Secondary: logical[3], Data: logical[5 : len(logical)-1]}
		if requestType, err := ClassifyTelegram(onWire); err != nil || requestType != RequestIdentification {
			t.Fatalf("contender sent % X (%q, %v)", logical, requestType, err)
		}
	}
	if got := len(fixture.enhPeer.observed()); got != contenderTelegramCount {
		t.Fatalf("contender sent %d telegrams; want %d", got, contenderTelegramCount)
	}
	if got := strings.Join(fixture.ebusd.received(), "|"); got != "state|scan 15|hex 15070400" {
		t.Fatalf("ebusd received %q", got)
	}

	commands := strings.Join(fixture.commandsRun(t), " ")
	for _, want := range []string{"smoke-PX01-synthetic-enh-setup-a", "smoke-AD06-ebusd-setup-a", "smoke-AD01-none-setup-a"} {
		if !strings.Contains(commands, want) {
			t.Fatalf("commands %q miss %q", commands, want)
		}
	}
	if strings.Contains(commands, "AD05") {
		t.Fatalf("a reserved cell ran a command: %q", commands)
	}
	if _, err := os.Stat(filepath.Join(fixture.output, "ENH-PX01", "logs", "client.json")); err != nil {
		t.Fatalf("client record missing: %v", err)
	}
	if index := readV2Index(t, fixture.output); len(index.Cases) != 4 {
		t.Fatalf("index holds %d cases; want 4", len(index.Cases))
	}
}

func TestAdapterMuxV2AdmissionFailsBeforeAnyCommand(t *testing.T) {
	t.Parallel()

	missing := []struct {
		name   string
		cases  []string
		mutate func(*RunnerOptions)
	}{
		{"mux address for a PX cell", []string{"ENS-PX01"}, func(options *RunnerOptions) { options.MuxENHAddr = "" }},
		{"ebusd address for AD06", []string{"ENS-AD06"}, func(options *RunnerOptions) { options.EbusdCommandAddr = "" }},
		{"workload target", []string{"ENS-AD06"}, func(options *RunnerOptions) { options.WorkloadTarget = "" }},
		{"workload initiator for the contender", []string{"ENS-PX12"}, func(options *RunnerOptions) { options.WorkloadInitiator = "" }},
		{"explorer command", []string{"ENS-CX01"}, func(options *RunnerOptions) { options.ExternalClient = NorthboundVRCExplorer }},
		{"smoke command", []string{"ENS-DS01"}, func(options *RunnerOptions) { options.SmokeCommand = "" }},
		{"operator confirmation for W", []string{"UDP-DS01"}, nil},
	}
	for _, tc := range missing {
		fixture, err := newExecuteFixture(t, tc.cases, tc.mutate)
		if err != nil {
			t.Fatalf("%s: fixture: %v", tc.name, err)
		}
		if _, err := fixture.runner.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "admission") {
			t.Fatalf("%s: err = %v; want an admission failure", tc.name, err)
		}
		if commands := fixture.commandsRun(t); len(commands) != 0 {
			t.Fatalf("%s: commands ran: %v", tc.name, commands)
		}
		if _, err := os.Stat(filepath.Join(fixture.output, tc.cases[0])); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s: a case directory was written", tc.name)
		}
	}
}

func TestAdapterMuxV2AdapterActionNeedsOperatorConfirmation(t *testing.T) {
	t.Parallel()

	var asked []string
	declined, err := newExecuteFixture(t, []string{"UDP-DS01"}, func(options *RunnerOptions) {
		options.ConfirmAdapterAction = func(caseID string, action string) bool {
			asked = append(asked, caseID)
			return false
		}
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	verdicts, err := declined.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(asked) != 1 || asked[0] != "UDP-DS01" {
		t.Fatalf("asked = %v", asked)
	}
	if verdicts[0].Outcome != caseOutcomeFail || len(verdicts[0].Commands) != 0 || len(declined.commandsRun(t)) != 0 {
		t.Fatalf("declined W cell = %+v, commands %v", verdicts[0], declined.commandsRun(t))
	}

	confirmed, err := newExecuteFixture(t, []string{"UDP-DS01"}, func(options *RunnerOptions) {
		options.ConfirmAdapterAction = func(string, string) bool { return true }
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	verdicts, err = confirmed.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if verdicts[0].Outcome != caseOutcomePass {
		t.Fatalf("confirmed W cell = %+v", verdicts[0])
	}
}

func TestAdapterMuxV2ExplorerScanIsGuarded(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	wrapper := filepath.Join(t.TempDir(), "fake-explorer.sh")
	script := fmt.Sprintf("#!/bin/sh\n%s=1 exec '%s' \"$@\"\n", fakeExplorerEnv, executable)
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	fixture, err := newExecuteFixture(t, []string{"ENS-CX01"}, func(options *RunnerOptions) {
		options.ExternalClient = NorthboundVRCExplorer
		options.ExplorerCommand = wrapper
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	verdicts, err := fixture.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	verdict := verdicts[0]
	if verdict.NorthboundClient != NorthboundVRCExplorer {
		t.Fatalf("northbound_client = %q", verdict.NorthboundClient)
	}
	// The fake explorer sends one write-class telegram: the relay refuses it,
	// ebusd never sees it, and the cell fails.
	if verdict.Outcome != caseOutcomeFail || !strings.Contains(verdict.Error, "refused") {
		t.Fatalf("verdict = %+v", verdict)
	}
	if got := strings.Join(fixture.ebusd.received(), "|"); got != "info|hex -s f7 15b52406020003001600" {
		t.Fatalf("ebusd received %q; want only the admitted commands", got)
	}
	data, err := os.ReadFile(filepath.Join(fixture.output, "ENS-CX01", "logs", "client.json"))
	if err != nil {
		t.Fatalf("read client record: %v", err)
	}
	var record struct {
		Record ExplorerRecord `json:"record"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode client record: %v", err)
	}
	if record.Record.Forwarded != 2 || record.Record.Refused != 1 || record.Record.ExitCode != 0 || record.Record.Args[0] != "scan" {
		t.Fatalf("explorer record = %+v", record.Record)
	}
	if commands := strings.Join(fixture.commandsRun(t), " "); !strings.Contains(commands, "ebusd-start") || !strings.Contains(commands, "ebusd-stop") {
		t.Fatalf("the explorer cell did not run ebusd: %q", commands)
	}
	if _, err := os.Stat(filepath.FromSlash(record.Record.LogFile)); err != nil {
		t.Fatalf("explorer log: %v", err)
	}
}
