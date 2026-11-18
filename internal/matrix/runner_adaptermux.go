package matrix

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const (
	actualStatusReserved = "reserved"

	contenderTelegramCount = 3
	clientStepTimeout      = 5 * time.Second

	// ebusdCommandAddrEnv carries --ebusd-command-addr to the cell commands.
	ebusdCommandAddrEnv = "MATRIX_EBUSD_COMMAND_ADDR"
	// smokeReadyFileEnv names the file the smoke command creates once it has
	// taken its baseline reading; the client workload starts only then.
	smokeReadyFileEnv = "MATRIX_SMOKE_READY_FILE"
	smokeReadyFile    = "smoke.ready"
	smokeReadyPoll    = 20 * time.Millisecond
	// muxStartDeadline is the absorb bound: the mux default START deadline.
	// A replaced or cancelled bid's STARTED or FAILED can still arrive
	// until then, so the contender watches for it that long.
	muxStartDeadline = 5 * time.Second
	// bidInFlightAttempts bounds the STARTs a contender sends to get one
	// bid shown in flight. A STARTED or FAILED that arrives before that
	// proof only makes the contender bid again.
	bidInFlightAttempts = 5
	// fenceInfoID is the INFO request the contender writes after each START
	// (adapter version). The mux session answers INFO from its cache, without
	// adapter traffic, and releases that answer only after the grant step
	// whose snapshot included the START has returned from RequestStart.
	fenceInfoID = 0x00
	// absorbExpiryWait is how long the session that follows a cancelled
	// in-flight bid waits for the grant. It outlasts the absorb bound, so
	// a grant that waits the barrier out still arrives.
	absorbExpiryWait = 8 * time.Second
)

// labelPattern bounds the opaque session, candidate and setup labels.
var labelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ipv4LikePattern catches dotted-number labels such as 10.0.0.12.
var ipv4LikePattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+){3}$`)

type workloadAddresses struct {
	initiator    byte
	initiatorSet bool
	target       byte
	targetSet    bool
}

func validateLabel(flagName string, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%s is required and must not be empty", flagName)
		}
		return nil
	}
	if !labelPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not an opaque label (letters, digits, '.', '_', '-', at most 64)", flagName, value)
	}
	if net.ParseIP(value) != nil || ipv4LikePattern.MatchString(value) {
		return fmt.Errorf("%s %q looks like a network address; use an opaque label", flagName, value)
	}
	return nil
}

func parseWorkloadByte(flagName string, value string) (byte, bool, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(trimmed), "0x"), 16, 8)
	if err != nil {
		return 0, false, fmt.Errorf("%s %q is not a hex byte", flagName, value)
	}
	return byte(parsed), true, nil
}

func validateAdapterMuxOptions(options *RunnerOptions) (workloadAddresses, error) {
	var workload workloadAddresses
	transports, err := normalizeTransportTokens(options.Transports)
	if err != nil {
		return workload, err
	}
	options.Transports = transports
	options.SessionID = strings.TrimSpace(options.SessionID)
	options.Candidate = strings.TrimSpace(options.Candidate)
	options.HardwareSetup = strings.TrimSpace(options.HardwareSetup)

	required := len(transports) > 0 || options.Execute
	for _, label := range []struct {
		flag  string
		value string
	}{
		{flag: "--session-id", value: options.SessionID},
		{flag: "--candidate", value: options.Candidate},
		{flag: "--hardware-setup", value: options.HardwareSetup},
	} {
		if err := validateLabel(label.flag, label.value, required); err != nil {
			return workload, fmt.Errorf("%s: %w", SuiteAdapterMuxV2, err)
		}
	}

	if len(options.ExpectedFailures) > 0 {
		return workload, fmt.Errorf("%s uses its own expected-failure inventory (XF-01); --expected-failures is not accepted", SuiteAdapterMuxV2)
	}
	if strings.TrimSpace(options.StartProxy) != "" || strings.TrimSpace(options.StopProxy) != "" {
		return workload, fmt.Errorf("%s does not use --start-proxy/--stop-proxy", SuiteAdapterMuxV2)
	}

	client := strings.ToLower(strings.TrimSpace(options.ExternalClient))
	switch client {
	case "":
		client = NorthboundEbusd
	case NorthboundEbusd, NorthboundVRCExplorer:
	default:
		return workload, fmt.Errorf("unsupported --external-client %q (allowed: ebusd, vrc-explorer)", options.ExternalClient)
	}
	options.ExternalClient = client

	options.EbusdCommandAddr = strings.TrimSpace(options.EbusdCommandAddr)
	if exported := strings.TrimSpace(os.Getenv(ebusdCommandAddrEnv)); exported != "" && exported != options.EbusdCommandAddr {
		return workload, fmt.Errorf("%s in the runner's environment differs from --ebusd-command-addr; "+
			"the runner hands --ebusd-command-addr to the cell commands, so unset the variable or export the same host:port", ebusdCommandAddrEnv)
	}

	workload.initiator, workload.initiatorSet, err = parseWorkloadByte("--workload-initiator-addr", options.WorkloadInitiator)
	if err != nil {
		return workload, err
	}
	workload.target, workload.targetSet, err = parseWorkloadByte("--workload-target-addr", options.WorkloadTarget)
	if err != nil {
		return workload, err
	}
	if workload.initiatorSet && !protocol.IsInitiatorCapableAddress(workload.initiator) {
		return workload, fmt.Errorf("--workload-initiator-addr 0x%02X is not an initiator address", workload.initiator)
	}
	if workload.targetSet {
		if _, err := ClassifyTelegram(GuardTelegram{Target: workload.target, Primary: 0x07, Secondary: 0x04}); err != nil {
			return workload, fmt.Errorf("--workload-target-addr: %w", err)
		}
	}
	return workload, nil
}

func rejectAdapterMuxOptions(options RunnerOptions) error {
	set := func(value string) bool { return strings.TrimSpace(value) != "" }
	if len(options.Transports) > 0 || set(options.SessionID) || set(options.Candidate) ||
		set(options.HardwareSetup) || set(options.ExternalClient) || set(options.MuxENHAddr) ||
		set(options.EbusdCommandAddr) || set(options.ExplorerCommand) ||
		set(options.WorkloadInitiator) || set(options.WorkloadTarget) {
		return fmt.Errorf("the adaptermux-v2 options apply only to --suite %s", SuiteAdapterMuxV2)
	}
	return nil
}

// admitAdapterMuxExecution checks, before any command runs, that every
// selected executable cell has what its workload needs.
func (runner *Runner) admitAdapterMuxExecution(cases []TopologyCase) error {
	options := runner.options
	need := func(value string, flagName string, caseID string) error {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s requires %s", caseID, flagName)
		}
		return nil
	}
	for _, testCase := range cases {
		cell := testCase.AdapterMux
		if !cell.Executable() {
			continue
		}
		checks := []error{
			need(options.StartGateway, "--start-gateway", testCase.ID),
			need(options.StopGateway, "--stop-gateway", testCase.ID),
			need(options.SmokeCommand, "--smoke-command", testCase.ID),
		}
		if testCase.UsesEbusd {
			checks = append(checks,
				need(options.StartEbusd, "--start-ebusd", testCase.ID),
				need(options.StopEbusd, "--stop-ebusd", testCase.ID))
		}
		switch cell.NorthboundClient {
		case NorthboundSyntheticENH:
			checks = append(checks, need(options.MuxENHAddr, "--mux-enh-addr", testCase.ID))
		case NorthboundEbusd:
			checks = append(checks, need(options.EbusdCommandAddr, "--ebusd-command-addr", testCase.ID))
		case NorthboundVRCExplorer:
			checks = append(checks,
				need(options.EbusdCommandAddr, "--ebusd-command-addr", testCase.ID),
				need(options.ExplorerCommand, "--vrc-explorer-command", testCase.ID))
		}
		if cell.NorthboundClient != NorthboundNone {
			if !runner.workload.targetSet {
				checks = append(checks, fmt.Errorf("%s requires --workload-target-addr", testCase.ID))
			}
			if !runner.workload.initiatorSet && cell.NorthboundClient != NorthboundEbusd {
				checks = append(checks, fmt.Errorf("%s requires --workload-initiator-addr", testCase.ID))
			}
		}
		if cell.Class == CellClassAdapterAction && options.ConfirmAdapterAction == nil {
			checks = append(checks, fmt.Errorf("%s is class W and needs an operator confirmation at action time", testCase.ID))
		}
		if err := errors.Join(checks...); err != nil {
			return err
		}
	}
	return nil
}

func (runner *Runner) identity() adapterMuxIdentity {
	return adapterMuxIdentity{
		Target:    runner.options.Target,
		SessionID: runner.options.SessionID,
		Candidate: runner.options.Candidate,
	}
}

// cellHardwareSetup is the setup label a cell records. UART cells record
// none: the setup they would need does not exist (BI-02).
func (runner *Runner) cellHardwareSetup(cell *AdapterMuxCell) string {
	if cell.Reserved != nil && cell.Reserved.ID == CategoryBI02.ID {
		return ""
	}
	return runner.options.HardwareSetup
}

func (runner *Runner) adapterMuxVerdictSkeleton(testCase TopologyCase) CaseVerdict {
	cell := testCase.AdapterMux
	verdict := CaseVerdict{
		CaseID:              testCase.ID,
		Suite:               SuiteAdapterMuxV2,
		Kind:                testCase.Kind,
		Target:              runner.options.Target,
		Status:              actualStatusPlanned,
		Outcome:             caseOutcomePlanned,
		Expected:            expectedStatusPass,
		SouthboundTransport: string(cell.Southbound),
		Invariant:           cell.Invariant,
		InvariantFamily:     cell.Family,
		Composition:         cell.Composition,
		NorthboundClient:    cell.NorthboundClient,
		HardwareSetup:       runner.cellHardwareSetup(cell),
		SessionID:           runner.options.SessionID,
		Candidate:           runner.options.Candidate,
		ConfigFiles:         []string{},
		LogFiles:            []string{},
		Commands:            []commandResult{},
	}
	if cell.ExpectedFailure != nil {
		verdict.Expected = expectedStatusFail
		verdict.Expectation = cell.ExpectedFailure.Reason
	}
	return verdict
}

func (runner *Runner) runAdapterMuxV2(ctx context.Context) ([]CaseVerdict, error) {
	inventory := GenerateAdapterMuxV2Cases(runner.options.ExternalClient)
	cases, err := selectAdapterMuxCases(inventory, runner.options.Transports, runner.options.IncludeIDs)
	if err != nil {
		return nil, err
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("no topology cases selected")
	}
	if runner.options.Execute {
		if err := runner.admitAdapterMuxExecution(cases); err != nil {
			return nil, fmt.Errorf("%s admission: %w", SuiteAdapterMuxV2, err)
		}
	}
	if err := os.MkdirAll(runner.options.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	selected := make(map[string]struct{}, len(cases))
	for _, testCase := range cases {
		selected[testCase.ID] = struct{}{}
	}
	existing, err := loadAdapterMuxVerdicts(runner.options.OutputDir, inventory, selected)
	if err != nil {
		return nil, fmt.Errorf("aggregate preflight: %w", err)
	}
	projected := existing
	for _, testCase := range cases {
		projected = append(projected, runner.adapterMuxVerdictSkeleton(testCase))
	}
	if err := validateAdapterMuxAggregate(projected, runner.identity(), inventory); err != nil {
		return nil, fmt.Errorf("aggregate preflight: %w", err)
	}

	verdicts := make([]CaseVerdict, 0, len(cases))
	for _, testCase := range cases {
		select {
		case <-ctx.Done():
			return verdicts, ctx.Err()
		default:
		}
		caseDir := filepath.Join(runner.options.OutputDir, testCase.ID)
		if err := os.RemoveAll(caseDir); err != nil {
			return verdicts, fmt.Errorf("replace case directory %s: %w", testCase.ID, err)
		}
		verdict, runErr := runner.runAdapterMuxCase(ctx, testCase)
		verdicts = append(verdicts, verdict)
		if runErr != nil {
			return verdicts, runErr
		}
	}

	if err := rebuildAdapterMuxIndex(runner.options.OutputDir, runner.identity(), inventory, runner.nowUTC()); err != nil {
		return verdicts, fmt.Errorf("rebuild matrix index: %w", err)
	}
	return verdicts, nil
}

func (runner *Runner) runAdapterMuxCase(ctx context.Context, testCase TopologyCase) (CaseVerdict, error) {
	cell := testCase.AdapterMux
	verdict := runner.adapterMuxVerdictSkeleton(testCase)
	verdict.StartedAt = runner.nowUTC().Format(time.RFC3339)

	caseDir := filepath.Join(runner.options.OutputDir, testCase.ID)
	configDir := filepath.Join(caseDir, "configs")
	logDir := filepath.Join(caseDir, "logs")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return CaseVerdict{}, fmt.Errorf("create config dir: %w", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return CaseVerdict{}, fmt.Errorf("create log dir: %w", err)
	}
	configFiles, err := runner.writeAdapterMuxConfigs(testCase, verdict, configDir)
	if err != nil {
		return CaseVerdict{}, err
	}
	verdict.ConfigFiles = configFiles

	commandLogPath := filepath.Join(logDir, "runner.log")
	logFiles := []string{filepath.ToSlash(commandLogPath)}

	switch {
	case cell.Reserved != nil:
		line := fmt.Sprintf("reserved cell: no command executed (%s %s)\n", cell.Reserved.ID, cell.Reserved.Reason)
		if err := os.WriteFile(commandLogPath, []byte(line), 0o644); err != nil {
			return CaseVerdict{}, fmt.Errorf("write reserved log: %w", err)
		}
		verdict.Status = actualStatusReserved
		verdict.Outcome = cell.Reserved.Outcome
		if cell.Reserved.Outcome == caseOutcomeBlocked {
			verdict.InfraReason = cell.Reserved.Reason
		} else {
			verdict.PlannedReason = cell.Reserved.Reason
		}
	case !runner.options.Execute:
		if err := os.WriteFile(commandLogPath, []byte(strings.Join(runner.adapterMuxDryRunLines(testCase), "\n")+"\n"), 0o644); err != nil {
			return CaseVerdict{}, fmt.Errorf("write dry-run log: %w", err)
		}
	default:
		commands, extraLogs, status, firstError, infraReason := runner.executeAdapterMuxCase(ctx, testCase, caseDir, configDir, logDir, commandLogPath)
		verdict.Commands = commands
		logFiles = append(logFiles, extraLogs...)
		verdict.Status = status
		verdict.Error = firstError
		verdict.InfraReason = infraReason
		verdict.Outcome = classifyCaseOutcome(status, verdict.Expected, infraReason)
	}

	verdict.LogFiles = uniqueStrings(logFiles)
	verdict.EndedAt = runner.nowUTC().Format(time.RFC3339)
	if err := writeJSON(filepath.Join(caseDir, "verdict.json"), verdict); err != nil {
		return CaseVerdict{}, fmt.Errorf("write verdict: %w", err)
	}
	return verdict, nil
}

func (runner *Runner) adapterMuxDryRunLines(testCase TopologyCase) []string {
	options := runner.options
	cell := testCase.AdapterMux
	planned := []string{options.StartGateway}
	if testCase.UsesEbusd {
		planned = append(planned, options.StartEbusd)
	}
	planned = append(planned, options.SmokeCommand)
	if cell.NorthboundClient != NorthboundNone {
		planned = append(planned, "client workload: "+cell.NorthboundClient)
	}
	if testCase.UsesEbusd {
		planned = append(planned, options.StopEbusd)
	}
	planned = append(planned, options.StopGateway)

	lines := []string{"dry-run mode: no command executed"}
	if cell.Class == CellClassAdapterAction {
		lines = append(lines, "class W: operator confirmation required at action time")
	}
	for _, value := range planned {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func (runner *Runner) executeAdapterMuxCase(
	ctx context.Context,
	testCase TopologyCase,
	caseDir string,
	configDir string,
	logDir string,
	commandLogPath string,
) ([]commandResult, []string, string, string, string) {
	cell := testCase.AdapterMux
	commands := make([]commandResult, 0, 8)
	logFiles := make([]string, 0, 4)
	status := actualStatusPlanned
	var firstError, infraReason string

	if cell.Class == CellClassAdapterAction {
		action := fmt.Sprintf("%s performs an adapter-side action (%s, %s composition on %s)", testCase.ID, cell.Assertion, cell.Composition, cell.Southbound)
		if runner.options.ConfirmAdapterAction == nil || !runner.options.ConfirmAdapterAction(testCase.ID, action) {
			_ = os.WriteFile(commandLogPath, []byte("adapter-side action not confirmed: no command executed\n"), 0o644)
			return commands, logFiles, actualStatusFailed, "adapter-side action not confirmed by the operator", ""
		}
	}

	caseCtx := ctx
	if runner.options.CaseTimeout > 0 {
		var cancel context.CancelFunc
		caseCtx, cancel = context.WithTimeout(ctx, runner.options.CaseTimeout)
		defer cancel()
	}
	readyPath := filepath.Join(logDir, smokeReadyFile)
	env := append(
		buildCaseEnv(testCase, runner.options.Target, runner.options.Suite, caseDir, configDir, logDir),
		runner.adapterMuxEnv(testCase)...,
	)
	env = append(env, smokeReadyFileEnv+"="+readyPath)
	appendResult := func(result commandResult) {
		commands = append(commands, result)
		logFiles = append(logFiles, filepath.ToSlash(result.LogFile))
		if result.Status == actualStatusFailed && firstError == "" {
			firstError = result.Error
			status = actualStatusFailed
			infraReason = inferInfraReasonFromLog(result.LogFile)
		}
	}

	startPlan := []struct {
		name    string
		command string
		enabled bool
	}{
		{name: "gateway-start", command: runner.options.StartGateway, enabled: true},
		{name: "ebusd-start", command: runner.options.StartEbusd, enabled: testCase.UsesEbusd},
	}
	for _, step := range startPlan {
		result := runner.runPlannedCommand(caseCtx, commandLogPath, step.name, step.command, step.enabled, env)
		appendResult(result)
		if result.Status == actualStatusFailed {
			break
		}
	}
	if firstError == "" && runner.options.SettleDelay > 0 {
		time.Sleep(runner.options.SettleDelay)
	}

	if firstError == "" {
		var clientResult commandResult
		var wait sync.WaitGroup
		hasClient := cell.NorthboundClient != NorthboundNone
		smokeDone := make(chan struct{})
		if hasClient {
			wait.Add(1)
			go func() {
				defer wait.Done()
				// The client starts only after the smoke command has taken its
				// baseline reading, so its first frames fall inside the
				// measured window.
				if err := awaitSmokeBaseline(caseCtx, readyPath, smokeDone); err != nil {
					clientResult = runner.clientNotStarted(testCase, logDir, err)
					return
				}
				clientResult = runner.runClientWorkload(caseCtx, testCase, logDir)
			}()
		}
		smokeResult := runner.runPlannedCommand(caseCtx, commandLogPath, "smoke", runner.options.SmokeCommand, true, env)
		close(smokeDone)
		wait.Wait()
		appendResult(smokeResult)
		if hasClient {
			appendResult(clientResult)
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), cleanupCommandTimeout)
	defer stopCancel()
	stopPlan := []struct {
		name    string
		command string
		enabled bool
	}{
		{name: "ebusd-stop", command: runner.options.StopEbusd, enabled: testCase.UsesEbusd},
		{name: "gateway-stop", command: runner.options.StopGateway, enabled: true},
	}
	for _, step := range stopPlan {
		appendResult(runner.runPlannedCommand(stopCtx, commandLogPath, step.name, step.command, step.enabled, env))
	}

	if firstError == "" {
		status = actualStatusPassed
	}
	return commands, logFiles, status, firstError, infraReason
}

func (runner *Runner) adapterMuxEnv(testCase TopologyCase) []string {
	cell := testCase.AdapterMux
	return []string{
		"MATRIX_SOUTHBOUND_TRANSPORT=" + string(cell.Southbound),
		"MATRIX_INVARIANT=" + cell.Invariant,
		"MATRIX_INVARIANT_FAMILY=" + cell.Family,
		"MATRIX_COMPOSITION=" + cell.Composition,
		"MATRIX_NORTHBOUND_CLIENT=" + cell.NorthboundClient,
		"MATRIX_HARDWARE_SETUP=" + runner.cellHardwareSetup(cell),
		"MATRIX_SESSION_ID=" + runner.options.SessionID,
		"MATRIX_CANDIDATE=" + runner.options.Candidate,
		"MATRIX_CELL_CLASS=" + string(cell.Class),
		"MATRIX_OUTPUT_DIR=" + runner.options.OutputDir,
		"MATRIX_AD10_PAIRED_CASES=" + strings.Join(cell.PairedCases, ","),
		ebusdCommandAddrEnv + "=" + runner.options.EbusdCommandAddr,
	}
}

// errSmokeEndedBeforeBaseline marks a client that never started because the
// smoke command returned without signalling its baseline.
var errSmokeEndedBeforeBaseline = errors.New("the smoke command ended before it signalled its baseline reading")

// awaitSmokeBaseline waits until the smoke command has created readyPath.
func awaitSmokeBaseline(ctx context.Context, readyPath string, smokeDone <-chan struct{}) error {
	ticker := time.NewTicker(smokeReadyPoll)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(readyPath); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for the smoke command's baseline reading: %w", ctx.Err())
		case <-smokeDone:
			if _, err := os.Stat(readyPath); err == nil {
				return nil
			}
			return errSmokeEndedBeforeBaseline
		case <-ticker.C:
		}
	}
}

// clientNotStarted records a client workload that was never started.
func (runner *Runner) clientNotStarted(testCase TopologyCase, logDir string, cause error) commandResult {
	cell := testCase.AdapterMux
	recordPath := filepath.Join(logDir, "client.json")
	result := commandResult{
		Name:      "client-" + cell.NorthboundClient,
		Command:   cell.Workload,
		LogFile:   filepath.ToSlash(recordPath),
		StartedAt: runner.nowUTC().Format(time.RFC3339),
		Status:    actualStatusFailed,
		Error:     "client not started: " + cause.Error(),
	}
	_ = writeJSON(recordPath, map[string]any{"client": cell.NorthboundClient, "record": nil, "error": result.Error})
	result.EndedAt = runner.nowUTC().Format(time.RFC3339)
	return result
}

func (runner *Runner) writeAdapterMuxConfigs(testCase TopologyCase, verdict CaseVerdict, configDir string) ([]string, error) {
	cell := testCase.AdapterMux
	configFiles := make([]string, 0, 3)

	helianthusConfig := map[string]any{
		"case_id":              testCase.ID,
		"suite":                SuiteAdapterMuxV2,
		"kind":                 testCase.Kind,
		"target":               runner.options.Target,
		"southbound_transport": cell.Southbound,
		"invariant":            cell.Invariant,
		"invariant_family":     cell.Family,
		"composition":          cell.Composition,
		"northbound_client":    cell.NorthboundClient,
		"hardware_setup":       verdict.HardwareSetup,
		"session_id":           verdict.SessionID,
		"candidate":            verdict.Candidate,
		"gateway_transport":    testCase.GatewayTransport,
		"uses_ebusd":           testCase.UsesEbusd,
		"adapter_addr_env":     "MATRIX_ADAPTER_ADDR",
		"mux_enh_addr_env":     "MATRIX_MUX_ENH_ADDR",
		"ebusd_tcp_addr_env":   "MATRIX_EBUSD_TCP_ADDR",
	}
	helianthusPath := filepath.Join(configDir, "helianthus.json")
	if err := writeJSON(helianthusPath, helianthusConfig); err != nil {
		return nil, fmt.Errorf("write helianthus config: %w", err)
	}
	configFiles = append(configFiles, filepath.ToSlash(helianthusPath))

	workloadConfig := map[string]any{
		"case_id":                   testCase.ID,
		"matrix_inventory_version":  AdapterMuxV2InventoryVersion,
		"cell":                      cell,
		"allowlisted_request_types": allowlistedRequestTypes(),
	}
	workloadPath := filepath.Join(configDir, "workload.json")
	if err := writeJSON(workloadPath, workloadConfig); err != nil {
		return nil, fmt.Errorf("write workload config: %w", err)
	}
	configFiles = append(configFiles, filepath.ToSlash(workloadPath))

	if testCase.UsesEbusd {
		upstream, deviceEnv := "mux", "MATRIX_MUX_ENH_ADDR"
		if cell.Composition == CompositionEbusdTCP {
			upstream, deviceEnv = "adapter", "MATRIX_ADAPTER_ADDR"
		}
		ebusdConfig := map[string]any{
			"case_id":         testCase.ID,
			"target":          runner.options.Target,
			"transport":       TransportENH,
			"upstream":        upstream,
			"network_device":  deviceEnv,
			"config_path_env": "MATRIX_EBUSD_CONFIG_PATH",
			"command_port":    "--ebusd-command-addr",
			"scanconfig":      true,
		}
		ebusdPath := filepath.Join(configDir, "ebusd.json")
		if err := writeJSON(ebusdPath, ebusdConfig); err != nil {
			return nil, fmt.Errorf("write ebusd config: %w", err)
		}
		configFiles = append(configFiles, filepath.ToSlash(ebusdPath))
	}
	return configFiles, nil
}

func allowlistedRequestTypes() []RequestType {
	return []RequestType{RequestIdentification, RequestB509Read, RequestB524Read, RequestB555Read}
}

// runClientWorkload drives the cell's northbound client and records its
// result in logs/client.json.
func (runner *Runner) runClientWorkload(ctx context.Context, testCase TopologyCase, logDir string) commandResult {
	cell := testCase.AdapterMux
	recordPath := filepath.Join(logDir, "client.json")
	result := commandResult{
		Name:      "client-" + cell.NorthboundClient,
		Command:   cell.Workload,
		LogFile:   filepath.ToSlash(recordPath),
		StartedAt: runner.nowUTC().Format(time.RFC3339),
	}

	var record any
	var err error
	switch cell.NorthboundClient {
	case NorthboundSyntheticENH:
		record, err = runner.runContenderStimulus(ctx, cell.Invariant)
	case NorthboundEbusd:
		record, err = runner.runEbusdClient(ctx)
	case NorthboundVRCExplorer:
		record, err = runner.runExplorerClient(ctx, logDir)
	default:
		err = fmt.Errorf("unknown northbound client %q", cell.NorthboundClient)
	}
	if writeErr := writeJSON(recordPath, map[string]any{"client": cell.NorthboundClient, "record": record, "error": errorText(err)}); writeErr != nil && err == nil {
		err = writeErr
	}
	result.EndedAt = runner.nowUTC().Format(time.RFC3339)
	if err != nil {
		result.Status = actualStatusFailed
		result.Error = err.Error()
		return result
	}
	result.Status = actualStatusPassed
	return result
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Contender stimuli: each PX cell drives the stimulus of its own invariant
// on the mux ENH listener, then the identification workload, which must
// complete after the stimulus. Every telegram passes ClassifyTelegram before
// the first byte is written; a refused workload never opens a connection.
const (
	// StimulusReplacedBid (PX01): one START, then, once the bid is shown
	// forwarded to the adapter (awaitBidForwarded), a START for a different
	// initiator, so the first bid is replaced. A STARTED or FAILED that
	// arrives before that proof makes the contender bid again; it does not
	// end the stimulus. The workload bids with the replacement initiator. A
	// STARTED or FAILED naming the replaced initiator, or one that no
	// outstanding START explains, fails the cell from the replacement START
	// on, during the workload and until the absorb bound after it.
	StimulusReplacedBid = "replaced-bid"
	// StimulusCancelledBid (PX02): one START, then the session closes once
	// the bid is shown forwarded to the adapter, so the absorb barrier is
	// armed. A STARTED or FAILED before that proof makes the contender bid
	// again. The next session bids with a different initiator: a grant handed
	// out before the barrier absorbed the cancelled bid's result reaches it
	// as a STARTED or FAILED naming the cancelled initiator, or as one that
	// no outstanding START explains, and fails the cell until the absorb
	// bound after the close.
	StimulusCancelledBid = "cancelled-bid"
	// StimulusCommandWithoutCRC (PX03): the contender wins the bus, sends the
	// command up to its length byte and stops before the CRC, so no command
	// ACK can come and the bus closes the telegram with SYN. The contender
	// bids again at once.
	StimulusCommandWithoutCRC = "command-stopped-before-crc"
	// StimulusResponseUnacknowledged (PX04): the contender wins the bus and
	// completes its command, then the target stops mid-response. SYN while
	// response bytes are still missing reopens arbitration, and the next bid
	// is granted from that state. A target that delivers the whole response
	// has not exercised the cell.
	StimulusResponseUnacknowledged = "syn-while-response-missing"
	// StimulusNonOwnerSend (PX12): another session holds an open transaction
	// and this session sends one SYN without owning the bus. The mux answers
	// ERROR_HOST. SYN is the bus's idle symbol, so even a forwarded symbol
	// cannot form a request.
	StimulusNonOwnerSend = "non-owner-send"
)

// contenderStimuli maps each executable PX invariant to its stimulus.
var contenderStimuli = map[string]string{
	"PX01": StimulusReplacedBid,
	"PX02": StimulusCancelledBid,
	"PX03": StimulusCommandWithoutCRC,
	"PX04": StimulusResponseUnacknowledged,
	"PX12": StimulusNonOwnerSend,
}

// StimulusRecord is the client record of one PX cell.
type StimulusRecord struct {
	Invariant string `json:"invariant"`
	Stimulus  string `json:"stimulus"`
	// Observed is what the contender saw as the answer to its stimulus.
	Observed string `json:"observed,omitempty"`
	// Workload is the identification workload run after the stimulus.
	Workload ContenderResult `json:"workload"`
}

var errStimulusNotObserved = errors.New("contender stimulus not answered as the invariant requires")

func (runner *Runner) runContenderStimulus(ctx context.Context, invariant string) (StimulusRecord, error) {
	record := StimulusRecord{Invariant: invariant, Stimulus: contenderStimuli[invariant]}
	source := runner.workload.initiator
	telegrams := IdentificationWorkload(runner.workload.target, contenderTelegramCount)
	record.Workload.Transactions = len(telegrams)
	fail := func(err error) (StimulusRecord, error) {
		if record.Workload.Error == "" {
			record.Workload.Error = err.Error()
		}
		return record, err
	}
	if record.Stimulus == "" {
		return fail(fmt.Errorf("%w: %s has no contender stimulus", ErrWriteGuardRefused, invariant))
	}
	if !protocol.IsInitiatorCapableAddress(source) {
		record.Workload.RefusedBeforeWrite = true
		return fail(fmt.Errorf("%w: source 0x%02X is not an initiator address", ErrWriteGuardRefused, source))
	}
	for _, telegram := range telegrams {
		requestType, err := ClassifyTelegram(telegram)
		if err != nil {
			record.Workload.RefusedBeforeWrite = true
			record.Workload.RequestTypes = nil
			return fail(err)
		}
		record.Workload.RequestTypes = append(record.Workload.RequestTypes, requestType)
	}

	// absorbFrom is when the replaced or cancelled bid left the contender;
	// its result can still arrive until the absorb bound after it.
	var absorbFrom time.Time
	if record.Stimulus == StimulusCancelledBid {
		first, err := runner.openContenderSession(ctx)
		if err != nil {
			return fail(err)
		}
		bidErr := bidUntilInFlight(first, source)
		absorbFrom = time.Now()
		_ = first.conn.Close()
		if bidErr != nil {
			return fail(fmt.Errorf("cancelled bid: %w", bidErr))
		}
		record.Observed = "in-flight bid left for the absorb barrier"
	}

	session, err := runner.openContenderSession(ctx)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = session.conn.Close() }()

	switch record.Stimulus {
	case StimulusReplacedBid:
		// replaceInFlightBid installs the result watch before it writes the
		// replacement START.
		observed, replacedAt, err := replaceInFlightBid(session, source)
		record.Observed = observed
		absorbFrom = replacedAt
		if err != nil {
			return fail(err)
		}
	case StimulusCommandWithoutCRC:
		observed, err := leaveTelegramOpen(session, source, telegrams[0])
		record.Observed = observed
		if err != nil {
			return fail(err)
		}
	case StimulusResponseUnacknowledged:
		observed, err := synWhileResponseMissing(session, source, telegrams[0])
		record.Observed = observed
		if err != nil {
			return fail(err)
		}
	case StimulusNonOwnerSend:
		observed, err := runner.nonOwnerSendDuringActive(ctx, session, source, telegrams[0])
		record.Observed = observed
		if err != nil {
			return fail(err)
		}
	}

	workloadSource := source
	watchAbsorb := record.Stimulus == StimulusReplacedBid || record.Stimulus == StimulusCancelledBid
	if watchAbsorb {
		// The workload must not reuse the initiator of the bid left behind:
		// a late STARTED of that bid would be indistinguishable from the
		// workload's grant.
		workloadSource = otherInitiator(source)
	}
	if record.Stimulus == StimulusCancelledBid {
		session.conn = &resultWatchConn{Conn: session.conn, initiator: source}
	}
	if record.Stimulus == StimulusCancelledBid {
		// The absorb barrier holds the next grant until the cancelled bid's
		// result is absorbed, at the latest until the absorb bound.
		session.timeout = absorbExpiryWait
	}
	for _, telegram := range telegrams {
		failed, err := session.transact(workloadSource, telegram, 5)
		record.Workload.ArbitrationFailed += failed
		if err != nil {
			return fail(err)
		}
		record.Workload.Completed++
	}
	if watchAbsorb {
		// Keep watching until the absorb bound: a result of the bid left
		// behind that arrives after the workload still fails the cell.
		if remaining := time.Until(absorbFrom.Add(muxStartDeadline)); remaining > 0 {
			stray, err := strayArbitrationResult(session, remaining)
			if err != nil {
				return fail(err)
			}
			if stray != "" {
				record.Observed = stray
				return fail(fmt.Errorf("%w: %s", errStimulusNotObserved, stray))
			}
		}
	}
	return record, nil
}

// openContenderSession dials the mux ENH listener and completes INIT.
func (runner *Runner) openContenderSession(ctx context.Context) (*enhContenderSession, error) {
	conn, err := runner.dial(ctx, runner.options.MuxENHAddr)
	if err != nil {
		return nil, fmt.Errorf("dial mux ENH listener: %w", err)
	}
	session := &enhContenderSession{ctx: ctx, conn: conn, timeout: clientStepTimeout}
	if err := session.send(transport.ENHReqInit, 0x00); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := session.await(func(message transport.ENHMessage) (bool, error) {
		return message.Command == transport.ENHResResetted, nil
	}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("await RESETTED after INIT: %w", err)
	}
	return session, nil
}

// otherInitiator is an initiator address different from source, so a second
// START replaces the in-flight bid instead of moving its waiter.
func otherInitiator(source byte) byte {
	for _, candidate := range []byte{0xF1, 0xF3, 0xF7, 0x71, 0x31, 0x13, 0x17, 0x10, 0x30, 0x70, 0xFF} {
		if candidate != source && protocol.IsInitiatorCapableAddress(candidate) {
			return candidate
		}
	}
	return 0xFF
}

// bidForwarded is what the contender saw of an outstanding START.
// answered is a STARTED or FAILED that arrived before the bid was shown
// forwarded, or together with that proof.
type bidForwarded struct {
	answered bool
	message  transport.ENHMessage
}

// infoFence follows the answers to the INFO requests the contender writes
// after its STARTs. The mux answers an INFO with a length frame and that
// many data frames, or with ERROR_HOST when it has no cached answer; the
// answers arrive in request order.
type infoFence struct {
	// requested counts INFO requests whose answer has not completed.
	requested int
	// tail counts the data frames still owed by the answer in progress.
	tail int
	// skip counts full answers to ignore. A bid that was answered before
	// its INFO completed still receives that answer; it must not satisfy
	// the next bid's fence.
	skip int
	// abandoning is set when the open answer's length byte was already
	// consumed and the rest of that answer should be ignored.
	abandoning bool
}

func (fence *infoFence) sent() { fence.requested++ }

// done is true once every requested answer has completed and no abandoned
// answer is still on the way.
func (fence *infoFence) done() bool {
	return fence.requested == 0 && fence.tail == 0 && fence.skip == 0 && !fence.abandoning
}

// abandon marks the outstanding answer, if any, so its bytes do not complete
// a later bid's fence. A bid answered before that answer arrived still has
// the answer coming.
func (fence *infoFence) abandon() {
	if fence.tail > 0 {
		fence.abandoning = true
		fence.requested = 0
		return
	}
	if fence.requested > 0 {
		fence.skip++
		fence.requested = 0
	}
}

// consume takes one INFO frame of an answer and reports whether it was one.
func (fence *infoFence) consume(message transport.ENHMessage) bool {
	if message.Command != transport.ENHResInfo {
		return false
	}
	if fence.tail > 0 {
		fence.tail--
	} else {
		fence.tail = int(message.Data)
	}
	if fence.tail == 0 {
		fence.completeAnswer()
	}
	return true
}

// completeAnswer closes the answer whose last frame just arrived.
func (fence *infoFence) completeAnswer() {
	if fence.abandoning {
		fence.abandoning = false
		return
	}
	if fence.skip > 0 {
		fence.skip--
		return
	}
	if fence.requested > 0 {
		fence.requested--
	}
}

// consumeErrorHost takes an ERROR_HOST that answers an INFO request.
func (fence *infoFence) consumeErrorHost() bool {
	if fence.tail != 0 {
		return false
	}
	if fence.skip > 0 {
		fence.skip--
		return true
	}
	if fence.requested == 0 {
		return false
	}
	fence.requested--
	return true
}

// awaitBidForwarded waits until the outstanding START has been handed to
// the adapter. The mux delivers the selecting SYN before its grant step,
// and it releases the INFO answer only after the grant step whose snapshot
// included this START has returned from RequestStart. A SYN observed while
// that answer is still open is one this bid could have waited for, and the
// answer completing after that SYN is the proof. An answer that finishes
// with no such SYN was released for a bid the snapshot excluded; a SYN
// after that answer, and the symbol after the SYN, leave the bid queued.
// Silence is not proof. A STARTED or FAILED before the proof, or buffered
// with it, is returned as answered, not as an error.
func awaitBidForwarded(session *enhContenderSession, fence *infoFence) (bidForwarded, error) {
	saved := session.timeout
	defer func() { session.timeout = saved }()
	deadline := time.Now().Add(saved)
	queued := fmt.Errorf("%w: START stayed queued: the INFO answer did not complete after a SYN observed since the START", errStimulusNotObserved)
	synSinceStart := false
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return bidForwarded{}, queued
		}
		session.timeout = remaining
		message, err := session.next()
		if err != nil {
			if message.Command == transport.ENHResErrorHost && fence.consumeErrorHost() {
				if synSinceStart && fence.done() {
					return forwardedOrBuffered(session, fence)
				}
				continue
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return bidForwarded{}, queued
			}
			return bidForwarded{}, fmt.Errorf("await the bid to go in flight: %w", err)
		}
		if fence.consume(message) {
			if synSinceStart && fence.done() {
				return forwardedOrBuffered(session, fence)
			}
			continue
		}
		switch message.Command {
		case transport.ENHResReceived:
			if message.Data == protocol.SymbolSyn && !fence.done() {
				synSinceStart = true
			}
		case transport.ENHResStarted, transport.ENHResFailed:
			return bidForwarded{answered: true, message: message}, nil
		case transport.ENHResResetted:
			return bidForwarded{}, fmt.Errorf("%w: RESETTED while the bid was outstanding", errContenderProtocol)
		}
	}
}

// forwardedOrBuffered reports the bid in flight, or a STARTED or FAILED
// that was already buffered with the completing INFO answer.
func forwardedOrBuffered(session *enhContenderSession, fence *infoFence) (bidForwarded, error) {
	if result, ok := bufferedResult(session, fence); ok {
		return bidForwarded{answered: true, message: result}, nil
	}
	return bidForwarded{}, nil
}

// bufferedResult takes the frames already received with the proof that a
// bid was forwarded and returns a STARTED or FAILED among them. A result
// that came in the same read as that proof may have been produced before
// the bid was replaced or abandoned, so it ends the attempt instead.
func bufferedResult(session *enhContenderSession, fence *infoFence) (transport.ENHMessage, bool) {
	for len(session.pending) > 0 {
		message := session.pending[0]
		session.pending = session.pending[1:]
		if fence.consume(message) {
			continue
		}
		if message.Command == transport.ENHResErrorHost {
			fence.consumeErrorHost()
			continue
		}
		if message.Command == transport.ENHResStarted || message.Command == transport.ENHResFailed {
			return message, true
		}
	}
	return transport.ENHMessage{}, false
}

// bidUntilInFlight sends START for initiator, followed by the INFO fence,
// until one bid is shown forwarded with its result still outstanding. A
// bid answered before that proof does not end the stimulus: a STARTED is
// released and the contender bids again, a FAILED is followed by another
// bid.
func bidUntilInFlight(session *enhContenderSession, initiator byte) error {
	var fence infoFence
	for attempt := 0; attempt < bidInFlightAttempts; attempt++ {
		if err := session.send(transport.ENHReqStart, initiator); err != nil {
			return err
		}
		if err := session.send(transport.ENHReqInfo, fenceInfoID); err != nil {
			return err
		}
		fence.sent()
		forwarded, err := awaitBidForwarded(session, &fence)
		if err != nil {
			return err
		}
		if !forwarded.answered {
			return nil
		}
		// The attempt ended on STARTED or FAILED before the fence
		// completed. The answer is still delivered; skip it so it cannot
		// complete the next bid's fence.
		fence.abandon()
		if forwarded.message.Command == transport.ENHResStarted {
			if err := session.send(transport.ENHReqStart, protocol.SymbolSyn); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("%w: each of %d STARTs was answered before it was shown in flight",
		errStimulusNotObserved, bidInFlightAttempts)
}

// replaceInFlightBid leaves one bid in flight and replaces it with a
// different initiator. The result watch is installed before the
// replacement START is written, so a STARTED or FAILED naming the replaced
// initiator fails the cell even when it arrives in the same read as the
// replacement grant. replacedAt is when the replacement START was written.
func replaceInFlightBid(session *enhContenderSession, source byte) (string, time.Time, error) {
	replacement := otherInitiator(source)
	if err := bidUntilInFlight(session, source); err != nil {
		return "", time.Time{}, err
	}
	session.conn = &resultWatchConn{Conn: session.conn, initiator: source}
	replacedAt := time.Now()
	if err := session.send(transport.ENHReqStart, replacement); err != nil {
		return "", replacedAt, err
	}
	message, err := session.await(func(message transport.ENHMessage) (bool, error) {
		switch message.Command {
		case transport.ENHResStarted, transport.ENHResFailed:
			return true, nil
		case transport.ENHResResetted:
			return false, fmt.Errorf("%w: RESETTED while the replacement was outstanding", errContenderProtocol)
		default:
			return false, nil
		}
	})
	if err != nil {
		return "", replacedAt, fmt.Errorf("await the replacement grant: %w", err)
	}
	if message.Command == transport.ENHResFailed || message.Data == source {
		return "", replacedAt, fmt.Errorf("%w: stale arbitration result 0x%X (0x%02X) of the replaced bid",
			errStimulusNotObserved, byte(message.Command), message.Data)
	}
	if message.Command != transport.ENHResStarted || message.Data != replacement {
		return "", replacedAt, fmt.Errorf("%w: replacement grant 0x%X (0x%02X), want STARTED 0x%02X",
			errStimulusNotObserved, byte(message.Command), message.Data, replacement)
	}
	// Release the replacement grant so the identification workload can bid.
	if err := session.send(transport.ENHReqStart, protocol.SymbolSyn); err != nil {
		return "", replacedAt, err
	}
	return "replaced bid absorbed and its late result did not arrive", replacedAt, nil
}

// resultWatchConn fails the read that carries a STARTED or FAILED naming
// initiator, the bid left behind, or one that no outstanding START of this
// connection explains. Such a frame is not passed on, so it cannot be taken
// as the grant of a later bid. Every START other than a SYN cancel written
// through the connection is outstanding until its result is read.
type resultWatchConn struct {
	net.Conn
	initiator   byte
	outstanding int
	readParser  transport.ENHParser
	writeParser transport.ENHParser
	failed      error
}

func (conn *resultWatchConn) Write(p []byte) (int, error) {
	messages, _ := conn.writeParser.Parse(append([]byte(nil), p...))
	for _, message := range messages {
		if message.Command == transport.ENHReqStart && message.Data != protocol.SymbolSyn {
			conn.outstanding++
		}
	}
	return conn.Conn.Write(p)
}

func (conn *resultWatchConn) Read(p []byte) (int, error) {
	if conn.failed != nil {
		return 0, conn.failed
	}
	n, err := conn.Conn.Read(p)
	if n > 0 {
		messages, _ := conn.readParser.Parse(append([]byte(nil), p[:n]...))
		for _, message := range messages {
			if message.Command != transport.ENHResStarted && message.Command != transport.ENHResFailed {
				continue
			}
			switch {
			case message.Data == conn.initiator:
				conn.failed = fmt.Errorf("%w: stale arbitration result 0x%X (0x%02X) of the bid left behind",
					errStimulusNotObserved, byte(message.Command), message.Data)
			case conn.outstanding == 0:
				conn.failed = fmt.Errorf("%w: stale arbitration result 0x%X (0x%02X) that no outstanding START explains",
					errStimulusNotObserved, byte(message.Command), message.Data)
			default:
				conn.outstanding--
				continue
			}
			return 0, conn.failed
		}
	}
	return n, err
}

// leaveTelegramOpen wins the bus, sends the command up to its length byte
// and waits for the bus to close it with SYN before the CRC.
func leaveTelegramOpen(session *enhContenderSession, source byte, telegram GuardTelegram) (string, error) {
	if err := session.send(transport.ENHReqStart, source); err != nil {
		return "", err
	}
	message, err := session.await(func(message transport.ENHMessage) (bool, error) {
		switch message.Command {
		case transport.ENHResStarted:
			return true, nil
		case transport.ENHResFailed, transport.ENHResResetted:
			return false, fmt.Errorf("%w: 0x%X before the open telegram", errContenderProtocol, byte(message.Command))
		default:
			return false, nil
		}
	})
	if err != nil {
		return "", fmt.Errorf("await arbitration: %w", err)
	}
	if message.Data != source {
		return "", fmt.Errorf("%w: STARTED 0x%02X for source 0x%02X", errContenderProtocol, message.Data, source)
	}
	logical := append([]byte{source, telegram.Target, telegram.Primary, telegram.Secondary, byte(len(telegram.Data))}, telegram.Data...)
	for _, symbol := range escapeSymbols(logical[1:]) {
		if err := session.sendSymbol(symbol); err != nil {
			return "", err
		}
	}
	for {
		message, err := session.next()
		if err != nil {
			return "", fmt.Errorf("await the closing SYN: %w", err)
		}
		switch message.Command {
		case transport.ENHResReceived:
			if message.Data == protocol.SymbolSyn {
				return "bus closed the command before its CRC with SYN", nil
			}
		default:
			return "", fmt.Errorf("%w: ENH command 0x%X while the telegram was open", errContenderProtocol, byte(message.Command))
		}
	}
}

// synWhileResponseMissing wins the bus, completes the command and requires
// SYN before the target response is complete. The following bid is the
// identification workload.
func synWhileResponseMissing(session *enhContenderSession, source byte, telegram GuardTelegram) (string, error) {
	if err := session.send(transport.ENHReqStart, source); err != nil {
		return "", err
	}
	message, err := session.await(func(message transport.ENHMessage) (bool, error) {
		switch message.Command {
		case transport.ENHResStarted:
			return true, nil
		case transport.ENHResFailed, transport.ENHResResetted:
			return false, fmt.Errorf("%w: 0x%X before the open telegram", errContenderProtocol, byte(message.Command))
		default:
			return false, nil
		}
	})
	if err != nil {
		return "", fmt.Errorf("await arbitration: %w", err)
	}
	if message.Data != source {
		return "", fmt.Errorf("%w: STARTED 0x%02X for source 0x%02X", errContenderProtocol, message.Data, source)
	}
	logical := append([]byte{source, telegram.Target, telegram.Primary, telegram.Secondary, byte(len(telegram.Data))}, telegram.Data...)
	logical = append(logical, protocol.CRC(logical))
	for _, symbol := range escapeSymbols(logical[1:]) {
		if err := session.sendSymbol(symbol); err != nil {
			return "", err
		}
	}
	ack, err := session.receive()
	if err != nil {
		return "", fmt.Errorf("await target ACK: %w", err)
	}
	if ack != protocol.SymbolAck {
		return "", fmt.Errorf("target answered 0x%02X instead of ACK", ack)
	}
	got, need := 0, -1
	for {
		message, err := session.next()
		if err != nil {
			return "", fmt.Errorf("await SYN while response bytes are missing: %w", err)
		}
		if message.Command != transport.ENHResReceived {
			return "", fmt.Errorf("%w: ENH command 0x%X while waiting for response bytes", errContenderProtocol, byte(message.Command))
		}
		if message.Data == protocol.SymbolSyn {
			if need >= 0 && got >= need {
				return "", fmt.Errorf("%w: target delivered its response before SYN", errStimulusNotObserved)
			}
			return "SYN while response bytes were still missing", nil
		}
		got++
		if got == 1 {
			need = 1 + int(message.Data) + 1
		}
		if need > 0 && got >= need {
			return "", fmt.Errorf("%w: target delivered its response before SYN", errStimulusNotObserved)
		}
	}
}

// nonOwnerSendDuringActive has another session hold an open transaction,
// then sends one SYN from contender. The mux must refuse that SEND.
func (runner *Runner) nonOwnerSendDuringActive(ctx context.Context, contender *enhContenderSession, source byte, telegram GuardTelegram) (string, error) {
	owner, err := runner.openContenderSession(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = owner.conn.Close() }()
	if err := owner.send(transport.ENHReqStart, source); err != nil {
		return "", err
	}
	message, err := owner.await(func(message transport.ENHMessage) (bool, error) {
		switch message.Command {
		case transport.ENHResStarted:
			return true, nil
		case transport.ENHResFailed, transport.ENHResResetted:
			return false, fmt.Errorf("%w: 0x%X before the active transaction", errContenderProtocol, byte(message.Command))
		default:
			return false, nil
		}
	})
	if err != nil {
		return "", fmt.Errorf("await the active owner: %w", err)
	}
	if message.Data != source {
		return "", fmt.Errorf("%w: STARTED 0x%02X for source 0x%02X", errContenderProtocol, message.Data, source)
	}
	logical := append([]byte{source, telegram.Target, telegram.Primary, telegram.Secondary, byte(len(telegram.Data))}, telegram.Data...)
	symbols := escapeSymbols(logical[1:])
	if len(symbols) == 0 {
		return "", fmt.Errorf("%w: active transaction has no command byte", errContenderProtocol)
	}
	if err := owner.sendSymbol(symbols[0]); err != nil {
		return "", err
	}
	return sendWithoutOwnership(contender)
}

// sendWithoutOwnership sends one SYN symbol without owning the bus and
// expects the mux to refuse it with ERROR_HOST while another session holds
// the bus. The refusal must come within one step timeout of the SEND: bus
// traffic that keeps arriving does not extend the wait.
func sendWithoutOwnership(session *enhContenderSession) (string, error) {
	if err := session.send(transport.ENHReqSend, protocol.SymbolSyn); err != nil {
		return "", err
	}
	saved := session.timeout
	defer func() { session.timeout = saved }()
	deadline := time.Now().Add(saved)
	traffic := false
	unanswered := func() error {
		why := fmt.Sprintf("no ERROR_HOST within %s of the SEND", saved)
		if traffic {
			why += " while bus symbols kept arriving"
		}
		return fmt.Errorf("await the refusal of a non-owner SEND: %w: %s", errStimulusNotObserved, why)
	}
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", unanswered()
		}
		session.timeout = remaining
		message, err := session.next()
		if err != nil {
			if message.Command == transport.ENHResErrorHost {
				return "non-owner SEND refused with ERROR_HOST during an active transaction", nil
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return "", unanswered()
			}
			return "", fmt.Errorf("await the refusal of a non-owner SEND: %w", err)
		}
		switch message.Command {
		case transport.ENHResReceived:
			// Bus traffic of other owners keeps arriving; a forwarded SYN
			// cannot be told apart from it, so only the refusal counts, and
			// the traffic does not extend the wait.
			traffic = true
		default:
			return "", fmt.Errorf("%w: ENH command 0x%X instead of the refusal of a non-owner SEND", errStimulusNotObserved, byte(message.Command))
		}
	}
}

// strayArbitrationResult listens for window and reports a STARTED or FAILED
// that no bid of the session explains.
func strayArbitrationResult(session *enhContenderSession, window time.Duration) (string, error) {
	saved := session.timeout
	defer func() { session.timeout = saved }()
	deadline := time.Now().Add(window)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", nil
		}
		session.timeout = remaining
		message, err := session.next()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return "", nil
			}
			return "", err
		}
		switch message.Command {
		case transport.ENHResStarted, transport.ENHResFailed:
			return fmt.Sprintf("stale arbitration result 0x%X (0x%02X) after the workload", byte(message.Command), message.Data), nil
		}
	}
}

func (runner *Runner) runEbusdClient(ctx context.Context) (EbusdWorkloadResult, error) {
	commands := EbusdIdentificationCommands(runner.workload.target)
	for _, command := range commands {
		if err := CheckEbusdCommand(command); err != nil {
			return EbusdWorkloadResult{Commands: commands, RefusedBeforeWrite: true, Error: err.Error()}, err
		}
	}
	conn, err := runner.dial(ctx, runner.options.EbusdCommandAddr)
	if err != nil {
		return EbusdWorkloadResult{}, fmt.Errorf("dial ebusd command port: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return RunEbusdWorkload(ctx, conn, commands, clientStepTimeout)
}

// ExplorerRecord is the record of one VRC Explorer scan through the guarded
// ebusd command relay.
type ExplorerRecord struct {
	Args      []string `json:"args"`
	LogFile   string   `json:"log_file"`
	ExitCode  int      `json:"exit_code"`
	Forwarded int      `json:"forwarded"`
	Refused   int      `json:"refused"`
	Refusals  []string `json:"refusals,omitempty"`
}

// explorerScanArgs is the only VRC Explorer invocation the harness runs: a
// read-only scan whose sole endpoint is the guarded relay.
func explorerScanArgs(host string, port string, initiator byte) []string {
	return []string{
		"scan",
		"--transport", "tcp",
		"--host", host,
		"--port", port,
		"--initiator-address", fmt.Sprintf("0x%02X", initiator),
		"--preset", "recommended",
	}
}

func (runner *Runner) runExplorerClient(ctx context.Context, logDir string) (ExplorerRecord, error) {
	record := ExplorerRecord{ExitCode: -1}
	listener, err := runner.listen("tcp", "127.0.0.1:0")
	if err != nil {
		return record, fmt.Errorf("listen for guarded relay: %w", err)
	}
	defer func() { _ = listener.Close() }()
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return record, err
	}

	relayCtx, cancelRelay := context.WithCancel(ctx)
	stats := &GuardedRelayStats{}
	var relays sync.WaitGroup
	relays.Add(1)
	go func() {
		defer relays.Done()
		for {
			client, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			relays.Add(1)
			go func() {
				defer relays.Done()
				_ = ServeGuardedEbusdRelay(relayCtx, client, func(dialCtx context.Context) (net.Conn, error) {
					return runner.dial(dialCtx, runner.options.EbusdCommandAddr)
				}, stats)
			}()
		}
	}()

	record.Args = explorerScanArgs(host, port, runner.workload.initiator)
	logPath := filepath.Join(logDir, "vrc-explorer.log")
	record.LogFile = filepath.ToSlash(logPath)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		cancelRelay()
		_ = listener.Close()
		relays.Wait()
		return record, err
	}
	cmd := exec.CommandContext(ctx, runner.options.ExplorerCommand, record.Args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	runErr := cmd.Run()
	_ = logFile.Close()
	if cmd.ProcessState != nil {
		record.ExitCode = cmd.ProcessState.ExitCode()
	}

	_ = listener.Close()
	cancelRelay()
	relays.Wait()
	record.Forwarded, record.Refused, record.Refusals = stats.Snapshot()

	if runErr != nil {
		return record, fmt.Errorf("vrc explorer scan: %w", runErr)
	}
	if record.Refused > 0 {
		return record, fmt.Errorf("%w: %d explorer command(s) refused", ErrWriteGuardRefused, record.Refused)
	}
	return record, nil
}
