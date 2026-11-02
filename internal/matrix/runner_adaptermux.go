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
)

const (
	actualStatusReserved = "reserved"

	contenderTelegramCount = 3
	clientStepTimeout      = 5 * time.Second
)

// labelPattern bounds the opaque session, candidate and setup labels.
var labelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

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
	env := append(
		buildCaseEnv(testCase, runner.options.Target, runner.options.Suite, caseDir, configDir, logDir),
		runner.adapterMuxEnv(testCase)...,
	)
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
		if hasClient {
			wait.Add(1)
			go func() {
				defer wait.Done()
				clientResult = runner.runClientWorkload(caseCtx, testCase, logDir)
			}()
		}
		smokeResult := runner.runPlannedCommand(caseCtx, commandLogPath, "smoke", runner.options.SmokeCommand, true, env)
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
	}
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
		record, err = runner.runContenderClient(ctx)
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

func (runner *Runner) runContenderClient(ctx context.Context) (ContenderResult, error) {
	config := ContenderConfig{
		Source:      runner.workload.initiator,
		Telegrams:   IdentificationWorkload(runner.workload.target, contenderTelegramCount),
		StepTimeout: clientStepTimeout,
	}
	// The guard also runs before dialing, so a refused workload never opens
	// a connection to the listener.
	for _, telegram := range config.Telegrams {
		if _, err := ClassifyTelegram(telegram); err != nil {
			return ContenderResult{RefusedBeforeWrite: true, Error: err.Error()}, err
		}
	}
	conn, err := runner.dial(ctx, runner.options.MuxENHAddr)
	if err != nil {
		return ContenderResult{}, fmt.Errorf("dial mux ENH listener: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return RunSyntheticContender(ctx, conn, config)
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
