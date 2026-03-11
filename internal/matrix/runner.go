package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type RunnerOptions struct {
	OutputDir        string
	Target           string
	Suite            string
	IncludeIDs       []string
	ExpectedFailures map[string]string
	Execute          bool
	SettleDelay      time.Duration
	CaseTimeout      time.Duration
	StartGateway     string
	StopGateway      string
	StartProxy       string
	StopProxy        string
	StartEbusd       string
	StopEbusd        string
	SmokeCommand     string
}

type commandResult struct {
	Name      string `json:"name"`
	Command   string `json:"command"`
	LogFile   string `json:"log_file"`
	Status    string `json:"status"`
	ExitCode  int    `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
	EndedAt   string `json:"ended_at,omitempty"`
}

type CaseVerdict struct {
	CaseID      string          `json:"case_id"`
	Suite       string          `json:"suite,omitempty"`
	Kind        TopologyKind    `json:"kind"`
	PassiveMode string          `json:"passive_mode,omitempty"`
	Target      string          `json:"target"`
	Status      string          `json:"status"`
	Outcome     string          `json:"outcome"`
	InfraReason string          `json:"infra_reason,omitempty"`
	Expected    string          `json:"expected"`
	Expectation string          `json:"expectation,omitempty"`
	Error       string          `json:"error,omitempty"`
	StartedAt   string          `json:"started_at"`
	EndedAt     string          `json:"ended_at"`
	ConfigFiles []string        `json:"config_files"`
	LogFiles    []string        `json:"log_files"`
	Commands    []commandResult `json:"commands"`
}

type Runner struct {
	options RunnerOptions
	nowUTC  func() time.Time
}

const (
	actualStatusPlanned = "planned"
	actualStatusPassed  = "passed"
	actualStatusFailed  = "failed"

	expectedStatusPass = "pass"
	expectedStatusFail = "fail"

	caseOutcomePlanned = "planned"
	caseOutcomePass    = "pass"
	caseOutcomeFail    = "fail"
	caseOutcomeXFail   = "xfail"
	caseOutcomeXPass   = "xpass"
	caseOutcomeBlocked = "blocked-infra"

	infraReasonAdapterNoSignal = "adapter_no_signal"
	cleanupCommandTimeout      = 30 * time.Second
)

func NewRunner(options RunnerOptions) (*Runner, error) {
	if strings.TrimSpace(options.OutputDir) == "" {
		options.OutputDir = "results"
	}
	options.Target = strings.TrimSpace(strings.ToLower(options.Target))
	if options.Target == "" {
		options.Target = "local"
	}
	if options.Target != "local" && options.Target != "ha-addon" {
		return nil, fmt.Errorf("unsupported target %q (allowed: local, ha-addon)", options.Target)
	}
	options.Suite = normalizeSuite(options.Suite)
	if _, err := CasesForSuite(options.Suite); err != nil {
		return nil, err
	}
	if options.SettleDelay < 0 {
		return nil, fmt.Errorf("settle delay must be >= 0")
	}
	if options.CaseTimeout < 0 {
		return nil, fmt.Errorf("case timeout must be >= 0")
	}
	options.ExpectedFailures = normalizeExpectedFailures(options.ExpectedFailures)
	return &Runner{
		options: options,
		nowUTC: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (runner *Runner) Run(ctx context.Context) ([]CaseVerdict, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	allCases, err := CasesForSuite(runner.options.Suite)
	if err != nil {
		return nil, err
	}
	cases := FilterCases(allCases, runner.options.IncludeIDs)
	if len(cases) == 0 {
		return nil, fmt.Errorf("no topology cases selected")
	}

	if err := os.MkdirAll(runner.options.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	verdicts := make([]CaseVerdict, 0, len(cases))
	for _, testCase := range cases {
		select {
		case <-ctx.Done():
			return verdicts, ctx.Err()
		default:
		}

		verdict, runErr := runner.runCase(ctx, testCase)
		verdicts = append(verdicts, verdict)
		if runErr != nil {
			return verdicts, runErr
		}
	}

	indexPath := filepath.Join(runner.options.OutputDir, "index.json")
	if err := writeJSON(indexPath, map[string]any{
		"generated_at": runner.nowUTC().Format(time.RFC3339),
		"target":       runner.options.Target,
		"suite":        runner.options.Suite,
		"cases":        verdicts,
	}); err != nil {
		return verdicts, fmt.Errorf("write matrix index: %w", err)
	}

	return verdicts, nil
}

func (runner *Runner) runCase(ctx context.Context, testCase TopologyCase) (CaseVerdict, error) {
	startedAt := runner.nowUTC()
	caseDir := filepath.Join(runner.options.OutputDir, testCase.ID)
	configDir := filepath.Join(caseDir, "configs")
	logDir := filepath.Join(caseDir, "logs")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return CaseVerdict{}, fmt.Errorf("create config dir: %w", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return CaseVerdict{}, fmt.Errorf("create log dir: %w", err)
	}

	configFiles, err := runner.writeCaseConfigs(testCase, configDir)
	if err != nil {
		return CaseVerdict{}, err
	}

	commandLogPath := filepath.Join(logDir, "runner.log")
	logFiles := []string{filepath.ToSlash(commandLogPath)}
	commands := make([]commandResult, 0, 8)
	status := actualStatusPlanned
	var firstError string
	var infraReason string
	expected := expectedStatusPass
	expectation := defaultExpectedFailure(testCase)
	if expectation != "" {
		expected = expectedStatusFail
	}
	if reason, ok := runner.options.ExpectedFailures[testCase.ID]; ok {
		expected = expectedStatusFail
		expectation = reason
	}

	if !runner.options.Execute {
		planned := []string{
			runner.options.StartGateway,
			runner.options.StartProxy,
			runner.options.StartEbusd,
			runner.options.SmokeCommand,
			runner.options.StopEbusd,
			runner.options.StopProxy,
			runner.options.StopGateway,
		}
		lines := make([]string, 0, len(planned)+2)
		lines = append(lines, "dry-run mode: no command executed")
		for _, value := range planned {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				continue
			}
			lines = append(lines, trimmed)
		}
		if err := os.WriteFile(commandLogPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			return CaseVerdict{}, fmt.Errorf("write dry-run log: %w", err)
		}
	} else {
		caseCtx := ctx
		var cancel context.CancelFunc
		if runner.options.CaseTimeout > 0 {
			caseCtx, cancel = context.WithTimeout(ctx, runner.options.CaseTimeout)
		}
		if cancel != nil {
			defer cancel()
		}

		env := buildCaseEnv(testCase, runner.options.Target, runner.options.Suite, caseDir, configDir, logDir)
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
			{name: "proxy-start", command: runner.options.StartProxy, enabled: testCase.UsesProxy},
			{name: "ebusd-start", command: runner.options.StartEbusd, enabled: testCase.UsesEbusd},
		}
		stopPlan := []struct {
			name    string
			command string
			enabled bool
		}{
			{name: "ebusd-stop", command: runner.options.StopEbusd, enabled: testCase.UsesEbusd},
			{name: "proxy-stop", command: runner.options.StopProxy, enabled: testCase.UsesProxy},
			{name: "gateway-stop", command: runner.options.StopGateway, enabled: true},
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
			smokeResult := runner.runPlannedCommand(
				caseCtx,
				commandLogPath,
				"smoke",
				runner.options.SmokeCommand,
				strings.TrimSpace(runner.options.SmokeCommand) != "",
				env,
			)
			appendResult(smokeResult)
		}

		stopCtx, stopCancel := context.WithTimeout(context.Background(), cleanupCommandTimeout)
		defer stopCancel()
		for _, step := range stopPlan {
			result := runner.runPlannedCommand(stopCtx, commandLogPath, step.name, step.command, step.enabled, env)
			appendResult(result)
		}

		if firstError == "" {
			status = actualStatusPassed
		}
	}

	outcome := classifyCaseOutcome(status, expected, infraReason)

	verdict := CaseVerdict{
		CaseID:      testCase.ID,
		Suite:       runner.options.Suite,
		Kind:        testCase.Kind,
		PassiveMode: testCase.PassiveMode,
		Target:      runner.options.Target,
		Status:      status,
		Outcome:     outcome,
		InfraReason: infraReason,
		Expected:    expected,
		Expectation: expectation,
		Error:       firstError,
		StartedAt:   startedAt.Format(time.RFC3339),
		EndedAt:     runner.nowUTC().Format(time.RFC3339),
		ConfigFiles: configFiles,
		LogFiles:    uniqueStrings(logFiles),
		Commands:    commands,
	}

	verdictPath := filepath.Join(caseDir, "verdict.json")
	if err := writeJSON(verdictPath, verdict); err != nil {
		return CaseVerdict{}, fmt.Errorf("write verdict: %w", err)
	}
	return verdict, nil
}

func (runner *Runner) writeCaseConfigs(testCase TopologyCase, configDir string) ([]string, error) {
	configFiles := make([]string, 0, 3)

	helianthusConfig := map[string]any{
		"case_id":            testCase.ID,
		"suite":              runner.options.Suite,
		"kind":               testCase.Kind,
		"passive_mode":       testCase.PassiveMode,
		"target":             runner.options.Target,
		"gateway_transport":  testCase.GatewayTransport,
		"uses_proxy":         testCase.UsesProxy,
		"uses_ebusd":         testCase.UsesEbusd,
		"ebusd_via_proxy":    testCase.EbusdViaProxy,
		"adapter_addr_env":   "MATRIX_ADAPTER_ADDR",
		"proxy_addr_env":     "MATRIX_PROXY_ADDR",
		"ebusd_tcp_addr_env": "MATRIX_EBUSD_TCP_ADDR",
	}
	helianthusPath := filepath.Join(configDir, "helianthus.json")
	if err := writeJSON(helianthusPath, helianthusConfig); err != nil {
		return nil, fmt.Errorf("write helianthus config: %w", err)
	}
	configFiles = append(configFiles, filepath.ToSlash(helianthusPath))

	if testCase.UsesProxy {
		proxyConfig := map[string]any{
			"case_id":               testCase.ID,
			"target":                runner.options.Target,
			"southbound_transport":  testCase.ProxyTransport,
			"southbound_addr_env":   "MATRIX_ADAPTER_ADDR",
			"northbound_enh_env":    "MATRIX_PROXY_ENH_ADDR",
			"northbound_ens_env":    "MATRIX_PROXY_ENS_ADDR",
			"northbound_tcp_env":    "MATRIX_PROXY_TCP_ADDR",
			"northbound_udp_env":    "MATRIX_PROXY_UDP_ADDR",
			"max_parallel_sessions": 2,
		}
		proxyPath := filepath.Join(configDir, "proxy.json")
		if err := writeJSON(proxyPath, proxyConfig); err != nil {
			return nil, fmt.Errorf("write proxy config: %w", err)
		}
		configFiles = append(configFiles, filepath.ToSlash(proxyPath))
	}

	if testCase.UsesEbusd {
		target := "adapter"
		deviceEnv := "MATRIX_ADAPTER_ADDR"
		if testCase.EbusdViaProxy {
			target = "proxy"
			switch testCase.EbusdTransport {
			case TransportUDPPlain:
				deviceEnv = "MATRIX_PROXY_UDP_ADDR"
			case TransportTCPPlain:
				deviceEnv = "MATRIX_PROXY_TCP_ADDR"
			case TransportENH:
				deviceEnv = "MATRIX_PROXY_ENH_ADDR"
			default:
				deviceEnv = "MATRIX_PROXY_ENS_ADDR"
			}
		}
		ebusdConfig := map[string]any{
			"case_id":          testCase.ID,
			"target":           runner.options.Target,
			"transport":        testCase.EbusdTransport,
			"upstream":         target,
			"network_device":   deviceEnv,
			"config_path_env":  "MATRIX_EBUSD_CONFIG_PATH",
			"mqtt_host_env":    "MATRIX_MQTT_HOST",
			"mqtt_port_env":    "MATRIX_MQTT_PORT",
			"http_port_env":    "MATRIX_EBUSD_HTTP_PORT",
			"scanconfig":       true,
			"commandline_opts": "--enablehex",
		}
		ebusdPath := filepath.Join(configDir, "ebusd.json")
		if err := writeJSON(ebusdPath, ebusdConfig); err != nil {
			return nil, fmt.Errorf("write ebusd config: %w", err)
		}
		configFiles = append(configFiles, filepath.ToSlash(ebusdPath))
	}

	return configFiles, nil
}

func (runner *Runner) runPlannedCommand(
	ctx context.Context,
	logFilePath string,
	name string,
	command string,
	enabled bool,
	env []string,
) commandResult {
	result := commandResult{
		Name:    name,
		Command: strings.TrimSpace(command),
		LogFile: filepath.ToSlash(logFilePath),
		Status:  "skipped",
	}

	if !enabled {
		return result
	}
	if result.Command == "" {
		result.Status = "failed"
		result.Error = fmt.Sprintf("%s command is empty", name)
		return result
	}

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() {
		_ = logFile.Close()
	}()

	start := runner.nowUTC()
	result.StartedAt = start.Format(time.RFC3339)
	_, _ = fmt.Fprintf(logFile, "\n[%s] %s\n", name, result.Command)

	cmd := exec.CommandContext(ctx, "/bin/sh", "-lc", result.Command)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	runErr := cmd.Run()

	end := runner.nowUTC()
	result.EndedAt = end.Format(time.RFC3339)
	if runErr == nil {
		result.Status = "passed"
		return result
	}

	result.Status = "failed"
	result.Error = runErr.Error()
	if exitErr, ok := runErr.(*exec.ExitError); ok {
		result.ExitCode = exitErr.ExitCode()
	}
	return result
}

func buildCaseEnv(
	testCase TopologyCase,
	target string,
	suite string,
	caseDir string,
	configDir string,
	logDir string,
) []string {
	return []string{
		"MATRIX_CASE_ID=" + testCase.ID,
		"MATRIX_SUITE=" + normalizeSuite(suite),
		"MATRIX_CASE_KIND=" + string(testCase.Kind),
		"MATRIX_PASSIVE_MODE=" + testCase.PassiveMode,
		"MATRIX_TARGET=" + target,
		"MATRIX_CASE_DIR=" + caseDir,
		"MATRIX_CONFIG_DIR=" + configDir,
		"MATRIX_LOG_DIR=" + logDir,
		"MATRIX_GATEWAY_TRANSPORT=" + string(testCase.GatewayTransport),
		"MATRIX_PROXY_TRANSPORT=" + string(testCase.ProxyTransport),
		"MATRIX_EBUSD_TRANSPORT=" + string(testCase.EbusdTransport),
		"MATRIX_USES_PROXY=" + boolString(testCase.UsesProxy),
		"MATRIX_USES_EBUSD=" + boolString(testCase.UsesEbusd),
		"MATRIX_EBUSD_VIA_PROXY=" + boolString(testCase.EbusdViaProxy),
	}
}

func boolString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		filtered = append(filtered, trimmed)
	}
	return filtered
}

func classifyCaseOutcome(actualStatus string, expectedStatus string, infraReason string) string {
	if actualStatus == actualStatusPlanned {
		return caseOutcomePlanned
	}
	if strings.TrimSpace(infraReason) != "" {
		return caseOutcomeBlocked
	}
	if expectedStatus == expectedStatusFail {
		if actualStatus == actualStatusFailed {
			return caseOutcomeXFail
		}
		if actualStatus == actualStatusPassed {
			return caseOutcomeXPass
		}
	}
	if actualStatus == actualStatusPassed {
		return caseOutcomePass
	}
	return caseOutcomeFail
}

func inferInfraReasonFromLog(logFilePath string) string {
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		return ""
	}
	lower := strings.ToLower(string(data))
	if strings.Contains(lower, "adapter preflight failed: ebus signal is not acquired") {
		return infraReasonAdapterNoSignal
	}
	if strings.Contains(lower, "adapter reports ebus signal: no signal") {
		return infraReasonAdapterNoSignal
	}
	if strings.Contains(lower, "ebus signal: no signal") {
		return infraReasonAdapterNoSignal
	}
	return ""
}

func normalizeExpectedFailures(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(values))
	for key, value := range values {
		caseID := strings.TrimSpace(strings.ToUpper(key))
		if caseID == "" {
			continue
		}
		reason := strings.TrimSpace(value)
		if reason == "" {
			reason = "known limitation"
		}
		normalized[caseID] = reason
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func defaultExpectedFailure(testCase TopologyCase) string {
	if testCase.Kind == TopologyViaEbusdTCP {
		if testCase.EbusdTransport == TransportUDPPlain || testCase.EbusdTransport == TransportTCPPlain {
			return "ebusd direct udp/tcp to adapter reports no signal in matrix runs"
		}
		return ""
	}
	if testCase.Kind == TopologyProxyDual {
		if testCase.ProxyTransport == TransportUDPPlain {
			return "proxy dual-client with southbound udp reports no signal (ens/enh clients also show host comm framing)"
		}
		if testCase.ProxyTransport == TransportTCPPlain {
			return "proxy dual-client with southbound tcp reports no signal (ens/enh clients also show host comm framing)"
		}
		if testCase.EbusdTransport == TransportUDPPlain {
			return "proxy dual-client with ebusd northbound udp reports no signal"
		}
	}
	return ""
}
