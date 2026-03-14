package ebusgateway

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	defaultEnhTimeoutSec    = 10
	defaultScanTimeoutSec   = 5
	defaultMethodTimeoutSec = 10
)

var errSmokeConfigMissing = errors.New("smoke config missing")

type smokeConfig struct {
	ENH             enhConfig        `yaml:"enh"`
	ExpectedDevices []expectedDevice `yaml:"expected_devices"`
	Smoke           smokeBehavior    `yaml:"smoke"`
}

type enhConfig struct {
	Type       string `yaml:"type"`
	Path       string `yaml:"path"`
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	TimeoutSec int    `yaml:"timeout_sec"`
}

type smokeBehavior struct {
	Profile                       string  `yaml:"profile"`
	VerboseFrames                 bool    `yaml:"verbose_frames"`
	ScanTimeoutSec                int     `yaml:"scan_timeout_sec"`
	MethodTimeoutSec              int     `yaml:"method_timeout_sec"`
	SourceAddress                 hexByte `yaml:"source_address"`
	WireLogPath                   string  `yaml:"wire_log_path"`
	ReportJSONOutput              string  `yaml:"report_json_output"`
	RegisterDumpTSP               string  `yaml:"register_dump_tsp"`
	RegisterDumpTarget            hexByte `yaml:"register_dump_target"`
	RegisterDumpOutput            string  `yaml:"register_dump_output"`
	RegisterDumpJSONOutput        string  `yaml:"register_dump_json_output"`
	RegisterDumpUploadURL         string  `yaml:"register_dump_upload_url"`
	RegisterDumpTimeoutSec        int     `yaml:"register_dump_timeout_sec"`
	RegisterDumpLimit             int     `yaml:"register_dump_limit"`
	IdentifyB50928xx              bool    `yaml:"identify_b509_28xx"`
	RegisterDumpRetryEmpty        bool    `yaml:"register_dump_retry_empty"`
	RegisterDumpRetryDelay        int     `yaml:"register_dump_retry_delay_ms"`
	RegisterDumpProbe             bool    `yaml:"register_dump_probe"`
	RegisterDumpProbeStart        hexWord `yaml:"register_dump_probe_start"`
	RegisterDumpProbeEnd          hexWord `yaml:"register_dump_probe_end"`
	RegisterDumpProbeGroup        hexByte `yaml:"register_dump_probe_group"`
	RegisterDumpProbeInst         hexByte `yaml:"register_dump_probe_instance"`
	RegisterDumpProbeMethod       string  `yaml:"register_dump_probe_method"`
	RegisterDumpProbeTimeout      int     `yaml:"register_dump_probe_timeout_ms"`
	RegisterDumpProbeDelay        int     `yaml:"register_dump_probe_delay_ms"`
	RegisterDumpProbeOutput       string  `yaml:"register_dump_probe_output"`
	RegisterDumpProbeOnly         bool    `yaml:"register_dump_probe_only"`
	RegisterDumpProbeManufacturer string  `yaml:"register_dump_probe_manufacturer"`
}

type expectedDevice struct {
	Address      hexByte `yaml:"address"`
	Description  string  `yaml:"description"`
	Manufacturer string  `yaml:"manufacturer"`
	DeviceID     string  `yaml:"device_id"`
	SWVersion    string  `yaml:"sw_version"`
	HWVersion    string  `yaml:"hw_version"`
}

type hexByte byte

func (b *hexByte) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.ScalarNode {
		return fmt.Errorf("address must be a scalar")
	}
	value := strings.TrimSpace(node.Value)
	if value == "" {
		return fmt.Errorf("address empty")
	}
	parsed, err := strconv.ParseInt(value, 0, 16)
	if err != nil {
		return fmt.Errorf("address %q invalid: %w", value, err)
	}
	if parsed < 0 || parsed > 0xFF {
		return fmt.Errorf("address %q out of range", value)
	}
	*b = hexByte(byte(parsed))
	return nil
}

type hexWord uint16

func (w *hexWord) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.ScalarNode {
		return fmt.Errorf("address must be a scalar")
	}
	value := strings.TrimSpace(node.Value)
	if value == "" {
		return fmt.Errorf("address empty")
	}
	parsed, err := strconv.ParseInt(value, 0, 16)
	if err != nil {
		return err
	}
	if parsed < 0 || parsed > 0xFFFF {
		return fmt.Errorf("address out of range: %s", value)
	}
	*w = hexWord(parsed)
	return nil
}

func (w hexWord) Uint16() uint16 {
	return uint16(w)
}

func (b hexByte) Byte() byte {
	return byte(b)
}

func loadSmokeConfig(rootDir string) (smokeConfig, string, error) {
	if rootDir == "" {
		var err error
		rootDir, err = findRepoRoot()
		if err != nil {
			return smokeConfig{}, "", err
		}
	}
	paths := []string{
		filepath.Join(rootDir, "AGENT-local.md"),
		filepath.Join(rootDir, "AGENTS-local.md"),
	}
	var lastErr error
	for _, path := range paths {
		cfg, err := loadSmokeConfigFile(path)
		if err == nil {
			return cfg, path, nil
		}
		lastErr = err
		if !errors.Is(err, errSmokeConfigMissing) {
			return smokeConfig{}, path, err
		}
	}
	return smokeConfig{}, paths[0], lastErr
}

func loadSmokeConfigFile(path string) (smokeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return smokeConfig{}, fmt.Errorf("%w: %s", errSmokeConfigMissing, path)
		}
		return smokeConfig{}, err
	}

	yamlText := extractYAMLBlocks(string(data))
	if strings.TrimSpace(yamlText) == "" {
		return smokeConfig{}, fmt.Errorf("smoke config missing yaml blocks: %s", path)
	}

	var cfg smokeConfig
	if err := yaml.Unmarshal([]byte(yamlText), &cfg); err != nil {
		return smokeConfig{}, fmt.Errorf("smoke config parse: %w", err)
	}

	if err := cfg.normalize(); err != nil {
		return smokeConfig{}, err
	}
	return cfg, nil
}

func (cfg *smokeConfig) normalize() error {
	cfg.ENH.Type = strings.ToLower(strings.TrimSpace(cfg.ENH.Type))
	cfg.ENH.Path = strings.TrimSpace(cfg.ENH.Path)
	cfg.ENH.Host = strings.TrimSpace(cfg.ENH.Host)
	cfg.Smoke.Profile = strings.ToLower(strings.TrimSpace(cfg.Smoke.Profile))
	cfg.Smoke.WireLogPath = strings.TrimSpace(cfg.Smoke.WireLogPath)
	cfg.Smoke.ReportJSONOutput = strings.TrimSpace(cfg.Smoke.ReportJSONOutput)
	cfg.Smoke.RegisterDumpTSP = strings.TrimSpace(cfg.Smoke.RegisterDumpTSP)
	cfg.Smoke.RegisterDumpOutput = strings.TrimSpace(cfg.Smoke.RegisterDumpOutput)
	cfg.Smoke.RegisterDumpJSONOutput = strings.TrimSpace(cfg.Smoke.RegisterDumpJSONOutput)

	if cfg.Smoke.Profile == "" {
		cfg.Smoke.Profile = string(TransportENH)
	}
	switch cfg.Smoke.Profile {
	case string(TransportENH), string(TransportENS), string(TransportEbusdTCP):
	case "ebusd":
		cfg.Smoke.Profile = string(TransportEbusdTCP)
	default:
		return fmt.Errorf("smoke config unsupported smoke.profile %q (allowed: enh, ens, ebusd-tcp)", cfg.Smoke.Profile)
	}

	if cfg.ENH.Type == "" {
		return fmt.Errorf("smoke config missing enh.type")
	}
	switch cfg.ENH.Type {
	case "unix":
		if cfg.ENH.Path == "" {
			return fmt.Errorf("smoke config missing enh.path")
		}
	case "tcp":
		if cfg.ENH.Host == "" {
			return fmt.Errorf("smoke config missing enh.host")
		}
		if cfg.ENH.Port <= 0 || cfg.ENH.Port > 65535 {
			return fmt.Errorf("smoke config invalid enh.port")
		}
	default:
		return fmt.Errorf("smoke config unsupported enh.type %q", cfg.ENH.Type)
	}

	if cfg.ENH.TimeoutSec <= 0 {
		cfg.ENH.TimeoutSec = defaultEnhTimeoutSec
	}
	if cfg.Smoke.ScanTimeoutSec <= 0 {
		cfg.Smoke.ScanTimeoutSec = defaultScanTimeoutSec
	}
	if cfg.Smoke.MethodTimeoutSec <= 0 {
		cfg.Smoke.MethodTimeoutSec = defaultMethodTimeoutSec
	}
	return nil
}

func extractYAMLBlocks(content string) string {
	lines := strings.Split(content, "\n")
	var blocks []string
	var current []string
	inBlock := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			if inBlock {
				blocks = append(blocks, strings.Join(current, "\n"))
				current = nil
				inBlock = false
				continue
			}
			if strings.HasPrefix(trimmed, "```yaml") || strings.HasPrefix(trimmed, "```yml") {
				inBlock = true
				continue
			}
		}
		if inBlock {
			current = append(current, line)
		}
	}
	if inBlock && len(current) > 0 {
		blocks = append(blocks, strings.Join(current, "\n"))
	}
	return strings.Join(blocks, "\n")
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repo root not found from %s", wd)
		}
		dir = parent
	}
}
