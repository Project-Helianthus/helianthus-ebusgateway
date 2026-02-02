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
	VerboseFrames    bool `yaml:"verbose_frames"`
	ScanTimeoutSec   int  `yaml:"scan_timeout_sec"`
	MethodTimeoutSec int  `yaml:"method_timeout_sec"`
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
	path := filepath.Join(rootDir, "AGENT-local.md")
	cfg, err := loadSmokeConfigFile(path)
	return cfg, path, err
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
