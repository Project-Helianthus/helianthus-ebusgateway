package ebusdscan

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

var ErrAgentLocalMissing = errors.New("agent local missing")

type AgentDefaults struct {
	Host               string
	Port               int
	TSP                string
	IdentifyB50928xx   bool
	Source             byte
	HasSource          bool
	IdentifyPrefixHint byte
}

type agentLocalConfig struct {
	ENH struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"enh"`
	Smoke struct {
		SourceAddress     optionalHexByte `yaml:"source_address"`
		RegisterDumpTSP   string          `yaml:"register_dump_tsp"`
		IdentifyB50928xx  bool            `yaml:"identify_b509_28xx"`
		IdentifyPrefixHex optionalHexByte `yaml:"identify_prefix"`
	} `yaml:"smoke"`
}

type optionalHexByte struct {
	Value byte
	Set   bool
}

func (b *optionalHexByte) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.ScalarNode {
		return nil
	}
	value := strings.TrimSpace(node.Value)
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 0, 16)
	if err != nil {
		return err
	}
	if parsed < 0 || parsed > 0xFF {
		return fmt.Errorf("value out of range: %s", value)
	}
	b.Value = byte(parsed)
	b.Set = true
	return nil
}

func LoadAgentLocal(path string) (AgentDefaults, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return AgentDefaults{}, fmt.Errorf("%w: %s", ErrAgentLocalMissing, path)
		}
		return AgentDefaults{}, err
	}
	yamlText := extractYAMLBlocks(string(data))
	if strings.TrimSpace(yamlText) == "" {
		return AgentDefaults{}, fmt.Errorf("agent local missing yaml blocks: %s", path)
	}
	var cfg agentLocalConfig
	if err := yaml.Unmarshal([]byte(yamlText), &cfg); err != nil {
		return AgentDefaults{}, fmt.Errorf("agent local parse: %w", err)
	}

	defaults := AgentDefaults{
		Host:             strings.TrimSpace(cfg.ENH.Host),
		Port:             cfg.ENH.Port,
		TSP:              strings.TrimSpace(cfg.Smoke.RegisterDumpTSP),
		IdentifyB50928xx: cfg.Smoke.IdentifyB50928xx,
	}
	if cfg.Smoke.SourceAddress.Set {
		defaults.Source = cfg.Smoke.SourceAddress.Value
		defaults.HasSource = true
	}
	if cfg.Smoke.IdentifyPrefixHex.Set {
		defaults.IdentifyPrefixHint = cfg.Smoke.IdentifyPrefixHex.Value
	}
	return defaults, nil
}

func FindAgentLocal(startDir string) (string, error) {
	dir := startDir
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd
	}
	for {
		path := filepath.Join(dir, "AGENT-local.md")
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("agent local not found from %s", startDir)
		}
		dir = parent
	}
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
