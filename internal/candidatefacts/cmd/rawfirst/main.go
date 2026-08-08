package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

type summaryV1 struct {
	SourceBundleFileSHA256 string         `json:"source_bundle_file_sha256"`
	SourceReplayFileSHA256 string         `json:"source_replay_file_sha256"`
	GraphFileSHA256        string         `json:"graph_file_sha256"`
	ReplayFileSHA256       string         `json:"replay_file_sha256"`
	GraphID                string         `json:"graph_id"`
	GraphHash              string         `json:"graph_hash"`
	FactCount              int            `json:"fact_count"`
	Statuses               map[string]int `json:"statuses"`
	TerminalFamilies       map[string]int `json:"terminal_families"`
	EEBusPathFacts         int            `json:"eebus_path_facts"`
	CloudOnlyFacts         int            `json:"cloud_only_facts"`
	ByteIdenticalRebuild   bool           `json:"byte_identical_rebuild"`
	ByteIdenticalReplay    bool           `json:"byte_identical_replay"`
}

func main() {
	var bundlePath string
	var outputDirectory string
	flag.StringVar(&bundlePath, "source-bundle", "", "verified synchronized-evidence bundle")
	flag.StringVar(&outputDirectory, "output-dir", "", "new or empty private output directory")
	flag.Parse()
	if bundlePath == "" || outputDirectory == "" || flag.NArg() != 0 {
		fatal(errors.New("source-bundle and output-dir are required"))
	}

	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		fatal(err)
	}
	sourceReplay, err := syncevidence.Replay(bundle)
	if err != nil {
		fatal(err)
	}
	graph1, replay1, err := candidatefacts.BuildRawFirstV1(bundle, sourceReplay)
	if err != nil {
		fatal(err)
	}
	graph2, replay2, err := candidatefacts.BuildRawFirstV1(bundle, sourceReplay)
	if err != nil {
		fatal(err)
	}
	if !bytes.Equal(graph1, graph2) || !bytes.Equal(replay1, replay2) {
		fatal(errors.New("raw-first build is not byte-identical"))
	}

	result, err := summarize(bundle, sourceReplay, graph1, replay1)
	if err != nil {
		fatal(err)
	}
	summaryRaw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatal(err)
	}
	summaryRaw = append(summaryRaw, '\n')
	if err := ensurePrivateOutputDirectory(outputDirectory); err != nil {
		fatal(err)
	}
	outputs := []struct {
		name    string
		content []byte
	}{
		{name: "source-replay.json", content: sourceReplay},
		{name: "candidate-graph.json", content: graph1},
		{name: "candidate-replay.json", content: replay1},
		{name: "summary.json", content: summaryRaw},
	}
	for _, output := range outputs {
		if _, err := os.Lstat(filepath.Join(outputDirectory, output.name)); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				err = errors.New("output file already exists")
			}
			fatal(err)
		}
	}
	for _, output := range outputs {
		if err := writeNewPrivateFile(filepath.Join(outputDirectory, output.name), output.content); err != nil {
			fatal(err)
		}
	}
	_, _ = os.Stdout.Write(summaryRaw)
}

func summarize(bundle, sourceReplay, graphRaw, replay []byte) (summaryV1, error) {
	var graph candidatefacts.GraphV1
	if err := json.Unmarshal(graphRaw, &graph); err != nil {
		return summaryV1{}, err
	}
	result := summaryV1{
		SourceBundleFileSHA256: digest(bundle), SourceReplayFileSHA256: digest(sourceReplay),
		GraphFileSHA256: digest(graphRaw), ReplayFileSHA256: digest(replay),
		GraphID: graph.GraphID, GraphHash: graph.GraphHash, FactCount: len(graph.Facts),
		Statuses: map[string]int{}, TerminalFamilies: map[string]int{},
		ByteIdenticalRebuild: true, ByteIdenticalReplay: true,
	}
	for _, fact := range graph.Facts {
		result.Statuses[fact.Status]++
		if fact.Provenance.SourceTerminal != nil {
			result.TerminalFamilies[fact.Provenance.SourceTerminal.EBusIdentity.Family]++
		}
		if fact.Provenance.EEBus != nil {
			result.EEBusPathFacts++
		}
		if fact.TerminalNegativeState != nil && *fact.TerminalNegativeState == "CLOUD_ONLY" {
			result.CloudOnlyFacts++
		}
	}
	return result, nil
}

func ensurePrivateOutputDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("output-dir must be a real directory with mode 0700")
	}
	return nil
}

func writeNewPrivateFile(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := error(nil)
	if err := file.Chmod(0o600); err != nil {
		writeErr = err
	} else if _, err := file.Write(content); err != nil {
		writeErr = err
	} else if err := file.Sync(); err != nil {
		writeErr = err
	}
	return errors.Join(writeErr, file.Close())
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "candidate raw-first replay:", err)
	os.Exit(1)
}
