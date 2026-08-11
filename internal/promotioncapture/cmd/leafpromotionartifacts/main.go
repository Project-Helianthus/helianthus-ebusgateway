package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
)

const maximumInputBytes = 16 << 20

type artifactSummary struct {
	CampaignHash         string   `json:"campaign_hash"`
	OutputFile           string   `json:"output_file"`
	PromotedCandidateIDs []string `json:"promoted_candidate_ids"`
}

func main() {
	var manifestPath string
	var prePath string
	var postPath string
	var outputDirectory string
	flag.StringVar(&manifestPath, "manifest", "", "private live campaign manifest")
	flag.StringVar(&prePath, "pre", "", "PRE_RESTART checkpoint")
	flag.StringVar(&postPath, "post", "", "POST_RESTART checkpoint")
	flag.StringVar(&outputDirectory, "output-dir", "", "existing empty owner-only 0700 output directory")
	flag.Parse()
	if manifestPath == "" || prePath == "" || postPath == "" || outputDirectory == "" || flag.NArg() != 0 {
		fatal(errors.New("manifest, pre, post, and output-dir are required"))
	}

	var manifest promotioncapture.CampaignAssemblyManifest
	if err := readPrivateJSON(manifestPath, &manifest); err != nil {
		fatal(fmt.Errorf("manifest: %w", err))
	}
	var pre promotioncapture.WindowCheckpoint
	if err := readPrivateJSON(prePath, &pre); err != nil {
		fatal(fmt.Errorf("pre: %w", err))
	}
	var post promotioncapture.WindowCheckpoint
	if err := readPrivateJSON(postPath, &post); err != nil {
		fatal(fmt.Errorf("post: %w", err))
	}
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		fatal(err)
	}
	campaign, err := promotioncapture.AssembleCampaign(registry, manifest, pre, post)
	if err != nil {
		fatal(err)
	}
	raw, err := promotioncapture.CanonicalJSON(campaign)
	if err != nil {
		fatal(err)
	}
	raw = append(raw, '\n')
	if err := writePrivateOutputs(outputDirectory, []privateOutput{{name: "private-campaign.json", content: raw}}); err != nil {
		fatal(err)
	}
	if err := writeArtifactSummary(os.Stdout, campaign); err != nil {
		fatal(err)
	}
}

func writeArtifactSummary(output io.Writer, campaign promotioncapture.Campaign) error {
	if output == nil {
		return errors.New("summary output is nil")
	}
	promoted := make([]string, 0)
	for _, candidate := range campaign.Candidates {
		if candidate.Decision == promotioncapture.DecisionPromoted {
			promoted = append(promoted, candidate.CandidateID)
		}
	}
	return json.NewEncoder(output).Encode(artifactSummary{
		CampaignHash: campaign.CampaignHash, OutputFile: "private-campaign.json",
		PromotedCandidateIDs: promoted,
	})
}

func readPrivateJSON(path string, target any) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("input path must be absolute and clean")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 || info.Size() < 1 || info.Size() > maximumInputBytes {
		return errors.New("input must be an owner-only regular file within the size limit")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil || !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return errors.New("input changed during open")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximumInputBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > maximumInputBytes {
		return errors.Join(readErr, closeErr, errors.New("input read failed"))
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "leaf promotion artifacts:", err)
	os.Exit(1)
}
