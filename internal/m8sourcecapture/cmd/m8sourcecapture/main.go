package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcecapture"
)

func main() {
	var sourceRoot, destination, phase, windowID, authScopeHash, capturedAt string
	var clockID, monotonicEpochID, wallAnchorUTC string
	var startOffset, endOffset int64
	flag.StringVar(&sourceRoot, "source-root", "", "absolute raw source directory")
	flag.StringVar(&destination, "destination", "", "absolute destination generation")
	flag.StringVar(&phase, "phase", "", "PRE_RESTART or POST_RESTART")
	flag.StringVar(&windowID, "window-id", "", "capture window identity")
	flag.StringVar(&authScopeHash, "auth-scope-hash", "", "effective M8 auth-scope SHA-256")
	flag.StringVar(&capturedAt, "captured-at", "", "RFC3339 UTC capture timestamp")
	flag.StringVar(&clockID, "clock-id", "", "shared PRE/POST capture clock identity")
	flag.StringVar(&monotonicEpochID, "monotonic-epoch-id", "", "shared monotonic epoch identity")
	flag.StringVar(&wallAnchorUTC, "wall-anchor-utc", "", "shared RFC3339 UTC clock anchor")
	flag.Int64Var(&startOffset, "capture-start-offset-ns", -1, "monotonic capture start offset")
	flag.Int64Var(&endOffset, "capture-end-offset-ns", -1, "monotonic capture end offset")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("unexpected positional arguments"))
	}
	if err := validateOutputPaths(sourceRoot, destination); err != nil {
		fatal(err)
	}

	inputs, err := m8sourcecapture.ReadInputs(sourceRoot)
	if err != nil {
		fatal(err)
	}
	instant, err := time.Parse(time.RFC3339Nano, capturedAt)
	if err != nil || instant.Location() != time.UTC || instant.Format(time.RFC3339Nano) != capturedAt {
		fatal(errors.New("captured-at must be canonical RFC3339 UTC"))
	}
	wallAnchor, err := time.Parse(time.RFC3339Nano, wallAnchorUTC)
	if err != nil || wallAnchor.Location() != time.UTC || wallAnchor.Format(time.RFC3339Nano) != wallAnchorUTC {
		fatal(errors.New("wall-anchor-utc must be canonical RFC3339 UTC"))
	}
	processID, err := processInstanceID(inputs)
	if err != nil {
		fatal(err)
	}
	_, err = m8sourcecapture.PublishGeneration(destination, m8sourcecapture.Metadata{
		Phase: m8sourcecapture.Phase(phase), WindowID: windowID, AuthScopeHash: authScopeHash,
		ProcessInstanceID: processID, ClockID: clockID, MonotonicEpochID: monotonicEpochID,
		WallAnchorUTC: wallAnchor, CaptureStartOffsetNS: startOffset,
		CaptureEndOffsetNS: endOffset, CapturedAt: instant,
	}, inputs)
	if err != nil {
		fatal(err)
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s %s %s\n",
		filepath.Join(destination, m8sourcecapture.SourceDirectory),
		filepath.Join(destination, m8sourcecapture.ManifestFilename), processID)
}

func processInstanceID(inputs []m8sourcecapture.Input) (string, error) {
	var raw []byte
	for _, input := range inputs {
		if input.ID == "container.inspect" {
			raw = input.Payload
			break
		}
	}
	var inspect []struct {
		ID    string `json:"Id"`
		State struct {
			StartedAt string `json:"StartedAt"`
		} `json:"State"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &inspect) != nil || len(inspect) != 1 || inspect[0].ID == "" || inspect[0].State.StartedAt == "" {
		return "", errors.New("container.inspect does not bind one process")
	}
	digest := sha256.Sum256([]byte(inspect[0].ID + "\x00" + inspect[0].State.StartedAt))
	return "process-" + hex.EncodeToString(digest[:16]), nil
}

func validateOutputPaths(sourceRoot, destination string) error {
	if sourceRoot == "" || !filepath.IsAbs(sourceRoot) || filepath.Clean(sourceRoot) != sourceRoot ||
		destination == "" || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return errors.New("source and destination paths are not a safe capture layout")
	}
	if sourceRoot == destination || strings.HasPrefix(destination+string(filepath.Separator), sourceRoot+string(filepath.Separator)) {
		return errors.New("source and output paths must be disjoint")
	}
	return nil
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "m8sourcecapture:", strconv.Quote(err.Error()))
	os.Exit(1)
}
