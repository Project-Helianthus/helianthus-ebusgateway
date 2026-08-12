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
	var sourceRoot, destination, manifestPath, phase, windowID, authScopeHash, capturedAt string
	var startOffset, endOffset int64
	flag.StringVar(&sourceRoot, "source-root", "", "absolute raw source directory")
	flag.StringVar(&destination, "destination", "", "absolute destination source root")
	flag.StringVar(&manifestPath, "manifest", "", "absolute output manifest path")
	flag.StringVar(&phase, "phase", "", "PRE_RESTART or POST_RESTART")
	flag.StringVar(&windowID, "window-id", "", "capture window identity")
	flag.StringVar(&authScopeHash, "auth-scope-hash", "", "effective M8 auth-scope SHA-256")
	flag.StringVar(&capturedAt, "captured-at", "", "RFC3339 UTC capture timestamp")
	flag.Int64Var(&startOffset, "capture-start-offset-ns", -1, "monotonic capture start offset")
	flag.Int64Var(&endOffset, "capture-end-offset-ns", -1, "monotonic capture end offset")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("unexpected positional arguments"))
	}
	if err := validateOutputPaths(sourceRoot, destination, manifestPath); err != nil {
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
	processID, err := processInstanceID(inputs)
	if err != nil {
		fatal(err)
	}
	manifest, err := m8sourcecapture.Publish(destination, m8sourcecapture.Metadata{
		Phase: m8sourcecapture.Phase(phase), WindowID: windowID, AuthScopeHash: authScopeHash,
		ProcessInstanceID: processID, CaptureStartOffsetNS: startOffset,
		CaptureEndOffsetNS: endOffset, CapturedAt: instant,
	}, inputs)
	if err != nil {
		fatal(err)
	}
	if err := writeManifest(manifestPath, manifest); err != nil {
		_ = os.RemoveAll(destination)
		fatal(err)
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s %s %s\n", destination, manifestPath, processID)
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

func validateOutputPaths(sourceRoot, destination, manifestPath string) error {
	if sourceRoot == "" || !filepath.IsAbs(sourceRoot) || filepath.Clean(sourceRoot) != sourceRoot ||
		destination == "" || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination ||
		manifestPath != destination+".manifest.json" || filepath.Dir(destination) != filepath.Dir(manifestPath) {
		return errors.New("source, destination, and manifest paths are not a safe capture layout")
	}
	if sourceRoot == destination || sourceRoot == manifestPath || strings.HasPrefix(destination+string(filepath.Separator), sourceRoot+string(filepath.Separator)) {
		return errors.New("source and output paths must be disjoint")
	}
	return nil
}

func writeManifest(path string, manifest []byte) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("unsafe manifest path")
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe manifest parent")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.Remove(path)
		}
	}()
	written, writeErr := file.Write(manifest)
	syncErr := file.Sync()
	closeErr := file.Close()
	if written != len(manifest) {
		writeErr = errors.Join(writeErr, errors.New("short manifest write"))
	}
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	complete = true
	return nil
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "m8sourcecapture:", strconv.Quote(err.Error()))
	os.Exit(1)
}
