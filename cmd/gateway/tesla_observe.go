package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/teslaobserve"
)

// teslaObserveCommand selects the NO_SEND Tesla Gen3 HSC activation
// observation run from argv, before any gateway configuration exists. It is
// separate from the gateway flags and from the non-send retained owner.
const teslaObserveCommand = "tesla-observe"

const (
	teslaObserveExitOK      = 0
	teslaObserveExitRun     = 1
	teslaObserveExitRefused = 2
)

type teslaObservePorts struct {
	OpenSerial func(path string) (teslaobserve.ByteSource, error)
	Now        func() time.Time
	Stderr     io.Writer
	Context    func() (context.Context, context.CancelFunc)
}

func newTeslaObservePorts() teslaObservePorts {
	return teslaObservePorts{
		OpenSerial: openTeslaObserveSerial,
		Now:        time.Now,
		Stderr:     os.Stderr,
		Context: func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		},
	}
}

func dispatchTeslaObserve(argv []string, ports func() teslaObservePorts) (int, bool) {
	if len(argv) < 2 || argv[1] != teslaObserveCommand {
		return 0, false
	}
	return runTeslaObserve(argv[2:], ports()), true
}

// redactTeslaSerialError drops the local path that file errors carry, so no
// cause written to evidence can expose it. Context errors stay matchable.
func redactTeslaSerialError(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("serial %s: %w", pathErr.Op, pathErr.Err)
	}
	return err
}

func runTeslaObserve(args []string, ports teslaObservePorts) int {
	flags := flag.NewFlagSet(teslaObserveCommand, flag.ContinueOnError)
	flags.SetOutput(ports.Stderr)
	serialPath := flags.String("serial-path", "", "local RS-485 serial device; used only to open the port, never written to evidence")
	prepare := flags.Bool("prepare", false, "open the port once, apply the raw receive-only settings, flush, close and exit; observes and records nothing and accepts only -serial-path; run it before the RS-485 pair is connected to the bus")
	endpoint := flags.String("endpoint-id", "tesla-wc3-rs485", "public endpoint label written to evidence ([a-z0-9._-], never a path)")
	kind := flags.String("case", "", "attempt case: initial, recovery or idle")
	repeats := flags.Int("repeats", 1, "independent attempts, each on a freshly opened port (1..16)")
	window := flags.Duration("window", 30*time.Second, "observation deadline per attempt (at most 60s)")
	windowBytes := flags.Int("window-bytes", 64, "requested byte count per attempt (1..4096)")
	segmentCap := flags.Int("segment-cap", 64, "segment cap per attempt (1..1024)")
	segmentGap := flags.Duration("segment-gap", 2*time.Millisecond, "host receipt gap that starts a new segment")
	pause := flags.Duration("pause", 0, "wait between attempts (at most 10m)")
	out := flags.String("out", "", "local evidence file (JSON, owner-only)")
	appendTo := flags.Bool("append", false, "append attempts to an existing evidence file")
	worksheetOut := flags.String("worksheet-out", "", "optional Markdown rendering of the worksheet rows")
	var operator teslaobserve.OperatorRecord
	flags.StringVar(&operator.PriorState, "prior-state", "unknown", "fresh_session, already_active, recovery, reset, unavailable or unknown")
	flags.StringVar(&operator.PriorStateBasis, "prior-state-basis", "", "basis for the prior state, without names or credentials")
	flags.StringVar(&operator.OtherInitiators, "other-initiators", "unknown", "excluded, present or unknown")
	flags.StringVar(&operator.OtherInitiatorsBasis, "other-initiators-basis", "", "basis for the initiator statement")
	flags.StringVar(&operator.Condition, "condition", "", "what differs for these attempts, without site data")
	flags.StringVar(&operator.ActionTimeAuthorization, "action-time-authorization", "unknown", "recorded or unknown; names and credentials are never written")
	if err := flags.Parse(args); err != nil {
		return teslaObserveExitRefused
	}
	refuse := func(reason string) int {
		_, _ = fmt.Fprintf(ports.Stderr, "%s: refused: %s\n", teslaObserveCommand, reason)
		return teslaObserveExitRefused
	}
	switch {
	case flags.NArg() != 0:
		return refuse("unexpected positional arguments")
	case *serialPath == "":
		return refuse("-serial-path is required")
	case *prepare:
		if name := teslaObserveObservationFlag(flags); name != "" {
			return refuse("-prepare accepts only -serial-path; remove -" + name)
		}
		return prepareTeslaObserveSerial(*serialPath, ports)
	case *out == "":
		return refuse("-out is required")
	}
	if *worksheetOut != "" {
		same, err := sameTeslaObserveOutput(*out, *worksheetOut)
		if err != nil {
			return refuse("cannot resolve output paths")
		}
		if same {
			return refuse("-worksheet-out must differ from -out")
		}
	}
	// Use the same canonical target for locking, reading and replacement.
	resolvedOut, err := resolveTeslaObserveOutput(*out)
	if err != nil {
		return refuse("cannot resolve -out")
	}
	*out = resolvedOut
	if *worksheetOut != "" {
		resolvedWorksheet, err := resolveTeslaObserveOutput(*worksheetOut)
		if err != nil {
			return refuse("cannot resolve -worksheet-out")
		}
		*worksheetOut = resolvedWorksheet
	}
	// Lock every canonical output in a stable order, including a worksheet
	// shared by commands with different evidence files. Directory locks
	// survive replacement and remain held through the entire transaction.
	targets := []string{*out}
	if *worksheetOut != "" {
		targets = append(targets, *worksheetOut)
	}
	sort.Strings(targets)
	for _, target := range targets {
		lock := target + ".lock"
		if err := os.Mkdir(lock, 0o700); err != nil {
			return refuse("cannot lock output; another observation may be running")
		}
		defer func(path string) { _ = os.Remove(path) }(lock)
	}
	if *appendTo {
		for _, path := range []string{*out, *worksheetOut} {
			if path == "" {
				continue
			}
			if err := checkTeslaObserveAppendTarget(path); err != nil {
				return refuse("append outputs must be regular files with a single hard link")
			}
		}
	}
	var prior *teslaobserve.Evidence
	existing, err := os.ReadFile(*out)
	switch {
	case err == nil && !*appendTo:
		return refuse("-out exists; pass -append to add attempts")
	case err == nil:
		decoded, decodeErr := teslaobserve.DecodeEvidence(existing)
		if decodeErr != nil {
			return refuse(decodeErr.Error())
		}
		prior = &decoded
	case !errors.Is(err, fs.ErrNotExist):
		return refuse("cannot read -out")
	case *appendTo:
		return refuse("-append needs an existing -out file")
	}
	if *worksheetOut != "" && !*appendTo {
		if _, statErr := os.Lstat(*worksheetOut); statErr == nil {
			return refuse("-worksheet-out exists; pass -append to replace it")
		} else if !errors.Is(statErr, fs.ErrNotExist) {
			return refuse("cannot inspect -worksheet-out")
		}
	}
	path := *serialPath
	config := teslaobserve.RunConfig{
		Endpoint: *endpoint, Case: *kind, Repeats: *repeats, Window: *window,
		RequestedBytes: *windowBytes, SegmentCap: *segmentCap, SegmentGap: *segmentGap, Pause: *pause,
		Operator: operator, Now: ports.Now,
		Open: func() (teslaobserve.ByteSource, error) { return ports.OpenSerial(path) },
	}
	if err := config.Validate(); err != nil {
		return refuse(err.Error())
	}
	ctx, cancel := ports.Context()
	defer cancel()
	evidence, runErr := teslaobserve.Run(ctx, config, prior)
	if runErr != nil && len(evidence.Attempts) == 0 {
		return refuse(runErr.Error())
	}
	data, err := teslaobserve.EncodeEvidence(evidence)
	if err == nil {
		if *worksheetOut == "" {
			err = writeTeslaObserveFile(*out, data, prior != nil)
		} else {
			err = writeTeslaObservePair(*out, data, *worksheetOut, []byte(teslaobserve.RenderMarkdown(evidence)), existing, *appendTo)
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(ports.Stderr, "%s: writing evidence failed: %v\n", teslaObserveCommand, redactTeslaSerialError(err))
		return teslaObserveExitRun
	}
	_, _ = fmt.Fprintf(ports.Stderr, "%s: %d attempts recorded, frames sent %d, mode %s\n",
		teslaObserveCommand, len(evidence.Attempts), evidence.FramesSent, evidence.Mode)
	if runErr != nil {
		_, _ = fmt.Fprintf(ports.Stderr, "%s: run failed: %v\n", teslaObserveCommand, runErr)
		return teslaObserveExitRun
	}
	return teslaObserveExitOK
}

// teslaObserveObservationFlag returns the name of an explicitly set flag that
// only an observation or its evidence output can use. -prepare observes and
// records nothing, so any such flag would suggest a result that never exists.
func teslaObserveObservationFlag(flags *flag.FlagSet) string {
	var name string
	flags.Visit(func(f *flag.Flag) {
		if name == "" && f.Name != "prepare" && f.Name != "serial-path" {
			name = f.Name
		}
	})
	return name
}

// prepareTeslaObserveSerial opens the port through the same receive-only
// opener as an observation, so the raw settings and the queue flush are
// applied, and closes it again. The driver keeps those settings across close,
// so the later observation open inherits them. Nothing is read, observed or
// written to a file.
func prepareTeslaObserveSerial(path string, ports teslaObservePorts) int {
	source, err := ports.OpenSerial(path)
	if err == nil && source == nil {
		err = errors.New("serial opener returned no source")
	}
	if err == nil {
		err = source.Close()
	}
	if err != nil {
		_, _ = fmt.Fprintf(ports.Stderr, "%s: prepare failed: %v\n", teslaObserveCommand, redactTeslaSerialError(err))
		return teslaObserveExitRun
	}
	_, _ = fmt.Fprintf(ports.Stderr, "%s: port prepared: raw receive-only settings applied and queues flushed; nothing observed or recorded, frames sent 0\n", teslaObserveCommand)
	return teslaObserveExitOK
}

// teslaObserveWriteData is a variable so tests can force a write failure.
var teslaObserveWriteData = writeAndSync

// teslaObserveSyncDirectory lets tests inject a publication durability failure.
var teslaObserveSyncDirectory = syncTeslaObserveDirectory

func syncTeslaObserveDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	err = directory.Sync()
	if closeErr := directory.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeAndSync(file *os.File, data []byte) error {
	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}

// writeTeslaObserveFile stages and syncs an owner-only file before publishing
// it. New outputs use no-replace publication; replacements swap a complete
// staged file into place. Success also requires syncing the parent directory.
func writeTeslaObserveFile(path string, data []byte, replace bool) error {
	staged, err := stageTeslaObserveFile(path, data)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(staged) }()
	if err := teslaObservePublishFile(staged, path, replace); err != nil {
		return err
	}
	return teslaObserveSyncDirectory(filepath.Dir(path))
}

// checkTeslaObserveAppendTarget rejects inode aliases that cannot share the
// canonical path lock. A missing worksheet is allowed on an append.
func checkTeslaObserveAppendTarget(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		return errors.New("append target is not a singly linked regular file")
	}
	return nil
}

// teslaObservePublishFile allows tests to fail publication after staging.
var teslaObservePublishFile = publishTeslaObserveFile

func publishTeslaObserveFile(staged, path string, replace bool) error {
	if replace {
		return os.Rename(staged, path)
	}
	// Link publishes the complete staged contents without overwriting a target
	// created since preflight. The caller removes the staging name afterwards.
	return os.Link(staged, path)
}

func stageTeslaObserveFile(path string, data []byte) (string, error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".tesla-observe-*")
	if err != nil {
		return "", err
	}
	name := file.Name()
	err = teslaObserveWriteData(file, data)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// writeTeslaObservePair stages both complete, synced outputs before publication.
// If worksheet publication fails, restore the prior evidence (or remove the
// newly created evidence). Two independent paths cannot be crash-atomic; after
// both publications succeed, directory sync failures retain the matching pair.
func writeTeslaObservePair(out string, data []byte, worksheet string, markdown, prior []byte, replace bool) error {
	stagedOut, err := stageTeslaObserveFile(out, data)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(stagedOut) }()
	stagedWorksheet, err := stageTeslaObserveFile(worksheet, markdown)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(stagedWorksheet) }()
	var backup string
	if replace {
		backup, err = stageTeslaObserveFile(out, prior)
		if err != nil {
			return err
		}
		defer func() {
			if backup != "" {
				_ = os.Remove(backup)
			}
		}()
	}
	if err := teslaObservePublishFile(stagedOut, out, replace); err != nil {
		return err
	}
	if err := teslaObservePublishFile(stagedWorksheet, worksheet, replace); err != nil {
		var rollbackErr error
		if replace {
			rollbackErr = os.Rename(backup, out)
			// Retain the recovery copy if the filesystem also refuses rollback.
			backup = ""
		} else {
			rollbackErr = os.Remove(out)
		}
		return errors.Join(err, rollbackErr, teslaObserveSyncDirectory(filepath.Dir(out)))
	}
	return errors.Join(teslaObserveSyncDirectory(filepath.Dir(out)), teslaObserveSyncDirectory(filepath.Dir(worksheet)))
}

// sameTeslaObserveOutput resolves relative paths and symlinked directories,
// including paths not yet created, and detects existing hard-link aliases.
func sameTeslaObserveOutput(first, second string) (bool, error) {
	a, err := resolveTeslaObserveOutput(first)
	if err != nil {
		return false, err
	}
	b, err := resolveTeslaObserveOutput(second)
	if err != nil {
		return false, err
	}
	if a == b {
		return true, nil
	}
	firstInfo, firstErr := os.Stat(a)
	secondInfo, secondErr := os.Stat(b)
	return firstErr == nil && secondErr == nil && os.SameFile(firstInfo, secondInfo), nil
}

func resolveTeslaObserveOutput(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	// ENOENT may describe a dangling symlink target rather than an absent
	// directory entry. Refuse that existing link before collecting evidence.
	// Recursing through parents applies the same check to dangling directories.
	info, statErr := os.Lstat(absolute)
	if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", err
	}
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return "", statErr
	}
	parent := filepath.Dir(absolute)
	if parent == absolute {
		return "", err
	}
	resolved, err = resolveTeslaObserveOutput(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, filepath.Base(absolute)), nil
}
