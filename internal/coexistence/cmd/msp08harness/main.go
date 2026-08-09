package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence"
)

const boundedInputBytes = int64(2_097_153)

func main() {
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	evidence := flags.String("evidence", "", "evidence input")
	registry := flags.String("registry", "", "registry input")
	m7Graph := flags.String("m7-graph", "", "M7 graph input")
	m7Replay := flags.String("m7-replay", "", "M7 replay input")
	m7Registry := flags.String("m7-registry", "", "M7 registry input")
	m7SourceBundle := flags.String("m7-source-bundle", "", "M7 source bundle input")
	m7SourceReplay := flags.String("m7-source-replay", "", "M7 source replay input")
	m7LiveStatus := flags.String("m7-live-status", "", "M7 public-redacted live status input")
	m7TerminalGraph := flags.String("m7-terminal-graph", "", "M7 terminal graph input")
	m7TerminalReplay := flags.String("m7-terminal-replay", "", "M7 terminal replay input")
	m7TerminalSourceBundle := flags.String("m7-terminal-source-bundle", "", "M7 terminal source bundle input")
	m7TerminalSourceReplay := flags.String("m7-terminal-source-replay", "", "M7 terminal source replay input")
	baselineRuntime := flags.String("baseline-runtime", "", "baseline runtime identity input")
	comparedRuntime := flags.String("compared-runtime", "", "compared runtime identity input")
	captureClock := flags.String("capture-clock", "", "capture clock input")
	captureTimestamps := flags.String("capture-timestamps", "", "capture timestamps input")
	maskedSubjects := flags.String("masked-subjects", "", "masked subjects input")
	outputRoot := flags.String("output-root", "", "artifact output root")
	if err := flags.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}

	switch command {
	case "verify":
		if err := coexistence.Verify(readInputs(
			*evidence, *registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay,
			*m7LiveStatus, *m7TerminalGraph, *m7TerminalReplay, *m7TerminalSourceBundle, *m7TerminalSourceReplay,
		)); err != nil {
			fail(err)
		}
		fmt.Println("ok")
	case "verify-public":
		if err := coexistence.VerifyPublic(readInputs(
			*evidence, *registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay,
			*m7LiveStatus, *m7TerminalGraph, *m7TerminalReplay, *m7TerminalSourceBundle, *m7TerminalSourceReplay,
		)); err != nil {
			fail(err)
		}
		fmt.Println("public-only-ok")
	case "report":
		report, err := coexistence.Report(readInputs(
			*evidence, *registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay,
			*m7LiveStatus, *m7TerminalGraph, *m7TerminalReplay, *m7TerminalSourceBundle, *m7TerminalSourceReplay,
		))
		if err != nil {
			fail(err)
		}
		_, _ = os.Stdout.Write(report)
	case "generate":
		artifacts, err := coexistence.Generate(readGenerateInputs(
			*registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay,
			*baselineRuntime, *comparedRuntime, *captureClock, *captureTimestamps, *maskedSubjects,
		))
		if err != nil {
			fail(err)
		}
		if err := os.MkdirAll(*outputRoot, 0o700); err != nil {
			panic(err)
		}
		if err := os.WriteFile(filepath.Join(*outputRoot, "evidence.json"), artifacts.Evidence, 0o600); err != nil {
			panic(err)
		}
		if err := os.WriteFile(filepath.Join(*outputRoot, "report.json"), artifacts.Report, 0o600); err != nil {
			panic(err)
		}
	default:
		os.Exit(2)
	}
}

func readInputs(
	evidence, registry, graph, replay, m7Registry, sourceBundle, sourceReplay,
	liveStatus, terminalGraph, terminalReplay, terminalSourceBundle, terminalSourceReplay string,
) coexistence.InputsV1 {
	return coexistence.InputsV1{
		Evidence:               readOptional(evidence),
		Registry:               readOptional(registry),
		M7Graph:                readOptional(graph),
		M7Replay:               readOptional(replay),
		M7Registry:             readOptional(m7Registry),
		M7SourceBundle:         readOptional(sourceBundle),
		M7SourceReplay:         readOptional(sourceReplay),
		M7LiveStatus:           readOptional(liveStatus),
		M7TerminalGraph:        readOptional(terminalGraph),
		M7TerminalReplay:       readOptional(terminalReplay),
		M7TerminalSourceBundle: readOptional(terminalSourceBundle),
		M7TerminalSourceReplay: readOptional(terminalSourceReplay),
	}
}

func readGenerateInputs(registry, graph, replay, m7Registry, sourceBundle, sourceReplay, baselineRuntime, comparedRuntime, captureClock, captureTimestamps, maskedSubjects string) coexistence.GenerateInputsV1 {
	return coexistence.GenerateInputsV1{
		Registry:          readOptional(registry),
		M7Graph:           readOptional(graph),
		M7Replay:          readOptional(replay),
		M7Registry:        readOptional(m7Registry),
		M7SourceBundle:    readOptional(sourceBundle),
		M7SourceReplay:    readOptional(sourceReplay),
		BaselineRuntime:   readOptional(baselineRuntime),
		ComparedRuntime:   readOptional(comparedRuntime),
		CaptureClock:      readOptional(captureClock),
		CaptureTimestamps: readOptional(captureTimestamps),
		MaskedSubjects:    readOptional(maskedSubjects),
	}
}

func readOptional(path string) []byte {
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	raw, err := io.ReadAll(io.LimitReader(file, boundedInputBytes))
	closeErr := file.Close()
	if err != nil {
		panic(err)
	}
	if closeErr != nil {
		panic(closeErr)
	}
	return raw
}

func fail(err error) {
	fmt.Println(err.Error())
	os.Exit(1)
}
