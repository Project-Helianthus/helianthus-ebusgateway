package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence"
)

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
	case "binding":
		writeJSON(coexistence.Binding())
	case "selftest":
		selftest(
			readInputs(*evidence, *registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay),
			readGenerateInputs(*registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay, *baselineRuntime, *comparedRuntime, *captureClock, *captureTimestamps, *maskedSubjects),
		)
	case "verify":
		if err := coexistence.Verify(readInputs(*evidence, *registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay)); err != nil {
			fail(err)
		}
		fmt.Println("ok")
	case "report":
		report, err := coexistence.Report(readInputs(*evidence, *registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay))
		if err != nil {
			fail(err)
		}
		_, _ = os.Stdout.Write(report)
	case "generate":
		artifacts, err := coexistence.Generate(readGenerateInputs(*registry, *m7Graph, *m7Replay, *m7Registry, *m7SourceBundle, *m7SourceReplay, *baselineRuntime, *comparedRuntime, *captureClock, *captureTimestamps, *maskedSubjects))
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

func readInputs(evidence, registry, graph, replay, m7Registry, sourceBundle, sourceReplay string) coexistence.InputsV1 {
	return coexistence.InputsV1{
		Evidence:       readOptional(evidence),
		Registry:       readOptional(registry),
		M7Graph:        readOptional(graph),
		M7Replay:       readOptional(replay),
		M7Registry:     readOptional(m7Registry),
		M7SourceBundle: readOptional(sourceBundle),
		M7SourceReplay: readOptional(sourceReplay),
	}
}

func readOptional(path string) []byte {
	if path == "" {
		return nil
	}
	value, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return value
}

func selftest(inputs coexistence.InputsV1, generation coexistence.GenerateInputsV1) {
	assertExactAPI()
	snapshot := cloneInputs(inputs)
	generationSnapshot := cloneGenerateInputs(generation)
	if err := coexistence.Verify(inputs); err != nil {
		fail(err)
	}
	if !equalInputs(inputs, snapshot) {
		panic("Verify mutated supplied input bytes")
	}

	firstBinding := coexistence.Binding()
	firstBinding.ArtifactSHA256["probe"] = "mutated"
	if _, exists := coexistence.Binding().ArtifactSHA256["probe"]; exists {
		panic("Binding did not defensively copy artifact hashes")
	}

	firstReport, err := coexistence.Report(inputs)
	if err != nil {
		fail(err)
	}
	wantReport := bytes.Clone(firstReport)
	firstReport[0] ^= 0xff
	secondReport, err := coexistence.Report(inputs)
	if err != nil {
		fail(err)
	}
	if !bytes.Equal(secondReport, wantReport) {
		panic("Report returned shared or nondeterministic bytes")
	}

	generated, err := coexistence.Generate(generation)
	if err != nil {
		fail(err)
	}
	wantEvidence := bytes.Clone(generated.Evidence)
	wantGeneratedReport := bytes.Clone(generated.Report)
	generated.Evidence[0] ^= 0xff
	generated.Report[0] ^= 0xff
	again, err := coexistence.Generate(generation)
	if err != nil {
		fail(err)
	}
	if !bytes.Equal(again.Evidence, wantEvidence) || !bytes.Equal(again.Report, wantGeneratedReport) {
		panic("Generate returned shared or nondeterministic bytes")
	}
	if !equalInputs(inputs, snapshot) {
		panic("Report mutated supplied verification input bytes")
	}
	if !equalGenerateInputs(generation, generationSnapshot) {
		panic("Generate mutated supplied generation input bytes")
	}
	fmt.Println("ok")
}

func assertExactAPI() {
	inputsType := reflect.TypeOf(coexistence.InputsV1{})
	generationType := reflect.TypeOf(coexistence.GenerateInputsV1{})
	if inputsType == generationType {
		panic("InputsV1 and GenerateInputsV1 must be distinct named types")
	}
	assertFields(inputsType, map[string]reflect.Type{
		"Evidence": reflect.TypeOf([]byte(nil)), "Registry": reflect.TypeOf([]byte(nil)),
		"M7Graph": reflect.TypeOf([]byte(nil)), "M7Replay": reflect.TypeOf([]byte(nil)),
		"M7Registry": reflect.TypeOf([]byte(nil)), "M7SourceBundle": reflect.TypeOf([]byte(nil)),
		"M7SourceReplay": reflect.TypeOf([]byte(nil)),
	})
	assertFields(generationType, map[string]reflect.Type{
		"Registry": reflect.TypeOf([]byte(nil)), "M7Graph": reflect.TypeOf([]byte(nil)),
		"M7Replay": reflect.TypeOf([]byte(nil)), "M7Registry": reflect.TypeOf([]byte(nil)),
		"M7SourceBundle": reflect.TypeOf([]byte(nil)), "M7SourceReplay": reflect.TypeOf([]byte(nil)),
		"BaselineRuntime": reflect.TypeOf([]byte(nil)), "ComparedRuntime": reflect.TypeOf([]byte(nil)),
		"CaptureClock": reflect.TypeOf([]byte(nil)), "CaptureTimestamps": reflect.TypeOf([]byte(nil)),
		"MaskedSubjects": reflect.TypeOf([]byte(nil)),
	})
	if _, exists := generationType.FieldByName("Evidence"); exists {
		panic("Evidence must not be representable in GenerateInputsV1")
	}
	assertFunctionInput("Verify", reflect.TypeOf(coexistence.Verify), inputsType)
	assertFunctionInput("Report", reflect.TypeOf(coexistence.Report), inputsType)
	assertFunctionInput("Generate", reflect.TypeOf(coexistence.Generate), generationType)
}

func assertFields(actual reflect.Type, expected map[string]reflect.Type) {
	got := make([]string, actual.NumField())
	for index := 0; index < actual.NumField(); index++ {
		field := actual.Field(index)
		wantType, exists := expected[field.Name]
		if !exists || field.Type != wantType {
			panic("unexpected API field " + actual.Name() + "." + field.Name)
		}
		got[index] = field.Name
	}
	want := make([]string, 0, len(expected))
	for name := range expected {
		want = append(want, name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		panic("API field set mismatch for " + actual.Name())
	}
}

func assertFunctionInput(name string, function, expected reflect.Type) {
	if function.Kind() != reflect.Func || function.NumIn() != 1 || function.In(0) != expected {
		panic(name + " has the wrong input boundary")
	}
}

func cloneInputs(input coexistence.InputsV1) coexistence.InputsV1 {
	return coexistence.InputsV1{
		Evidence:       bytes.Clone(input.Evidence),
		Registry:       bytes.Clone(input.Registry),
		M7Graph:        bytes.Clone(input.M7Graph),
		M7Replay:       bytes.Clone(input.M7Replay),
		M7Registry:     bytes.Clone(input.M7Registry),
		M7SourceBundle: bytes.Clone(input.M7SourceBundle),
		M7SourceReplay: bytes.Clone(input.M7SourceReplay),
	}
}

func equalInputs(left, right coexistence.InputsV1) bool {
	return bytes.Equal(left.Evidence, right.Evidence) &&
		bytes.Equal(left.Registry, right.Registry) &&
		bytes.Equal(left.M7Graph, right.M7Graph) &&
		bytes.Equal(left.M7Replay, right.M7Replay) &&
		bytes.Equal(left.M7Registry, right.M7Registry) &&
		bytes.Equal(left.M7SourceBundle, right.M7SourceBundle) &&
		bytes.Equal(left.M7SourceReplay, right.M7SourceReplay)
}

func cloneGenerateInputs(input coexistence.GenerateInputsV1) coexistence.GenerateInputsV1 {
	return coexistence.GenerateInputsV1{
		Registry:          bytes.Clone(input.Registry),
		M7Graph:           bytes.Clone(input.M7Graph),
		M7Replay:          bytes.Clone(input.M7Replay),
		M7Registry:        bytes.Clone(input.M7Registry),
		M7SourceBundle:    bytes.Clone(input.M7SourceBundle),
		M7SourceReplay:    bytes.Clone(input.M7SourceReplay),
		BaselineRuntime:   bytes.Clone(input.BaselineRuntime),
		ComparedRuntime:   bytes.Clone(input.ComparedRuntime),
		CaptureClock:      bytes.Clone(input.CaptureClock),
		CaptureTimestamps: bytes.Clone(input.CaptureTimestamps),
		MaskedSubjects:    bytes.Clone(input.MaskedSubjects),
	}
}

func equalGenerateInputs(left, right coexistence.GenerateInputsV1) bool {
	return bytes.Equal(left.Registry, right.Registry) &&
		bytes.Equal(left.M7Graph, right.M7Graph) &&
		bytes.Equal(left.M7Replay, right.M7Replay) &&
		bytes.Equal(left.M7Registry, right.M7Registry) &&
		bytes.Equal(left.M7SourceBundle, right.M7SourceBundle) &&
		bytes.Equal(left.M7SourceReplay, right.M7SourceReplay) &&
		bytes.Equal(left.BaselineRuntime, right.BaselineRuntime) &&
		bytes.Equal(left.ComparedRuntime, right.ComparedRuntime) &&
		bytes.Equal(left.CaptureClock, right.CaptureClock) &&
		bytes.Equal(left.CaptureTimestamps, right.CaptureTimestamps) &&
		bytes.Equal(left.MaskedSubjects, right.MaskedSubjects)
}

func writeJSON(value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	encoded = append(encoded, '\n')
	_, _ = os.Stdout.Write(encoded)
}

func fail(err error) {
	fmt.Println(err.Error())
	os.Exit(1)
}
