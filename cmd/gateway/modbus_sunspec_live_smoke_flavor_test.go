package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

// Decision lines are pinned whole: the leading fields keep their order and the
// capability and flavor reasons follow them.
const sunSpecLiveSmokeLinePrefix = "modbus sunspec live smoke "

// newSunSpecLiveSmokeEndpointDriver wires the production live-smoke driver
// (real adapter, real producer, real registry) to a local fake SunSpec endpoint
// that replays words. It returns the endpoint address so tests can prove it
// never reaches a decision line.
func newSunSpecLiveSmokeEndpointDriver(t *testing.T, words []uint16) (*modbusSunSpecLiveSmokeDriver, string) {
	t.Helper()
	listener, _, err := sunspectest.ServeSunSpecChain(words)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{
		Enabled: true, Endpoint: "tcp://" + listener.Addr().String(), DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	config.SemanticPV = newTestSemanticPVClass(t)
	adapter, err := modbusadapter.Start(context.Background(), config, (&net.Dialer{}).DialContext, func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return modbus.NewTCPEndpoint(config)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := modbusadapter.NewSunSpecProducer(adapter, modbusadapter.SunSpecProducerConfig{
		UnitID:             sunSpecLiveSmokeUnitID,
		AuthorizationScope: sunSpecLiveSmokeAuthorizationScope,
		ReadTimeout:        sunSpecLiveSmokeReadTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &modbusSunSpecLiveSmokeDriver{adapter: adapter, producer: producer}, listener.Addr().String()
}

func TestSunSpecLiveSmokeDecisionThroughRealQualificationPath(t *testing.T) {
	const (
		reasonsOtherFirmware = "capability=ADMITTED flavors=1.0.0:FIRMWARE_MISMATCH,1.1.0:FIRMWARE_MISMATCH,1.2.0:FIRMWARE_MISMATCH"
	)
	for _, test := range []struct {
		name         string
		words        []uint16
		wantDecision sunSpecLiveSmokeDecision
		wantOutcome  string
		wantCategory string
		wantFlavor   string
		wantReasons  string
	}{
		{
			name:         "GO at the current firmware tuple selects flavor 1.2.0",
			words:        sunspectest.ObservedFroniusFloatControlsWordsForFirmware(sunspectest.FroniusFirmwareCurrent),
			wantDecision: sunSpecLiveSmokeDecisionGO, wantOutcome: "qualified", wantCategory: "registry_match",
			wantFlavor:  modbusreg.SunSpecFroniusObservedFlavorV12ID,
			wantReasons: "capability=ADMITTED flavors=1.0.0:FIRMWARE_MISMATCH,1.1.0:FIRMWARE_MISMATCH,1.2.0:MATCHED",
		},
		{
			name:         "GO at the earlier firmware controls chain still selects flavor 1.1.0",
			words:        sunspectest.ObservedFroniusFloatControlsWords(),
			wantDecision: sunSpecLiveSmokeDecisionGO, wantOutcome: "qualified", wantCategory: "registry_match",
			wantFlavor:  modbusreg.SunSpecFroniusObservedFlavorV11ID,
			wantReasons: "capability=ADMITTED flavors=1.0.0:CHAIN_MISMATCH,1.1.0:MATCHED,1.2.0:FIRMWARE_MISMATCH",
		},
		{
			name:         "GO at the earlier firmware base chain still selects flavor 1.0.0",
			words:        sunspectest.ObservedFroniusFloatWords(),
			wantDecision: sunSpecLiveSmokeDecisionGO, wantOutcome: "qualified", wantCategory: "registry_match",
			wantFlavor:  modbusreg.SunSpecFroniusObservedFlavorID,
			wantReasons: "capability=ADMITTED flavors=1.0.0:MATCHED,1.1.0:CHAIN_MISMATCH,1.2.0:FIRMWARE_MISMATCH",
		},
		{
			name:         "NO_GO at a later patch firmware shows FIRMWARE_MISMATCH for every flavor",
			words:        sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.42.10-1"),
			wantDecision: sunSpecLiveSmokeDecisionNoGo, wantOutcome: "not_qualified", wantCategory: "registry_no_match",
			wantReasons: reasonsOtherFirmware,
		},
		{
			name:         "NO_GO at a different revision of the current firmware",
			words:        sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.42.9-2"),
			wantDecision: sunSpecLiveSmokeDecisionNoGo, wantOutcome: "not_qualified", wantCategory: "registry_no_match",
			wantReasons: reasonsOtherFirmware,
		},
		{
			name:         "NO_GO at a prefix of the current firmware",
			words:        sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.42.9"),
			wantDecision: sunSpecLiveSmokeDecisionNoGo, wantOutcome: "not_qualified", wantCategory: "registry_no_match",
			wantReasons: reasonsOtherFirmware,
		},
		{
			name:         "NO_GO at an unknown newer firmware",
			words:        sunspectest.ObservedFroniusFloatControlsWordsForFirmware("1.43.0-1"),
			wantDecision: sunSpecLiveSmokeDecisionNoGo, wantOutcome: "not_qualified", wantCategory: "registry_no_match",
			wantReasons: reasonsOtherFirmware,
		},
		{
			name:         "NO_GO at the current firmware without the controls model shows CHAIN_MISMATCH for 1.2.0",
			words:        sunspectest.ObservedFroniusFloatWordsForFirmware(sunspectest.FroniusFirmwareCurrent),
			wantDecision: sunSpecLiveSmokeDecisionNoGo, wantOutcome: "not_qualified", wantCategory: "registry_no_match",
			wantReasons: "capability=ADMITTED flavors=1.0.0:FIRMWARE_MISMATCH,1.1.0:FIRMWARE_MISMATCH,1.2.0:CHAIN_MISMATCH",
		},
		{
			name:         "NO_GO for an admitted capability with an unobserved chain",
			words:        sunspectest.AdmittedFloatChainWords(),
			wantDecision: sunSpecLiveSmokeDecisionNoGo, wantOutcome: "not_qualified", wantCategory: "registry_no_match",
			wantReasons: "capability=ADMITTED flavors=1.0.0:CHAIN_MISMATCH,1.1.0:CHAIN_MISMATCH,1.2.0:FIRMWARE_MISMATCH",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, endpoint := newSunSpecLiveSmokeEndpointDriver(t, test.words)
			var lines []string
			result := runSunSpecLiveSmoke(context.Background(), 5*time.Second, driver, modbusSunSpecLiveSmokeQualifier{}, func(format string, args ...any) {
				lines = append(lines, fmt.Sprintf(format, args...))
			})
			if result.Decision != test.wantDecision || result.Outcome != test.wantOutcome ||
				result.Category != test.wantCategory || result.Flavor != test.wantFlavor {
				t.Fatalf("result = %#v; want decision=%s outcome=%s category=%s flavor=%q",
					result, test.wantDecision, test.wantOutcome, test.wantCategory, test.wantFlavor)
			}
			if test.wantDecision == sunSpecLiveSmokeDecisionGO {
				if result.Sample == "" || result.Capability != modbusreg.SunSpecThreePhaseMonitoringCapabilityID {
					t.Fatalf("GO result = %#v; want sample and capability", result)
				}
			} else if result.Sample != "" || result.Capability != "" || result.Flavor != "" {
				t.Fatalf("%s result = %#v; want no sample, capability or flavor", test.wantDecision, result)
			}
			wantLine := sunSpecLiveSmokeLinePrefix + "decision=" + string(test.wantDecision) +
				" outcome=" + test.wantOutcome + " category=" + test.wantCategory +
				" attempts=1 recovered=false " + test.wantReasons
			if len(lines) != 1 || lines[0] != wantLine {
				t.Fatalf("decision lines = %q; want exactly %q", lines, wantLine)
			}
			for _, private := range []string{
				endpoint, "127.0.0.1", "tcp://", "synthetic", "Fronius", "Symo",
				"sunspec-", "sunspec.flavor", "sunspec.inverter", "FRONIUS",
			} {
				if strings.Contains(lines[0], private) {
					t.Fatalf("decision line leaks %q: %s", private, lines[0])
				}
			}
		})
	}
}

func TestSunSpecLiveSmokeDecisionLineCarriesReasonsForEveryQualificationOutcome(t *testing.T) {
	adaptedReasons := []modbusadapter.SunSpecFlavorEvaluation{
		{FlavorID: modbusreg.SunSpecFroniusObservedFlavorID, Reason: modbusreg.SunSpecFroniusFlavorReasonFirmwareMismatch},
		{FlavorID: modbusreg.SunSpecFroniusObservedFlavorV11ID, Reason: modbusreg.SunSpecFroniusFlavorReasonFirmwareMismatch},
	}
	for _, test := range []struct {
		name          string
		qualification sunSpecLiveSmokeQualification
		wantTail      string
	}{
		{
			name: "NO_GO with a capability refusal and no flavor evaluation",
			qualification: sunSpecLiveSmokeQualification{
				Outcome: modbusadapter.SunSpecQualificationNoGo, CapabilityReason: modbusreg.SunSpecCapabilityReasonSourceAbsent,
			},
			wantTail: "decision=NO_GO outcome=not_qualified category=registry_no_match attempts=1 recovered=false capability=SOURCE_ABSENT flavors=-",
		},
		{
			name: "NO_GO with the reasons of an older registry",
			qualification: sunSpecLiveSmokeQualification{
				Outcome: modbusadapter.SunSpecQualificationNoGo, CapabilityReason: modbusreg.SunSpecCapabilityReasonAdmitted,
				FlavorEvaluations: adaptedReasons,
			},
			wantTail: "decision=NO_GO outcome=not_qualified category=registry_no_match attempts=1 recovered=false capability=ADMITTED flavors=1.0.0:FIRMWARE_MISMATCH,1.1.0:FIRMWARE_MISMATCH",
		},
		{
			name: "STOP from the registry keeps its reasons",
			qualification: sunSpecLiveSmokeQualification{
				Outcome: modbusadapter.SunSpecQualificationStop, CapabilityReason: modbusreg.SunSpecCapabilityReasonInvalidChain,
			},
			wantTail: "decision=STOP outcome=incoherent category=registry_stop attempts=1 recovered=false capability=INVALID_CHAIN flavors=-",
		},
		{
			name: "STOP for an unsupported matched flavor keeps its reasons",
			qualification: sunSpecLiveSmokeQualification{
				Outcome: modbusadapter.SunSpecQualificationGO, Sample: "sample-1",
				Capability:       modbusreg.SunSpecThreePhaseMonitoringCapabilityID,
				Flavor:           "sunspec.flavor.fronius.unselected@1.0.0",
				CapabilityReason: modbusreg.SunSpecCapabilityReasonAdmitted, FlavorReason: modbusreg.SunSpecFroniusFlavorReasonMatched,
				FlavorEvaluations: []modbusadapter.SunSpecFlavorEvaluation{
					{FlavorID: "sunspec.flavor.fronius.unselected@1.0.0", Reason: modbusreg.SunSpecFroniusFlavorReasonMatched},
				},
			},
			wantTail: "decision=STOP outcome=incoherent category=invalid_qualification attempts=1 recovered=false capability=ADMITTED flavors=1.0.0:MATCHED",
		},
		{
			name:          "qualifier error has no registry reasons",
			qualification: sunSpecLiveSmokeQualification{Err: errors.New("qualifier failure")},
			wantTail:      "decision=STOP outcome=error category=qualification_error attempts=1 recovered=false capability=- flavors=-",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{{}}}
			qualifier := &sunSpecLiveSmokeFakeQualifier{qualifications: []sunSpecLiveSmokeQualification{test.qualification}}
			var lines []string
			runSunSpecLiveSmoke(context.Background(), time.Second, driver, qualifier, func(format string, args ...any) {
				lines = append(lines, fmt.Sprintf(format, args...))
			})
			if want := sunSpecLiveSmokeLinePrefix + test.wantTail; len(lines) != 1 || lines[0] != want {
				t.Fatalf("decision lines = %q; want exactly %q", lines, want)
			}
		})
	}
}

func TestSunSpecLiveSmokeDecisionLineWithoutQualificationPrintsPlaceholders(t *testing.T) {
	const want = sunSpecLiveSmokeLinePrefix +
		"decision=STOP outcome=error category=poll_error attempts=1 recovered=false capability=- flavors=-"
	driver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{{Err: errors.New("dial tcp 192.0.2.1:502: refused")}}}
	var lines []string
	runSunSpecLiveSmoke(context.Background(), time.Second, driver, &sunSpecLiveSmokeFakeQualifier{}, func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	})
	if len(lines) != 1 || lines[0] != want {
		t.Fatalf("decision lines = %q; want exactly %q", lines, want)
	}

	var configLines []string
	if worker := newGatewaySunSpecLiveSmokeWorker(context.Background(), nil, func(format string, args ...any) {
		configLines = append(configLines, fmt.Sprintf(format, args...))
	}); worker != nil {
		t.Fatal("worker over a nil adapter must not start")
	}
	const wantConfig = sunSpecLiveSmokeLinePrefix +
		"decision=STOP outcome=error category=configuration_error attempts=0 recovered=false capability=- flavors=-"
	if len(configLines) != 1 || configLines[0] != wantConfig {
		t.Fatalf("configuration lines = %q; want exactly %q", configLines, wantConfig)
	}
}

func TestSunSpecLiveSmokeReasonTokensStaySingleWords(t *testing.T) {
	qualification := sunSpecLiveSmokeQualification{
		Outcome:          modbusadapter.SunSpecQualificationNoGo,
		CapabilityReason: "ADMITTED extra=1\nnext=2",
		FlavorEvaluations: []modbusadapter.SunSpecFlavorEvaluation{
			{FlavorID: "sunspec.flavor.example@1.2.0 extra=1", Reason: "FIRMWARE MISMATCH\r\n"},
			{FlavorID: "", Reason: ""},
		},
	}
	result := mapSunSpecLiveSmokeQualification(qualification, sunSpecLiveSmokeResult{Attempts: 1})
	var line string
	logSunSpecLiveSmokeDecision(func(format string, args ...any) { line = fmt.Sprintf(format, args...) }, result)
	if strings.ContainsAny(line, "\r\n\t") {
		t.Fatalf("decision line is not a single record: %q", line)
	}
	if got, want := len(strings.Fields(line)), len(strings.Fields(sunSpecLiveSmokeLinePrefix))+7; got != want {
		t.Fatalf("decision line has %d words; want %d: %q", got, want, line)
	}
	const wantTail = " capability=ADMITTED_extra_1_next_2 flavors=1.2.0_extra_1:FIRMWARE_MISMATCH__,-:-"
	if !strings.HasSuffix(line, wantTail) {
		t.Fatalf("decision line = %q; want suffix %q", line, wantTail)
	}
}
