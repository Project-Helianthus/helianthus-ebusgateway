package modbusadapter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
)

func TestCanonicalPVMapperProjectsQualifiedFroniusObservation(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:canonical-pv", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	qualified, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 71, DeadlineIdentity: 81})
	if err != nil || qualified.Outcome != SunSpecQualificationGO {
		t.Fatalf("qualification=%#v err=%v", qualified, err)
	}
	observation, encoded, ok := adapter.SunSpecQualificationObservation(qualified.CapabilityID, qualified.SampleID)
	if !ok {
		t.Fatal("qualified observation was not retained")
	}
	mapper, err := NewCanonicalPVMapper()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := mapper.Map(observation, encoded, 100)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ContractID != pv.ContractV1 || snapshot.Capability.Outcome != pv.CapabilitySatisfied || len(snapshot.Facts) != 11 {
		t.Fatalf("canonical snapshot contract=%q capability=%#v facts=%d", snapshot.ContractID, snapshot.Capability, len(snapshot.Facts))
	}
	if len(snapshot.RequestedOutputs) != 14 || len(snapshot.ProjectionReport) != 14 {
		t.Fatalf("accounting requested=%d projection=%d", len(snapshot.RequestedOutputs), len(snapshot.ProjectionReport))
	}
	withheld := 0
	for _, projection := range snapshot.ProjectionReport {
		if projection.Outcome == pv.ProjectionWithheld {
			withheld++
		}
	}
	if withheld != 3 {
		t.Fatalf("withheld outputs=%d, want 3", withheld)
	}
	visible, _ := json.Marshal(snapshot)
	for _, forbidden := range []string{"tcp://", listener.Addr().String(), "raw_words", "wire_response_bytes"} {
		if strings.Contains(string(visible), forbidden) {
			t.Fatalf("canonical snapshot leaked %q", forbidden)
		}
	}
}
