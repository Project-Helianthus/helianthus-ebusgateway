package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
)

type sunSpecDriverProducerSpy struct {
	calls      []string
	identities []modbusadapter.SunSpecPollIdentity
	result     modbusadapter.SunSpecQualificationResult
	err        error
}

func (producer *sunSpecDriverProducerSpy) Qualify(_ context.Context, identity modbusadapter.SunSpecPollIdentity) (modbusadapter.SunSpecQualificationResult, error) {
	producer.calls = append(producer.calls, "Qualify")
	producer.identities = append(producer.identities, identity)
	return producer.result, producer.err
}

func (producer *sunSpecDriverProducerSpy) Refresh(_ context.Context, identity modbusadapter.SunSpecPollIdentity) (modbusadapter.SunSpecQualificationResult, error) {
	producer.calls = append(producer.calls, "Refresh")
	producer.identities = append(producer.identities, identity)
	return producer.result, producer.err
}

func TestModbusSunSpecDriverRefreshesOnlyAfterSuccessfulQualification(t *testing.T) {
	transportErr := errors.New("qualification transport failed")
	for _, test := range []struct {
		name       string
		outcome    modbusadapter.SunSpecQualificationOutcome
		err        error
		secondCall string
	}{
		{name: "first cycle succeeds", outcome: modbusadapter.SunSpecQualificationGO, secondCall: "Refresh"},
		{name: "no go", outcome: modbusadapter.SunSpecQualificationNoGo, secondCall: "Qualify"},
		{name: "stop", outcome: modbusadapter.SunSpecQualificationStop, secondCall: "Qualify"},
		{name: "transport error", err: transportErr, secondCall: "Qualify"},
		{name: "go with error", outcome: modbusadapter.SunSpecQualificationGO, err: transportErr, secondCall: "Qualify"},
	} {
		t.Run(test.name, func(t *testing.T) {
			producer := &sunSpecDriverProducerSpy{
				result: modbusadapter.SunSpecQualificationResult{Outcome: test.outcome}, err: test.err,
			}
			driver := &modbusSunSpecLiveSmokeDriver{adapter: &countingModbusMCPAdapter{}, producer: producer}
			poll := func(id uint64) {
				t.Helper()
				result, err := driver.Poll(context.Background(), sunSpecLiveSmokeAttempt{PollID: id, DeadlineID: id + 100})
				if !errors.Is(err, producer.err) || !errors.Is(result.Err, producer.err) || !reflect.DeepEqual(result.Qualification, producer.result) {
					t.Fatalf("Poll lost producer result: qualification=%#v result error=%v error=%v", result.Qualification, result.Err, err)
				}
			}
			poll(1)
			producer.result = modbusadapter.SunSpecQualificationResult{Outcome: modbusadapter.SunSpecQualificationGO}
			producer.err = nil
			poll(2)
			// The producer's real retention test proves Refresh is non-retaining;
			// this checks that the concrete driver always selects that operation.
			poll(3)
			producer.err = transportErr
			poll(4)
			producer.err = nil
			poll(5)
			if want := []string{"Qualify", test.secondCall, "Refresh", "Refresh", "Refresh"}; !reflect.DeepEqual(producer.calls, want) {
				t.Fatalf("producer calls = %v; want %v", producer.calls, want)
			}
			for index, identity := range producer.identities {
				id := uint64(index + 1)
				if identity != (modbusadapter.SunSpecPollIdentity{PollGeneration: id, DeadlineIdentity: id + 100}) {
					t.Fatalf("poll %d identity = %#v; want the caller's poll/deadline identity", id, identity)
				}
			}
		})
	}
}

type reconnectingSunSpecProducerSpy struct {
	sunSpecDriverProducerSpy
}

func (producer *reconnectingSunSpecProducerSpy) Qualify(ctx context.Context, identity modbusadapter.SunSpecPollIdentity) (modbusadapter.SunSpecQualificationResult, error) {
	result, err := producer.sunSpecDriverProducerSpy.Qualify(ctx, identity)
	if len(producer.calls) == 1 {
		return modbusadapter.SunSpecQualificationResult{}, errors.New("initial connection lost")
	}
	return result, err
}

func TestModbusSunSpecDriverReconnectRetryStillQualifiesThenRefreshes(t *testing.T) {
	qualification := goSunSpecQualification()
	producer := &reconnectingSunSpecProducerSpy{sunSpecDriverProducerSpy: sunSpecDriverProducerSpy{
		result: modbusadapter.SunSpecQualificationResult{
			Outcome: qualification.Outcome, SampleID: qualification.Sample,
			CapabilityID: qualification.Capability, FlavorID: qualification.Flavor,
			CapabilityReason: qualification.CapabilityReason, FlavorReason: qualification.FlavorReason,
		},
	}}
	adapter := &countingModbusMCPAdapter{reconnectRequired: true}
	driver := &modbusSunSpecLiveSmokeDriver{adapter: adapter, producer: producer}
	first := runSunSpecLiveSmoke(context.Background(), time.Second, driver, modbusSunSpecLiveSmokeQualifier{}, nil)
	if first.Decision != sunSpecLiveSmokeDecisionGO || first.Attempts != 2 || !first.Recovered || adapter.reconnects != 1 {
		t.Fatalf("initial cycle=%#v reconnects=%d; want one reconnect and successful second qualification", first, adapter.reconnects)
	}
	for cycle := 0; cycle < 3; cycle++ {
		result := runSunSpecLiveSmoke(context.Background(), time.Second, driver, modbusSunSpecLiveSmokeQualifier{}, nil)
		if result.Decision != sunSpecLiveSmokeDecisionGO || result.Attempts != 1 || result.Recovered {
			t.Fatalf("refresh cycle %d=%#v; want one successful refresh", cycle, result)
		}
	}
	if want := []string{"Qualify", "Qualify", "Refresh", "Refresh", "Refresh"}; !reflect.DeepEqual(producer.calls, want) {
		t.Fatalf("producer calls = %v; want %v", producer.calls, want)
	}
	if adapter.reconnects != 1 {
		t.Fatalf("reconnects = %d; want only the initial retry", adapter.reconnects)
	}
	firstID, retryID := producer.identities[0], producer.identities[1]
	if firstID.PollGeneration == 0 || firstID.DeadlineIdentity == 0 || retryID.PollGeneration == firstID.PollGeneration || retryID.DeadlineIdentity == firstID.DeadlineIdentity {
		t.Fatalf("retry identities = %#v, %#v; want distinct nonzero identities", firstID, retryID)
	}
}
