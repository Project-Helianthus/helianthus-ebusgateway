package graphql

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

func TestBroadcastHub_SubscribeAndBroadcast(t *testing.T) {
	hub := NewBroadcastHub(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := hub.Subscribe(ctx, 0xB5, 0x16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}

	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}
	if err := hub.OnBroadcast(frame); err != nil {
		t.Fatalf("OnBroadcast error = %v", err)
	}

	select {
	case value := <-ch:
		event, ok := value.(BroadcastEvent)
		if !ok {
			t.Fatalf("event type = %T; want BroadcastEvent", value)
		}
		if event.Source != 0x10 || event.Primary != 0xB5 || event.Secondary != 0x16 {
			t.Fatalf("event = %+v; want source=0x10 primary=0xB5 secondary=0x16", event)
		}
		if len(event.Data) != 2 || event.Data[0] != 0x01 || event.Data[1] != 0x02 {
			t.Fatalf("event data = %v; want [1 2]", event.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broadcast event")
	}
}

func TestBroadcastHub_SubscriptionsUpdated(t *testing.T) {
	updates := make(chan []router.Subscription, 2)
	var hub *BroadcastHub
	hub = NewBroadcastHub(func() {
		updates <- hub.Subscriptions()
	})

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, 0xB5, 0x16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}

	select {
	case subs := <-updates:
		if len(subs) != 1 || subs[0].Primary != 0xB5 || subs[0].Secondary != 0x16 {
			t.Fatalf("subscriptions = %+v; want [0xB5 0x16]", subs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for subscription update")
	}

	cancel()

	select {
	case <-updates:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for unsubscribe update")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("subscription channel should be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for subscription channel close")
	}
}

func TestBroadcastHub_SemanticUpdates(t *testing.T) {
	hub := NewBroadcastHub(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zoneCh, err := hub.SubscribeZones(ctx)
	if err != nil {
		t.Fatalf("SubscribeZones error = %v", err)
	}

	dhwCh, err := hub.SubscribeDHW(ctx)
	if err != nil {
		t.Fatalf("SubscribeDHW error = %v", err)
	}

	energyCh, err := hub.SubscribeEnergy(ctx)
	if err != nil {
		t.Fatalf("SubscribeEnergy error = %v", err)
	}

	radioCh, err := hub.SubscribeRadioDevices(ctx)
	if err != nil {
		t.Fatalf("SubscribeRadioDevices error = %v", err)
	}

	hub.PublishZoneUpdate(Zone{ID: "zone-1", Name: "Zone 1"})
	hub.PublishDHWUpdate(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	hub.PublishEnergyUpdate(&EnergyTotals{})
	hub.PublishRadioDevicesUpdate([]RadioDevice{{Group: 0x09, Instance: 0x01}})

	select {
	case value := <-zoneCh:
		zone, ok := value.(Zone)
		if !ok || zone.ID != "zone-1" {
			t.Fatalf("zone = %#v; want zone-1", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for zone update")
	}

	select {
	case value := <-dhwCh:
		status, ok := value.(*DhwStatus)
		if !ok || status.Config.OperatingMode != "auto" {
			t.Fatalf("dhw = %#v; want operatingMode=auto", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for dhw update")
	}

	select {
	case value := <-energyCh:
		if _, ok := value.(*EnergyTotals); !ok {
			t.Fatalf("energy = %#v; want EnergyTotals", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for energy update")
	}

	select {
	case value := <-radioCh:
		devices, ok := value.([]RadioDevice)
		if !ok || len(devices) != 1 || devices[0].Group != 0x09 || devices[0].Instance != 0x01 {
			t.Fatalf("radio devices = %#v; want one entry group=0x09 instance=0x01", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for radio devices update")
	}
}
