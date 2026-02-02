package graphql

import (
	"context"
	"testing"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/router"
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
