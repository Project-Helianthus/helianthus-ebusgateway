package graphql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/gorilla/websocket"
)

type emptyRegistry struct{}

func (emptyRegistry) Iterate(func(registry.DeviceEntry) bool) {}

// IterateSnapshots — P9.x. No-op for empty registry.
func (emptyRegistry) IterateSnapshots(func(registry.DeviceEntrySnapshot) bool) {}

type noopBus struct{}

func (noopBus) Send(context.Context, protocol.Frame) (*protocol.Frame, error) {
	return nil, nil
}

func TestBroadcastSubscriptions_Integration(t *testing.T) {
	query := `
		subscription($primary: Int!, $secondary: Int!) {
			broadcast(primary: $primary, secondary: $secondary) {
				source
				target
				primary
				secondary
				data
			}
		}
	`

	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}

	t.Run("websocket", func(t *testing.T) {
		server, hub, eventRouter := newSubscriptionServer(t)
		defer server.Close()

		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		dialer := websocket.Dialer{Subprotocols: []string{wsProtocolTransport}}
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("websocket dial error = %v", err)
		}
		defer func() { _ = conn.Close() }()

		if err := conn.WriteJSON(wsOutgoing{Type: wsTypeConnectionInit}); err != nil {
			t.Fatalf("connection_init send error = %v", err)
		}

		var ack wsMessage
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline error = %v", err)
		}
		if err := conn.ReadJSON(&ack); err != nil {
			t.Fatalf("connection_ack read error = %v", err)
		}
		if ack.Type != wsTypeConnectionAck {
			t.Fatalf("ack type = %s; want %s", ack.Type, wsTypeConnectionAck)
		}

		payload := wsPayload{
			Query: query,
			Variables: map[string]any{
				"primary":   int(frame.Primary),
				"secondary": int(frame.Secondary),
			},
		}
		subscribe := wsOutgoing{
			ID:      "1",
			Type:    wsTypeSubscribe,
			Payload: payload,
		}
		if err := conn.WriteJSON(subscribe); err != nil {
			t.Fatalf("subscribe send error = %v", err)
		}

		waitForSubscription(t, hub, frame.Primary, frame.Secondary)
		eventRouter.HandleBroadcast(frame)

		var msg wsMessage
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline error = %v", err)
		}
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatalf("next read error = %v", err)
		}
		if msg.Type != wsTypeNext {
			t.Fatalf("message type = %s; want %s", msg.Type, wsTypeNext)
		}

		var payloadData struct {
			Data struct {
				Broadcast struct {
					Source    int   `json:"source"`
					Target    int   `json:"target"`
					Primary   int   `json:"primary"`
					Secondary int   `json:"secondary"`
					Data      []int `json:"data"`
				} `json:"broadcast"`
			} `json:"data"`
		}
		if err := json.Unmarshal(msg.Payload, &payloadData); err != nil {
			t.Fatalf("payload decode error = %v", err)
		}
		if payloadData.Data.Broadcast.Primary != int(frame.Primary) || payloadData.Data.Broadcast.Secondary != int(frame.Secondary) {
			t.Fatalf("broadcast payload = %+v; want primary=0xB5 secondary=0x16", payloadData.Data.Broadcast)
		}
		if len(payloadData.Data.Broadcast.Data) != 2 || payloadData.Data.Broadcast.Data[0] != 1 || payloadData.Data.Broadcast.Data[1] != 2 {
			t.Fatalf("broadcast data = %v; want [1 2]", payloadData.Data.Broadcast.Data)
		}
	})

	t.Run("sse", func(t *testing.T) {
		server, hub, eventRouter := newSubscriptionServer(t)
		defer server.Close()

		body, err := json.Marshal(graphqlRequest{
			Query: query,
			Variables: map[string]any{
				"primary":   int(frame.Primary),
				"secondary": int(frame.Secondary),
			},
		})
		if err != nil {
			t.Fatalf("json.Marshal error = %v", err)
		}

		req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("http.NewRequest error = %v", err)
		}
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("SSE request error = %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("SSE status = %d; want 200", resp.StatusCode)
		}

		waitForSubscription(t, hub, frame.Primary, frame.Secondary)
		eventRouter.HandleBroadcast(frame)

		reader := bufio.NewReader(resp.Body)
		data := readSSEData(t, reader)

		var payloadData struct {
			Data struct {
				Broadcast struct {
					Primary   int   `json:"primary"`
					Secondary int   `json:"secondary"`
					Data      []int `json:"data"`
				} `json:"broadcast"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &payloadData); err != nil {
			t.Fatalf("payload decode error = %v", err)
		}
		if payloadData.Data.Broadcast.Primary != int(frame.Primary) || payloadData.Data.Broadcast.Secondary != int(frame.Secondary) {
			t.Fatalf("broadcast payload = %+v; want primary=0xB5 secondary=0x16", payloadData.Data.Broadcast)
		}
		if len(payloadData.Data.Broadcast.Data) != 2 || payloadData.Data.Broadcast.Data[0] != 1 || payloadData.Data.Broadcast.Data[1] != 2 {
			t.Fatalf("broadcast data = %v; want [1 2]", payloadData.Data.Broadcast.Data)
		}
	})
}

func newSubscriptionServer(t *testing.T) (*httptest.Server, *BroadcastHub, *router.BusEventRouter) {
	t.Helper()

	builder := NewBuilder(emptyRegistry{}, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start error = %v", err)
	}

	eventRouter := router.NewBusEventRouter(noopBus{})
	var hub *BroadcastHub
	hub = NewBroadcastHub(func() {
		eventRouter.SetPlanes([]router.Plane{hub})
	})
	eventRouter.SetPlanes([]router.Plane{hub})

	handler, err := NewSubscriptionHandler(builder, nil, nil, hub)
	if err != nil {
		t.Fatalf("NewSubscriptionHandler error = %v", err)
	}

	return httptest.NewServer(handler), hub, eventRouter
}

func waitForSubscription(t *testing.T, hub *BroadcastHub, primary, secondary byte) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, sub := range hub.Subscriptions() {
			if sub.Primary == primary && sub.Secondary == secondary {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscription not registered for 0x%02x 0x%02x", primary, secondary)
}

func readSSEData(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()

	resultCh := make(chan []byte, 1)
	go func() {
		var data string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				resultCh <- nil
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if data != "" {
					resultCh <- []byte(data)
					return
				}
				continue
			}
			if strings.HasPrefix(line, "data: ") {
				data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()

	select {
	case data := <-resultCh:
		if len(data) == 0 {
			t.Fatal("empty SSE payload")
		}
		return data
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SSE payload")
		return nil
	}
}
