package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/gorilla/websocket"
	graphqlgo "github.com/graphql-go/graphql"
)

const (
	wsProtocolTransport = "graphql-transport-ws"
	wsProtocolLegacy    = "graphql-ws"

	wsTypeConnectionInit = "connection_init"
	wsTypeConnectionAck  = "connection_ack"
	wsTypeSubscribe      = "subscribe"
	wsTypeStart          = "start"
	wsTypeNext           = "next"
	wsTypeError          = "error"
	wsTypeComplete       = "complete"
	wsTypeStop           = "stop"
	wsTypePing           = "ping"
	wsTypePong           = "pong"
)

type graphqlRequest struct {
	Query         string                 `json:"query"`
	Variables     map[string]any         `json:"variables"`
	OperationName string                 `json:"operationName"`
	Extensions    map[string]interface{} `json:"extensions,omitempty"`
}

type wsMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type wsPayload struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
	OperationName string         `json:"operationName"`
}

type wsOutgoing struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type"`
	Payload any    `json:"payload,omitempty"`
}

type subscriptionHandler struct {
	schema   graphqlgo.Schema
	upgrader websocket.Upgrader
}

func NewSubscriptionHandler(builder *Builder, registry InvokeRegistry, invoker Invoker, hub *BroadcastHub) (http.Handler, error) {
	schema, err := NewSchema(builder, registry, invoker, hub)
	if err != nil {
		return nil, err
	}

	return &subscriptionHandler{
		schema: schema,
		upgrader: websocket.Upgrader{
			Subprotocols: []string{wsProtocolTransport, wsProtocolLegacy},
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}, nil
}

func (handler *subscriptionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if handler == nil {
		http.Error(w, "subscription handler missing", http.StatusInternalServerError)
		return
	}

	if isWebSocketRequest(r) {
		handler.serveWebSocket(w, r)
		return
	}

	if wantsSSE(r) {
		handler.serveSSE(w, r)
		return
	}

	http.Error(w, "unsupported subscription transport", http.StatusBadRequest)
}

func isWebSocketRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	if !headerContainsToken(r.Header, "Connection", "upgrade") {
		return false
	}
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func wantsSSE(r *http.Request) bool {
	if r == nil {
		return false
	}
	if headerContainsToken(r.Header, "Accept", "text/event-stream") {
		return true
	}
	return r.URL.Query().Get("sse") == "1"
}

func headerContainsToken(header http.Header, key, token string) bool {
	values := header.Values(key)
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func (handler *subscriptionHandler) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := handler.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	send := make(chan wsOutgoing, 16) // Buffered to decouple writers from GraphQL results.
	// Goroutine exits when ctx.Done() is closed or send channel closes; serializes websocket writes.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-send:
				if !ok {
					return
				}
				payload, err := json.Marshal(msg)
				if err != nil {
					continue
				}
				if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	subscriptions := make(map[string]context.CancelFunc)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var msg wsMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			send <- wsOutgoing{Type: wsTypeError, Payload: map[string]any{"message": err.Error()}}
			continue
		}

		switch msg.Type {
		case wsTypeConnectionInit:
			send <- wsOutgoing{Type: wsTypeConnectionAck}
		case wsTypePing:
			send <- wsOutgoing{Type: wsTypePong}
		case wsTypeSubscribe, wsTypeStart:
			handler.handleSubscribe(ctx, send, subscriptions, msg)
		case wsTypeComplete, wsTypeStop:
			if cancelFn, ok := subscriptions[msg.ID]; ok {
				cancelFn()
				delete(subscriptions, msg.ID)
			}
		default:
			send <- wsOutgoing{Type: wsTypeError, ID: msg.ID, Payload: map[string]any{"message": "unsupported message type"}}
		}
	}

	cancel()
	for _, cancelFn := range subscriptions {
		cancelFn()
	}
}

func (handler *subscriptionHandler) handleSubscribe(ctx context.Context, send chan<- wsOutgoing, subscriptions map[string]context.CancelFunc, msg wsMessage) {
	if msg.ID == "" {
		send <- wsOutgoing{Type: wsTypeError, Payload: map[string]any{"message": "subscription id required"}}
		return
	}

	var payload wsPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		send <- wsOutgoing{Type: wsTypeError, ID: msg.ID, Payload: map[string]any{"message": err.Error()}}
		return
	}
	if payload.Query == "" {
		send <- wsOutgoing{Type: wsTypeError, ID: msg.ID, Payload: map[string]any{"message": "subscription query required"}}
		return
	}

	if cancelFn, ok := subscriptions[msg.ID]; ok {
		cancelFn()
	}

	subCtx, cancelFn := context.WithCancel(ctx)
	subscriptions[msg.ID] = cancelFn

	resultCh := graphqlgo.Subscribe(graphqlgo.Params{
		Schema:         handler.schema,
		RequestString:  payload.Query,
		VariableValues: payload.Variables,
		OperationName:  payload.OperationName,
		Context:        subCtx,
	})

	// Goroutine exits when subscription channel closes or ctx.Done() fires; forwards GraphQL results to the client.
	go func(id string, results <-chan *graphqlgo.Result) {
		defer func() {
			select {
			case <-ctx.Done():
				return
			case send <- wsOutgoing{Type: wsTypeComplete, ID: id}:
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-results:
				if !ok {
					return
				}
				if result == nil {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case send <- wsOutgoing{Type: wsTypeNext, ID: id, Payload: result}:
				}
			}
		}
	}(msg.ID, resultCh)
}

func (handler *subscriptionHandler) serveSSE(w http.ResponseWriter, r *http.Request) {
	request, err := parseGraphQLRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Query == "" {
		http.Error(w, "subscription query required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	resultCh := graphqlgo.Subscribe(graphqlgo.Params{
		Schema:         handler.schema,
		RequestString:  request.Query,
		VariableValues: request.Variables,
		OperationName:  request.OperationName,
		Context:        ctx,
	})

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-resultCh:
			if !ok {
				_, _ = io.WriteString(w, "event: complete\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			if result == nil {
				continue
			}
			payload, err := json.Marshal(result)
			if err != nil {
				continue
			}
			_, _ = io.WriteString(w, "event: next\ndata: ")
			_, _ = w.Write(payload)
			_, _ = io.WriteString(w, "\n\n")
			flusher.Flush()
		}
	}
}

func parseGraphQLRequest(r *http.Request) (graphqlRequest, error) {
	if r == nil {
		return graphqlRequest{}, fmt.Errorf("subscription request missing: %w", ebuserrors.ErrInvalidPayload)
	}

	switch r.Method {
	case http.MethodGet:
		return parseGraphQLRequestFromQuery(r.URL.Query())
	case http.MethodPost:
		return parseGraphQLRequestFromBody(r)
	default:
		return graphqlRequest{}, fmt.Errorf("subscription request unsupported method: %w", ebuserrors.ErrInvalidPayload)
	}
}

func parseGraphQLRequestFromQuery(values map[string][]string) (graphqlRequest, error) {
	query := firstValue(values["query"])
	if query == "" {
		return graphqlRequest{}, fmt.Errorf("subscription query missing: %w", ebuserrors.ErrInvalidPayload)
	}

	var variables map[string]any
	if raw := firstValue(values["variables"]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &variables); err != nil {
			return graphqlRequest{}, fmt.Errorf("subscription variables invalid: %w", ebuserrors.ErrInvalidPayload)
		}
	}

	return graphqlRequest{
		Query:         query,
		Variables:     variables,
		OperationName: firstValue(values["operationName"]),
	}, nil
}

func parseGraphQLRequestFromBody(r *http.Request) (graphqlRequest, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" || !strings.Contains(contentType, "application/json") {
		return graphqlRequest{}, fmt.Errorf("subscription body must be application/json: %w", ebuserrors.ErrInvalidPayload)
	}

	var request graphqlRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		return graphqlRequest{}, fmt.Errorf("subscription body invalid: %w", ebuserrors.ErrInvalidPayload)
	}
	if request.Query == "" {
		return graphqlRequest{}, fmt.Errorf("subscription query missing: %w", ebuserrors.ErrInvalidPayload)
	}
	return request, nil
}

func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
