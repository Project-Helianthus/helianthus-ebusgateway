package portal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type o09Route struct {
	name, prefix, mount, ingress, base, graphql string
	stripMount                                  bool
}

func o09Routes() []o09Route {
	return []o09Route{
		{name: "root", mount: "/", base: "/", graphql: "/graphql"},
		{name: "mount", mount: "/cards/", base: "/cards/", graphql: "/graphql"},
		{name: "prefix_one", prefix: "/proxy/one", mount: "/cards/", base: "/proxy/one/cards/", graphql: "/proxy/one/graphql"},
		{name: "prefix_two", prefix: "/embed/two", mount: "/cards/", base: "/embed/two/cards/", graphql: "/embed/two/graphql"},
		{name: "header_one", mount: "/cards/", ingress: "/ingress/one", base: "/ingress/one/cards/", graphql: "/ingress/one/graphql"},
		{name: "header_two", mount: "/cards/", ingress: "/entry/two", base: "/entry/two/cards/", graphql: "/entry/two/graphql"},
		{name: "composed", prefix: "/proxy/one", mount: "/cards/", ingress: "/ingress/one", base: "/ingress/one/proxy/one/cards/", graphql: "/ingress/one/proxy/one/graphql"},
		{name: "other_mount", prefix: "/proxy/one", mount: "/ui/panels/", ingress: "/ingress/one", base: "/ingress/one/proxy/one/ui/panels/", graphql: "/ingress/one/proxy/one/graphql"},
		{name: "stripped_mount", prefix: "/proxy/one", mount: "/cards/", ingress: "/ingress/one", base: "/ingress/one/proxy/one/cards/", graphql: "/ingress/one/proxy/one/graphql", stripMount: true},
	}
}

func o09Request(tc o09Route, target string) *http.Request {
	// Model only path stripping; no proxy, listener or authentication is involved.
	r := httptest.NewRequest(http.MethodGet, strings.TrimPrefix(target, tc.ingress+tc.prefix), nil)
	if tc.ingress != "" {
		r.Header.Set(IngressPathHeader, tc.ingress)
	}
	return r
}

func o09Handler(tc o09Route, list func() []RegistryDevice) http.Handler {
	h := NewHandler(Options{ExternalPrefix: tc.prefix, BasePath: tc.mount, ListRegistry: list})
	if tc.stripMount {
		return http.StripPrefix(strings.TrimSuffix(tc.mount, "/"), h)
	}
	return h
}

func o09Bootstrap(t *testing.T, tc o09Route, h http.Handler, available bool) string {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, o09Request(tc, tc.base+"api/v1/bootstrap"))
	var out struct {
		Endpoints    map[string]string `json:"endpoints"`
		Capabilities map[string]bool   `json:"capabilities"`
	}
	if w.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", w.Code, w.Body)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.Endpoints["graphql"]; got != tc.graphql {
		t.Fatalf("GraphQL endpoint=%q; want %q", got, tc.graphql)
	}
	want := tc.base + "api/v1/stream"
	if got := out.Endpoints["stream"]; got != want {
		t.Fatalf("stream endpoint=%q; want %q", got, want)
	}
	if out.Capabilities["stream"] != available {
		t.Fatalf("stream availability=%v; want %v", out.Capabilities["stream"], available)
	}
	return out.Endpoints["stream"]
}

type o09Frame struct {
	status int
	header http.Header
	body   string
}

// Bytes become observable only on Flush. The first Flush stays in flight until
// the reader has inspected its frame and canceled the request.
type o09StreamWriter struct {
	header  http.Header
	status  int
	pending bytes.Buffer
	first   chan o09Frame
	release chan struct{}
	abort   chan struct{}
	once    sync.Once
}

func (w *o09StreamWriter) Header() http.Header    { return w.header }
func (w *o09StreamWriter) WriteHeader(status int) { w.status = status }
func (w *o09StreamWriter) Write(p []byte) (int, error) {
	select {
	case <-w.abort:
		return 0, errors.New("O09 watchdog teardown")
	default:
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.pending.Write(p)
}
func (w *o09StreamWriter) Flush() {
	w.once.Do(func() {
		w.first <- o09Frame{status: w.status, header: w.header.Clone(), body: w.pending.String()}
		<-w.release
	})
	w.pending.Reset()
}

func TestO09PrefixedStreamFlushAndCancel(t *testing.T) {
	for _, tc := range o09Routes() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			h := o09Handler(tc, func() []RegistryDevice {
				calls.Add(1)
				return nil
			})
			endpoint := o09Bootstrap(t, tc, h, true)
			ctx, cancel := context.WithCancel(context.Background())
			w := &o09StreamWriter{header: make(http.Header), first: make(chan o09Frame, 1), release: make(chan struct{}), abort: make(chan struct{})}
			done := make(chan struct{})
			var release sync.Once
			go func() {
				defer close(done)
				h.ServeHTTP(w, o09Request(tc, endpoint+"?layers=registry&interval_ms=200&max_events_per_second=30").WithContext(ctx))
			}()
			t.Cleanup(func() {
				cancel()
				release.Do(func() { close(w.release) })
				// Only a failed watchdog enables write-error teardown; successful
				// cancellation must join without this alternate termination path.
				close(w.abort)
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("watchdog teardown could not join stream handler")
				}
			})
			select {
			case frame := <-w.first:
				if ctx.Err() != nil || calls.Load() == 0 {
					t.Fatal("first flush did not precede cancellation and provider work")
				}
				if frame.status != http.StatusOK || frame.header.Get("Content-Type") != "text/event-stream" || frame.header.Get("Cache-Control") != "no-store" {
					t.Fatalf("stream status/headers=%d %v", frame.status, frame.header)
				}
				if !strings.HasPrefix(frame.body, "event: update\ndata: ") || !strings.HasSuffix(frame.body, "\n\n") {
					t.Fatalf("first flushed frame=%q", frame.body)
				}
				var event struct {
					Layer   string                     `json:"layer"`
					Payload map[string]json.RawMessage `json:"payload"`
				}
				if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(frame.body, "event: update\ndata: "), "\n\n")), &event); err != nil {
					t.Fatal(err)
				}
				if event.Layer != "composite" || len(event.Payload["registry"]) == 0 {
					t.Fatalf("first event missing registry composite: %+v", event)
				}
			case <-done:
				t.Fatal("stream returned before first flush with a live request")
			case <-time.After(3 * time.Second):
				t.Fatal("first-event progress watchdog expired before cancellation")
			}
			cancel()
			release.Do(func() { close(w.release) })
			select {
			case <-done:
				// Joining establishes that this synchronous producer can do no
				// further callback or write work; no timing-based quiet period.
			case <-time.After(3 * time.Second):
				t.Fatal("canceled stream did not join; subsequent work has not stopped")
			}
		})
	}
}

func TestO09PrefixedStreamUnavailable(t *testing.T) {
	for _, tc := range o09Routes() {
		t.Run(tc.name, func(t *testing.T) {
			h := o09Handler(tc, nil)
			endpoint := o09Bootstrap(t, tc, h, false)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, o09Request(tc, endpoint))
			if w.Code != http.StatusServiceUnavailable || w.Flushed || w.Header().Get("Content-Type") == "text/event-stream" || strings.TrimSpace(w.Body.String()) != "stream unavailable" {
				t.Fatalf("unavailable stream status=%d flushed=%v headers=%v body=%q", w.Code, w.Flushed, w.Header(), w.Body.String())
			}
		})
	}
}
