package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

type consumerShutdownCloser struct {
	close func() error
}

func (c consumerShutdownCloser) Close() error { return c.close() }

func TestGatewayConsumerShutdownOrderAndFailures(t *testing.T) {
	for _, failure := range []string{"", "http", "listener", "deduplicator", "reconstructor", "bus observability", "mdns"} {
		t.Run("failure="+failure, func(t *testing.T) {
			var events, logs []string
			closeResource := func(name string) func() error {
				return func() error {
					events = append(events, name)
					if name == failure {
						return errors.New("synthetic close failure")
					}
					return nil
				}
			}
			resource := func(name string) io.Closer {
				return consumerShutdownCloser{closeResource(name)}
			}
			// Installation happens after registration, as it does for a first generation.
			var surface gatewayConsumerSurface
			owner := gatewayConsumerShutdown{
				stopTransportWatch: func() { events = append(events, "watch") },
				publishRetirement:  func() { events = append(events, "retire") },
				drainHTTP:          closeResource("http"),
				surface: func() gatewayConsumerSurface {
					events = append(events, "snapshot")
					return surface
				},
				deduplicator:     resource("deduplicator"),
				busObservability: resource("bus observability"),
				advertiser:       resource("mdns"),
				logf: func(format string, args ...any) {
					logs = append(logs, fmt.Sprintf(format, args...))
				},
			}
			surface = gatewayConsumerSurface{resource("listener"), resource("reconstructor")}
			parent, cancelParent := context.WithCancel(context.Background())
			control, cancelControl := newGatewayControlPlaneContext(parent)
			cancelParent()
			if control.Err() != nil {
				t.Fatal("parent cancellation reached the control plane before teardown")
			}
			func() {
				defer func() { cancelControl(); events = append(events, "cancel") }()
				defer owner.shutdown()
				defer func() { events = append(events, "generation join") }()
			}()
			want := []string{"generation join", "watch", "retire", "http", "snapshot", "listener", "deduplicator", "reconstructor", "bus observability", "mdns", "cancel"}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events = %v; want exactly %v", events, want)
			}
			if control.Err() != context.Canceled {
				t.Fatal("control-plane context was not canceled after cleanup")
			}
			var wantLogs []string
			if failure != "" {
				label := map[string]string{"http": "http server shutdown", "listener": "broadcast listener close", "deduplicator": "deduplicator close", "reconstructor": "reconstructor close", "bus observability": "bus observability close", "mdns": "mdns close"}[failure]
				wantLogs = []string{label + ": synthetic close failure"}
			}
			if !reflect.DeepEqual(logs, wantLogs) {
				t.Fatalf("logs = %v; want %v", logs, wantLogs)
			}
		})
	}
}

func TestGatewayConsumerShutdownNilResources(t *testing.T) {
	gatewayConsumerShutdown{}.shutdown()
	var events []string
	gatewayConsumerShutdown{
		stopTransportWatch: func() { events = append(events, "watch") },
		publishRetirement:  func() { events = append(events, "retire") },
		surface:            func() gatewayConsumerSurface { return gatewayConsumerSurface{} },
	}.shutdown()
	if !reflect.DeepEqual(events, []string{"watch", "retire"}) {
		t.Fatalf("nil resources changed transitions: %v", events)
	}
}

func TestGatewayConsumerShutdownLifecycleWiring(t *testing.T) {
	data, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	last := -1
	for _, step := range []string{"defer controlPlaneCancel()", "server, advertiser, err := startHTTPServerFn(", "consumerShutdown := gatewayConsumerShutdown{", "defer consumerShutdown.shutdown()", "control.CloseAdmission(closeCtx)", "control.BindTasks(ctx, generationTasks"} {
		position := strings.Index(text, step)
		if step == "control.CloseAdmission(closeCtx)" {
			position = strings.LastIndex(text, step)
		}
		if position <= last {
			t.Fatalf("missing or reversed lifecycle step %q", step)
		}
		last = position
	}
	for _, binding := range []string{
		"stopTransportWatch: stopTransportWatch,",
		"publishManagedTransportRetirement(busObservability, cfg)",
		"return shutdownHTTPControlPlane(server)",
		"surfaceMu.Lock()\n\t\t\tdefer surfaceMu.Unlock()",
		"surface.listener = listener", "surface.reconstructor = reconstructor",
		"consumerShutdown.deduplicator = deduplicator", "consumerShutdown.busObservability = busObservability",
		"consumerShutdown.advertiser = advertiser",
	} {
		if strings.Count(text, binding) != 1 {
			t.Fatalf("shutdown binding %q must occur exactly once", binding)
		}
	}
}

func TestGatewayConsumerShutdownWaitsForHTTPDrain(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var events []string
	owner := gatewayConsumerShutdown{
		drainHTTP: func() error {
			close(entered)
			<-release
			events = append(events, "drained")
			return nil
		},
		surface: func() gatewayConsumerSurface {
			events = append(events, "snapshot")
			return gatewayConsumerSurface{listener: consumerShutdownCloser{func() error {
				events = append(events, "listener")
				return nil
			}}}
		},
	}
	go func() { owner.shutdown(); close(done) }()
	<-entered
	select {
	case <-done:
		t.Fatal("cleanup returned before HTTP drain completed")
	default:
	}
	close(release)
	<-done
	if !reflect.DeepEqual(events, []string{"drained", "snapshot", "listener"}) {
		t.Fatalf("HTTP drain and release order = %v", events)
	}
}
