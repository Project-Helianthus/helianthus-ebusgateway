package main

import (
	"context"
	"errors"
	"fmt"
	"go/printer"
	"go/token"
	"io"
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

// requireStepsInOrder fails unless every step of want occurs in got, in the
// order of want.
func requireStepsInOrder(t *testing.T, label string, got, want []string) {
	t.Helper()
	next := 0
	for _, step := range got {
		if next < len(want) && step == want[next] {
			next++
		}
	}
	if next != len(want) {
		t.Fatalf("%s: %q is missing or out of order\n want in order: %s\n got: %s", label, want[next], strings.Join(want, " -> "), strings.Join(got, " -> "))
	}
}

// The process registers the cancellation of the control-plane context, starts
// the HTTP control plane, registers the consumer shutdown over its server and
// only then registers and binds the generation tasks. At shutdown the
// generation tasks join first, then the consumers close, then the control-plane
// context is canceled.
func TestGatewayConsumerShutdownLifecycleWiring(t *testing.T) {
	trace := observeProcessShellSteps(t)
	result := runRCCLifecycle(t, rccLifecycleOptions{dial: rccDialHealthyAfterRelease, scanOnStart: true, cancelAt: "background", releaseDialAtHTTP: true, observability: true})
	if result.runErr != nil {
		t.Fatalf("run result = %v; want nil", result.runErr)
	}
	requireStepsInOrder(t, "consumer shutdown wiring", trace.snapshot(), []string{
		"register:control-plane context",
		"acquire:HTTP control plane",
		"register:consumers",
		"register:generation tasks",
		"release:generation tasks",
		"release:consumers",
		"release:control-plane context",
	})

	fset := token.NewFileSet()
	files, compositionText := lifecycleCompositionSources(t, fset)
	// The generation-task release closes the admission of the runtime control,
	// which joins the generation tasks.
	var lifecycle strings.Builder
	if err := printer.Fprint(&lifecycle, fset, lifecycleFunctionDecl(t, files, "runGatewayLifecycle").Body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lifecycle.String(), `shell.onShutdown("generation tasks"`) || strings.Count(lifecycle.String(), "control.CloseAdmission(closeCtx)") != 1 {
		t.Fatal("the generation-task release of runGatewayLifecycle does not close the runtime-control admission")
	}
	for _, binding := range []string{
		"stopTransportWatch: stopTransportWatch,",
		"publishManagedTransportRetirement(busObservability, *cfg)",
		"return shutdownHTTPControlPlane(server)",
		"surface: retained.surface,",
		"consumerShutdown.deduplicator = deduplicator", "consumerShutdown.busObservability = busObservability",
		"consumerShutdown.advertiser = advertiser",
	} {
		if strings.Count(compositionText, binding) != 1 {
			t.Fatalf("shutdown binding %q must occur exactly once", binding)
		}
	}
	// The retained owner snapshots the listener and reconstructor under its
	// surface lock.
	method := lifecycleMethodDecl(t, files, "retainedEBusConsumers", "surface")
	var body strings.Builder
	if err := printer.Fprint(&body, fset, method.Body); err != nil {
		t.Fatal(err)
	}
	for _, binding := range []string{
		"c.surfaceMu.Lock()\n\tdefer c.surfaceMu.Unlock()",
		"surface.listener = c.listener", "surface.reconstructor = c.reconstructor",
	} {
		if strings.Count(body.String(), binding) != 1 {
			t.Fatalf("surface binding %q must occur exactly once in (*retainedEBusConsumers).surface", binding)
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
