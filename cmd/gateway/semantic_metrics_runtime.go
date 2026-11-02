package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticmetrics"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// gatewaySemanticMetricRegistry is the production Thermal and Infrastructure
// binding registry of the scrape route. The coverage contract requires every
// slot assignment to be durably persisted before publication and retired slots
// to stay reserved for the scrape target. No such store exists yet, so the
// production route configures no binding: it declares the availability family
// without samples and never captures.
func gatewaySemanticMetricRegistry() semanticmetrics.Registry {
	return semanticmetrics.Registry{}
}

// gatewaySemanticMetrics renders the configured semantic families of the
// existing scrape route from the HTTP shared hub snapshot provider, the same
// instance GraphQL, MCP and Portal read. A scrape captures at most once and
// never acquires native data, publishes or touches the transport store.
type gatewaySemanticMetrics struct {
	provider *hubSemanticSnapshotProvider
	registry semanticmetrics.Registry
	// assets are the distinct assets of the bindings that can still export a
	// value, in bytewise order. Validation bounds a registry to 16 slots per
	// domain, so the selection never exceeds the capture bound of 32 assets.
	assets []semreg.AssetID
	logf   func(string, ...any)

	mu          sync.Mutex
	lastFailure string
}

// newGatewaySemanticMetrics takes ownership of registry; callers must not
// mutate it afterwards. A malformed registry fails composition rather than
// silently omitting its configured series from every scrape.
func newGatewaySemanticMetrics(provider *hubSemanticSnapshotProvider, registry semanticmetrics.Registry) (*gatewaySemanticMetrics, error) {
	if _, err := semanticmetrics.Render(semanticview.View{}, registry); err != nil {
		return nil, fmt.Errorf("semantic metric registry: %w", err)
	}
	metrics := &gatewaySemanticMetrics{provider: provider, registry: registry, logf: log.Printf}
	// An unvalidated registry or a retired slot withholds the value whatever
	// the capture holds, so neither needs a hub read.
	if registry.Validated {
		seen := map[semreg.AssetID]bool{}
		for _, binding := range registry.Bindings {
			if !binding.Retired && !seen[binding.AssetID] {
				seen[binding.AssetID] = true
				metrics.assets = append(metrics.assets, binding.AssetID)
			}
		}
		sort.Slice(metrics.assets, func(i, j int) bool { return metrics.assets[i] < metrics.assets[j] })
	}
	return metrics, nil
}

// text renders every configured field from one coherent capture. A failed
// capture or view withholds every value: configured fields report
// availability 0 and no other read path is tried. Text is empty only when the
// renderer itself fails, which omits the semantic section from that scrape.
func (m *gatewaySemanticMetrics) text(ctx context.Context) string {
	view := semanticview.View{}
	var failure error
	if len(m.assets) > 0 {
		metadata, inputs, err := m.provider.Capture(ctx, m.assets)
		if err != nil {
			failure = fmt.Errorf("capture: %w", err)
		} else if view, err = semanticview.Build(metadata, inputs); err != nil {
			view, failure = semanticview.View{}, fmt.Errorf("view: %w", err)
		}
	}
	text, err := semanticmetrics.Render(view, m.registry)
	if err != nil {
		text, failure = "", fmt.Errorf("render: %w", err)
	}
	m.note(failure)
	return text
}

// note logs a failure once when it begins or changes, so a scrape interval
// cannot flood the log; the availability gauge carries the per-scrape state.
// A scraper abandoning its request is not a gateway failure.
func (m *gatewaySemanticMetrics) note(failure error) {
	if errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded) {
		return
	}
	message := ""
	if failure != nil {
		message = failure.Error()
	}
	m.mu.Lock()
	changed := message != m.lastFailure
	m.lastFailure = message
	m.mu.Unlock()
	if changed && message != "" {
		m.logf("semantic metrics withheld: %s", message)
	}
}

// newGatewayMetricsHandler serves the transport exposition with the status and
// headers of store.MetricsHandler unchanged, then appends the semantic
// families. The store renders and releases its lock before the semantic
// capture starts, so neither section waits on the other; a semantic failure
// affects only semantic samples.
func newGatewayMetricsHandler(store *ebusgateway.BusObservabilityStore, semantic *gatewaySemanticMetrics) http.Handler {
	if store == nil {
		return nil
	}
	transport := store.MetricsHandler()
	if semantic == nil {
		return transport
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transport.ServeHTTP(w, r)
		_, _ = io.WriteString(w, semantic.text(r.Context()))
	})
}
