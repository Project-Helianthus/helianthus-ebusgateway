package graphql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type countingWatchSummaryProvider struct {
	mu        sync.Mutex
	calls     int
	updatedAt time.Time
}

func (provider *countingWatchSummaryProvider) Snapshot() WatchSummary {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	provider.calls++
	call := provider.calls

	return WatchSummary{
		LastUpdatedAt: &provider.updatedAt,
		ActivationCounts: WatchSummaryActivationCounts{
			ActiveKeys: call,
		},
		Degraded: WatchSummaryDegraded{
			Active:           false,
			ShadowingEnabled: true,
		},
	}
}

func (provider *countingWatchSummaryProvider) CallCount() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

func TestWatchSummaryQuery_SharedSnapshotPerOperation(t *testing.T) {
	builder := NewBuilder(mockRegistry{}, nil)
	updatedAt := time.Date(2026, time.March, 13, 9, 30, 0, 0, time.UTC)
	provider := &countingWatchSummaryProvider{updatedAt: updatedAt}
	builder.SetWatchSummaryProvider(provider)

	handler, err := NewInvokeHandler(builder, nil, nil)
	if err != nil {
		t.Fatalf("NewInvokeHandler error = %v", err)
	}

	request := `{"query":"{ first: watchSummary { lastUpdatedAt activationCounts { activeKeys } } second: watchSummary { lastUpdatedAt activationCounts { activeKeys } } }"}`
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(request))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var response struct {
		Data struct {
			First struct {
				LastUpdatedAt    string `json:"lastUpdatedAt"`
				ActivationCounts struct {
					ActiveKeys int `json:"activeKeys"`
				} `json:"activationCounts"`
			} `json:"first"`
			Second struct {
				LastUpdatedAt    string `json:"lastUpdatedAt"`
				ActivationCounts struct {
					ActiveKeys int `json:"activeKeys"`
				} `json:"activationCounts"`
			} `json:"second"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal response: %v body=%s", err, rec.Body.String())
	}
	if len(response.Errors) != 0 {
		t.Fatalf("errors = %#v; want none", response.Errors)
	}
	if provider.CallCount() != 1 {
		t.Fatalf("Snapshot() calls = %d; want 1 shared operation snapshot", provider.CallCount())
	}
	if response.Data.First.ActivationCounts.ActiveKeys != 1 || response.Data.Second.ActivationCounts.ActiveKeys != 1 {
		t.Fatalf(
			"watchSummary activationCounts.activeKeys = (%d,%d); want (1,1)",
			response.Data.First.ActivationCounts.ActiveKeys,
			response.Data.Second.ActivationCounts.ActiveKeys,
		)
	}
	if response.Data.First.LastUpdatedAt != updatedAt.Format(time.RFC3339Nano) || response.Data.Second.LastUpdatedAt != updatedAt.Format(time.RFC3339Nano) {
		t.Fatalf(
			"watchSummary lastUpdatedAt = (%s,%s); want %s",
			response.Data.First.LastUpdatedAt,
			response.Data.Second.LastUpdatedAt,
			updatedAt.Format(time.RFC3339Nano),
		)
	}
}

func TestWatchSummaryQuery_DefaultZeroValueWhenProviderUnwired(t *testing.T) {
	builder := NewBuilder(mockRegistry{}, nil)

	handler, err := NewHandler(builder)
	if err != nil {
		t.Fatalf("NewHandler error = %v", err)
	}

	req := httptest.NewRequest(
		http.MethodPost,
		"/graphql",
		strings.NewReader(`{"query":"{ watchSummary { inventory { totalEntries pinnedEntries evictableEntries } activationCounts { catalogDescriptors activeKeys } degraded { active shadowingEnabled pinnedBudgetDegraded compactorDegraded reasons } } }"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var response struct {
		Data struct {
			WatchSummary struct {
				Inventory struct {
					TotalEntries     int `json:"totalEntries"`
					PinnedEntries    int `json:"pinnedEntries"`
					EvictableEntries int `json:"evictableEntries"`
				} `json:"inventory"`
				ActivationCounts struct {
					CatalogDescriptors int `json:"catalogDescriptors"`
					ActiveKeys         int `json:"activeKeys"`
				} `json:"activationCounts"`
				Degraded struct {
					Active               bool     `json:"active"`
					ShadowingEnabled     bool     `json:"shadowingEnabled"`
					PinnedBudgetDegraded bool     `json:"pinnedBudgetDegraded"`
					CompactorDegraded    bool     `json:"compactorDegraded"`
					Reasons              []string `json:"reasons"`
				} `json:"degraded"`
			} `json:"watchSummary"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal response: %v body=%s", err, rec.Body.String())
	}
	if len(response.Errors) != 0 {
		t.Fatalf("errors = %#v; want none", response.Errors)
	}
	if response.Data.WatchSummary.Inventory.TotalEntries != 0 ||
		response.Data.WatchSummary.Inventory.PinnedEntries != 0 ||
		response.Data.WatchSummary.Inventory.EvictableEntries != 0 {
		t.Fatalf("watchSummary.inventory = %+v; want zero values", response.Data.WatchSummary.Inventory)
	}
	if response.Data.WatchSummary.ActivationCounts.CatalogDescriptors != 0 ||
		response.Data.WatchSummary.ActivationCounts.ActiveKeys != 0 {
		t.Fatalf("watchSummary.activationCounts = %+v; want zero values", response.Data.WatchSummary.ActivationCounts)
	}
	if response.Data.WatchSummary.Degraded.Active {
		t.Fatalf("watchSummary.degraded.active = true; want false")
	}
	if response.Data.WatchSummary.Degraded.ShadowingEnabled {
		t.Fatalf("watchSummary.degraded.shadowingEnabled = true; want false for unwired static provider")
	}
	if len(response.Data.WatchSummary.Degraded.Reasons) != 0 {
		t.Fatalf("watchSummary.degraded.reasons = %v; want empty", response.Data.WatchSummary.Degraded.Reasons)
	}
}
