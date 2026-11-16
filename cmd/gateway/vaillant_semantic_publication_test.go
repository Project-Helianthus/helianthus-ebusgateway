package main

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

// TestVaillantSemanticPublicationAccessorsReturnEachCollaborator varies each
// collaborator of the publication owner on its own, so an accessor that
// returns another collaborator, or a constructor that swaps or drops an
// argument, leaves one case with a wrong identity:
//   - provider: liveProvider must return exactly the provider given;
//   - hub: broadcastHub must return exactly the hub given;
//   - cache: cachePersister must return exactly the persister given;
//   - boilerPublishMu: boilerPublishLock must return the owner's own lock,
//     the same one on every call and a different one for another owner.
func TestVaillantSemanticPublicationAccessorsReturnEachCollaborator(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	hub := graphql.NewBroadcastHub(nil)
	cache := &semanticCachePersisterSpy{}

	cases := []struct {
		name     string
		provider *graphql.LiveSemanticProvider
		hub      *graphql.BroadcastHub
		cache    semanticCachePersister
	}{
		{name: "provider only", provider: provider},
		{name: "hub only", hub: hub},
		{name: "cache only", cache: cache},
		{name: "all", provider: provider, hub: hub, cache: cache},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := &vaillantSemanticPoller{publication: newVaillantSemanticPublication(tc.provider, tc.hub, tc.cache)}
			if got := poller.liveProvider(); got != tc.provider {
				t.Fatalf("liveProvider = %p; want %p", got, tc.provider)
			}
			if got := poller.broadcastHub(); got != tc.hub {
				t.Fatalf("broadcastHub = %p; want %p", got, tc.hub)
			}
			if got := poller.cachePersister(); got != tc.cache {
				t.Fatalf("cachePersister = %v; want %v", got, tc.cache)
			}
			lock := poller.boilerPublishLock()
			if lock != &poller.publication.boilerPublishMu {
				t.Fatal("boilerPublishLock does not return the owner's lock")
			}
			if poller.boilerPublishLock() != lock {
				t.Fatal("boilerPublishLock returns a different lock on a second call")
			}
			other := &vaillantSemanticPoller{publication: newVaillantSemanticPublication(tc.provider, tc.hub, tc.cache)}
			if other.boilerPublishLock() == lock {
				t.Fatal("two publication owners share one boiler publication lock")
			}
		})
	}
}

// TestVaillantSemanticPollerWithoutPublicationOwnerIsInert covers the
// registry-only poller the startup scan builds: without a publication owner
// every collaborator is nil, the publish lock is a no-op and publishing
// returns without effect.
func TestVaillantSemanticPollerWithoutPublicationOwnerIsInert(t *testing.T) {
	for _, poller := range []*vaillantSemanticPoller{nil, {}} {
		if poller.liveProvider() != nil || poller.broadcastHub() != nil || poller.cachePersister() != nil {
			t.Fatalf("poller %v without an owner returns a collaborator", poller)
		}
		lock := poller.boilerPublishLock()
		if _, ok := lock.(vaillantPublicationNoLock); !ok {
			t.Fatalf("boilerPublishLock = %T; want the no-op lock", lock)
		}
		// A no-op lock can be taken twice without blocking.
		held := 0
		lock.Lock()
		held++
		lock.Lock()
		held++
		lock.Unlock()
		lock.Unlock()
		if held != 2 {
			t.Fatalf("no-op lock taken %d times; want 2", held)
		}
	}

	flow := 41.0
	poller := &vaillantSemanticPoller{boiler: &vaillantBoilerSnapshot{FlowTemperatureC: &flow}}
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	poller.publishZones(semanticSnapshotSourceLive)
	poller.persistSemanticSnapshot()
	if poller.publication != nil {
		t.Fatal("publishing created a publication owner")
	}
}

// TestNewVaillantSemanticPollerBuildsPublicationOwner pins that the poller
// constructor hands each collaborator to the owner in its own position.
func TestNewVaillantSemanticPollerBuildsPublicationOwner(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	hub := graphql.NewBroadcastHub(nil)
	cache := &semanticCachePersisterSpy{}

	poller := newVaillantSemanticPoller(ebusgateway.Config{}, &ebusgateway.Gateway{}, provider, hub, cache)
	if poller.publication == nil {
		t.Fatal("newVaillantSemanticPoller built no publication owner")
	}
	if poller.liveProvider() != provider || poller.broadcastHub() != hub || poller.cachePersister() != cache {
		t.Fatal("newVaillantSemanticPoller placed a collaborator in the wrong position")
	}
}

// TestVaillantBoilerStatusPublicationWaitsForBoilerPublishLock pins what the
// lock guards: while the thermal withdrawal holds it, a boiler-status
// publication neither reads nor writes the provider's boiler status.
func TestVaillantBoilerStatusPublicationWaitsForBoilerPublishLock(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	dhw := 47.5
	poller := &vaillantSemanticPoller{
		publication: newVaillantSemanticPublication(provider, nil, nil),
		boiler:      &vaillantBoilerSnapshot{DhwTemperatureC: &dhw},
	}

	lock := poller.boilerPublishLock()
	lock.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		poller.publishBoilerStatus(semanticSnapshotSourceLive)
	}()
	select {
	case <-done:
		lock.Unlock()
		t.Fatal("boiler-status publication finished while the boiler publish lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	if provider.BoilerStatus() != nil {
		lock.Unlock()
		t.Fatal("boiler status published while the boiler publish lock was held")
	}
	lock.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("boiler-status publication did not finish after the lock was released")
	}
	status := provider.BoilerStatus()
	if status == nil || status.State.DhwTemperatureC == nil || *status.State.DhwTemperatureC != dhw {
		t.Fatalf("published boiler status = %+v; want DHW temperature %.1f", status, dhw)
	}
}
