package b503session_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
)

const (
	testAdapter = "adapter-test-01"
	testEpoch   = uint64(1)
)

func newTK(epoch uint64) b503session.TransportKey {
	return b503session.TransportKey{AdapterInstanceID: testAdapter, TransportEpoch: epoch}
}

func okRefresh(newEpoch uint64) b503session.RefreshFunc {
	return func(ctx context.Context) (b503session.TransportKey, error) {
		return newTK(newEpoch), nil
	}
}

func downRefresh() b503session.RefreshFunc {
	return func(ctx context.Context) (b503session.TransportKey, error) {
		return b503session.TransportKey{}, b503session.ErrTransportDown
	}
}

func failRefresh() b503session.RefreshFunc {
	return func(ctx context.Context) (b503session.TransportKey, error) {
		return b503session.TransportKey{}, errors.New("boom")
	}
}

// 1. Enable then Disable happy path.
func TestSession_EnableThenDisable_Happy(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	key, err := m.Enable(context.Background())
	if err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	if key.IssuerToken == "" {
		t.Fatal("empty issuer token")
	}
	if key.Transport != newTK(testEpoch) {
		t.Fatalf("transport mismatch: %+v", key.Transport)
	}
	if s := m.State(); s != b503session.Active {
		t.Fatalf("state=%v want Active", s)
	}
	if err := m.Disable(key); err != nil {
		t.Fatalf("Disable err=%v", err)
	}
	if s := m.State(); s != b503session.Idle {
		t.Fatalf("state=%v want Idle after Disable", s)
	}
}

// 2. Second claimant rejected with ErrSessionBusy.
func TestSession_SecondClaimant_ReturnsBusy(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("first Enable err=%v", err)
	}
	if _, err := m.Enable(context.Background()); !errors.Is(err, b503session.ErrSessionBusy) {
		t.Fatalf("second Enable err=%v want ErrSessionBusy", err)
	}
}

// 3. Disable with wrong issuer token rejected.
func TestSession_DisableWrongToken_Rejected(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	key, err := m.Enable(context.Background())
	if err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	bad := key
	bad.IssuerToken = "deadbeefcafebabe"
	if err := m.Disable(bad); !errors.Is(err, b503session.ErrWrongToken) {
		t.Fatalf("Disable err=%v want ErrWrongToken", err)
	}
	if s := m.State(); s != b503session.Active {
		t.Fatalf("state=%v want Active (still held)", s)
	}
}

// 4. Read permitted with transport match, issuer token ignored.
func TestSession_ReadsPermittedWithoutToken(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	if err := m.Read(newTK(testEpoch)); err != nil {
		t.Fatalf("Read err=%v", err)
	}
	// Wrong transport epoch -> not active for that transport.
	if err := m.Read(newTK(testEpoch + 999)); !errors.Is(err, b503session.ErrNotActive) {
		t.Fatalf("Read wrong transport err=%v want ErrNotActive", err)
	}
}

// 5. Idle timeout auto-disable.
func TestSession_IdleTimeout_AutoDisable(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Millisecond, okRefresh(testEpoch))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if s := m.State(); s != b503session.Idle && s != b503session.Disabled {
		t.Fatalf("state=%v want Idle/Disabled after idle timeout", s)
	}
}

// 6. Read resets the idle timer.
func TestSession_ReadResetsIdleTimer(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Millisecond, okRefresh(testEpoch))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	// Read every 15ms for 5 iterations (~75ms total, > idle timeout).
	for i := 0; i < 5; i++ {
		time.Sleep(15 * time.Millisecond)
		if err := m.Read(newTK(testEpoch)); err != nil {
			t.Fatalf("Read iter=%d err=%v", i, err)
		}
	}
	if s := m.State(); s != b503session.Active {
		t.Fatalf("state=%v want Active (timer should have been reset)", s)
	}
}

// 7. Transport disconnect with owner held releases + transitions to Disabled.
func TestSession_TransportDisconnect_OwnerHeld_Releases(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	m.OnTransportDisconnect()
	if s := m.State(); s != b503session.Disabled && s != b503session.Idle {
		t.Fatalf("state=%v want Disabled/Idle after disconnect", s)
	}
	// Second call must not panic/double-release.
	m.OnTransportDisconnect()
}

// 8. Transport disconnect with no owner is a no-op.
func TestSession_TransportDisconnect_NoOwner_NoOp(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	m.OnTransportDisconnect()
	if s := m.State(); s != b503session.Idle {
		t.Fatalf("state=%v want Idle", s)
	}
	// Should still be enable-able.
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable after no-op disconnect err=%v", err)
	}
}

// 9. Gateway restart destroys all state.
func TestSession_GatewayRestart_DestroysState(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	oldKey, err := m.Enable(context.Background())
	if err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	m.ResetForRestart()
	if s := m.State(); s != b503session.Idle {
		t.Fatalf("state=%v want Idle after restart", s)
	}
	// Old key no longer valid.
	if err := m.Disable(oldKey); err == nil {
		t.Fatalf("Disable with stale key should fail")
	}
	// Fresh enable works.
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("fresh Enable err=%v", err)
	}
}

// 10. Epoch advance: refresh succeeds, Expired never publicly visible.
func TestSession_EpochAdvance_RefreshSucceeds_NeverExposesExpired(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch+1))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	// Concurrently observe state while triggering epoch advance.
	var observed atomic.Value
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				s := m.State()
				if s.String() == "Expired" {
					observed.Store(true)
					return
				}
			}
		}
	}()
	m.OnEpochAdvance(context.Background(), testEpoch+1)
	close(stop)
	<-done
	if v, ok := observed.Load().(bool); ok && v {
		t.Fatal("Expired state leaked publicly via State()")
	}
	if s := m.State(); s != b503session.Active {
		t.Fatalf("state=%v want Active post-refresh", s)
	}
	if err := m.Read(newTK(testEpoch + 1)); err != nil {
		t.Fatalf("Read on new epoch err=%v", err)
	}
}

// 11. Epoch advance: refresh returns TransportDown -> Disabled outcome.
func TestSession_EpochAdvance_RefreshTransportDown_DisabledOutcome(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, downRefresh())
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	m.OnEpochAdvance(context.Background(), testEpoch+1)
	if s := m.State(); s != b503session.Disabled && s != b503session.Idle {
		t.Fatalf("state=%v want Disabled/Idle", s)
	}
}

// 12. Epoch advance: refresh fails -> subsequent reads return Busy.
func TestSession_EpochAdvance_RefreshFails_SubsequentReadBusy(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, failRefresh())
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	m.OnEpochAdvance(context.Background(), testEpoch+1)
	err := m.Read(newTK(testEpoch + 1))
	if err == nil {
		t.Fatalf("Read after failed refresh should error")
	}
	if !errors.Is(err, b503session.ErrSessionBusy) && !errors.Is(err, b503session.ErrNotActive) {
		t.Fatalf("Read err=%v want ErrSessionBusy or ErrNotActive", err)
	}
}

// 13. Concurrent reads + poller sim, no deadlock under -race.
func TestSession_ConcurrentReadsAndPollerSim_NoDeadlock(t *testing.T) {
	m := b503session.New(newTK(testEpoch), 30*time.Second, okRefresh(testEpoch))
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatalf("Enable err=%v", err)
	}
	var wg sync.WaitGroup
	stop := time.After(100 * time.Millisecond)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.Read(newTK(testEpoch))
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.State()
			}
		}
	}()
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: concurrent reads+state never completed")
	}
}

// 14. State.String covers all public labels.
func TestState_String_AllPublicValues(t *testing.T) {
	cases := map[b503session.State]string{
		b503session.Idle:     "Idle",
		b503session.Enabling: "Enabling",
		b503session.Active:   "Active",
		b503session.Disabled: "Disabled",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("State(%d).String()=%q want %q", int(s), got, want)
		}
	}
}
