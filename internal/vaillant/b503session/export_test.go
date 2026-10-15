package b503session

import "time"

// ArmIdleTimerForTest lets external tests finish setup with the timer disabled
// before exercising the real AfterFunc wiring through armIdleTimerLocked.
func ArmIdleTimerForTest(m *Manager, timeout time.Duration) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.idleTimeout = timeout
	m.armIdleTimerLocked()
}
