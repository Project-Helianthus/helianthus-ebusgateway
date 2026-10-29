package adaptermux

import (
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"io"
	"testing"
	"time"
)

// closeOrLog invokes c.Close and, on error, surfaces the failure via
// t.Logf. Use it in test teardown (defer or terminal statements) in
// place of the historic `defer c.Close()` pattern that silently
// dropped errors. The test still passes on close error, but running
// with `-v` makes teardown regressions visible.
//
// The `what` label is used verbatim in the log line so stack reviews
// can identify the specific closer that failed.
func closeOrLog(t testing.TB, c io.Closer, what string) {
	t.Helper()
	if c == nil {
		return
	}
	if err := c.Close(); err != nil {
		t.Logf("close %s: %v", what, err)
	}
}

// grantQueuedAtSYN supplies the admission boundary explicitly. Tests that focus
// on payload delivery omit mirroring this fixture SYN to their client streams.
func grantQueuedAtSYN(t testing.TB, m *Mux) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !m.arb.hasPending() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !m.arb.hasPending() {
		t.Fatal("bid was not queued before admission SYN")
	}
	m.onLogicalReceived(protocol.SymbolSyn, false, false)
}
