package main

import "testing"

func TestSummaryDigestUsesSHA256(t *testing.T) {
	const want = "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
	if got := digest([]byte("{}")); got != want {
		t.Fatalf("digest = %s; want %s", got, want)
	}
}
