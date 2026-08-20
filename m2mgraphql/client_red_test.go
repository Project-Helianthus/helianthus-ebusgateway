package m2mgraphql

import "testing"

func TestPortalClientUsesTheEmbeddedClosedCanonicalQuery(t *testing.T) {
	if _, ok := any(fixedQuery).(string); !ok {
		t.Fatal("fixed query is not available to the Portal client")
	}
	if len(fixedQuery) == 0 {
		t.Fatal("fixed canonical query is empty")
	}
}
