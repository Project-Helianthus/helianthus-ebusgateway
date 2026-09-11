package main

import (
	"net/http"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portalgraphql"
)

// newPortalCatalogV1Handler starts with an intentionally empty, truthful
// registry. Gateway composition may publish only accepted descriptors and
// detached sources; no legacy/public API is consulted as a fallback.
func newPortalCatalogV1Handler() http.Handler {
	return portalgraphql.Handler{Catalog: catalogv1.New(contributionv1.NewStaticIndex(), nil, nil, nil)}
}
