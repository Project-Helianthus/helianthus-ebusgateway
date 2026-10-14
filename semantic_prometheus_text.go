package ebusgateway

import (
	"bytes"
	"time"
)

// RenderSemanticPrometheus renders only the detached SemReg section of the
// Prometheus target for domains at the explicit instant now. It is the same
// renderer the observability store uses, without the store's process clock.
func RenderSemanticPrometheus(domains []SemanticMetricsDomain, now time.Time) string {
	var buffer bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&buffer), domains, now)
	return buffer.String()
}
