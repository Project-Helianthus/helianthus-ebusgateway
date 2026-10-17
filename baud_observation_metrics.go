package ebusgateway

// writeBaudObservationMetrics renders the baud observation availability gauge.
// transport.BaudObservabilitySnapshot provides cumulative host ingress and
// fault counters, not a qualified native timing window. Configured baud,
// packet timing and counter rates cannot populate the measured baud series.
// Until the transport owns such a timing observation (including expiry and
// reconnect invalidation), expose unavailability and omit the value entirely.
func writeBaudObservationMetrics(writer *prometheusWriter, ebusConfigured bool) {
	if !ebusConfigured {
		return
	}
	writer.writeHelp("ebus_baud_observation_available", "Whether a current qualified native baud timing observation is available.")
	writer.writeType("ebus_baud_observation_available", "gauge")
	writer.writeGaugeSample("ebus_baud_observation_available", 0, nil)
}
