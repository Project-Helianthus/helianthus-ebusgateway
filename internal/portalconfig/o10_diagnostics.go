package portalconfig

import "errors"

// O10Diagnostics is a detached configuration-only projection for a future export
// owner. It contains no runtime application or native admission evidence.
type O10Diagnostics struct {
	Format              string `json:"format"`
	SelectionsAvailable bool   `json:"selections_available"`
	Status              string `json:"status"`
	Reason              string `json:"reason"`
	GraphQL             bool   `json:"graphql"`
	MCP                 bool   `json:"mcp"`
	AdapterProxy        bool   `json:"adapter_proxy"`
	Matter              bool   `json:"matter"`
}

// O10ProjectDiagnostics accepts an already accepted record and its load result.
// A failed load makes selections unavailable even if record retains history.
// It neither validates host constraints nor reads storage or listener state.
func O10ProjectDiagnostics(record Record, loadErr error) O10Diagnostics {
	d := O10Diagnostics{Format: Format, Status: "error", Reason: "evidence_unavailable"}
	if loadErr != nil {
		d.Reason = "persistence_failed"
		var typed *Error
		if errors.As(loadErr, &typed) && typed != nil {
			switch typed.Code {
			case "invalid_record", "unsupported_format", "unsupported_selection",
				"revision_conflict", "persistence_failed", "revision_exhausted",
				"evidence_unavailable", "listener_conflict", "invalid_binding":
				d.Reason = typed.Code
			}
		}
		return d
	}
	if record.Format != Format {
		return d
	}
	d.SelectionsAvailable = true
	d.Status, d.Reason = "pending", "listener_application_pending"
	d.GraphQL, d.MCP = record.Sharing.GraphQL, record.Sharing.MCP
	d.AdapterProxy, d.Matter = record.AdapterProxy, record.OutputBindings.Matter
	return d
}
