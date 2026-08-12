package m8sourcestate

// DebugState is the restart-stable structural projection owned by the live
// eBUS observability store. It intentionally omits timestamps and cumulative
// counters while retaining the observed error and frame classifications.
type DebugState struct {
	Status              DebugStatus  `json:"status"`
	ErrorClasses        []DebugError `json:"error_classes"`
	FrameClasses        []DebugFrame `json:"frame_classes"`
	ReconstructorIssues []string     `json:"reconstructor_issues"`
}

type DebugStatus struct {
	TransportClass         string          `json:"transport_class"`
	PublisherCadenceSource string          `json:"publisher_cadence_source"`
	Capability             DebugCapability `json:"capability"`
	Warmup                 DebugWarmup     `json:"warmup"`
	TimingQuality          DebugTiming     `json:"timing_quality"`
	Degraded               DebugDegraded   `json:"degraded"`
	Admission              *DebugAdmission `json:"admission"`
	StartupPhase           string          `json:"startup_phase"`
	FeatureFlags           DebugFeatures   `json:"feature_flags"`
}

type DebugCapability struct {
	ActiveSupported    bool   `json:"active_supported"`
	PassiveSupported   bool   `json:"passive_supported"`
	BroadcastSupported bool   `json:"broadcast_supported"`
	PassiveAvailable   bool   `json:"passive_available"`
	PassiveState       string `json:"passive_state"`
	PassiveReason      string `json:"passive_reason,omitempty"`
	EndpointState      string `json:"endpoint_state,omitempty"`
	TapConnected       bool   `json:"tap_connected"`
}

type DebugWarmup struct {
	State                string `json:"state"`
	Blocker              string `json:"blocker,omitempty"`
	RequiredTransactions int    `json:"required_transactions"`
	CompletionMode       string `json:"completion_mode,omitempty"`
}

type DebugTiming struct {
	Active      string `json:"active"`
	Passive     string `json:"passive"`
	Busy        string `json:"busy"`
	Periodicity string `json:"periodicity"`
}

type DebugDegraded struct {
	Active  bool     `json:"active"`
	Reasons []string `json:"reasons"`
}

type DebugAdmission struct {
	State                   string `json:"state"`
	Mode                    string `json:"mode,omitempty"`
	Outcome                 string `json:"outcome,omitempty"`
	Reason                  string `json:"reason,omitempty"`
	SelectedSource          *uint8 `json:"selected_source,omitempty"`
	FailedSource            *uint8 `json:"failed_source,omitempty"`
	CompanionTarget         *uint8 `json:"companion_target,omitempty"`
	Retryable               bool   `json:"retryable"`
	NextAction              string `json:"next_action,omitempty"`
	LastSuccessfulSource    *uint8 `json:"last_successful_source,omitempty"`
	AutomaticRetryScheduled bool   `json:"automatic_retry_scheduled"`
}

type DebugFeatures struct {
	ObserveFirstEnabled      bool     `json:"observe_first_enabled"`
	PassiveStateDirectApply  bool     `json:"passive_state_direct_apply"`
	PassiveConfigDirectApply bool     `json:"passive_config_direct_apply"`
	ExternalWritePolicy      string   `json:"external_write_policy"`
	Normalizations           []string `json:"normalizations"`
}

type DebugError struct {
	Scope string `json:"scope"`
	Class string `json:"class"`
	Phase string `json:"phase"`
}

type DebugFrame struct {
	Scope     string `json:"scope"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	Family    string `json:"family"`
	FrameType string `json:"frame_type"`
}

// CommandRoute describes one route owned by the installed command writer.
type CommandRoute struct {
	SemanticPath string `json:"semantic_path"`
	Source       string `json:"source"`
	Available    bool   `json:"available"`
}

// CommandRoutingFragment is the owner-local contribution of one writer.
type CommandRoutingFragment struct {
	Routes   []CommandRoute `json:"routes"`
	Fallback *CommandRoute  `json:"fallback"`
}

// CommandRouting is the complete routing state assembled from all writers.
type CommandRouting struct {
	Fallback *CommandRoute  `json:"fallback"`
	Routes   []CommandRoute `json:"routes"`
}

// SemanticLeaf is one currently materialized leaf in the eBUS semantic owner.
type SemanticLeaf struct {
	Path           string `json:"path"`
	PromotionState string `json:"promotion_state"`
	Source         string `json:"source"`
}

// SemanticRegistry is the owner-local semantic inventory used by M8 evidence.
type SemanticRegistry struct {
	Authority string         `json:"authority"`
	Leaves    []SemanticLeaf `json:"leaves"`
}
