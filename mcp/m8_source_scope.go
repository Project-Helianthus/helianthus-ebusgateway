package mcp

import "context"

const (
	m8SourceScopeHeader = "X-Helianthus-Evidence-Scope"
	m8SourceScopeV1     = "m8-read-only-v1"
)

type m8SourceScopeContextKey struct{}

var m8SourceToolNamesV1 = []string{
	toolDevicesV1Name,
	toolSemanticSnapshotName,
	eebusV1RuntimeStatusTool,
	eebusV1ServicesListTool,
	eebusV1ServicesGetTool,
	eebusV1SessionsListTool,
	eebusV1SessionsGetTool,
	eebusV1TopologyGetTool,
	eebusV1SnapshotCaptureTool,
	eebusV1SnapshotDropTool,
	eebusV1PairingStatusTool,
}

func m8SourceScopeFromContext(ctx context.Context) bool {
	return ctx != nil && ctx.Value(m8SourceScopeContextKey{}) == true
}

func m8SourceToolAllowed(name string) bool {
	for _, allowed := range m8SourceToolNamesV1 {
		if name == allowed {
			return true
		}
	}
	return false
}

func (s *Server) m8SourceTools() []Tool {
	byName := make(map[string]Tool, len(s.tools))
	for _, tool := range s.tools {
		byName[tool.Name] = tool
	}
	tools := make([]Tool, 0, len(m8SourceToolNamesV1))
	for _, name := range m8SourceToolNamesV1 {
		if tool, ok := byName[name]; ok {
			tools = append(tools, tool)
		}
	}
	return tools
}
