package main

import (
	"context"
	"net/http"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type semanticHubCapture interface {
	Capture(context.Context, []semreg.AssetID, func() time.Time) (semhub.CoherentCapture, error)
}

type hubSemanticSnapshotProvider struct {
	hub semanticHubCapture
	now func() time.Time
}

var _ graphql.SemanticSnapshotProvider = (*hubSemanticSnapshotProvider)(nil)
var _ mcp.SemanticSnapshotProvider = (*hubSemanticSnapshotProvider)(nil)

func newGatewaySemanticSnapshotProvider(control *gatewayRuntimeControl) *hubSemanticSnapshotProvider {
	provider := &hubSemanticSnapshotProvider{now: time.Now}
	if control != nil && control.hub != nil {
		provider.hub = control.hub
	}
	return provider
}

func (p *hubSemanticSnapshotProvider) Capture(ctx context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	if p == nil || p.hub == nil {
		return nil, nil, semhub.ErrCaptureUnavailable
	}
	capture, err := p.hub.Capture(ctx, ids, p.now)
	if err != nil {
		return nil, nil, err
	}
	inputs := make([]semanticview.Input, 0, len(capture.Assets))
	for _, asset := range capture.Assets {
		inputs = append(inputs, semanticview.Input{AssetID: asset.AssetID, Snapshot: asset.Snapshot, Evaluation: asset.Evaluation, UnavailableReason: asset.UnavailableReason})
	}
	return capture.Metadata, inputs, nil
}

// The ordinary HTTP composition installs the same adapter on both surfaces,
// before either handler can serve requests. Each surface captures once per read.
func newHTTPControlPlaneSemanticHandlers(gateway *ebusgateway.Gateway, builder *graphql.Builder, broadcast *graphql.BroadcastHub, provider *hubSemanticSnapshotProvider) (http.Handler, http.Handler, http.Handler, *mcp.Server, error) {
	builder.SetSemanticSnapshotProvider(provider)
	query, snapshot, subscription, err := newHTTPControlPlaneGraphQLHandlers(gateway, builder, broadcast)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	server, err := mcp.NewServer(gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := mcp.RegisterSemanticSnapshotTool(server, provider); err != nil {
		return nil, nil, nil, nil, err
	}
	return query, snapshot, subscription, server, nil
}
