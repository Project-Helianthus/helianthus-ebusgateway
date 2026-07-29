package eebuscommand

import (
	"context"
	"reflect"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

// Router is the gateway-owned policy boundary for raw eeBUS commands.
type Router struct {
	runtime eebusruntime.Runtime
}

// New constructs a raw command router over the read-only runtime boundary.
func New(runtime eebusruntime.Runtime) *Router {
	if nilInterface(runtime) {
		runtime = nil
	}
	return &Router{runtime: runtime}
}

// FeaturesGet delegates a canonical raw feature lookup.
func (router *Router) FeaturesGet(
	ctx context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	runtime, terminal := router.readRuntime()
	if terminal != nil {
		return eebusraw.FeaturesGetDataV1{}, terminal
	}
	return runtime.FeaturesGet(ctx, auth, request)
}

// FeaturesDataGet delegates a canonical raw feature-data lookup.
func (router *Router) FeaturesDataGet(
	ctx context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	runtime, terminal := router.readRuntime()
	if terminal != nil {
		return eebusraw.FeatureDataGetDataV1{}, terminal
	}
	return runtime.FeaturesDataGet(ctx, auth, request)
}

// FeaturesDataSet delegates an authorized raw feature-data mutation.
func (router *Router) FeaturesDataSet(
	ctx context.Context,
	auth eebusraw.WriteAuthorizationV1,
	request eebusraw.FeatureDataSetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	if terminal := eebusraw.ValidateWriteAuthorizationV1(auth, eebusraw.ToolV1FeaturesDataSet); terminal != nil {
		return eebusruntime.RawMutationOutcomeV1{}, terminal
	}
	runtime, terminal := router.mutationRuntime()
	if terminal != nil {
		return eebusruntime.RawMutationOutcomeV1{}, terminal
	}
	return runtime.FeaturesDataSet(ctx, auth, request)
}

// MutationsGet delegates an authorized raw mutation status lookup.
func (router *Router) MutationsGet(
	ctx context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.MutationGetRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	if terminal := eebusraw.ValidateReadAuthorizationV1(auth, eebusraw.ToolV1MutationsGet); terminal != nil {
		return eebusruntime.RawMutationOutcomeV1{}, terminal
	}
	runtime, terminal := router.mutationRuntime()
	if terminal != nil {
		return eebusruntime.RawMutationOutcomeV1{}, terminal
	}
	return runtime.MutationsGet(ctx, auth, request)
}

// MutationsRollback delegates an authorized raw mutation rollback.
func (router *Router) MutationsRollback(
	ctx context.Context,
	auth eebusraw.WriteAuthorizationV1,
	request eebusraw.MutationRollbackRequestV1,
) (eebusruntime.RawMutationOutcomeV1, *eebusraw.ErrorV1) {
	if terminal := eebusraw.ValidateWriteAuthorizationV1(auth, eebusraw.ToolV1MutationsRollback); terminal != nil {
		return eebusruntime.RawMutationOutcomeV1{}, terminal
	}
	runtime, terminal := router.mutationRuntime()
	if terminal != nil {
		return eebusruntime.RawMutationOutcomeV1{}, terminal
	}
	return runtime.MutationsRollback(ctx, auth, request)
}

func (router *Router) readRuntime() (eebusruntime.Runtime, *eebusraw.ErrorV1) {
	if router == nil || nilInterface(router.runtime) {
		return nil, eebusraw.NewErrorV1(
			eebusraw.ErrorCodeV1Disconnected,
			"raw eeBUS runtime is unavailable",
			true,
			eebusraw.SourceLayerV1Runtime,
		)
	}
	return router.runtime, nil
}

func (router *Router) mutationRuntime() (eebusruntime.RawMutationRuntimeV1, *eebusraw.ErrorV1) {
	if router == nil || nilInterface(router.runtime) {
		return nil, mutationCapabilityUnavailable()
	}
	runtime, ok := router.runtime.(eebusruntime.RawMutationRuntimeV1)
	if !ok || nilInterface(runtime) {
		return nil, mutationCapabilityUnavailable()
	}
	return runtime, nil
}

func mutationCapabilityUnavailable() *eebusraw.ErrorV1 {
	return eebusraw.NewErrorV1(
		eebusraw.ErrorCodeV1UnsupportedOperation,
		"raw eeBUS mutation capability is unavailable",
		false,
		eebusraw.SourceLayerV1Runtime,
	)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
