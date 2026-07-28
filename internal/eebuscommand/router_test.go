package eebuscommand

import (
	"context"
	"reflect"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type issue747ContextKey struct{}

type issue747CallCounts struct {
	Start             int
	Shutdown          int
	Snapshot          int
	PairingState      int
	FeaturesGet       int
	FeaturesDataGet   int
	FeaturesDataSet   int
	MutationsGet      int
	MutationsRollback int
}

func (counts issue747CallCounts) total() int {
	return counts.Start +
		counts.Shutdown +
		counts.Snapshot +
		counts.PairingState +
		counts.FeaturesGet +
		counts.FeaturesDataGet +
		counts.FeaturesDataSet +
		counts.MutationsGet +
		counts.MutationsRollback
}

func issue747AssertCallDelta(
	t *testing.T,
	before issue747CallCounts,
	after issue747CallCounts,
	want issue747CallCounts,
) {
	t.Helper()
	got := issue747CallCounts{
		Start:             after.Start - before.Start,
		Shutdown:          after.Shutdown - before.Shutdown,
		Snapshot:          after.Snapshot - before.Snapshot,
		PairingState:      after.PairingState - before.PairingState,
		FeaturesGet:       after.FeaturesGet - before.FeaturesGet,
		FeaturesDataGet:   after.FeaturesDataGet - before.FeaturesDataGet,
		FeaturesDataSet:   after.FeaturesDataSet - before.FeaturesDataSet,
		MutationsGet:      after.MutationsGet - before.MutationsGet,
		MutationsRollback: after.MutationsRollback - before.MutationsRollback,
	}
	if got != want || got.total() != want.total() {
		t.Fatalf("runtime call delta = %+v (total %d), want %+v (total %d)",
			got, got.total(), want, want.total())
	}
}

type issue747ReadRuntime struct {
	startCalls        int
	shutdownCalls     int
	snapshotCalls     int
	pairingStateCalls int

	featuresGetCalls   int
	featuresGetContext context.Context
	featuresGetAuth    eebusraw.ReadAuthorizationV1
	featuresGetRequest eebusraw.FeaturesGetRequestV1
	featuresGetResult  eebusraw.FeaturesGetDataV1
	featuresGetError   *eebusraw.ErrorV1

	featuresDataGetCalls   int
	featuresDataGetContext context.Context
	featuresDataGetAuth    eebusraw.ReadAuthorizationV1
	featuresDataGetRequest eebusraw.FeatureDataGetRequestV1
	featuresDataGetResult  eebusraw.FeatureDataGetDataV1
	featuresDataGetError   *eebusraw.ErrorV1
}

var (
	_ eebusruntime.RawFeatureRuntimeV1 = (*issue747ReadRuntime)(nil)
	_ eebusruntime.Runtime             = (*issue747ReadRuntime)(nil)
)

func (runtime *issue747ReadRuntime) Start(context.Context) error {
	runtime.startCalls++
	return nil
}

func (runtime *issue747ReadRuntime) Shutdown() error {
	runtime.shutdownCalls++
	return nil
}

func (runtime *issue747ReadRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	runtime.snapshotCalls++
	return eebusruntime.SnapshotV1{}, nil
}

func (runtime *issue747ReadRuntime) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	runtime.pairingStateCalls++
	return nil, nil
}

func (runtime *issue747ReadRuntime) FeaturesGet(
	ctx context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	runtime.featuresGetCalls++
	runtime.featuresGetContext = ctx
	runtime.featuresGetAuth = auth
	runtime.featuresGetRequest = request
	return runtime.featuresGetResult, runtime.featuresGetError
}

func (runtime *issue747ReadRuntime) FeaturesDataGet(
	ctx context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	runtime.featuresDataGetCalls++
	runtime.featuresDataGetContext = ctx
	runtime.featuresDataGetAuth = auth
	runtime.featuresDataGetRequest = request
	return runtime.featuresDataGetResult, runtime.featuresDataGetError
}

func (runtime *issue747ReadRuntime) callCounts() issue747CallCounts {
	return issue747CallCounts{
		Start:           runtime.startCalls,
		Shutdown:        runtime.shutdownCalls,
		Snapshot:        runtime.snapshotCalls,
		PairingState:    runtime.pairingStateCalls,
		FeaturesGet:     runtime.featuresGetCalls,
		FeaturesDataGet: runtime.featuresDataGetCalls,
	}
}

type issue747MutationRuntime struct {
	*issue747ReadRuntime

	featuresDataSetCalls   int
	featuresDataSetContext context.Context
	featuresDataSetAuth    eebusraw.WriteAuthorizationV1
	featuresDataSetRequest eebusraw.FeatureDataSetRequestV1
	featuresDataSetResult  eebusraw.MutationV1
	featuresDataSetError   *eebusraw.ErrorV1

	mutationsGetCalls   int
	mutationsGetContext context.Context
	mutationsGetAuth    eebusraw.ReadAuthorizationV1
	mutationsGetRequest eebusraw.MutationGetRequestV1
	mutationsGetResult  eebusraw.MutationV1
	mutationsGetError   *eebusraw.ErrorV1

	mutationsRollbackCalls   int
	mutationsRollbackContext context.Context
	mutationsRollbackAuth    eebusraw.WriteAuthorizationV1
	mutationsRollbackRequest eebusraw.MutationRollbackRequestV1
	mutationsRollbackResult  eebusraw.MutationV1
	mutationsRollbackError   *eebusraw.ErrorV1
}

var _ eebusruntime.RawMutationRuntimeV1 = (*issue747MutationRuntime)(nil)

func (runtime *issue747MutationRuntime) FeaturesDataSet(
	ctx context.Context,
	auth eebusraw.WriteAuthorizationV1,
	request eebusraw.FeatureDataSetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	runtime.featuresDataSetCalls++
	runtime.featuresDataSetContext = ctx
	runtime.featuresDataSetAuth = auth
	runtime.featuresDataSetRequest = request
	return runtime.featuresDataSetResult, runtime.featuresDataSetError
}

func (runtime *issue747MutationRuntime) MutationsGet(
	ctx context.Context,
	auth eebusraw.ReadAuthorizationV1,
	request eebusraw.MutationGetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	runtime.mutationsGetCalls++
	runtime.mutationsGetContext = ctx
	runtime.mutationsGetAuth = auth
	runtime.mutationsGetRequest = request
	return runtime.mutationsGetResult, runtime.mutationsGetError
}

func (runtime *issue747MutationRuntime) MutationsRollback(
	ctx context.Context,
	auth eebusraw.WriteAuthorizationV1,
	request eebusraw.MutationRollbackRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	runtime.mutationsRollbackCalls++
	runtime.mutationsRollbackContext = ctx
	runtime.mutationsRollbackAuth = auth
	runtime.mutationsRollbackRequest = request
	return runtime.mutationsRollbackResult, runtime.mutationsRollbackError
}

func (runtime *issue747MutationRuntime) callCounts() issue747CallCounts {
	counts := runtime.issue747ReadRuntime.callCounts()
	counts.FeaturesDataSet = runtime.featuresDataSetCalls
	counts.MutationsGet = runtime.mutationsGetCalls
	counts.MutationsRollback = runtime.mutationsRollbackCalls
	return counts
}

type issue747WrongVersionRuntime struct {
	*issue747ReadRuntime
	setCalls      int
	statusCalls   int
	rollbackCalls int
}

func (runtime *issue747WrongVersionRuntime) FeaturesDataSet(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.FeatureDataSetRequestV1,
) (eebusraw.MutationV1, error) {
	runtime.setCalls++
	return eebusraw.MutationV1{}, nil
}

func (runtime *issue747WrongVersionRuntime) MutationsGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.MutationGetRequestV1,
) (eebusraw.MutationV1, error) {
	runtime.statusCalls++
	return eebusraw.MutationV1{}, nil
}

func (runtime *issue747WrongVersionRuntime) MutationsRollback(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.MutationRollbackRequestV1,
) (eebusraw.MutationV1, error) {
	runtime.rollbackCalls++
	return eebusraw.MutationV1{}, nil
}

func (runtime *issue747WrongVersionRuntime) callCounts() issue747CallCounts {
	counts := runtime.issue747ReadRuntime.callCounts()
	counts.FeaturesDataSet = runtime.setCalls
	counts.MutationsGet = runtime.statusCalls
	counts.MutationsRollback = runtime.rollbackCalls
	return counts
}

type issue747PartialMutationRuntime struct {
	*issue747ReadRuntime
	setCalls      int
	rollbackCalls int
}

func (runtime *issue747PartialMutationRuntime) FeaturesDataSet(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.FeatureDataSetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	runtime.setCalls++
	return eebusraw.MutationV1{}, nil
}

func (runtime *issue747PartialMutationRuntime) MutationsRollback(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.MutationRollbackRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	runtime.rollbackCalls++
	return eebusraw.MutationV1{}, nil
}

func (runtime *issue747PartialMutationRuntime) callCounts() issue747CallCounts {
	counts := runtime.issue747ReadRuntime.callCounts()
	counts.FeaturesDataSet = runtime.setCalls
	counts.MutationsRollback = runtime.rollbackCalls
	return counts
}

type issue747StatusOnlyMutationRuntime struct {
	*issue747ReadRuntime
	statusCalls int
}

func (runtime *issue747StatusOnlyMutationRuntime) MutationsGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.MutationGetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	runtime.statusCalls++
	return eebusraw.MutationV1{}, nil
}

func (runtime *issue747StatusOnlyMutationRuntime) callCounts() issue747CallCounts {
	counts := runtime.issue747ReadRuntime.callCounts()
	counts.MutationsGet = runtime.statusCalls
	return counts
}

type issue747TypedNilRuntime struct{}

var (
	issue747TypedNilCalls issue747CallCounts

	_ eebusruntime.RawFeatureRuntimeV1  = (*issue747TypedNilRuntime)(nil)
	_ eebusruntime.RawMutationRuntimeV1 = (*issue747TypedNilRuntime)(nil)
	_ eebusruntime.Runtime              = (*issue747TypedNilRuntime)(nil)
)

func (*issue747TypedNilRuntime) Start(context.Context) error {
	issue747TypedNilCalls.Start++
	return nil
}

func (*issue747TypedNilRuntime) Shutdown() error {
	issue747TypedNilCalls.Shutdown++
	return nil
}

func (*issue747TypedNilRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	issue747TypedNilCalls.Snapshot++
	return eebusruntime.SnapshotV1{}, nil
}

func (*issue747TypedNilRuntime) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	issue747TypedNilCalls.PairingState++
	return nil, nil
}

func (*issue747TypedNilRuntime) FeaturesGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	issue747TypedNilCalls.FeaturesGet++
	return eebusraw.FeaturesGetDataV1{}, nil
}

func (*issue747TypedNilRuntime) FeaturesDataGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	issue747TypedNilCalls.FeaturesDataGet++
	return eebusraw.FeatureDataGetDataV1{}, nil
}

func (*issue747TypedNilRuntime) FeaturesDataSet(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.FeatureDataSetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	issue747TypedNilCalls.FeaturesDataSet++
	return eebusraw.MutationV1{}, nil
}

func (*issue747TypedNilRuntime) MutationsGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.MutationGetRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	issue747TypedNilCalls.MutationsGet++
	return eebusraw.MutationV1{}, nil
}

func (*issue747TypedNilRuntime) MutationsRollback(
	context.Context,
	eebusraw.WriteAuthorizationV1,
	eebusraw.MutationRollbackRequestV1,
) (eebusraw.MutationV1, *eebusraw.ErrorV1) {
	issue747TypedNilCalls.MutationsRollback++
	return eebusraw.MutationV1{}, nil
}

func TestRouterReadOperationsDelegateExactlyOnce(t *testing.T) {
	featuresGetError := &eebusraw.ErrorV1{Message: "features-get-sentinel"}
	featuresDataGetError := &eebusraw.ErrorV1{Message: "features-data-get-sentinel"}
	runtime := &issue747ReadRuntime{
		featuresGetResult: eebusraw.FeaturesGetDataV1{
			Description: "feature-inventory-sentinel",
		},
		featuresGetError: featuresGetError,
		featuresDataGetResult: eebusraw.FeatureDataGetDataV1{
			Complete: true,
		},
		featuresDataGetError: featuresDataGetError,
	}
	beforeNew := runtime.callCounts()
	router := New(runtime)
	issue747AssertCallDelta(t, beforeNew, runtime.callCounts(), issue747CallCounts{})

	featuresGetContext := context.WithValue(context.Background(), issue747ContextKey{}, "features-get")
	featuresGetAuth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          "eebus.raw.read",
		Tool:           "eebus.v1.features.get",
		MaskTier:       "raw",
	}
	featuresGetRequest := eebusraw.FeaturesGetRequestV1{}
	beforeFeaturesGet := runtime.callCounts()
	gotFeatures, gotFeaturesError := router.FeaturesGet(featuresGetContext, featuresGetAuth, featuresGetRequest)

	issue747AssertCallDelta(t, beforeFeaturesGet, runtime.callCounts(), issue747CallCounts{FeaturesGet: 1})
	if runtime.featuresGetContext != featuresGetContext ||
		!reflect.DeepEqual(runtime.featuresGetAuth, featuresGetAuth) ||
		!reflect.DeepEqual(runtime.featuresGetRequest, featuresGetRequest) {
		t.Fatal("FeaturesGet did not receive the exact context, authorization, and request")
	}
	if !reflect.DeepEqual(gotFeatures, runtime.featuresGetResult) || gotFeaturesError != featuresGetError {
		t.Fatal("FeaturesGet did not return the exact runtime result")
	}

	featuresDataGetContext := context.WithValue(context.Background(), issue747ContextKey{}, "features-data-get")
	featuresDataGetAuth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          "eebus.raw.read",
		Tool:           "eebus.v1.features.data.get",
		MaskTier:       "raw",
	}
	featuresDataGetRequest := eebusraw.FeatureDataGetRequestV1{TimeoutMS: 1234}
	beforeFeaturesDataGet := runtime.callCounts()
	gotData, gotDataError := router.FeaturesDataGet(featuresDataGetContext, featuresDataGetAuth, featuresDataGetRequest)

	issue747AssertCallDelta(t, beforeFeaturesDataGet, runtime.callCounts(), issue747CallCounts{FeaturesDataGet: 1})
	if runtime.featuresDataGetContext != featuresDataGetContext ||
		!reflect.DeepEqual(runtime.featuresDataGetAuth, featuresDataGetAuth) ||
		!reflect.DeepEqual(runtime.featuresDataGetRequest, featuresDataGetRequest) {
		t.Fatal("FeaturesDataGet did not receive the exact context, authorization, and request")
	}
	if !reflect.DeepEqual(gotData, runtime.featuresDataGetResult) || gotDataError != featuresDataGetError {
		t.Fatal("FeaturesDataGet did not return the exact runtime result")
	}
}

func TestRouterMutationOperationsDelegateExactlyOnce(t *testing.T) {
	setError := &eebusraw.ErrorV1{Message: "set-sentinel"}
	statusError := &eebusraw.ErrorV1{Message: "status-sentinel"}
	rollbackError := &eebusraw.ErrorV1{Message: "rollback-sentinel"}
	runtime := &issue747MutationRuntime{
		issue747ReadRuntime: &issue747ReadRuntime{},
		featuresDataSetResult: eebusraw.MutationV1{
			MutationRef: "mutation-set",
			State:       "applied",
		},
		featuresDataSetError: setError,
		mutationsGetResult: eebusraw.MutationV1{
			MutationRef: "mutation-status",
			State:       "verify_pending",
		},
		mutationsGetError: statusError,
		mutationsRollbackResult: eebusraw.MutationV1{
			MutationRef: "mutation-rollback",
			State:       "rolled_back",
		},
		mutationsRollbackError: rollbackError,
	}
	beforeNew := runtime.callCounts()
	router := New(runtime)
	issue747AssertCallDelta(t, beforeNew, runtime.callCounts(), issue747CallCounts{})

	setContext := context.WithValue(context.Background(), issue747ContextKey{}, "set")
	setAuth := eebusraw.WriteAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          "eebus.raw.write",
		Tool:           "eebus.v1.features.data.set",
		MaskTier:       "raw",
	}
	setRequest := eebusraw.FeatureDataSetRequestV1{
		ReadToken:      "read-token",
		IdempotencyKey: "set-idempotency",
		Mode:           "probe",
	}
	beforeSet := runtime.callCounts()
	gotSet, gotSetError := router.FeaturesDataSet(setContext, setAuth, setRequest)
	issue747AssertCallDelta(t, beforeSet, runtime.callCounts(), issue747CallCounts{FeaturesDataSet: 1})
	if runtime.featuresDataSetContext != setContext ||
		!reflect.DeepEqual(runtime.featuresDataSetAuth, setAuth) ||
		!reflect.DeepEqual(runtime.featuresDataSetRequest, setRequest) ||
		!reflect.DeepEqual(gotSet, runtime.featuresDataSetResult) ||
		gotSetError != setError {
		t.Fatal("FeaturesDataSet did not delegate and return values exactly")
	}

	statusContext := context.WithValue(context.Background(), issue747ContextKey{}, "status")
	statusAuth := eebusraw.ReadAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          "eebus.raw.read",
		Tool:           "eebus.v1.mutations.get",
		MaskTier:       "raw",
	}
	statusRequest := eebusraw.MutationGetRequestV1{MutationRef: "mutation-status"}
	beforeStatus := runtime.callCounts()
	gotStatus, gotStatusError := router.MutationsGet(statusContext, statusAuth, statusRequest)
	issue747AssertCallDelta(t, beforeStatus, runtime.callCounts(), issue747CallCounts{MutationsGet: 1})
	if runtime.mutationsGetContext != statusContext ||
		!reflect.DeepEqual(runtime.mutationsGetAuth, statusAuth) ||
		!reflect.DeepEqual(runtime.mutationsGetRequest, statusRequest) ||
		!reflect.DeepEqual(gotStatus, runtime.mutationsGetResult) ||
		gotStatusError != statusError {
		t.Fatal("MutationsGet did not delegate and return values exactly")
	}

	rollbackContext := context.WithValue(context.Background(), issue747ContextKey{}, "rollback")
	rollbackAuth := eebusraw.WriteAuthorizationV1{
		PrincipalClass: "owner",
		Scope:          "eebus.raw.write",
		Tool:           "eebus.v1.mutations.rollback",
		MaskTier:       "raw",
	}
	rollbackRequest := eebusraw.MutationRollbackRequestV1{
		MutationRef:    "mutation-rollback",
		IdempotencyKey: "rollback-idempotency",
	}
	beforeRollback := runtime.callCounts()
	gotRollback, gotRollbackError := router.MutationsRollback(rollbackContext, rollbackAuth, rollbackRequest)
	issue747AssertCallDelta(t, beforeRollback, runtime.callCounts(), issue747CallCounts{MutationsRollback: 1})
	if runtime.mutationsRollbackContext != rollbackContext ||
		!reflect.DeepEqual(runtime.mutationsRollbackAuth, rollbackAuth) ||
		!reflect.DeepEqual(runtime.mutationsRollbackRequest, rollbackRequest) ||
		!reflect.DeepEqual(gotRollback, runtime.mutationsRollbackResult) ||
		gotRollbackError != rollbackError {
		t.Fatal("MutationsRollback did not delegate and return values exactly")
	}
}

func TestRouterMutationCapabilityFailsClosedBeforeRuntimeContact(t *testing.T) {
	readOnly := &issue747ReadRuntime{}
	wrongVersion := &issue747WrongVersionRuntime{issue747ReadRuntime: &issue747ReadRuntime{}}
	partial := &issue747PartialMutationRuntime{issue747ReadRuntime: &issue747ReadRuntime{}}
	statusOnly := &issue747StatusOnlyMutationRuntime{issue747ReadRuntime: &issue747ReadRuntime{}}
	var typedNil *issue747TypedNilRuntime
	issue747TypedNilCalls = issue747CallCounts{}

	tests := []struct {
		name       string
		runtime    eebusruntime.Runtime
		callCounts func() issue747CallCounts
	}{
		{
			name:       "missing capability",
			runtime:    readOnly,
			callCounts: readOnly.callCounts,
		},
		{
			name:       "wrong-version capability",
			runtime:    wrongVersion,
			callCounts: wrongVersion.callCounts,
		},
		{
			name:       "partial capability",
			runtime:    partial,
			callCounts: partial.callCounts,
		},
		{
			name:       "status-only partial capability",
			runtime:    statusOnly,
			callCounts: statusOnly.callCounts,
		},
		{
			name:    "typed nil capability",
			runtime: typedNil,
			callCounts: func() issue747CallCounts {
				return issue747TypedNilCalls
			},
		},
		{
			name:    "nil capability",
			runtime: nil,
			callCounts: func() issue747CallCounts {
				return issue747CallCounts{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeNew := test.callCounts()
			router := New(test.runtime)
			issue747AssertCallDelta(t, beforeNew, test.callCounts(), issue747CallCounts{})

			operations := []struct {
				name string
				call func() (eebusraw.MutationV1, *eebusraw.ErrorV1)
			}{
				{
					name: "FeaturesDataSet",
					call: func() (eebusraw.MutationV1, *eebusraw.ErrorV1) {
						return router.FeaturesDataSet(
							context.Background(),
							eebusraw.WriteAuthorizationV1{},
							eebusraw.FeatureDataSetRequestV1{},
						)
					},
				},
				{
					name: "MutationsGet",
					call: func() (eebusraw.MutationV1, *eebusraw.ErrorV1) {
						return router.MutationsGet(
							context.Background(),
							eebusraw.ReadAuthorizationV1{},
							eebusraw.MutationGetRequestV1{},
						)
					},
				},
				{
					name: "MutationsRollback",
					call: func() (eebusraw.MutationV1, *eebusraw.ErrorV1) {
						return router.MutationsRollback(
							context.Background(),
							eebusraw.WriteAuthorizationV1{},
							eebusraw.MutationRollbackRequestV1{},
						)
					},
				},
			}

			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					before := test.callCounts()
					result, terminal := operation.call()
					issue747AssertCallDelta(t, before, test.callCounts(), issue747CallCounts{})
					if terminal == nil {
						t.Fatalf("%s succeeded without the complete RawMutationRuntimeV1", operation.name)
					}
					if !reflect.DeepEqual(result, eebusraw.MutationV1{}) {
						t.Fatalf("%s result = %+v, want zero mutation", operation.name, result)
					}
				})
			}
		})
	}
}
