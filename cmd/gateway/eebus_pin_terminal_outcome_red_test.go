package main

import (
	"reflect"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

// The Portal must never manufacture a terminal PIN outcome from the immediate
// connection_started acknowledgement. eebusreg owns the one volatile,
// identity-free action snapshot which is polled through the admin status.
func TestIssue848EEBusregExposesTerminalPINOutcomeSurface(t *testing.T) {
	admin := reflect.TypeOf((*eebusruntime.AdminV1)(nil)).Elem()
	connect, ok := admin.MethodByName("Connect")
	if !ok || connect.Type.NumOut() != 2 || connect.Type.Out(0) != reflect.TypeOf(eebusruntime.ConnectResultV1{}) {
		t.Fatal("eebusreg AdminV1 Connect must return ConnectResultV1")
	}
	snapshot := reflect.TypeOf(eebusruntime.AdminSnapshotV1{})
	field, ok := snapshot.FieldByName("ActiveAction")
	if !ok || field.Type != reflect.TypeOf((*eebusruntime.ActiveActionV1)(nil)) {
		t.Fatal("eebusreg AdminSnapshotV1 must expose the identity-free ActiveActionV1 status")
	}
}
