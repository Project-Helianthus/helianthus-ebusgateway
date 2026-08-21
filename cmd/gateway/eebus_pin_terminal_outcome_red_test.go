package main

import (
	"os"
	"reflect"
	"strings"
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

func TestIssue848GatewayPINDecoderNeverConstructsAnImmutableString(t *testing.T) {
	source, err := os.ReadFile("../../internal/eebusadmin/server.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "json.Unmarshal(body.PIN, &value)") || strings.Contains(string(source), "[]byte(value)") {
		t.Fatal("gateway PIN decoder constructs an immutable Go string instead of decoding into owned mutable bytes")
	}
}
