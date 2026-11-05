package modbusadapter

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

func TestH10ReadOnlySessionMethodSetAndAdmission(t *testing.T) {
	typ := reflect.TypeOf((*ReadOnlySession)(nil))
	var methods []string
	for i := 0; i < typ.NumMethod(); i++ {
		methods = append(methods, typ.Method(i).Name)
	}
	if !reflect.DeepEqual(methods, []string{"Close", "ReadDeviceIdentification", "ReadHolding"}) {
		t.Fatalf("read-only method set: %v", methods)
	}
	endpoint := &fakeEndpoint{}
	adapter := &Adapter{endpoint: endpoint}
	session, err := NewReadOnlySession(adapter)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []struct {
		function modbus.FunctionCode
		quantity uint16
	}{{modbus.FunctionReadInputRegisters, 1}, {modbus.FunctionReadHoldingRegisters, 124}} {
		request, err := modbus.NewReadRegistersRequest(spec.function, 0, spec.quantity)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.ReadHolding(context.Background(), 1, request); err == nil {
			t.Fatal("unadmitted request accepted")
		}
	}
	if _, err := session.ReadDeviceIdentification(context.Background(), 1, modbusreg.HuaweiIdentificationRequest{ReadDeviceIDCode: 4}); err == nil {
		t.Fatal("individual access accepted")
	}
	if endpoint.enqueueCalls != 0 {
		t.Fatal("unadmitted request touched transport")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	request, _ := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 0, 1)
	if _, err := session.ReadHolding(context.Background(), 1, request); err == nil {
		t.Fatal("closed session read admitted")
	}
	if endpoint.closeCalls != 0 {
		t.Fatal("session stole endpoint close ownership")
	}
}
func TestH10SessionDeadlineClassification(t *testing.T) {
	if !errors.Is(sessionReadError(os.ErrDeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("native timeout lost")
	}
}
