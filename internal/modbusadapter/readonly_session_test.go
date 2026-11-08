package modbusadapter

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

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

type h10LateEndpoint struct {
	fakeEndpoint
	reads int
	delay time.Duration
}

func (*h10LateEndpoint) Dispatch() (modbus.TCPDispatch, bool) { return modbus.TCPDispatch{}, true }
func (e *h10LateEndpoint) Read(context.Context, modbus.TCPConnectionHandle) (modbus.TCPReadBatch, error) {
	e.reads++
	if e.reads == 1 {
		return modbus.TCPReadBatch{}, nil
	}
	time.Sleep(e.delay)
	return modbus.TCPReadBatch{}, errors.New("h10 stop")
}

// A read that first consumes a non-deliverable batch must stamp the pacing
// completion time when the whole session read ends, not after the first batch.
func TestH10ReadHoldingPacingCompletesAfterLateResponses(t *testing.T) {
	endpoint := &h10LateEndpoint{delay: 80 * time.Millisecond}
	adapter := &Adapter{endpoint: endpoint, config: Config{Pacing: &Pacing{Gap: time.Second}}}
	session, err := NewReadOnlySession(adapter)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 0, 1)
	_, _ = session.ReadHolding(context.Background(), 1, request)
	if endpoint.reads < 2 {
		t.Fatalf("late-response path not exercised: reads=%d", endpoint.reads)
	}
	if since := time.Since(adapter.completedAt); since > 40*time.Millisecond {
		t.Fatalf("completion stamped %v before the read ended", since)
	}
}

// A caller whose deadline expires during the pacing wait sent nothing, so the
// completion time must stay at the previous transaction; otherwise repeated
// short-deadline attempts keep postponing the next wire request.
func TestH10ReadHoldingCanceledPacingWaitKeepsCompletion(t *testing.T) {
	endpoint := &fakeEndpoint{}
	previous := time.Now()
	adapter := &Adapter{endpoint: endpoint, config: Config{Pacing: &Pacing{Gap: time.Second}}, completedAt: previous}
	session, err := NewReadOnlySession(adapter)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 0, 1)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err := session.ReadHolding(ctx, 1, request)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt %d: err = %v; want deadline", i, err)
		}
	}
	if endpoint.enqueueCalls != 0 {
		t.Fatalf("canceled pacing wait enqueued %d requests", endpoint.enqueueCalls)
	}
	if !adapter.completedAt.Equal(previous) {
		t.Fatalf("canceled pacing wait advanced completion by %v", adapter.completedAt.Sub(previous))
	}
}
