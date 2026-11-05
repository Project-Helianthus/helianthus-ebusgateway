package main

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// All frames below are synthetic structural values only.

var growattBMSCANTestInterface = growattBMSCANInterface{Name: "can7", Index: 7}

func growattBMSCANTestRecord(rawIdentifier uint32, length, len8DLC byte, data ...byte) []byte {
	record := make([]byte, growattBMSCANRecordSize)
	binary.NativeEndian.PutUint32(record[0:4], rawIdentifier)
	record[4] = length
	record[7] = len8DLC
	copy(record[8:], data)
	return record
}

func growattBMSCANTestFrame(identifier uint32, data ...byte) growattBMSCANRecord {
	return growattBMSCANRecord{Interface: growattBMSCANTestInterface, Raw: growattBMSCANTestRecord(identifier, byte(len(data)), 0, data...)}
}

func growattBMSCANLimits() growattBMSCANRecord {
	return growattBMSCANTestFrame(0x311, 0x02, 0x38, 0x00, 0xfa, 0x01, 0xf4, 0x00, 0x43)
}

func growattBMSCANStatus() growattBMSCANRecord {
	return growattBMSCANTestFrame(0x312, 0x00, 0x80, 0x00, 0x00, 0x02, 0xaa, 0xbb, 0x10)
}

func growattBMSCANMeasurements() growattBMSCANRecord {
	return growattBMSCANTestFrame(0x313, 0x14, 0x00, 0xff, 0xf6, 0x01, 0x2c, 0x50, 0xff)
}

func growattBMSCANFiller() growattBMSCANRecord {
	return growattBMSCANTestFrame(0x301, 0, 0, 0, 0, 0, 0, 0, 0)
}

type growattBMSCANTestClock struct {
	mu  sync.Mutex
	now time.Duration
}

func (clock *growattBMSCANTestClock) Now() time.Duration {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *growattBMSCANTestClock) Set(now time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = now
}

// newGrowattBMSCANTestDriver builds a driver without a receive loop so the
// admission window can be driven synchronously.
func newGrowattBMSCANTestDriver(clock *growattBMSCANTestClock) *growattBMSCANDriver {
	return &growattBMSCANDriver{identity: growattBMSCANTestInterface, staleAfter: time.Minute, now: clock.Now}
}

func growattBMSCANObserveAll(t *testing.T, driver *growattBMSCANDriver, records ...growattBMSCANRecord) {
	t.Helper()
	for _, record := range records {
		if !driver.observe(record) {
			t.Fatal("observe stopped")
		}
	}
}

func growattBMSCANRequireNotAdmitted(t *testing.T, driver *growattBMSCANDriver) {
	t.Helper()
	snapshot, err := driver.GrowattBMSV104Snapshot(context.Background())
	if !errors.Is(err, mcp.ErrGrowattBMSV104NotAdmitted) || !reflect.DeepEqual(snapshot, mcp.GrowattBMSV104ProviderSnapshot{}) {
		t.Fatalf("snapshot = %#v, err = %v; want not admitted", snapshot, err)
	}
}

func growattBMSCANRequireSnapshot(t *testing.T, driver *growattBMSCANDriver) mcp.GrowattBMSV104ProviderSnapshot {
	t.Helper()
	snapshot, err := driver.GrowattBMSV104Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot err = %v", err)
	}
	return snapshot
}

func TestGrowattBMSCANQualificationCardTupleProjectsNativeValues(t *testing.T) {
	clock := &growattBMSCANTestClock{}
	driver := newGrowattBMSCANTestDriver(clock)
	for index, record := range []growattBMSCANRecord{growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements()} {
		clock.Set(time.Duration(index+1) * time.Millisecond)
		growattBMSCANObserveAll(t, driver, record)
	}
	runtime, err := mcp.NewGrowattBMSV104Runtime(driver)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := mcp.GrowattBMSV104Result{
		Profile: mcp.GrowattBMSV104Profile, Admitted: true, OutboundAllowed: false,
		Limits: mcp.GrowattBMSV104Limits{ChargeVoltageDecivolts: 568, ChargeCurrentDeciamps: 250, DischargeCurrentDeciamps: 500, RawStatus: 0x0043},
		Status: mcp.GrowattBMSV104Status{Protection: [2]byte{0x00, 0x80}, Warning: [2]byte{0x00, 0x00}, PackCount: 2, Manufacturer: [2]byte{0xaa, 0xbb}, TotalCellCount: 16},
		Measurements: mcp.GrowattBMSV104Measurements{
			VoltageCentivolts: 5120, CurrentDeciamps: -10, MaximumCellTemperatureDeciC: 300, SOCPercent: 80, SOHValue: 127, SOHValid: true,
		},
		RawEvidence: []mcp.GrowattBMSV104RawEvidence{
			{Interface: "can7", Sequence: 1, MonotonicNanos: int64(time.Millisecond), Identifier: 0x311, DLC: 8, RawDLC: 8, Data: [8]byte{0x02, 0x38, 0x00, 0xfa, 0x01, 0xf4, 0x00, 0x43}},
			{Interface: "can7", Sequence: 2, MonotonicNanos: int64(2 * time.Millisecond), Identifier: 0x312, DLC: 8, RawDLC: 8, Data: [8]byte{0x00, 0x80, 0x00, 0x00, 0x02, 0xaa, 0xbb, 0x10}},
			{Interface: "can7", Sequence: 3, MonotonicNanos: int64(3 * time.Millisecond), Identifier: 0x313, DLC: 8, RawDLC: 8, Data: [8]byte{0x14, 0x00, 0xff, 0xf6, 0x01, 0x2c, 0x50, 0xff}},
		},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("result = %#v\nwant %#v", result, want)
	}
}

func TestGrowattBMSCANSOHFlagIsSeparateFromValue(t *testing.T) {
	driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
	growattBMSCANObserveAll(t, driver, growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANTestFrame(0x313, 0x14, 0x00, 0xff, 0xf6, 0x01, 0x2c, 0x64, 0x7f))
	snapshot := growattBMSCANRequireSnapshot(t, driver)
	if snapshot.Measurements.SOHValue != 127 || snapshot.Measurements.SOHValid || snapshot.Measurements.SOCPercent != 100 {
		t.Fatalf("measurements = %#v", snapshot.Measurements)
	}
}

func TestGrowattBMSCANNegativeControlsProduceNoResult(t *testing.T) {
	foreign := growattBMSCANLimits()
	foreign.Interface = growattBMSCANInterface{Name: "can8", Index: 8}
	sameNameOtherIndex := growattBMSCANLimits()
	sameNameOtherIndex.Interface = growattBMSCANInterface{Name: "can7", Index: 8}
	extended := growattBMSCANLimits()
	binary.NativeEndian.PutUint32(extended.Raw[0:4], 0x311|growattBMSCANExtendedFlag)
	rtr := growattBMSCANTestRecord(0x311|growattBMSCANRTRFlag, 8, 0)
	rawDLC9 := growattBMSCANLimits()
	rawDLC9.Raw[7] = 9
	short := growattBMSCANTestFrame(0x312, 0x00, 0x80, 0x00, 0x00, 0x02, 0xaa, 0xbb)

	cases := map[string][]growattBMSCANRecord{
		"no frames":                 nil,
		"missing limits":            {growattBMSCANStatus(), growattBMSCANMeasurements()},
		"missing status":            {growattBMSCANLimits(), growattBMSCANMeasurements()},
		"missing measurements":      {growattBMSCANLimits(), growattBMSCANStatus()},
		"query frame only":          {growattBMSCANFiller(), growattBMSCANFiller()},
		"short listed payload":      {growattBMSCANLimits(), short, growattBMSCANMeasurements()},
		"pack count zero":           {growattBMSCANLimits(), growattBMSCANTestFrame(0x312, 0x00, 0x80, 0x00, 0x00, 0x00, 0xaa, 0xbb, 0x10), growattBMSCANMeasurements()},
		"total cell count zero":     {growattBMSCANLimits(), growattBMSCANTestFrame(0x312, 0x00, 0x80, 0x00, 0x00, 0x02, 0xaa, 0xbb, 0x00), growattBMSCANMeasurements()},
		"soc above percent range":   {growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANTestFrame(0x313, 0x14, 0x00, 0xff, 0xf6, 0x01, 0x2c, 0x65, 0xff)},
		"wrong interface name":      {foreign, growattBMSCANStatus(), growattBMSCANMeasurements()},
		"wrong interface index":     {sameNameOtherIndex, growattBMSCANStatus(), growattBMSCANMeasurements()},
		"extended limits resets":    {growattBMSCANLimits(), growattBMSCANStatus(), extended, growattBMSCANMeasurements()},
		"rtr limits resets":         {growattBMSCANLimits(), growattBMSCANStatus(), {Interface: growattBMSCANTestInterface, Raw: rtr}, growattBMSCANMeasurements()},
		"raw dlc beyond 8 resets":   {growattBMSCANLimits(), growattBMSCANStatus(), rawDLC9, growattBMSCANMeasurements()},
		"optional frame malformed":  {growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANTestFrame(0x315, 0x0c, 0x80), growattBMSCANMeasurements()},
		"malformed capacity resets": {growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANTestFrame(0x314, 0x01, 0x02, 0x03), growattBMSCANMeasurements()},
	}
	for name, records := range cases {
		t.Run(name, func(t *testing.T) {
			driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
			growattBMSCANObserveAll(t, driver, records...)
			growattBMSCANRequireNotAdmitted(t, driver)
		})
	}
}

func TestGrowattBMSCANUnlistedFramesDoNotResetTheWindow(t *testing.T) {
	driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
	growattBMSCANObserveAll(t, driver,
		growattBMSCANLimits(),
		growattBMSCANTestFrame(0x31a, 0x01, 0x02, 0x03),
		growattBMSCANTestFrame(0x301, 0x01),
		growattBMSCANStatus(),
		growattBMSCANRecord{Interface: growattBMSCANTestInterface, Raw: growattBMSCANTestRecord(0x18ff0311|growattBMSCANExtendedFlag, 2, 0, 0x01, 0x02)},
		growattBMSCANMeasurements(),
	)
	snapshot := growattBMSCANRequireSnapshot(t, driver)
	if got := []uint64{snapshot.RawEvidence[0].Sequence, snapshot.RawEvidence[1].Sequence, snapshot.RawEvidence[2].Sequence}; !reflect.DeepEqual(got, []uint64{1, 4, 6}) {
		t.Fatalf("source sequences = %v", got)
	}
	if diagnostics := driver.Diagnostics(); diagnostics.MalformedListed != 0 || diagnostics.AcceptedFrames != 6 {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
}

func TestGrowattBMSCANWindowExpiryBoundary(t *testing.T) {
	for _, test := range []struct {
		fillers  int
		admitted bool
	}{{fillers: 13, admitted: true}, {fillers: 14, admitted: false}} {
		driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
		records := []growattBMSCANRecord{growattBMSCANLimits()}
		for index := 0; index < test.fillers; index++ {
			records = append(records, growattBMSCANFiller())
		}
		records = append(records, growattBMSCANStatus(), growattBMSCANMeasurements())
		growattBMSCANObserveAll(t, driver, records...)
		if test.admitted {
			growattBMSCANRequireSnapshot(t, driver)
		} else {
			growattBMSCANRequireNotAdmitted(t, driver)
		}
	}
}

func TestGrowattBMSCANWindowRolloverUsesLatestLimits(t *testing.T) {
	driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
	later := growattBMSCANTestFrame(0x311, 0x02, 0x40, 0x01, 0x00, 0x02, 0x00, 0x00, 0x02)
	growattBMSCANObserveAll(t, driver, growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements(), later)
	snapshot := growattBMSCANRequireSnapshot(t, driver)
	if snapshot.Limits != (mcp.GrowattBMSV104Limits{ChargeVoltageDecivolts: 576, ChargeCurrentDeciamps: 256, DischargeCurrentDeciamps: 512, RawStatus: 2}) {
		t.Fatalf("limits = %#v", snapshot.Limits)
	}
	if got := []uint64{snapshot.RawEvidence[0].Sequence, snapshot.RawEvidence[1].Sequence, snapshot.RawEvidence[2].Sequence}; !reflect.DeepEqual(got, []uint64{2, 3, 4}) {
		t.Fatalf("source sequences = %v", got)
	}
	if snapshot.RawEvidence[2].Identifier != 0x311 || snapshot.RawEvidence[2].Data != [8]byte{0x02, 0x40, 0x01, 0x00, 0x02, 0x00, 0x00, 0x02} {
		t.Fatalf("limits evidence = %#v", snapshot.RawEvidence[2])
	}
}

func TestGrowattBMSCANMalformedFrameKeepsLastKnownGoodPublication(t *testing.T) {
	driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
	growattBMSCANObserveAll(t, driver, growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements())
	before := growattBMSCANRequireSnapshot(t, driver)

	later := growattBMSCANTestFrame(0x311, 0x02, 0x40, 0x01, 0x00, 0x02, 0x00, 0x00, 0x02)
	growattBMSCANObserveAll(t, driver, growattBMSCANTestFrame(0x312, 0x00), later, growattBMSCANStatus())
	if after := growattBMSCANRequireSnapshot(t, driver); !reflect.DeepEqual(after, before) {
		t.Fatalf("reset window replaced the publication: %#v", after)
	}
	if diagnostics := driver.Diagnostics(); diagnostics.MalformedListed != 1 || diagnostics.Publications != 1 {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}

	growattBMSCANObserveAll(t, driver, growattBMSCANMeasurements())
	if after := growattBMSCANRequireSnapshot(t, driver); after.Limits.ChargeVoltageDecivolts != 576 || after.RawEvidence[0].Sequence != 5 {
		t.Fatalf("fresh tuple not published: %#v", after)
	}
}

func TestGrowattBMSCANRejectedRecordsAreDiagnosticsOnly(t *testing.T) {
	driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
	growattBMSCANObserveAll(t, driver, growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements())
	before := growattBMSCANRequireSnapshot(t, driver)

	rejected := [][]byte{
		make([]byte, growattBMSCANFDRecordSize),
		make([]byte, growattBMSCANRecordSize-1),
		growattBMSCANTestRecord(0x311|growattBMSCANErrorFlag, 8, 0),
		growattBMSCANTestRecord(0x800, 8, 0),
		growattBMSCANTestRecord(0x311, 9, 0),
		growattBMSCANTestRecord(0x311, 7, 9),
		growattBMSCANTestRecord(0x311, 8, 8),
		growattBMSCANTestRecord(0x311, 8, 16),
	}
	for _, raw := range rejected {
		if _, err := decodeGrowattBMSCANRecord(raw); !errors.Is(err, errGrowattBMSCANRecord) {
			t.Fatalf("decode(%x) err = %v", raw, err)
		}
		growattBMSCANObserveAll(t, driver, growattBMSCANRecord{Interface: growattBMSCANTestInterface, Raw: raw})
	}
	if after := growattBMSCANRequireSnapshot(t, driver); !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected record changed the publication: %#v", after)
	}
	if diagnostics := driver.Diagnostics(); diagnostics.RejectedRecords != uint64(len(rejected)) || diagnostics.AcceptedFrames != 3 || diagnostics.MalformedListed != 0 {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	growattBMSCANObserveAll(t, driver, growattBMSCANFiller())
	if driver.sequence != 4 {
		t.Fatalf("rejected records consumed listener sequences: %d", driver.sequence)
	}
}

func TestGrowattBMSCANDecodePreservesEveryNativeField(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  []byte
		want growattBMSCANObservation
	}{
		{"standard", growattBMSCANTestRecord(0x123, 3, 0, 0xa1, 0xb2, 0xc3, 0xd4), growattBMSCANObservation{Identifier: 0x123, DLC: 3, RawDLC: 3, Data: [8]byte{0xa1, 0xb2, 0xc3}}},
		{"extended", growattBMSCANTestRecord(0x1abcdef0|growattBMSCANExtendedFlag, 8, 0, 1, 2, 3, 4, 5, 6, 7, 8), growattBMSCANObservation{Identifier: 0x1abcdef0, Extended: true, DLC: 8, RawDLC: 8, Data: [8]byte{1, 2, 3, 4, 5, 6, 7, 8}}},
		{"rtr", growattBMSCANTestRecord(0x7ff|growattBMSCANRTRFlag, 4, 0, 9, 9, 9, 9), growattBMSCANObservation{Identifier: 0x7ff, RTR: true, DLC: 4, RawDLC: 4}},
		{"raw dlc 15", growattBMSCANTestRecord(0x314, 8, 15, 8, 7, 6, 5, 4, 3, 2, 1), growattBMSCANObservation{Identifier: 0x314, DLC: 8, RawDLC: 15, Data: [8]byte{8, 7, 6, 5, 4, 3, 2, 1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeGrowattBMSCANRecord(test.raw)
			if err != nil || got != test.want {
				t.Fatalf("decode = %#v, %v; want %#v", got, err, test.want)
			}
		})
	}
}

func TestGrowattBMSCANStaleTupleIsWithheld(t *testing.T) {
	clock := &growattBMSCANTestClock{now: time.Second}
	driver := newGrowattBMSCANTestDriver(clock)
	growattBMSCANObserveAll(t, driver, growattBMSCANLimits())
	clock.Set(2 * time.Second)
	growattBMSCANObserveAll(t, driver, growattBMSCANStatus(), growattBMSCANMeasurements())

	clock.Set(time.Second + time.Minute)
	if snapshot := growattBMSCANRequireSnapshot(t, driver); snapshot.RawEvidence[0].MonotonicNanos != int64(time.Second) {
		t.Fatalf("evidence = %#v", snapshot.RawEvidence)
	}
	clock.Set(time.Second + time.Minute + 1)
	growattBMSCANRequireNotAdmitted(t, driver)
	if _, err := driver.GrowattBMSV104Snapshot(context.Background()); !errors.Is(err, errGrowattBMSCANStale) {
		t.Fatalf("err = %v", err)
	}
	clock.Set(time.Second - 1)
	growattBMSCANRequireNotAdmitted(t, driver)
}

func TestGrowattBMSCANSnapshotHonorsCallerCancellation(t *testing.T) {
	driver := newGrowattBMSCANTestDriver(&growattBMSCANTestClock{})
	growattBMSCANObserveAll(t, driver, growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := driver.GrowattBMSV104Snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	var nilDriver *growattBMSCANDriver
	if _, err := nilDriver.GrowattBMSV104Snapshot(context.Background()); !errors.Is(err, mcp.ErrGrowattBMSV104ProviderUnavailable) {
		t.Fatalf("nil driver err = %v", err)
	}
}

// growattBMSCANFakeReceiver hands out queued records; calls signals each
// Receive entry, so a test knows every previously handed record was observed.
type growattBMSCANFakeReceiver struct {
	identity growattBMSCANInterface
	records  chan growattBMSCANRecord
	failures chan error
	calls    chan struct{}

	mu       sync.Mutex
	closed   int
	inflight bool
}

func newGrowattBMSCANFakeReceiver(identity growattBMSCANInterface) *growattBMSCANFakeReceiver {
	return &growattBMSCANFakeReceiver{identity: identity, records: make(chan growattBMSCANRecord), failures: make(chan error), calls: make(chan struct{}, 64)}
}

func (receiver *growattBMSCANFakeReceiver) Identity() growattBMSCANInterface {
	return receiver.identity
}

func (receiver *growattBMSCANFakeReceiver) Receive(ctx context.Context) (growattBMSCANRecord, error) {
	receiver.mu.Lock()
	if receiver.closed != 0 {
		receiver.mu.Unlock()
		return growattBMSCANRecord{}, errors.New("receive after close")
	}
	receiver.inflight = true
	receiver.mu.Unlock()
	defer func() {
		receiver.mu.Lock()
		receiver.inflight = false
		receiver.mu.Unlock()
	}()
	receiver.calls <- struct{}{}
	select {
	case record := <-receiver.records:
		return record, nil
	case err := <-receiver.failures:
		return growattBMSCANRecord{}, err
	case <-ctx.Done():
		return growattBMSCANRecord{}, ctx.Err()
	}
}

func (receiver *growattBMSCANFakeReceiver) Close() error {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.inflight {
		return errors.New("descriptor closed under an in-flight receive")
	}
	receiver.closed++
	return nil
}

func (receiver *growattBMSCANFakeReceiver) closeCount() int {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	return receiver.closed
}

func (receiver *growattBMSCANFakeReceiver) feed(t *testing.T, records ...growattBMSCANRecord) {
	t.Helper()
	<-receiver.calls
	for _, record := range records {
		receiver.records <- record
		<-receiver.calls
	}
}

func growattBMSCANEnabledConfig() growattBMSCANConfig {
	return growattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: time.Minute}
}

func TestGrowattBMSCANDriverComposesReceiverIntoV104Runtime(t *testing.T) {
	receiver := newGrowattBMSCANFakeReceiver(growattBMSCANTestInterface)
	clock := &growattBMSCANTestClock{}
	var opened []string
	driver, runtime, err := startGrowattBMSCANDriverWithReceiver(growattBMSCANEnabledConfig(), func(name string) (growattBMSCANReceiver, error) {
		opened = append(opened, name)
		return receiver, nil
	}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(opened, []string{"can7"}) {
		t.Fatalf("opened = %v", opened)
	}
	receiver.feed(t, growattBMSCANLimits(), growattBMSCANStatus())
	if _, err := runtime.SnapshotGet(context.Background()); !errors.Is(err, mcp.ErrGrowattBMSV104NotAdmitted) {
		t.Fatalf("partial tuple err = %v", err)
	}
	receiver.records <- growattBMSCANMeasurements()
	<-receiver.calls
	result, err := runtime.SnapshotGet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Profile != mcp.GrowattBMSV104Profile || !result.Admitted || result.OutboundAllowed || len(result.RawEvidence) != 3 || result.Limits.ChargeVoltageDecivolts != 568 {
		t.Fatalf("result = %#v", result)
	}

	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	if receiver.closeCount() != 1 {
		t.Fatalf("receiver closed %d times", receiver.closeCount())
	}
	if _, err := runtime.SnapshotGet(context.Background()); !errors.Is(err, mcp.ErrGrowattBMSV104ProviderUnavailable) {
		t.Fatalf("closed driver err = %v", err)
	}
}

func TestGrowattBMSCANReceiveFailureWithdrawsTheSnapshot(t *testing.T) {
	receiver := newGrowattBMSCANFakeReceiver(growattBMSCANTestInterface)
	driver, runtime, err := startGrowattBMSCANDriverWithReceiver(growattBMSCANEnabledConfig(), func(string) (growattBMSCANReceiver, error) { return receiver, nil }, (&growattBMSCANTestClock{}).Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Error(err)
		}
	})
	receiver.feed(t, growattBMSCANLimits(), growattBMSCANStatus(), growattBMSCANMeasurements())
	if _, err := runtime.SnapshotGet(context.Background()); err != nil {
		t.Fatal(err)
	}
	transport := errors.New("synthetic receive failure")
	receiver.failures <- transport
	<-driver.done
	_, err = runtime.SnapshotGet(context.Background())
	if !errors.Is(err, mcp.ErrGrowattBMSV104ProviderUnavailable) || !errors.Is(err, errGrowattBMSCANReceiveStopped) || !errors.Is(err, transport) {
		t.Fatalf("err = %v", err)
	}
	if !driver.Diagnostics().ReceiveTerminated {
		t.Fatal("receive termination not recorded")
	}
}

func TestGrowattBMSCANStartupFailsClosed(t *testing.T) {
	var opened atomic.Int32
	refuse := func(string) (growattBMSCANReceiver, error) {
		opened.Add(1)
		return nil, errors.New("receiver opened for a rejected configuration")
	}
	cases := map[string]struct {
		mutate func(*growattBMSCANConfig)
		want   error
	}{
		"listen-only not recorded": {func(config *growattBMSCANConfig) { config.ListenOnlyConfirmed = false }, errGrowattBMSCANListenOnly},
		"empty interface":          {func(config *growattBMSCANConfig) { config.Interface = "" }, errGrowattBMSCANConfig},
		"interface too long":       {func(config *growattBMSCANConfig) { config.Interface = "can0123456789abc" }, errGrowattBMSCANConfig},
		"interface path":           {func(config *growattBMSCANConfig) { config.Interface = "../can0" }, errGrowattBMSCANConfig},
		"interface whitespace":     {func(config *growattBMSCANConfig) { config.Interface = "can 0" }, errGrowattBMSCANConfig},
		"zero stale bound":         {func(config *growattBMSCANConfig) { config.StaleAfter = 0 }, errGrowattBMSCANConfig},
		"negative stale bound":     {func(config *growattBMSCANConfig) { config.StaleAfter = -time.Second }, errGrowattBMSCANConfig},
		"disabled with fields":     {func(config *growattBMSCANConfig) { config.Enabled = false }, errGrowattBMSCANDisabledConfig},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			config := growattBMSCANEnabledConfig()
			test.mutate(&config)
			driver, runtime, err := startGrowattBMSCANDriverWithReceiver(config, refuse, nil)
			if !errors.Is(err, test.want) || driver != nil || runtime != nil {
				t.Fatalf("driver = %v, runtime = %v, err = %v", driver, runtime, err)
			}
		})
	}

	driver, runtime, err := startGrowattBMSCANDriverWithReceiver(growattBMSCANConfig{}, refuse, nil)
	if err != nil || driver != nil || runtime != nil {
		t.Fatalf("disabled: driver = %v, runtime = %v, err = %v", driver, runtime, err)
	}
	if opened.Load() != 0 {
		t.Fatalf("receiver opened %d times for rejected or disabled configurations", opened.Load())
	}

	openFailure := errors.New("synthetic open failure")
	if _, _, err := startGrowattBMSCANDriverWithReceiver(growattBMSCANEnabledConfig(), func(string) (growattBMSCANReceiver, error) { return nil, openFailure }, nil); !errors.Is(err, openFailure) {
		t.Fatalf("open failure err = %v", err)
	}

	for _, identity := range []growattBMSCANInterface{{Name: "can8", Index: 7}, {Name: "can7", Index: 0}} {
		receiver := newGrowattBMSCANFakeReceiver(identity)
		_, _, err := startGrowattBMSCANDriverWithReceiver(growattBMSCANEnabledConfig(), func(string) (growattBMSCANReceiver, error) { return receiver, nil }, nil)
		if !errors.Is(err, errGrowattBMSCANIdentity) || receiver.closeCount() != 1 {
			t.Fatalf("identity %v: err = %v, closed = %d", identity, err, receiver.closeCount())
		}
	}
}

func TestGrowattBMSCANProductionOpenRejectsInvalidNameBeforeIO(t *testing.T) {
	driver, runtime, err := startGrowattBMSCANDriver(growattBMSCANConfig{})
	if err != nil || driver != nil || runtime != nil {
		t.Fatalf("disabled: driver = %v, runtime = %v, err = %v", driver, runtime, err)
	}
	if receiver, err := openGrowattBMSCANSocket("bad/name"); !errors.Is(err, errGrowattBMSCANConfig) || receiver != nil {
		t.Fatalf("receiver = %v, err = %v", receiver, err)
	}
}

func TestGrowattBMSCANReceiverHasNoTransmitSurface(t *testing.T) {
	receiverType := reflect.TypeOf((*growattBMSCANReceiver)(nil)).Elem()
	var methods []string
	for index := 0; index < receiverType.NumMethod(); index++ {
		methods = append(methods, receiverType.Method(index).Name)
	}
	sort.Strings(methods)
	if !reflect.DeepEqual(methods, []string{"Close", "Identity", "Receive"}) {
		t.Fatalf("receiver methods = %v", methods)
	}
	driverType := reflect.TypeOf((*growattBMSCANDriver)(nil))
	for index := 0; index < driverType.NumMethod(); index++ {
		switch name := driverType.Method(index).Name; name {
		case "Close", "Diagnostics", "GrowattBMSV104Snapshot":
		default:
			t.Fatalf("unexpected exported driver method %s", name)
		}
	}
}
