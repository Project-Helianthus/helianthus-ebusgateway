package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// The receive-only Growatt low-voltage BMS CAN V1.04 driver. It composes a
// SocketCAN receiver with the passive V1.04 admission window and feeds the
// injected mcp.GrowattBMSV104SnapshotProvider boundary. It has no transmit,
// acknowledge, probe or interface-configuration path.

const (
	growattBMSCANWindowSize   = 16
	growattBMSCANRecordSize   = 16 // Linux struct can_frame.
	growattBMSCANFDRecordSize = 72 // Linux struct canfd_frame; recognized only to reject it.

	growattBMSCANExtendedFlag = uint32(0x80000000)
	growattBMSCANRTRFlag      = uint32(0x40000000)
	growattBMSCANErrorFlag    = uint32(0x20000000)
	growattBMSCANStandardMask = uint32(0x7ff)
	growattBMSCANExtendedMask = uint32(0x1fffffff)

	growattBMSCANInterfaceNameMaximum = 15 // IFNAMSIZ without the terminator.
)

var (
	errGrowattBMSCANConfig         = errors.New("enabled Growatt BMS CAN configuration is incomplete")
	errGrowattBMSCANDisabledConfig = errors.New("disabled Growatt BMS CAN configuration contains active fields")
	errGrowattBMSCANListenOnly     = errors.New("growatt BMS CAN controller listen-only precondition is not recorded")
	errGrowattBMSCANIdentity       = errors.New("growatt BMS CAN receiver identity does not match the configured interface")
	errGrowattBMSCANRecord         = errors.New("growatt BMS CAN record rejected")
	errGrowattBMSCANReceiveStopped = errors.New("growatt BMS CAN receive loop stopped")
	errGrowattBMSCANStale          = errors.New("growatt BMS CAN V1.04 snapshot is stale")
)

// growattBMSCANConfig is the explicit, disabled-by-default composition input.
type growattBMSCANConfig struct {
	Enabled   bool
	Interface string
	// ListenOnlyConfirmed records the integrator's admission precondition:
	// the controller is already in physical listen-only mode before the
	// descriptor opens. The driver cannot verify or set controller mode.
	ListenOnlyConfirmed bool
	// StaleAfter bounds the host receipt age of the oldest source frame of
	// the published tuple; an older tuple is withheld, never served as current.
	StaleAfter time.Duration
}

type growattBMSCANInterface struct {
	Name  string
	Index int
}

// growattBMSCANRecord is one raw native struct can_frame record as read from
// the bound receive socket.
type growattBMSCANRecord struct {
	Interface growattBMSCANInterface
	Raw       []byte
}

// growattBMSCANReceiver is receive-only by type: it has no transmit method.
// Receive must return when ctx is done.
type growattBMSCANReceiver interface {
	Identity() growattBMSCANInterface
	Receive(context.Context) (growattBMSCANRecord, error)
	Close() error
}

// growattBMSCANObservation is one transport-accepted native observation.
type growattBMSCANObservation struct {
	Interface      growattBMSCANInterface
	Sequence       uint64
	MonotonicNanos int64
	Identifier     uint32
	Extended       bool
	RTR            bool
	DLC            uint8
	RawDLC         uint8
	Data           [8]byte
}

type growattBMSCANPublication struct {
	limits       mcp.GrowattBMSV104Limits
	status       mcp.GrowattBMSV104Status
	measurements mcp.GrowattBMSV104Measurements
	sources      [3]growattBMSCANObservation // 0x311, 0x312, 0x313.
}

type growattBMSCANDiagnostics struct {
	RejectedRecords   uint64
	ForeignInterface  uint64
	MalformedListed   uint64
	AcceptedFrames    uint64
	Publications      uint64
	ReceiveTerminated bool
}

type growattBMSCANDriver struct {
	identity   growattBMSCANInterface
	staleAfter time.Duration
	receiver   growattBMSCANReceiver
	now        func() time.Duration

	mu          sync.Mutex
	window      [growattBMSCANWindowSize]growattBMSCANObservation
	count, next int
	sequence    uint64
	published   growattBMSCANPublication
	have        bool
	receiveErr  error
	closed      bool
	diagnostics growattBMSCANDiagnostics

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ mcp.GrowattBMSV104SnapshotProvider = (*growattBMSCANDriver)(nil)

var openGrowattBMSCANReceiver = openGrowattBMSCANSocket

func startGrowattBMSCANDriver(config growattBMSCANConfig) (*growattBMSCANDriver, *mcp.GrowattBMSV104Runtime, error) {
	return startGrowattBMSCANDriverWithReceiver(config, openGrowattBMSCANReceiver, nil)
}

func startGrowattBMSCANDriverWithReceiver(config growattBMSCANConfig, open func(string) (growattBMSCANReceiver, error), now func() time.Duration) (*growattBMSCANDriver, *mcp.GrowattBMSV104Runtime, error) {
	if !config.Enabled {
		if config != (growattBMSCANConfig{}) {
			return nil, nil, errGrowattBMSCANDisabledConfig
		}
		return nil, nil, nil
	}
	if !validGrowattBMSCANInterfaceName(config.Interface) || config.StaleAfter <= 0 || open == nil {
		return nil, nil, errGrowattBMSCANConfig
	}
	if !config.ListenOnlyConfirmed {
		return nil, nil, errGrowattBMSCANListenOnly
	}
	receiver, err := open(config.Interface)
	if err != nil {
		return nil, nil, fmt.Errorf("growatt BMS CAN receiver open: %w", err)
	}
	if receiver == nil {
		return nil, nil, errGrowattBMSCANConfig
	}
	identity := receiver.Identity()
	if identity.Name != config.Interface || identity.Index <= 0 {
		return nil, nil, errors.Join(errGrowattBMSCANIdentity, receiver.Close())
	}
	if now == nil {
		start := time.Now()
		now = func() time.Duration { return time.Since(start) }
	}
	lifetime, cancel := context.WithCancel(context.Background())
	driver := &growattBMSCANDriver{
		identity: identity, staleAfter: config.StaleAfter, receiver: receiver, now: now,
		cancel: cancel, done: make(chan struct{}),
	}
	runtime, err := mcp.NewGrowattBMSV104Runtime(driver)
	if err != nil {
		cancel()
		return nil, nil, errors.Join(err, receiver.Close())
	}
	go driver.run(lifetime)
	return driver, runtime, nil
}

func validGrowattBMSCANInterfaceName(name string) bool {
	if name == "" || len(name) > growattBMSCANInterfaceNameMaximum || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsAny(name, "/:\x00 \t\r\n")
}

func (driver *growattBMSCANDriver) run(ctx context.Context) {
	defer close(driver.done)
	for {
		record, err := driver.receiver.Receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			driver.terminate(err)
			return
		}
		if !driver.observe(record) {
			return
		}
	}
}

func (driver *growattBMSCANDriver) terminate(err error) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	driver.receiveErr = errors.Join(errGrowattBMSCANReceiveStopped, err)
	driver.diagnostics.ReceiveTerminated = true
}

// observe admits one raw record. A record that is not transport-valid or not
// from the bound interface is a diagnostic only: it consumes no sequence,
// leaves the admission window untouched and never clears a publication.
// It returns false only when the sequence space is exhausted.
func (driver *growattBMSCANDriver) observe(record growattBMSCANRecord) bool {
	received := driver.now()
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if record.Interface != driver.identity {
		driver.diagnostics.ForeignInterface++
		return true
	}
	observation, err := decodeGrowattBMSCANRecord(record.Raw)
	if err != nil {
		driver.diagnostics.RejectedRecords++
		return true
	}
	if driver.sequence == math.MaxUint64 {
		driver.receiveErr = errors.Join(errGrowattBMSCANReceiveStopped, errors.New("growatt BMS CAN listener sequence exhausted"))
		driver.diagnostics.ReceiveTerminated = true
		return false
	}
	driver.sequence++
	observation.Interface = driver.identity
	observation.Sequence = driver.sequence
	observation.MonotonicNanos = int64(received)
	driver.diagnostics.AcceptedFrames++

	if growattBMSCANListed(observation.Identifier) && !growattBMSCANValid(observation) {
		// A malformed listed frame resets the admission window. The previous
		// publication stays as last-known-good.
		driver.diagnostics.MalformedListed++
		driver.count, driver.next = 0, 0
	}
	driver.window[driver.next] = observation
	driver.next = (driver.next + 1) % growattBMSCANWindowSize
	if driver.count < growattBMSCANWindowSize {
		driver.count++
	}
	if publication, ok := driver.aggregateLocked(); ok {
		driver.published, driver.have = publication, true
		driver.diagnostics.Publications++
	}
	return true
}

// aggregateLocked returns the latest valid 0x311, 0x312 and 0x313 within the
// most recent accepted observations; all three are required.
func (driver *growattBMSCANDriver) aggregateLocked() (growattBMSCANPublication, bool) {
	var (
		latest [3]growattBMSCANObservation
		found  [3]bool
	)
	for index := 0; index < driver.count; index++ {
		observation := driver.window[index]
		slot := int(observation.Identifier) - 0x311
		if slot < 0 || slot > 2 || !growattBMSCANValid(observation) {
			continue
		}
		if !found[slot] || observation.Sequence > latest[slot].Sequence {
			latest[slot], found[slot] = observation, true
		}
	}
	if !found[0] || !found[1] || !found[2] {
		return growattBMSCANPublication{}, false
	}
	limits, status, measures := latest[0].Data, latest[1].Data, latest[2].Data
	return growattBMSCANPublication{
		limits: mcp.GrowattBMSV104Limits{
			ChargeVoltageDecivolts:   binary.BigEndian.Uint16(limits[0:2]),
			ChargeCurrentDeciamps:    binary.BigEndian.Uint16(limits[2:4]),
			DischargeCurrentDeciamps: binary.BigEndian.Uint16(limits[4:6]),
			RawStatus:                binary.BigEndian.Uint16(limits[6:8]),
		},
		status: mcp.GrowattBMSV104Status{
			Protection:     [2]byte{status[0], status[1]},
			Warning:        [2]byte{status[2], status[3]},
			PackCount:      status[4],
			Manufacturer:   [2]byte{status[5], status[6]},
			TotalCellCount: status[7],
		},
		measurements: mcp.GrowattBMSV104Measurements{
			VoltageCentivolts:           int16(binary.BigEndian.Uint16(measures[0:2])),
			CurrentDeciamps:             int16(binary.BigEndian.Uint16(measures[2:4])),
			MaximumCellTemperatureDeciC: int16(binary.BigEndian.Uint16(measures[4:6])),
			SOCPercent:                  measures[6],
			SOHValue:                    measures[7] & 0x7f,
			SOHValid:                    measures[7]&0x80 != 0,
		},
		sources: latest,
	}, true
}

// GrowattBMSV104Snapshot serves the last published tuple. It performs no
// transport I/O; it withholds a stale tuple and any tuple of a stopped driver.
func (driver *growattBMSCANDriver) GrowattBMSV104Snapshot(ctx context.Context) (mcp.GrowattBMSV104ProviderSnapshot, error) {
	if driver == nil {
		return mcp.GrowattBMSV104ProviderSnapshot{}, mcp.ErrGrowattBMSV104ProviderUnavailable
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return mcp.GrowattBMSV104ProviderSnapshot{}, err
		}
	}
	now := driver.now()
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.closed {
		return mcp.GrowattBMSV104ProviderSnapshot{}, mcp.ErrGrowattBMSV104ProviderUnavailable
	}
	if driver.receiveErr != nil {
		return mcp.GrowattBMSV104ProviderSnapshot{}, errors.Join(mcp.ErrGrowattBMSV104ProviderUnavailable, driver.receiveErr)
	}
	if !driver.have {
		return mcp.GrowattBMSV104ProviderSnapshot{}, mcp.ErrGrowattBMSV104NotAdmitted
	}
	sources := driver.published.sources
	sort.Slice(sources[:], func(i, j int) bool { return sources[i].Sequence < sources[j].Sequence })
	age := now - time.Duration(sources[0].MonotonicNanos)
	if age < 0 || age > driver.staleAfter {
		return mcp.GrowattBMSV104ProviderSnapshot{}, errors.Join(mcp.ErrGrowattBMSV104NotAdmitted, errGrowattBMSCANStale)
	}
	evidence := make([]mcp.GrowattBMSV104RawEvidence, 0, len(sources))
	for _, source := range sources {
		evidence = append(evidence, mcp.GrowattBMSV104RawEvidence{
			Interface: source.Interface.Name, Sequence: source.Sequence, MonotonicNanos: source.MonotonicNanos,
			Identifier: source.Identifier, Extended: source.Extended, RTR: source.RTR,
			DLC: source.DLC, RawDLC: source.RawDLC, Data: source.Data,
		})
	}
	return mcp.GrowattBMSV104ProviderSnapshot{
		Profile: mcp.GrowattBMSV104Profile, Admitted: true, OutboundAllowed: false,
		Limits: driver.published.limits, Status: driver.published.status, Measurements: driver.published.measurements,
		RawEvidence: evidence,
	}, nil
}

func (driver *growattBMSCANDriver) Diagnostics() growattBMSCANDiagnostics {
	if driver == nil {
		return growattBMSCANDiagnostics{}
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.diagnostics
}

// Close stops the receive loop before the descriptor closes, so the
// descriptor is never closed underneath an in-flight receive.
func (driver *growattBMSCANDriver) Close() error {
	if driver == nil {
		return nil
	}
	driver.closeOnce.Do(func() {
		driver.cancel()
		<-driver.done
		driver.mu.Lock()
		driver.closed = true
		driver.mu.Unlock()
		driver.closeErr = driver.receiver.Close()
	})
	return driver.closeErr
}

// decodeGrowattBMSCANRecord decodes one native-endian classic struct
// can_frame. FD records, error frames and inconsistent lengths are rejected.
func decodeGrowattBMSCANRecord(record []byte) (growattBMSCANObservation, error) {
	if len(record) == growattBMSCANFDRecordSize {
		return growattBMSCANObservation{}, fmt.Errorf("%w: CAN FD record", errGrowattBMSCANRecord)
	}
	if len(record) != growattBMSCANRecordSize {
		return growattBMSCANObservation{}, fmt.Errorf("%w: record length %d", errGrowattBMSCANRecord, len(record))
	}
	raw := binary.NativeEndian.Uint32(record[0:4])
	if raw&growattBMSCANErrorFlag != 0 {
		return growattBMSCANObservation{}, fmt.Errorf("%w: error frame", errGrowattBMSCANRecord)
	}
	observation := growattBMSCANObservation{
		Extended: raw&growattBMSCANExtendedFlag != 0,
		RTR:      raw&growattBMSCANRTRFlag != 0,
	}
	identifier := raw &^ (growattBMSCANExtendedFlag | growattBMSCANRTRFlag)
	if observation.Extended {
		observation.Identifier = identifier & growattBMSCANExtendedMask
	} else {
		if identifier&^growattBMSCANStandardMask != 0 {
			return growattBMSCANObservation{}, fmt.Errorf("%w: unflagged identifier %#x", errGrowattBMSCANRecord, identifier)
		}
		observation.Identifier = identifier
	}
	length, len8DLC := record[4], record[7]
	if length > 8 {
		return growattBMSCANObservation{}, fmt.Errorf("%w: payload length %d", errGrowattBMSCANRecord, length)
	}
	switch {
	case len8DLC == 0:
		observation.RawDLC = length
	case length == 8 && len8DLC >= 9 && len8DLC <= 15:
		observation.RawDLC = len8DLC
	default:
		return growattBMSCANObservation{}, fmt.Errorf("%w: len8_dlc %d with payload length %d", errGrowattBMSCANRecord, len8DLC, length)
	}
	observation.DLC = length
	if !observation.RTR {
		// Bytes beyond the effective payload length are not frame payload.
		copy(observation.Data[:], record[8:8+int(length)])
	}
	return observation, nil
}

func growattBMSCANListed(identifier uint32) bool {
	switch identifier {
	case 0x311, 0x312, 0x313, 0x314, 0x315, 0x316, 0x317, 0x318, 0x319, 0x320, 0x321:
		return true
	}
	return false
}

func growattBMSCANValid(observation growattBMSCANObservation) bool {
	if observation.Extended || observation.RTR || observation.DLC != 8 || observation.RawDLC != 8 {
		return false
	}
	switch observation.Identifier {
	case 0x312:
		return observation.Data[4] != 0 && observation.Data[7] != 0
	case 0x313:
		return observation.Data[6] <= 100
	}
	return true
}
