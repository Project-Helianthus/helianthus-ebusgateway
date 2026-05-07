package portal

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// ExplorerBus is the interface for sending frames on the eBUS.
// *protocol.Bus satisfies this interface.
type ExplorerBus interface {
	Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error)
}

// Scan phases.
const (
	ExplorerPhaseIdle              = "idle"
	ExplorerPhaseGroupDiscovery    = "group_discovery"
	ExplorerPhaseInstanceDiscovery = "instance_discovery"
	ExplorerPhaseRegisterScan      = "register_scan"
	ExplorerPhaseDone              = "done"
	ExplorerPhaseCancelled         = "cancelled"
	ExplorerPhaseError             = "error"
)

// Scan kinds.
const (
	ExplorerKindB524 = "b524"
	ExplorerKindB509 = "b509"
)

// ExplorerScanRequest is the JSON body for POST /api/v1/explorer/scans.
type ExplorerScanRequest struct {
	Kind        string `json:"kind"`          // "b524" or "b509"
	Target      byte   `json:"target"`        // device address
	Source      byte   `json:"source"`        // source address (0 = default)
	Opcode      byte   `json:"opcode"`        // B524: 0x02 local, 0x06 remote; B509: 0x0D read, 0x29 passive
	GroupMin    byte   `json:"group_min"`     // B524: first group to scan
	GroupMax    byte   `json:"group_max"`     // B524: last group to scan
	InstanceMax byte   `json:"instance_max"`  // B524: max instance per group
	RegisterMax uint16 `json:"register_max"`  // B524: max register address
	B509AddrMin uint16 `json:"b509_addr_min"` // B509: first address
	B509AddrMax uint16 `json:"b509_addr_max"` // B509: last address
}

// ExplorerGroupResult holds the result of a group discovery probe.
type ExplorerGroupResult struct {
	Group     byte    `json:"group"`
	GroupHex  string  `json:"group_hex"`
	Exists    bool    `json:"exists"`
	Instanced bool    `json:"instanced"`
	Value     float32 `json:"value"`
}

// ExplorerRegisterResult holds one register read result.
type ExplorerRegisterResult struct {
	Group        byte    `json:"group"`
	Instance     byte    `json:"instance"`
	Addr         uint16  `json:"addr"`
	AddrHex      string  `json:"addr_hex"`
	RawHex       string  `json:"raw_hex"`
	RawLen       int     `json:"raw_len"`
	DefaultFloat float64 `json:"default_float"`
	Error        string  `json:"error,omitempty"`
}

// ExplorerScanState is the current scan state returned to the frontend.
type ExplorerScanState struct {
	Phase          string                   `json:"phase"`
	Kind           string                   `json:"kind"`
	Target         byte                     `json:"target"`
	Source         byte                     `json:"source"`
	Opcode         byte                     `json:"opcode"`
	StartedUTC     string                   `json:"started_utc"`
	FinishedUTC    string                   `json:"finished_utc,omitempty"`
	Error          string                   `json:"error,omitempty"`
	Groups         []ExplorerGroupResult    `json:"groups,omitempty"`
	Results        []ExplorerRegisterResult `json:"results,omitempty"`
	Progress       ExplorerProgress         `json:"progress"`
	TotalReads     int                      `json:"total_reads"`
	CompletedReads int                      `json:"completed_reads"`
}

// ExplorerProgress provides detailed progress per phase.
type ExplorerProgress struct {
	CurrentGroup    byte   `json:"current_group"`
	CurrentInstance byte   `json:"current_instance"`
	CurrentAddr     uint16 `json:"current_addr"`
	Percent         int    `json:"percent"`
	Description     string `json:"description"`
}

// ExplorerScanIDResult holds the result of a ScanID serial read.
type ExplorerScanIDResult struct {
	Target byte     `json:"target"`
	Source byte     `json:"source"`
	Chunks []string `json:"chunks"`
	Serial string   `json:"serial"`
	Error  string   `json:"error,omitempty"`
}

type explorerStore struct {
	mu      sync.Mutex
	current *ExplorerScanState
	bus     ExplorerBus
	source  byte
	cancel  context.CancelFunc

	// SSE subscribers
	subMu   sync.Mutex
	subs    map[uint64]chan struct{}
	subNext uint64
}

func newExplorerStore(bus ExplorerBus, source byte) *explorerStore {
	if source == 0 {
		source = 0xF0
	}
	return &explorerStore{
		bus:    bus,
		source: source,
		subs:   make(map[uint64]chan struct{}),
	}
}

func (es *explorerStore) notifySubscribers() {
	es.subMu.Lock()
	defer es.subMu.Unlock()
	for _, ch := range es.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (es *explorerStore) subscribe() (uint64, chan struct{}) {
	es.subMu.Lock()
	defer es.subMu.Unlock()
	id := es.subNext
	es.subNext++
	ch := make(chan struct{}, 1)
	es.subs[id] = ch
	return id, ch
}

func (es *explorerStore) unsubscribe(id uint64) {
	es.subMu.Lock()
	defer es.subMu.Unlock()
	delete(es.subs, id)
}

// getState returns a snapshot of the current scan state (safe for JSON).
func (es *explorerStore) getState() *ExplorerScanState {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.current == nil {
		return &ExplorerScanState{Phase: ExplorerPhaseIdle}
	}
	// Shallow copy to avoid data races on slices.
	snap := *es.current
	snap.Groups = append([]ExplorerGroupResult(nil), es.current.Groups...)
	snap.Results = append([]ExplorerRegisterResult(nil), es.current.Results...)
	return &snap
}

// getResults returns a page of results.
func (es *explorerStore) getResults(offset, limit int) []ExplorerRegisterResult {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.current == nil {
		return nil
	}
	results := es.current.Results
	if offset < 0 {
		offset = 0
	}
	if offset >= len(results) {
		return nil
	}
	end := offset + limit
	if end > len(results) {
		end = len(results)
	}
	out := make([]ExplorerRegisterResult, end-offset)
	copy(out, results[offset:end])
	return out
}

// cancelScan cancels the running scan.
func (es *explorerStore) cancelScan() bool {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.current == nil || es.cancel == nil {
		return false
	}
	switch es.current.Phase {
	case ExplorerPhaseGroupDiscovery, ExplorerPhaseInstanceDiscovery, ExplorerPhaseRegisterScan:
		es.cancel()
		return true
	}
	return false
}

// startB524Scan starts a B524 scan in the background. Returns error if a scan is already running.
func (es *explorerStore) startB524Scan(req ExplorerScanRequest) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.current != nil {
		switch es.current.Phase {
		case ExplorerPhaseGroupDiscovery, ExplorerPhaseInstanceDiscovery, ExplorerPhaseRegisterScan:
			return fmt.Errorf("scan already running")
		}
	}

	source := req.Source
	if source == 0 {
		source = es.source
	}
	opcode := req.Opcode
	if opcode == 0 {
		opcode = 0x02
	}

	ctx, cancel := context.WithCancel(context.Background())
	es.cancel = cancel

	state := &ExplorerScanState{
		Phase:      ExplorerPhaseGroupDiscovery,
		Kind:       ExplorerKindB524,
		Target:     req.Target,
		Source:     source,
		Opcode:     opcode,
		StartedUTC: time.Now().UTC().Format(time.RFC3339),
		Groups:     make([]ExplorerGroupResult, 0),
		Results:    make([]ExplorerRegisterResult, 0),
	}
	es.current = state

	go es.runB524Scan(ctx, req, state, source, opcode)
	return nil
}

// startB509Scan starts a B509 scan in the background.
func (es *explorerStore) startB509Scan(req ExplorerScanRequest) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.current != nil {
		switch es.current.Phase {
		case ExplorerPhaseGroupDiscovery, ExplorerPhaseInstanceDiscovery, ExplorerPhaseRegisterScan:
			return fmt.Errorf("scan already running")
		}
	}

	source := req.Source
	if source == 0 {
		source = es.source
	}
	opcode := req.Opcode
	if opcode == 0 {
		opcode = 0x0D
	}

	ctx, cancel := context.WithCancel(context.Background())
	es.cancel = cancel

	state := &ExplorerScanState{
		Phase:      ExplorerPhaseRegisterScan,
		Kind:       ExplorerKindB509,
		Target:     req.Target,
		Source:     source,
		Opcode:     opcode,
		StartedUTC: time.Now().UTC().Format(time.RFC3339),
		Results:    make([]ExplorerRegisterResult, 0),
	}
	es.current = state

	go es.runB509Scan(ctx, req, state, source, opcode)
	return nil
}

func (es *explorerStore) runB524Scan(ctx context.Context, req ExplorerScanRequest, state *ExplorerScanState, source, opcode byte) {
	defer func() {
		es.mu.Lock()
		if state.Phase != ExplorerPhaseCancelled {
			if state.Error != "" {
				state.Phase = ExplorerPhaseError
			} else {
				state.Phase = ExplorerPhaseDone
			}
		}
		state.FinishedUTC = time.Now().UTC().Format(time.RFC3339)
		es.mu.Unlock()
		es.notifySubscribers()
	}()

	groupMin := int(req.GroupMin)
	groupMax := int(req.GroupMax)
	if groupMax == 0 {
		groupMax = 0x10
	}
	instanceMax := int(req.InstanceMax)
	if instanceMax == 0 {
		instanceMax = 0x0A
	}
	registerMax := int(req.RegisterMax)
	if registerMax == 0 {
		registerMax = 0x20
	}

	// Phase 1: Group Discovery
	for g := groupMin; g <= groupMax; g++ {
		if ctx.Err() != nil {
			es.mu.Lock()
			state.Phase = ExplorerPhaseCancelled
			es.mu.Unlock()
			return
		}

		group := byte(g)
		val, err := es.readB524GroupDescriptor(ctx, req.Target, source, opcode, group)

		gr := ExplorerGroupResult{
			Group:    group,
			GroupHex: fmt.Sprintf("%02x", group),
		}
		// NaN/Inf are not JSON-serializable; store 0 instead.
		if err == nil && !math.IsNaN(float64(val)) && !math.IsInf(float64(val), 0) {
			gr.Value = val
		}
		if err != nil {
			// Error reading = group doesn't exist or bus error.
			gr.Exists = false
		} else if math.IsNaN(float64(val)) {
			// NaN = end of groups
			gr.Exists = false
		} else if val == 0.0 {
			// 0.0 = hole (skip)
			gr.Exists = false
		} else {
			gr.Exists = true
			gr.Instanced = val == 1.0
		}

		es.mu.Lock()
		state.Groups = append(state.Groups, gr)
		state.Progress = ExplorerProgress{
			CurrentGroup: group,
			Percent:      ((g - groupMin + 1) * 100) / (groupMax - groupMin + 1),
			Description:  fmt.Sprintf("Discovering group 0x%02x", group),
		}
		es.mu.Unlock()
		es.notifySubscribers()
	}

	// Phase 2: Instance Discovery + Register Scan
	es.mu.Lock()
	state.Phase = ExplorerPhaseInstanceDiscovery
	// Count total reads for progress.
	var totalReads int
	for _, gr := range state.Groups {
		if !gr.Exists {
			continue
		}
		if gr.Instanced {
			totalReads += instanceMax + 1 // instance probes
		}
		// Register reads counted after instance discovery.
	}
	state.TotalReads = totalReads
	groups := append([]ExplorerGroupResult(nil), state.Groups...)
	es.mu.Unlock()
	es.notifySubscribers()

	type groupInstance struct {
		group    byte
		instance byte
	}
	var targets []groupInstance

	for _, gr := range groups {
		if !gr.Exists {
			continue
		}
		if ctx.Err() != nil {
			es.mu.Lock()
			state.Phase = ExplorerPhaseCancelled
			es.mu.Unlock()
			return
		}

		if gr.Instanced {
			for i := 0; i <= instanceMax; i++ {
				if ctx.Err() != nil {
					es.mu.Lock()
					state.Phase = ExplorerPhaseCancelled
					es.mu.Unlock()
					return
				}
				inst := byte(i)
				present := es.probeInstance(ctx, req.Target, source, opcode, gr.Group, inst)
				es.mu.Lock()
				state.CompletedReads++
				state.Progress = ExplorerProgress{
					CurrentGroup:    gr.Group,
					CurrentInstance: inst,
					Percent:         (state.CompletedReads * 100) / max(state.TotalReads, 1),
					Description:     fmt.Sprintf("Probing group 0x%02x instance 0x%02x", gr.Group, inst),
				}
				es.mu.Unlock()
				es.notifySubscribers()

				if present {
					targets = append(targets, groupInstance{group: gr.Group, instance: inst})
				}
			}
		} else {
			targets = append(targets, groupInstance{group: gr.Group, instance: 0x00})
		}
	}

	// Phase 3: Register Scan
	es.mu.Lock()
	state.Phase = ExplorerPhaseRegisterScan
	state.TotalReads = len(targets) * (registerMax + 1)
	state.CompletedReads = 0
	es.mu.Unlock()
	es.notifySubscribers()

	for _, tgt := range targets {
		for r := 0; r <= registerMax; r++ {
			if ctx.Err() != nil {
				es.mu.Lock()
				state.Phase = ExplorerPhaseCancelled
				es.mu.Unlock()
				return
			}

			addr := uint16(r)
			result := es.readB524Register(ctx, req.Target, source, opcode, tgt.group, tgt.instance, addr)

			es.mu.Lock()
			state.Results = append(state.Results, result)
			state.CompletedReads++
			state.Progress = ExplorerProgress{
				CurrentGroup:    tgt.group,
				CurrentInstance: tgt.instance,
				CurrentAddr:     addr,
				Percent:         (state.CompletedReads * 100) / max(state.TotalReads, 1),
				Description:     fmt.Sprintf("Reading GG=0x%02x II=0x%02x RR=0x%04x", tgt.group, tgt.instance, addr),
			}
			es.mu.Unlock()
			es.notifySubscribers()
		}
	}
}

func (es *explorerStore) runB509Scan(ctx context.Context, req ExplorerScanRequest, state *ExplorerScanState, source, opcode byte) {
	defer func() {
		es.mu.Lock()
		if state.Phase != ExplorerPhaseCancelled {
			if state.Error != "" {
				state.Phase = ExplorerPhaseError
			} else {
				state.Phase = ExplorerPhaseDone
			}
		}
		state.FinishedUTC = time.Now().UTC().Format(time.RFC3339)
		es.mu.Unlock()
		es.notifySubscribers()
	}()

	addrMin := int(req.B509AddrMin)
	addrMax := int(req.B509AddrMax)
	if addrMax == 0 {
		addrMax = 0xFF
	}

	total := addrMax - addrMin + 1
	es.mu.Lock()
	state.TotalReads = total
	es.mu.Unlock()

	for a := addrMin; a <= addrMax; a++ {
		if ctx.Err() != nil {
			es.mu.Lock()
			state.Phase = ExplorerPhaseCancelled
			es.mu.Unlock()
			return
		}

		addr := uint16(a)
		result := es.readB509Register(ctx, req.Target, source, addr, opcode)

		es.mu.Lock()
		state.Results = append(state.Results, result)
		state.CompletedReads++
		state.Progress = ExplorerProgress{
			CurrentAddr: addr,
			Percent:     (state.CompletedReads * 100) / max(total, 1),
			Description: fmt.Sprintf("Reading B509 addr 0x%04x", addr),
		}
		es.mu.Unlock()
		es.notifySubscribers()
	}
}

// readB524GroupDescriptor probes a group descriptor via opcode 0x00.
func (es *explorerStore) readB524GroupDescriptor(ctx context.Context, target, source, opcode, group byte) (float32, error) {
	// Group discovery: Data = [0x00, group, 0x00]
	// Per VRC Explorer protocol, the discovery opcode is 0x00 in Data[0],
	// but the request opcode for B524 is the configured one.
	frame := protocol.Frame{
		FrameType: protocol.FrameTypeForTarget(target),
		Source:    source,
		Target:    target,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{opcode, 0x00, group, 0x00, 0x00, 0x00},
	}

	resp, err := explorerSendWithRetry(ctx, es.bus, frame)
	if err != nil {
		return 0, err
	}
	if len(resp.Data) < 4 {
		return 0, fmt.Errorf("empty or short response")
	}

	// Extract float32 from response data.
	data := resp.Data
	// Parse the B524 reply payload to get the register data.
	payload := extractB524Payload(data, group, 0x0000)
	if len(payload) < 4 {
		return 0, fmt.Errorf("payload too short: %d bytes", len(payload))
	}

	bits := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
	val := math.Float32frombits(bits)
	return val, nil
}

// probeInstance checks if instance exists by reading register 0x0000.
func (es *explorerStore) probeInstance(ctx context.Context, target, source, opcode, group, instance byte) bool {
	frame := protocol.Frame{
		FrameType: protocol.FrameTypeForTarget(target),
		Source:    source,
		Target:    target,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{opcode, 0x00, group, instance, 0x00, 0x00},
	}

	resp, err := explorerSendWithRetry(ctx, es.bus, frame)
	if err != nil {
		return false
	}
	if len(resp.Data) == 0 {
		return false
	}
	// A non-error response with data indicates the instance exists.
	// Single byte 0x00 is an error/empty marker.
	if len(resp.Data) == 1 && resp.Data[0] == 0x00 {
		return false
	}
	return true
}

// readB524Register reads a single B524 register.
func (es *explorerStore) readB524Register(ctx context.Context, target, source, opcode, group, instance byte, addr uint16) ExplorerRegisterResult {
	result := ExplorerRegisterResult{
		Group:    group,
		Instance: instance,
		Addr:     addr,
		AddrHex:  fmt.Sprintf("%04x", addr),
	}

	frame := protocol.Frame{
		FrameType: protocol.FrameTypeForTarget(target),
		Source:    source,
		Target:    target,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{opcode, 0x00, group, instance, byte(addr), byte(addr >> 8)},
	}

	resp, err := explorerSendWithRetry(ctx, es.bus, frame)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	payload := extractB524Payload(resp.Data, group, addr)
	if len(payload) == 0 {
		result.Error = "no data in response"
		return result
	}

	result.RawHex = hex.EncodeToString(payload)
	result.RawLen = len(payload)

	// Default float interpretation (LE float32 if 4+ bytes).
	if len(payload) >= 4 {
		bits := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
		f := float64(math.Float32frombits(bits))
		if !math.IsNaN(f) && !math.IsInf(f, 0) {
			result.DefaultFloat = f
		}
	}

	return result
}

// readB509Register reads a single B509 register.
func (es *explorerStore) readB509Register(ctx context.Context, target, source byte, addr uint16, opcode byte) ExplorerRegisterResult {
	result := ExplorerRegisterResult{
		Addr:    addr,
		AddrHex: fmt.Sprintf("%04x", addr),
	}

	if opcode == 0 {
		opcode = 0x0D
	}
	frame := protocol.Frame{
		FrameType: protocol.FrameTypeForTarget(target),
		Source:    source,
		Target:    target,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{opcode, byte(addr >> 8), byte(addr)},
	}

	resp, err := explorerSendWithRetry(ctx, es.bus, frame)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	if len(resp.Data) == 0 {
		result.Error = "no data in response"
		return result
	}

	result.RawHex = hex.EncodeToString(resp.Data)
	result.RawLen = len(resp.Data)

	if len(resp.Data) >= 4 {
		bits := uint32(resp.Data[0]) | uint32(resp.Data[1])<<8 | uint32(resp.Data[2])<<16 | uint32(resp.Data[3])<<24
		f := float64(math.Float32frombits(bits))
		if !math.IsNaN(f) && !math.IsInf(f, 0) {
			result.DefaultFloat = f
		}
	}

	return result
}

// extractB524Payload extracts register data from a B524 response.
// Mirrors the logic from parseB524ReadPayload in semantic_vaillant.go:
// - 5-byte header: [instance, group, addr_lo, addr_hi, ...data] when len >= 5 and group/addr match at positions [2],[3:4]
// - 4-byte header: [kind, group, addr_lo, addr_hi, ...data] when group/addr match at positions [1],[2:3]
// The group and addr parameters are from the original request, used to detect the header format.
func extractB524Payload(data []byte, group byte, addr uint16) []byte {
	if len(data) == 0 {
		return nil
	}
	if len(data) == 1 && data[0] == 0x00 {
		return nil
	}
	if len(data) < 4 {
		return nil
	}

	// Try 5-byte header format: [instance, group, addr_lo, addr_hi, ...data]
	if len(data) >= 5 {
		replyGroup := data[2]
		replyAddr := uint16(data[3]) | uint16(data[4])<<8
		if replyGroup == group && replyAddr == addr {
			if len(data) <= 5 {
				return nil
			}
			return data[5:]
		}
	}

	// Try 4-byte header format: [kind, group, addr_lo, addr_hi, ...data]
	replyGroup := data[1]
	replyAddr := uint16(data[2]) | uint16(data[3])<<8
	if replyGroup == group && replyAddr == addr {
		if len(data) <= 4 {
			return nil
		}
		return data[4:]
	}

	// No header match — return raw data as-is (best effort).
	return data
}

// singleB524Read performs a single synchronous B524 read.
func (es *explorerStore) singleB524Read(ctx context.Context, target, source, opcode, group, instance byte, addr uint16) ExplorerRegisterResult {
	if source == 0 {
		source = es.source
	}
	if opcode == 0 {
		opcode = 0x02
	}
	return es.readB524Register(ctx, target, source, opcode, group, instance, addr)
}

// singleB509Read performs a single synchronous B509 read.
func (es *explorerStore) singleB509Read(ctx context.Context, target, source byte, addr uint16, opcode byte) ExplorerRegisterResult {
	if source == 0 {
		source = es.source
	}
	if opcode == 0 {
		opcode = 0x0D
	}
	return es.readB509Register(ctx, target, source, addr, opcode)
}

// readScanID reads the device serial via B5.09 ScanID frames (QQ=0x24..0x27).
// Mirrors readVaillantScanID from ebusreg/registry/scan.go but uses explorerSendWithRetry.
//
// Some devices (e.g. BAI boiler) return 9 bytes where Data[0] is a 0x00 status
// byte and Data[1:9] holds 8 serial bytes.  Other devices (e.g. BASV2 controller)
// return 9 bytes of raw serial data with no status prefix.  We try the status-byte
// interpretation first; if it doesn't yield a formatted serial we fall back to
// the full-9-byte interpretation.
func (es *explorerStore) readScanID(ctx context.Context, target, source byte) ExplorerScanIDResult {
	if source == 0 {
		source = es.source
	}
	result := ExplorerScanIDResult{
		Target: target,
		Source: source,
		Chunks: make([]string, 0, 4),
	}

	// Collect raw 9-byte responses for each chunk.
	type chunkResp struct {
		data []byte
		err  string
	}
	chunks := make([]chunkResp, 0, 4)
	var errs []string
	for qq := byte(0x24); qq <= byte(0x27); qq++ {
		frame := protocol.Frame{
			FrameType: protocol.FrameTypeForTarget(target),
			Source:    source,
			Target:    target,
			Primary:   0xB5,
			Secondary: 0x09,
			Data:      []byte{qq},
		}
		resp, err := explorerSendWithRetry(ctx, es.bus, frame)
		if err != nil {
			msg := fmt.Sprintf("chunk 0x%02x: %v", qq, err)
			errs = append(errs, msg)
			chunks = append(chunks, chunkResp{err: msg})
			result.Chunks = append(result.Chunks, "")
			continue
		}
		if len(resp.Data) == 0 {
			msg := fmt.Sprintf("chunk 0x%02x: empty response", qq)
			errs = append(errs, msg)
			chunks = append(chunks, chunkResp{err: msg})
			result.Chunks = append(result.Chunks, "")
			continue
		}
		result.Chunks = append(result.Chunks, hex.EncodeToString(resp.Data))
		chunks = append(chunks, chunkResp{data: resp.Data})
	}

	// Try status-byte interpretation: Data[0]==0x00 → Data[1:9] are serial bytes.
	statusRaw := make([]byte, 0, 32)
	statusOK := true
	for _, c := range chunks {
		if c.err != "" || len(c.data) != 9 || c.data[0] != 0x00 {
			statusOK = false
			break
		}
		statusRaw = append(statusRaw, c.data[1:]...)
	}
	if statusOK {
		serial := formatScanIDSerial(string(trimScanIDPadding(statusRaw)))
		if serial != "" {
			result.Serial = serial
			return result
		}
	}

	// Fallback: no status byte — all 9 bytes per chunk are serial data.
	fullRaw := make([]byte, 0, 36)
	for _, c := range chunks {
		if c.err != "" {
			fullRaw = append(fullRaw, make([]byte, 9)...)
			continue
		}
		fullRaw = append(fullRaw, c.data...)
	}
	trimmed := trimScanIDPadding(fullRaw)
	serial := formatScanIDSerial(string(trimmed))
	if serial != "" {
		result.Serial = serial
	}
	if len(errs) > 0 {
		result.Error = strings.Join(errs, "; ")
	}
	return result
}

// trimScanIDPadding strips leading and trailing NUL/space/0xFF bytes.
func trimScanIDPadding(data []byte) []byte {
	start := 0
	for start < len(data) {
		b := data[start]
		if b == 0x00 || b == 0x20 || b == 0xFF {
			start++
			continue
		}
		break
	}
	end := len(data)
	for end > start {
		b := data[end-1]
		if b == 0x00 || b == 0x20 || b == 0xFF {
			end--
			continue
		}
		break
	}
	return data[start:end]
}

// formatScanIDSerial formats a raw Vaillant serial string.
// Mirrors formatVaillantSerial from ebusreg/registry/scan.go.
func formatScanIDSerial(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if len(raw) < 28 {
		return raw
	}
	candidate := raw[:28]
	for i := 0; i < 26; i++ {
		if candidate[i] < '0' || candidate[i] > '9' {
			return raw
		}
	}
	return fmt.Sprintf(
		"%s-%s-%s-%s-%s-%s-%s",
		candidate[0:2],
		candidate[2:4],
		candidate[4:6],
		candidate[6:16],
		candidate[16:20],
		candidate[20:26],
		candidate[26:28],
	)
}

// --- HTTP Handlers ---

func (h *handler) handleExplorerStartScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}

	var req ExplorerScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Target == 0 {
		http.Error(w, "target address required", http.StatusBadRequest)
		return
	}

	var err error
	switch req.Kind {
	case ExplorerKindB524, "":
		req.Kind = ExplorerKindB524
		err = h.explorer.startB524Scan(req)
	case ExplorerKindB509:
		err = h.explorer.startB509Scan(req)
	default:
		http.Error(w, "unknown scan kind: "+req.Kind, http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started"})
}

func (h *handler) handleExplorerGetScan(w http.ResponseWriter, r *http.Request) {
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, h.explorer.getState())
}

func (h *handler) handleExplorerCancelScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", http.MethodDelete)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	if h.explorer.cancelScan() {
		writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
	} else {
		writeJSON(w, http.StatusOK, map[string]string{"status": "no_active_scan"})
	}
}

func (h *handler) handleExplorerGetResults(w http.ResponseWriter, r *http.Request) {
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	results := h.explorer.getResults(offset, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"offset":  offset,
		"limit":   limit,
		"count":   len(results),
		"results": results,
	})
}

func (h *handler) handleExplorerStream(w http.ResponseWriter, r *http.Request) {
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	subID, ch := h.explorer.subscribe()
	defer h.explorer.unsubscribe(subID)

	ctx := r.Context()

	// Send initial state and exit early if already terminal.
	state := h.explorer.getState()
	data, _ := json.Marshal(state)
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return
	}
	flusher.Flush()

	switch state.Phase {
	case ExplorerPhaseDone, ExplorerPhaseCancelled, ExplorerPhaseError, ExplorerPhaseIdle:
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			state := h.explorer.getState()
			data, _ := json.Marshal(state)
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()

			// Stop streaming once scan is terminal.
			switch state.Phase {
			case ExplorerPhaseDone, ExplorerPhaseCancelled, ExplorerPhaseError:
				return
			}
		}
	}
}

func (h *handler) handleExplorerSingleB524(w http.ResponseWriter, r *http.Request) {
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	target, err := parseHexByte(q.Get("target"))
	if err != nil {
		http.Error(w, "invalid target", http.StatusBadRequest)
		return
	}
	source, _ := parseHexByte(q.Get("source"))
	opcode, _ := parseHexByte(q.Get("opcode"))
	group, err := parseHexByte(q.Get("group"))
	if err != nil {
		http.Error(w, "invalid group", http.StatusBadRequest)
		return
	}
	instance, _ := parseHexByte(q.Get("instance"))
	addr, err := parseHexUint16(q.Get("addr"))
	if err != nil {
		http.Error(w, "invalid addr", http.StatusBadRequest)
		return
	}
	result := h.explorer.singleB524Read(r.Context(), target, source, opcode, group, instance, addr)
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) handleExplorerSingleB509(w http.ResponseWriter, r *http.Request) {
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	target, err := parseHexByte(q.Get("target"))
	if err != nil {
		http.Error(w, "invalid target", http.StatusBadRequest)
		return
	}
	source, _ := parseHexByte(q.Get("source"))
	opcode, _ := parseHexByte(q.Get("opcode"))
	addr, err := parseHexUint16(q.Get("addr"))
	if err != nil {
		http.Error(w, "invalid addr", http.StatusBadRequest)
		return
	}
	result := h.explorer.singleB509Read(r.Context(), target, source, addr, opcode)
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) handleExplorerScanID(w http.ResponseWriter, r *http.Request) {
	if h.explorer == nil {
		http.Error(w, "explorer not available", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	target, err := parseHexByte(q.Get("target"))
	if err != nil {
		http.Error(w, "target address required", http.StatusBadRequest)
		return
	}
	source, _ := parseHexByte(q.Get("source"))
	result := h.explorer.readScanID(r.Context(), target, source)
	writeJSON(w, http.StatusOK, result)
}

// routeExplorer dispatches explorer API requests. Returns true if handled.
func (h *handler) routeExplorer(w http.ResponseWriter, r *http.Request, path string) bool {
	sub, ok := strings.CutPrefix(path, "explorer/")
	if !ok {
		return false
	}
	switch sub {
	case "scans":
		if r.Method == http.MethodPost {
			h.handleExplorerStartScan(w, r)
		} else {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "scans/current":
		switch r.Method {
		case http.MethodGet:
			h.handleExplorerGetScan(w, r)
		case http.MethodDelete:
			h.handleExplorerCancelScan(w, r)
		default:
			w.Header().Set("Allow", strings.Join([]string{http.MethodGet, http.MethodDelete}, ", "))
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "scans/current/results", "scans/current/stream", "read/b524", "read/b509", "read/scanid":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return true
		}
		switch sub {
		case "scans/current/results":
			h.handleExplorerGetResults(w, r)
		case "scans/current/stream":
			h.handleExplorerStream(w, r)
		case "read/b524":
			h.handleExplorerSingleB524(w, r)
		case "read/b509":
			h.handleExplorerSingleB509(w, r)
		case "read/scanid":
			h.handleExplorerScanID(w, r)
		}
	default:
		http.NotFound(w, r)
	}
	return true
}

// --- Helpers ---

const (
	explorerReadTimeout  = 5 * time.Second
	explorerMaxAttempts  = 3
	explorerRetryBackoff = 75 * time.Millisecond
)

// explorerSendWithRetry sends a frame on the bus with retry logic.
// Mirrors the retry pattern from semantic_vaillant.go readB524Value.
func explorerSendWithRetry(ctx context.Context, bus ExplorerBus, frame protocol.Frame) (*protocol.Frame, error) {
	var lastErr error
	for attempt := 0; attempt < explorerMaxAttempts; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, explorerReadTimeout)
		resp, err := bus.Send(reqCtx, frame)
		cancel()

		if err != nil {
			lastErr = err
		} else if resp == nil {
			lastErr = fmt.Errorf("empty response")
		} else {
			return resp, nil
		}

		if attempt < explorerMaxAttempts-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(explorerRetryBackoff):
			}
		}
	}
	return nil, lastErr
}

func parseHexByte(s string) (byte, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	v, err := strconv.ParseUint(s, 16, 8)
	return byte(v), err
}

func parseHexUint16(s string) (uint16, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	v, err := strconv.ParseUint(s, 16, 16)
	return uint16(v), err
}
