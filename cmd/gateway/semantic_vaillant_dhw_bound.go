package main

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// Controller identity for profile-scoped B524 bounds.
//
// Identity-matching choice (issue #1027): the ebusreg lookup matches the whole
// identity exactly (EID, raw software, raw hardware, API version, API revision).
// B509 identification only supplies EID/SW/HW, so the API qualifier is read
// from B524 OP00 ReadSystemInformation identifiers 0006h (api_version) and
// 0007h (api_revision) of the same controller. Both must be read and be
// integral; anything else leaves the qualifier empty, and an empty qualifier
// never matches a profile that recorded one, so the identity falls into the
// unmatched policy (conservative -20..20). The OP00 reply layout is documented
// as unconfirmed on the wire, so the parser accepts exactly one float32
// little-endian value (4 bytes) and fails closed on every other shape.
const (
	b524OpcodeReadSystemInformation = byte(0x00)
	b524SystemInfoAPIVersionID      = uint16(0x0006)
	b524SystemInfoAPIRevisionID     = uint16(0x0007)

	// controllerAPIRetryAfter bounds how often a failed OP00 read is retried,
	// so an unanswering controller does not add latency to every poll.
	controllerAPIRetryAfter = 15 * time.Minute
)

// controllerAPIIdentityCache holds the OP00 API qualifier read for one exact
// (controller address, EID, SW, HW) key. Guarded by vaillantSemanticPoller.mu.
type controllerAPIIdentityCache struct {
	key         string
	version     string
	revision    string
	ok          bool
	attemptedAt time.Time
}

func controllerAPICacheKey(controller byte, id vaillantsystem.B524ControllerIdentity) string {
	return strconv.Itoa(int(controller)) + "/" + id.EID + "/" + id.SoftwareRaw + "/" + id.HardwareRaw
}

// registryControllerIdentity returns the EID/SW/HW the registry holds for the
// B524 controller address, without the API qualifier.
func (p *vaillantSemanticPoller) registryControllerIdentity(controller byte) vaillantsystem.B524ControllerIdentity {
	if p == nil || p.reg == nil || controller == 0 {
		return vaillantsystem.B524ControllerIdentity{}
	}
	var identity vaillantsystem.B524ControllerIdentity
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !ebusgateway.SnapshotContainsAddress(snap, controller) {
			return true
		}
		identity = vaillantsystem.B524ControllerIdentity{
			EID:         strings.ToUpper(strings.TrimSpace(snap.DeviceID)),
			SoftwareRaw: strings.TrimSpace(snap.SoftwareVersion),
			HardwareRaw: strings.TrimSpace(snap.HardwareVersion),
		}
		return false
	})
	return identity
}

// controllerB524Identity returns the exact controller identity known right now:
// registry EID/SW/HW plus the cached OP00 API qualifier when it was read for
// this very (address, EID, SW, HW). It performs no bus I/O.
func (p *vaillantSemanticPoller) controllerB524Identity() vaillantsystem.B524ControllerIdentity {
	if p == nil {
		return vaillantsystem.B524ControllerIdentity{}
	}
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	identity := p.registryControllerIdentity(controller)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.controllerAPI.ok && p.controllerAPI.key == controllerAPICacheKey(controller, identity) {
		identity.APIVersion = p.controllerAPI.version
		identity.Revision = p.controllerAPI.revision
	}
	return identity
}

// ensureControllerAPIIdentity reads the OP00 API version/revision for the
// current controller identity unless a fresh result (or a recent failure) is
// cached. It is a read-only B524 request; it never writes and never blocks the
// caller on failure (the identity then stays unmatched).
func (p *vaillantSemanticPoller) ensureControllerAPIIdentity(ctx context.Context) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return
	}
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	identity := p.registryControllerIdentity(controller)
	// Without EID, SW and HW no profile can match; skip the read.
	if controller == 0 || identity.EID == "" || identity.SoftwareRaw == "" || identity.HardwareRaw == "" {
		return
	}
	key := controllerAPICacheKey(controller, identity)

	p.mu.Lock()
	cached := p.controllerAPI
	p.mu.Unlock()
	if cached.key == key && (cached.ok || time.Since(cached.attemptedAt) < controllerAPIRetryAfter) {
		return
	}

	next := controllerAPIIdentityCache{key: key, attemptedAt: time.Now()}
	version, versionOK := p.readB524SystemInformationInt(ctx, b524SystemInfoAPIVersionID)
	revision, revisionOK := p.readB524SystemInformationInt(ctx, b524SystemInfoAPIRevisionID)
	if versionOK && revisionOK {
		next.ok = true
		next.version = strconv.Itoa(version)
		next.revision = strconv.Itoa(revision)
	}

	p.mu.Lock()
	p.controllerAPI = next
	p.mu.Unlock()
}

// readB524SystemInformationInt performs one B524 OP00 ReadSystemInformation
// request and returns the reply as a non-negative integer. The reply must be
// exactly one float32 little-endian value that is finite, integral and in
// 0..65535; every other shape is rejected.
func (p *vaillantSemanticPoller) readB524SystemInformationInt(ctx context.Context, id uint16) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()
	if target == 0 {
		return 0, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	p.readMu.Lock()
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	response, err := p.sendSemanticFrame(reqCtx, protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   vaillantExtRegisterPrimary,
		Secondary: vaillantExtRegisterSecondary,
		Data:      []byte{b524OpcodeReadSystemInformation, byte(id), byte(id >> 8)},
	})
	cancel()
	p.readMu.Unlock()
	if err != nil || response == nil {
		return 0, false
	}
	return parseB524SystemInformationInt(response.Data)
}

func parseB524SystemInformationInt(payload []byte) (int, bool) {
	if len(payload) != 4 {
		return 0, false
	}
	value := float64(math.Float32frombits(uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24))
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 65535 || value != math.Trunc(value) {
		return 0, false
	}
	return int(value), true
}

func configBoundEquals(a, b *graphql.ConfigBound) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func mcpConfigBound(bound *graphql.ConfigBound) *mcp.ConfigBound {
	if bound == nil {
		return nil
	}
	return &mcp.ConfigBound{
		MinC:    bound.MinC,
		MaxC:    bound.MaxC,
		Source:  bound.Source,
		Reason:  bound.Reason,
		Profile: bound.Profile,
	}
}
