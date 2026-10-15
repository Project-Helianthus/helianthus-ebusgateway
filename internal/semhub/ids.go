// Package semhub is the gateway-internal semantic publication hub. It composes
// the SemReg publication kernels of every semantic publisher behind one
// transactional path, one epoch and generation fence and one ordered event
// stream. It is runtime composition only: it owns no semantic meaning, no
// protocol code and no public API surface.
package semhub

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// Kind names one hub-native identifier family.
type Kind string

const (
	KindAsset      Kind = "asset"
	KindSource     Kind = "source"
	KindBinding    Kind = "binding"
	KindService    Kind = "service"
	KindCapability Kind = "capability"
)

const idDomainPrefix = "helianthus.semhub.id.v1/"

var (
	familyRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	opaqueRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]*$`)
)

// ErrInvalidIdentity reports an identifier or identifier material that the
// hub refuses. It never carries the refused material.
var ErrInvalidIdentity = errors.New("semhub: invalid identity")

// ValidateOpaque applies the SemReg opaque identifier rule (1..256 bytes,
// leading alphanumeric, the restricted punctuation set) plus the gateway rule
// that an identifier carries no surrounding whitespace.
func ValidateOpaque(value string) error {
	if value != strings.TrimSpace(value) || len(value) < 1 || len(value) > 256 || !opaqueRE.MatchString(value) {
		return ErrInvalidIdentity
	}
	return nil
}

// NativeID mints a hub-native identifier: "<kind>:<family>:<h>", where h is
// the first 32 lowercase hex characters of sha256 over the domain
// "helianthus.semhub.id.v1/<kind>/<family>" followed by every field, each
// preceded by one 0x00 byte. Field order is fixed per kind: binding takes
// (asset, source, profile, epoch, generation), service and capability take
// (asset, definition, epoch, generation); asset and source take their native
// identity material. A field containing 0x00 is refused.
func NativeID(kind Kind, family string, fields ...string) (string, error) {
	if !familyRE.MatchString(family) {
		return "", ErrInvalidIdentity
	}
	want := map[Kind]int{KindBinding: 5, KindService: 4, KindCapability: 4}
	switch kind {
	case KindAsset, KindSource:
		if len(fields) == 0 {
			return "", ErrInvalidIdentity
		}
	case KindBinding, KindService, KindCapability:
		if len(fields) != want[kind] {
			return "", ErrInvalidIdentity
		}
	default:
		return "", ErrInvalidIdentity
	}
	for _, field := range fields {
		if strings.IndexByte(field, 0) >= 0 {
			return "", ErrInvalidIdentity
		}
	}
	id := string(kind) + ":" + family + ":" + idHash(idDomainPrefix+string(kind)+"/"+family, fields...)
	if err := ValidateOpaque(id); err != nil {
		return "", err
	}
	return id, nil
}

// NativeBindingID mints the per-incarnation binding of a hub-native source.
func NativeBindingID(family string, asset semreg.AssetID, source semreg.SourceID, profile string, epoch semreg.SourceEpochID, generation uint64) (semreg.NativeBindingID, error) {
	id, err := NativeID(KindBinding, family, string(asset), string(source), profile, string(epoch), strconv.FormatUint(generation, 10))
	return semreg.NativeBindingID(id), err
}

func idHash(domain string, fields ...string) string {
	digest := sha256.New()
	digest.Write([]byte(domain))
	for _, field := range fields {
		digest.Write([]byte{0})
		digest.Write([]byte(field))
	}
	return hex.EncodeToString(digest.Sum(nil))[:32]
}

// LegacyFamily is the closed set of publishers whose identifiers predate the
// hub. Their first admission in a hub incarnation reproduces the historical
// identifiers byte for byte.
type LegacyFamily string

const (
	LegacyGrowattStorage LegacyFamily = "growatt_storage"
	LegacyTeslaEVSE      LegacyFamily = "tesla_evse"
	LegacyPVSunSpec      LegacyFamily = "pv_sunspec"
)

func (family LegacyFamily) valid() bool {
	switch family {
	case LegacyGrowattStorage, LegacyTeslaEVSE, LegacyPVSunSpec:
		return true
	}
	return false
}

// Incarnation is one admitted (asset, source, epoch, generation) tuple as seen
// by a legacy minter. First is true only for the first admission of the
// source in this hub incarnation.
type Incarnation struct {
	Asset      semreg.AssetID
	Source     semreg.SourceID
	Epoch      semreg.SourceEpochID
	Generation uint64
	First      bool
}

// LegacyMinter mints the identifiers of one closed-set legacy family.
type LegacyMinter struct{ family LegacyFamily }

// NewLegacyMinter refuses any family outside the closed legacy set.
func NewLegacyMinter(family LegacyFamily) (LegacyMinter, error) {
	if !family.valid() {
		return LegacyMinter{}, fmt.Errorf("%w: unknown legacy family", ErrInvalidIdentity)
	}
	return LegacyMinter{family: family}, nil
}

// Family returns the minter's legacy family.
func (m LegacyMinter) Family() LegacyFamily { return m.family }

// Binding returns the binding identifier. The first admission keeps the
// historical form; any later admission is scoped by epoch and generation,
// because the retained kernel refuses to reuse a fenced or retired binding.
func (m LegacyMinter) Binding(inc Incarnation, profile string) semreg.NativeBindingID {
	generation := strconv.FormatUint(inc.Generation, 10)
	switch m.family {
	case LegacyGrowattStorage:
		if inc.First {
			return semreg.NativeBindingID("binding:semantic-storage:" + legacyHash(string(inc.Asset), string(inc.Source), profile)[:32])
		}
		return semreg.NativeBindingID("binding:semantic-storage:" + legacyHash(string(inc.Asset), string(inc.Source), string(inc.Epoch), generation)[:32])
	case LegacyTeslaEVSE:
		if inc.First {
			return semreg.NativeBindingID("binding:tesla-wc3:" + legacyHash(string(inc.Asset), string(inc.Source))[:32])
		}
		return semreg.NativeBindingID("binding:tesla-wc3:" + legacyHash(string(inc.Asset), string(inc.Source), string(inc.Epoch), generation)[:32])
	case LegacyPVSunSpec:
		return semreg.NativeBindingID("binding:semantic-pv:" + pvHash("pv-binding", string(inc.Asset)+"\x00"+string(inc.Source)+"\x00"+string(inc.Epoch)+"\x00"+generation)[:32])
	}
	return ""
}

// Service returns a service instance identifier. Growatt and PV service
// identifiers already follow their binding; Tesla service identifiers are
// scoped by epoch and generation after the first admission.
func (m LegacyMinter) Service(inc Incarnation, definition string, binding semreg.NativeBindingID) semreg.ServiceInstanceID {
	switch m.family {
	case LegacyGrowattStorage:
		return semreg.ServiceInstanceID("service:semantic-storage:" + legacyHash(string(inc.Asset), string(binding), definition)[:32])
	case LegacyTeslaEVSE:
		return semreg.ServiceInstanceID("service:tesla-wc3:" + m.teslaScoped(inc, definition))
	case LegacyPVSunSpec:
		return semreg.ServiceInstanceID("service:semantic-pv:" + pvHash("pv-service", string(inc.Asset)+"\x00"+string(binding)+"\x00"+definition)[:32])
	}
	return ""
}

// Capability returns a capability instance identifier with the same scoping
// rule as Service.
func (m LegacyMinter) Capability(inc Incarnation, definition string, binding semreg.NativeBindingID) semreg.CapabilityInstanceID {
	switch m.family {
	case LegacyGrowattStorage:
		return semreg.CapabilityInstanceID("capability:semantic-storage:" + legacyHash(string(inc.Asset), string(binding), definition)[:32])
	case LegacyTeslaEVSE:
		return semreg.CapabilityInstanceID("capability:tesla-wc3:" + m.teslaScoped(inc, definition))
	case LegacyPVSunSpec:
		return semreg.CapabilityInstanceID("capability:semantic-pv:" + pvHash("pv-capability", string(inc.Asset)+"\x00"+string(binding)+"\x00"+definition)[:32])
	}
	return ""
}

func (m LegacyMinter) teslaScoped(inc Incarnation, definition string) string {
	if inc.First {
		return legacyHash(string(inc.Asset), definition)[:32]
	}
	return legacyHash(string(inc.Asset), definition, string(inc.Epoch), strconv.FormatUint(inc.Generation, 10))[:32]
}

// PVAssetID is the historical PV asset identifier: a hash of the SunSpec
// Common identity material (manufacturer, model, serial). The material is
// hashed in memory and never retained.
func PVAssetID(identity []byte) semreg.AssetID {
	sum := sha256.Sum256(identity)
	return semreg.AssetID("pv-asset-" + hex.EncodeToString(sum[:])[:32])
}

// PVSourceID is the historical PV source identifier: a hash of the native
// resource tuple (endpoint, transport, unit, table, authorization scope).
func PVSourceID(native []byte) semreg.SourceID {
	return semreg.SourceID("source:semantic-pv:" + pvHash("pv-source", string(native))[:32])
}

// PVEpochID is the historical PV source epoch: a hash of the endpoint and the
// wall instant sampled after the endpoint factory, dial and open.
func PVEpochID(endpoint, startedRFC3339Nano string) semreg.SourceEpochID {
	return semreg.SourceEpochID("source-epoch:semantic-pv:" + pvHash("semantic-pv-source-epoch", endpoint+"\x00"+startedRFC3339Nano)[:32])
}

func legacyHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func pvHash(domain, payload string) string {
	sum := sha256.Sum256([]byte(domain + "\x00" + payload))
	return hex.EncodeToString(sum[:])
}
