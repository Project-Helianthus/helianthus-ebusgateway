package semgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// AnchorSource names where an anchor comes from.
type AnchorSource string

// The three admitted anchor sources.
const (
	SourceNativeToken    AnchorSource = "native_token"
	SourceNativePosition AnchorSource = "native_position"
	SourceOperatorLabel  AnchorSource = "operator_label"
)

// Sources that are never an anchor. Each is refused with
// ErrRefusedAnchorSource; any other unknown source is refused with
// ErrInvalidAnchor.
const (
	SourceBusAddress             AnchorSource = "bus_address"
	SourceNetworkAddress         AnchorSource = "network_address"
	SourceRoutedUnit             AnchorSource = "routed_unit"
	SourceEntityAddressPath      AnchorSource = "entity_address_path"
	SourceCredential             AnchorSource = "credential"
	SourceCertificateFingerprint AnchorSource = "certificate_fingerprint"
	SourcePackClass              AnchorSource = "pack_class"
	SourceProfileSelector        AnchorSource = "profile_selector"
	SourceDisplayName            AnchorSource = "display_name"
	SourceNonLocalIndex          AnchorSource = "non_local_index"
)

var refusedSources = map[AnchorSource]struct{}{
	SourceBusAddress: {}, SourceNetworkAddress: {}, SourceRoutedUnit: {},
	SourceEntityAddressPath: {}, SourceCredential: {}, SourceCertificateFingerprint: {},
	SourcePackClass: {}, SourceProfileSelector: {}, SourceDisplayName: {},
	SourceNonLocalIndex: {},
}

// sourceBasis is the one basis each admitted source establishes.
var sourceBasis = map[AnchorSource]semreg.AnchorBasis{
	SourceNativeToken:    semreg.AnchorNativeIdentity,
	SourceNativePosition: semreg.AnchorNativeSlot,
	SourceOperatorLabel:  semreg.AnchorOperatorSlot,
}

// Errors of anchor validation.
var (
	ErrInvalidAnchor       = errors.New("semgraph: invalid anchor")
	ErrRefusedAnchorSource = errors.New("semgraph: refused anchor source")
)

// issuerCodeRE admits a registry code, never a name: no spaces, a bounded
// alphabet and length.
var issuerCodeRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

const maxTokenBytes = 256

// Anchor is the identity basis a publisher supplies for one resource. Scheme,
// Issuer and Token are set exactly for a native token: Scheme names the
// identification scheme, Issuer is the code of the scheme's issuer registry
// (empty for a globally unique scheme) and Token is the token in the scheme's
// canonical form. The token stays native evidence; only its digest leaves the
// gateway. Parent is empty when the anchor has none.
type Anchor struct {
	Class    semreg.AnchorClass
	Basis    semreg.AnchorBasis
	Source   AnchorSource
	Scheme   semreg.DefinitionID
	Issuer   string
	Token    string
	Parent   semreg.ResourceID
	Slot     string
	Evidence []semreg.EvidenceRef
}

// AnchorKey is the anchor key of a resource: (kind, key) for a native
// identity and (kind, parent, slot) otherwise. It is comparable.
type AnchorKey struct {
	Kind   semreg.DefinitionID
	Key    semreg.Digest
	Parent semreg.ResourceID
	Slot   string
}

// Validate refuses an anchor whose source is not one of the three admitted
// sources, whose basis is not the one its source establishes, whose class
// lacks the parent or slot it needs, or whose record SemReg would refuse.
func (a Anchor) Validate() error {
	if _, refused := refusedSources[a.Source]; refused {
		return fmt.Errorf("%w: %s", ErrRefusedAnchorSource, a.Source)
	}
	basis, admitted := sourceBasis[a.Source]
	if !admitted {
		return fmt.Errorf("%w: unknown anchor source", ErrInvalidAnchor)
	}
	if a.Basis != basis {
		return fmt.Errorf("%w: source %s establishes basis %s", ErrInvalidAnchor, a.Source, basis)
	}
	native := a.Basis == semreg.AnchorNativeIdentity
	if native {
		if err := a.Scheme.Validate(); err != nil {
			return fmt.Errorf("%w: scheme", ErrInvalidAnchor)
		}
		if a.Issuer != "" && !issuerCodeRE.MatchString(a.Issuer) {
			return fmt.Errorf("%w: issuer is not a code", ErrInvalidAnchor)
		}
		if !validToken(a.Token) {
			return fmt.Errorf("%w: token", ErrInvalidAnchor)
		}
	} else if a.Scheme != "" || a.Issuer != "" || a.Token != "" {
		return fmt.Errorf("%w: scheme, issuer and token belong to a native token", ErrInvalidAnchor)
	}
	switch a.Class {
	case semreg.AnchorComponent, semreg.AnchorEpisode:
		if a.Parent == "" || a.Slot == "" {
			return fmt.Errorf("%w: %s anchor needs a parent and a slot", ErrInvalidAnchor, a.Class)
		}
	case semreg.AnchorFunction:
		if a.Slot == "" {
			return fmt.Errorf("%w: function anchor needs a slot", ErrInvalidAnchor)
		}
	case semreg.AnchorEquipment:
		if !native && a.Slot == "" {
			return fmt.Errorf("%w: equipment anchor needs a slot", ErrInvalidAnchor)
		}
	default:
		return fmt.Errorf("%w: class", ErrInvalidAnchor)
	}
	record, err := a.record()
	if err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAnchor, err)
	}
	return nil
}

// validToken admits 1 through 256 bytes of valid UTF-8 without control
// characters or surrounding white space.
func validToken(token string) bool {
	if token == "" || len(token) > maxTokenBytes || !utf8.ValidString(token) || strings.TrimSpace(token) != token {
		return false
	}
	return strings.IndexFunc(token, unicode.IsControl) < 0
}

// Record returns the SemReg form of a valid anchor. The key replaces the
// issuer and token; the scheme stays in clear.
func (a Anchor) Record() (semreg.ResourceAnchor, error) {
	if err := a.Validate(); err != nil {
		return semreg.ResourceAnchor{}, err
	}
	return a.record()
}

func (a Anchor) record() (semreg.ResourceAnchor, error) {
	record := semreg.ResourceAnchor{Class: a.Class, Basis: a.Basis}
	if a.Evidence != nil {
		record.Evidence = append(make([]semreg.EvidenceRef, 0, len(a.Evidence)), a.Evidence...)
	}
	if a.Basis == semreg.AnchorNativeIdentity {
		key, err := IdentityKey(a.Scheme, a.Issuer, a.Token)
		if err != nil {
			return semreg.ResourceAnchor{}, err
		}
		scheme := string(a.Scheme)
		record.Scheme, record.Key = &scheme, &key
	}
	if a.Parent != "" {
		parent := a.Parent
		record.Parent = &parent
	}
	if a.Slot != "" {
		slot := a.Slot
		record.Slot = &slot
	}
	return record, nil
}

// Key returns the anchor key of a valid anchor under kind.
func (a Anchor) Key(kind semreg.DefinitionID) (AnchorKey, error) {
	if err := kind.Validate(); err != nil {
		return AnchorKey{}, fmt.Errorf("%w: kind", ErrInvalidAnchor)
	}
	if err := a.Validate(); err != nil {
		return AnchorKey{}, err
	}
	if a.Basis == semreg.AnchorNativeIdentity {
		key, err := IdentityKey(a.Scheme, a.Issuer, a.Token)
		if err != nil {
			return AnchorKey{}, err
		}
		return AnchorKey{Kind: kind, Key: key}, nil
	}
	return AnchorKey{Kind: kind, Parent: a.Parent, Slot: a.Slot}, nil
}

// identityObject is the object a native key digests.
type identityObject struct {
	Scheme string `json:"scheme"`
	Issuer string `json:"issuer"`
	Token  string `json:"token"`
}

func (o identityObject) Validate() error {
	if semreg.DefinitionID(o.Scheme).Validate() != nil || (o.Issuer != "" && !issuerCodeRE.MatchString(o.Issuer)) || !validToken(o.Token) {
		return ErrInvalidAnchor
	}
	return nil
}

// IdentityKey is "sha256:" over the JCS bytes of the identity object
// {scheme, issuer, token}.
func IdentityKey(scheme semreg.DefinitionID, issuer, token string) (semreg.Digest, error) {
	canonical, err := semreg.CanonicalJSON(identityObject{Scheme: string(scheme), Issuer: issuer, Token: token})
	if err != nil {
		return "", fmt.Errorf("%w: identity object", ErrInvalidAnchor)
	}
	sum := sha256.Sum256(canonical)
	return semreg.Digest("sha256:" + hex.EncodeToString(sum[:])), nil
}
