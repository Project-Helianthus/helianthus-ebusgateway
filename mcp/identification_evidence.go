package mcp

import (
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

const toolBusIdentificationEvidenceListName = "ebus.v1.bus.identification_evidence.list"

const (
	identificationEvidenceClassRaw      = "raw"
	identificationEvidenceQualification = "none"
	identificationEvidenceNotice        = "Raw bytes of the directed 07 04 identification exchange as seen by the gateway bus observer. " +
		"request_hex is QQ ZZ PB SB NN [data]; response_hex is NN [data]; CRC and ACK bytes are not retained. " +
		"Nothing is decoded, normalised or qualified."
)

// IdentificationEvidenceExchange is one raw 07 04 exchange.
type IdentificationEvidenceExchange struct {
	Address    uint8
	Sequence   uint64
	SessionID  string
	CapturedAt time.Time
	Request    []byte
	Response   []byte
}

// IdentificationEvidenceSnapshot is the provider-side retained evidence.
type IdentificationEvidenceSnapshot struct {
	SessionID string
	Exchanges []IdentificationEvidenceExchange
}

// IdentificationEvidenceProvider supplies retained raw 07 04 evidence.
type IdentificationEvidenceProvider interface {
	IdentificationEvidence() IdentificationEvidenceSnapshot
}

// IdentificationEvidenceItem is the wire form of one exchange.
type IdentificationEvidenceItem struct {
	Address     uint8  `json:"address"`
	Sequence    uint64 `json:"sequence"`
	SessionID   string `json:"session_id"`
	CapturedAt  string `json:"captured_at"`
	RequestHex  string `json:"request_hex"`
	ResponseHex string `json:"response_hex"`
}

// IdentificationEvidenceList is the data payload of the list tool.
type IdentificationEvidenceList struct {
	EvidenceClass string                       `json:"evidence_class"`
	Decoded       bool                         `json:"decoded"`
	Qualification string                       `json:"qualification"`
	Notice        string                       `json:"notice"`
	SessionID     string                       `json:"session_id"`
	Count         int                          `json:"count"`
	Items         []IdentificationEvidenceItem `json:"items"`
}

func identificationEvidenceTools() []Tool {
	return []Tool{{
		Name:        toolBusIdentificationEvidenceListName,
		Description: "List retained raw request and response bytes of the directed 07 04 identification exchange per scanned address. Read-only raw evidence: no decoding, no qualification.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"limit": map[string]any{"type": "integer", "minimum": 1},
			},
			"additionalProperties": false,
		},
	}}
}

func (s *Server) SetIdentificationEvidenceProvider(provider IdentificationEvidenceProvider) {
	if s == nil || provider == nil {
		return
	}
	s.identificationEvidence = provider
	if !s.hasToolNamed(toolBusIdentificationEvidenceListName) {
		s.tools = append(s.tools, identificationEvidenceTools()...)
	}
}

func (s *Server) snapshotIdentificationEvidence(limit int) *IdentificationEvidenceList {
	list := &IdentificationEvidenceList{
		EvidenceClass: identificationEvidenceClassRaw,
		Qualification: identificationEvidenceQualification,
		Notice:        identificationEvidenceNotice,
		Items:         []IdentificationEvidenceItem{},
	}
	if s == nil || s.identificationEvidence == nil {
		return list
	}
	snapshot := s.identificationEvidence.IdentificationEvidence()
	list.SessionID = snapshot.SessionID
	exchanges := append([]IdentificationEvidenceExchange(nil), snapshot.Exchanges...)
	list.Count = len(exchanges)
	if limit > 0 && len(exchanges) > limit {
		sort.SliceStable(exchanges, func(i, j int) bool { return exchanges[i].Sequence > exchanges[j].Sequence })
		exchanges = exchanges[:limit]
	}
	sort.SliceStable(exchanges, func(i, j int) bool {
		if exchanges[i].Address != exchanges[j].Address {
			return exchanges[i].Address < exchanges[j].Address
		}
		return exchanges[i].Sequence < exchanges[j].Sequence
	})
	for _, ex := range exchanges {
		list.Items = append(list.Items, IdentificationEvidenceItem{
			Address:     ex.Address,
			Sequence:    ex.Sequence,
			SessionID:   ex.SessionID,
			CapturedAt:  ex.CapturedAt.UTC().Format(time.RFC3339Nano),
			RequestHex:  hex.EncodeToString(ex.Request),
			ResponseHex: hex.EncodeToString(ex.Response),
		})
	}
	return list
}

// rejectUnknownIdentificationEvidenceArgs enforces the advertised
// additionalProperties:false schema: only "limit" is accepted, matching the
// GraphQL query which rejects unknown arguments.
func rejectUnknownIdentificationEvidenceArgs(args map[string]any) error {
	unknown := make([]string, 0)
	for key := range args {
		if key != "limit" {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unknown argument %q: %w", unknown[0], ebuserrors.ErrInvalidPayload)
}
