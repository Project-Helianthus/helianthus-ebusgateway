package graphql

import (
	"encoding/hex"
	"sort"
	"time"

	graphqlgo "github.com/graphql-go/graphql"
)

const (
	identificationEvidenceClassRaw      = "raw"
	identificationEvidenceQualification = "none"
	identificationEvidenceNotice        = "Raw bytes of the directed 07 04 identification exchange as seen by the gateway bus observer. " +
		"requestHex is QQ ZZ PB SB NN [data]; responseHex is NN [data]; CRC and ACK bytes are not retained. " +
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

type identificationEvidenceItem struct {
	Address     int    `json:"address"`
	Sequence    int    `json:"sequence"`
	SessionID   string `json:"sessionId"`
	CapturedAt  string `json:"capturedAt"`
	RequestHex  string `json:"requestHex"`
	ResponseHex string `json:"responseHex"`
}

type identificationEvidenceList struct {
	EvidenceClass string                       `json:"evidenceClass"`
	Decoded       bool                         `json:"decoded"`
	Qualification string                       `json:"qualification"`
	Notice        string                       `json:"notice"`
	SessionID     string                       `json:"sessionId"`
	Count         int                          `json:"count"`
	Items         []identificationEvidenceItem `json:"items"`
}

func (b *Builder) SetIdentificationEvidenceProvider(provider IdentificationEvidenceProvider) {
	if b == nil || provider == nil {
		return
	}
	b.mu.Lock()
	b.identificationEvidence = provider
	b.mu.Unlock()
}

func (b *Builder) identificationEvidenceProvider() IdentificationEvidenceProvider {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.identificationEvidence
}

func buildIdentificationEvidenceType() *graphqlgo.Object {
	itemType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusIdentificationEvidenceItem",
		Fields: graphqlgo.Fields{
			"address":     &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"sequence":    &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"sessionId":   &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"capturedAt":  &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"requestHex":  &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"responseHex": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
		},
	})
	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusIdentificationEvidence",
		Fields: graphqlgo.Fields{
			"evidenceClass": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"decoded":       &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Boolean)},
			"qualification": &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"notice":        &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"sessionId":     &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.String)},
			"count":         &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.Int)},
			"items":         &graphqlgo.Field{Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(itemType)))},
		},
	})
}

func resolveIdentificationEvidence(builder *Builder, limit int) *identificationEvidenceList {
	list := &identificationEvidenceList{
		EvidenceClass: identificationEvidenceClassRaw,
		Qualification: identificationEvidenceQualification,
		Notice:        identificationEvidenceNotice,
		Items:         []identificationEvidenceItem{},
	}
	provider := builder.identificationEvidenceProvider()
	if provider == nil {
		return list
	}
	snapshot := provider.IdentificationEvidence()
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
		list.Items = append(list.Items, identificationEvidenceItem{
			Address:     int(ex.Address),
			Sequence:    int(ex.Sequence),
			SessionID:   ex.SessionID,
			CapturedAt:  ex.CapturedAt.UTC().Format(time.RFC3339Nano),
			RequestHex:  hex.EncodeToString(ex.Request),
			ResponseHex: hex.EncodeToString(ex.Response),
		})
	}
	return list
}
