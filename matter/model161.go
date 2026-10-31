package matter

import (
	"encoding/json"
	"reflect"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const (
	TargetVersion161            semreg.VersionLabel = "1.6.1"
	MappingRevision161          semreg.Uint64       = "1"
	DocsSemanticContractPath161                     = "api/v1/targets/matter-1.6.1-v1.json"
)

// Model161 identifies the inspected model composition, independently of the
// historical target. Revisions describe definitions, not runtime qualification.
type Model161 struct {
	ElectricalSensorRevision           uint32 `json:"electrical_sensor_revision"`
	PowerTopologyRevision              uint32 `json:"power_topology_revision"`
	ElectricalPowerMeasurementRevision uint32 `json:"electrical_power_measurement_revision"`
}

func model161() Model161 {
	return Model161{ElectricalSensorRevision: 1, PowerTopologyRevision: 1, ElectricalPowerMeasurementRevision: 3}
}

// Document161 is explicitly selected adoption-target data, with no endpoint or I/O.
type Document161 struct {
	Contract        string              `json:"contract"`
	TargetID        semreg.TargetID     `json:"target_id"`
	TargetVersion   semreg.VersionLabel `json:"target_version"`
	MappingRevision semreg.Uint64       `json:"mapping_revision"`
	Model           Model161            `json:"model"`
	Attributes      []TargetAttribute   `json:"attributes"`
}

type Result161 struct {
	Document Document161                 `json:"document"`
	Report   projection.ProjectionReport `json:"report"`
}

// Manifest161 uses the complete existing five-pack roster for the adopted model.
func Manifest161() projection.ProjectionManifest {
	manifest := Manifest()
	manifest.TargetVersion = TargetVersion161
	manifest.MappingRevision = MappingRevision161
	return manifest
}

// Project161 reuses the historical projector's validated correspondence,
// qualification, single-source policy and exact conversion. Accounting is built
// afresh against the adopted target manifest; the HTTP feed remains historical.
func Project161(snapshot semreg.Snapshot, evaluation semreg.EvaluationView) (Result161, error) {
	validated, err := Project(snapshot, evaluation)
	if err != nil {
		return Result161{}, err
	}
	report, err := projection.Project(snapshot, Manifest161(), validated.Report.Requested, validated.Report.Dispositions, nil)
	if err != nil {
		return Result161{}, err
	}
	result := Result161{Document: Document161{
		Contract: Contract, TargetID: TargetID, TargetVersion: TargetVersion161,
		MappingRevision: MappingRevision161, Model: model161(), Attributes: validated.Document.Attributes,
	}, Report: report}
	raw, err := CanonicalJSON161(result)
	if err != nil {
		return Result161{}, err
	}
	var detached Result161
	if err := json.Unmarshal(raw, &detached); err != nil {
		return Result161{}, err
	}
	return detached, nil
}

// CanonicalJSON161 refuses historical identities, invalid current values and
// disagreement between the document and its accounting report.
func CanonicalJSON161(result Result161) ([]byte, error) {
	d := result.Document
	if d.Contract != Contract || d.TargetID != TargetID || d.TargetVersion != TargetVersion161 || d.MappingRevision != MappingRevision161 || d.Model != model161() || !reflect.DeepEqual(result.Report.Manifest, Manifest161()) {
		return nil, errInput
	}
	if err := result.Report.Validate(); err != nil {
		return nil, err
	}
	if d.Attributes == nil || len(d.Attributes) > 1 {
		return nil, errInput
	}
	requestedLedger, ledger := Ledger()
	if !reflect.DeepEqual(result.Report.Requested, requestedLedger) || len(result.Report.Dispositions) != len(ledger) {
		return nil, errInput
	}
	expected := make(map[projection.RequestedItem]projection.ProjectionDisposition, len(ledger))
	for _, row := range ledger {
		expected[projection.RequestedItem{Kind: row.Kind, ItemID: row.ItemID}] = row
	}
	currentMapped := false
	for _, row := range result.Report.Dispositions {
		if row.Kind != projection.ItemFact || row.ItemID != "evse.ac.current" {
			// The current row is the contract's sole positive mapping; every
			// other row must equal its unmapped ledger entry.
			want, ok := expected[projection.RequestedItem{Kind: row.Kind, ItemID: row.ItemID}]
			if !ok || !reflect.DeepEqual(row, want) {
				return nil, errInput
			}
			continue
		}
		switch row.Outcome {
		case projection.ProjectionTransformed:
			// The ampere-to-milliampere mapping is always a transformation; it
			// must carry its single source and irreversible unit loss.
			if !currentTransformedRow(row) {
				return nil, errInput
			}
			currentMapped = true
		case projection.ProjectionUnknown:
			if !reflect.DeepEqual(row, expected[projection.RequestedItem{Kind: row.Kind, ItemID: row.ItemID}]) {
				return nil, errInput
			}
		default:
			// An exact outcome contradicts the projector, with or without an attribute.
			return nil, errInput
		}
	}
	if currentMapped != (len(d.Attributes) == 1) {
		return nil, errInput
	}
	for _, attribute := range d.Attributes {
		if attribute.Element != (TargetElement{DeviceType: 0x0510, Cluster: 0x0090, Kind: ElementAttribute, ElementID: 0x0005}) {
			return nil, errInput
		}
		// Interpret the stored integer as amperes scaled by 10^-3 so the
		// projector's exact conversion and signed target limits apply here too.
		ma, err := amperesToMilliampere(semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{
			Number: semreg.Decimal{Coefficient: attribute.Value.Milliampere, Exponent10: -3}, Unit: "unit.ampere",
		}})
		if err != nil || ma != attribute.Value.Milliampere {
			return nil, errInput
		}
	}
	return json.Marshal(result)
}

func currentTransformedRow(row projection.ProjectionDisposition) bool {
	if row.Reason == nil || *row.Reason != "matter.phase_endpoint_identity_loss.v1" || len(row.SourceKeys) != 1 {
		return false
	}
	key := row.SourceKeys[0]
	if key.PackID != "helianthus.pack.evse" || key.PackVersion != "1.0.0" || key.FactID != "evse.ac.current" {
		return false
	}
	// The row retains the key but not the value, so a zero-ampere value makes the
	// pack validator the projector uses judge only the key's dimensions.
	zero := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "0"}, Unit: "unit.ampere"}}
	if evse.New().ValidateFact(key, &zero) != nil {
		return false
	}
	return reflect.DeepEqual(row.Loss, []projection.LossDetail{{
		Kind:        projection.LossUnit,
		SourceItems: []semreg.DefinitionID{"evse.ac.current"},
		Description: "amperes are represented as integral milliamperes only when exact and representable",
		Reversible:  false,
	}})
}
