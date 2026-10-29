package matter

import (
	"encoding/json"
	"reflect"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
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

// CanonicalJSON161 refuses historical identities and document/manifest drift.
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
	for _, attribute := range d.Attributes {
		if attribute.Element != (TargetElement{DeviceType: 0x0510, Cluster: 0x0090, Kind: ElementAttribute, ElementID: 0x0005}) {
			return nil, errInput
		}
	}
	return json.Marshal(result)
}
