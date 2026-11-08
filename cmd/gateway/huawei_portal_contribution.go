package main

import (
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
)

// huaweiPortalNativeContract is the typed native contract that the Huawei
// instance contribution's diagnostics reference: the managed instance status
// served by the Huawei instance MCP tools. A member is a JSON path of it.
var huaweiPortalNativeContract = contributionv1.NativeContractRef{Owner: "helianthus-ebusgateway", Contract: "huawei-instance-status", Version: "1.0.0"}

// huaweiPortalManifestID is the per-instance contribution. Its owner is the
// instance driver, so every instance contributes under its own identity.
const huaweiPortalManifestID = "portal.huawei.instance"

// huaweiPortalDiagnostics is the closed member set: gateway identity class,
// child routing (routed unit and class), freshness, withheld reasons and
// asset IDs. Every member is a field of the instance status; none of them
// describes decoding, a register or a route.
var huaweiPortalDiagnostics = []struct{ id, member, label string }{
	{"gateway_family", "family", "Gateway identity class"},
	{"gateway_qualified", "qualified", "Gateway identified"},
	{"disposition", "disposition", "Cycle disposition"},
	{"child_unit", "children.unit", "Routed unit"},
	{"child_family", "children.family", "Child class"},
	{"child_evidence", "children.evidence", "Child freshness"},
	{"child_reason", "children.reason", "Child withheld reason"},
	{"child_refusal", "children.refusal", "Publication refusal"},
	{"child_asset", "children.asset_id", "Child asset"},
	{"published_assets", "published_asset_ids", "Published assets"},
	{"loss", "loss", "Withheld items"},
}

// huaweiPortalNativeMember admits exactly the Huawei status members, and only
// as typed fields of the Huawei native contract.
func huaweiPortalNativeMember(contract contributionv1.NativeContractRef, kind, member string) bool {
	if contract != huaweiPortalNativeContract || kind != "field" {
		return false
	}
	for _, item := range huaweiPortalDiagnostics {
		if item.member == member {
			return true
		}
	}
	return false
}

// gatewayPortalHuaweiDescriptor is one instance's contribution. It carries
// native diagnostics only: the PV assets the instance publishes are SemReg
// assets and render through the generic semantic surfaces, never through a
// Huawei product view.
func gatewayPortalHuaweiDescriptor(driver string) contributionv1.Manifest {
	const group = "instance"
	diagnostics := make([]contributionv1.Diagnostic, 0, len(huaweiPortalDiagnostics))
	ids := make([]string, 0, len(huaweiPortalDiagnostics))
	for order, item := range huaweiPortalDiagnostics {
		diagnostics = append(diagnostics, contributionv1.Diagnostic{ID: item.id, Group: group, Label: contributionv1.Label{Key: huaweiPortalManifestID + "." + item.id, Default: item.label}, MemberID: item.member, Kind: "field", Order: int32(order)})
		ids = append(ids, item.id)
	}
	return contributionv1.Manifest{Contract: contributionv1.Contract, ManifestID: huaweiPortalManifestID, ManifestVersion: "1.0.0",
		Contributor: contributionv1.Contributor{DriverID: driver, NativeContract: huaweiPortalNativeContract},
		Requires:    contributionv1.Requirements{SemanticKernel: contributionv1.SemanticKernel, Packs: []contributionv1.PackRef{catalogv1.FivePacks[1]}},
		Groups:      []contributionv1.Group{{ID: group, Label: contributionv1.Label{Key: huaweiPortalManifestID + ".group", Default: "Instance"}, ResourceContext: group}},
		Fields:      []contributionv1.Field{},
		Views:       []contributionv1.View{{ID: huaweiPortalManifestID + ".routing", Group: group, Label: contributionv1.Label{Key: huaweiPortalManifestID + ".routing", Default: "Routing"}, Renderer: "evidence_table", Slot: "native_diagnostics", FieldIDs: []string{}, DiagnosticIDs: ids}},
		Actions:     []contributionv1.Action{}, Diagnostics: diagnostics}
}

// huaweiPortalLease publishes one work generation's contribution and
// withdraws it exactly once. A withdrawal that precedes publication closes the
// lease, so a late Start can never reopen a stopped generation.
type huaweiPortalLease struct {
	mu            sync.Mutex
	contributions *gatewayPortalCatalogContributions
	owner         contributionv1.DriverGeneration
	lease         *gatewayPortalContributionLifecycle
	withdrawn     bool
}

func (l *huaweiPortalLease) publish(adapter *modbusadapter.Adapter) error {
	return l.publishActive(adapter != nil && adapter.PublicationActive())
}

// publishActive admits the contribution only while the generation's
// publication scope is active.
func (l *huaweiPortalLease) publishActive(active bool) error {
	if l == nil || l.contributions == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.withdrawn || l.lease != nil || !active {
		return nil
	}
	lease, err := startGatewayPortalContributionLifecycle(l.contributions, l.owner, []contributionv1.Manifest{gatewayPortalHuaweiDescriptor(l.owner.DriverID)})
	if err != nil {
		return err
	}
	l.lease = lease
	return nil
}

func (l *huaweiPortalLease) withdraw() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.withdrawn = true
	if l.lease != nil {
		l.lease.Stop()
		l.lease = nil
	}
}
