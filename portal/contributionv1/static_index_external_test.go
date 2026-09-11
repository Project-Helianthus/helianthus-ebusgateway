package contributionv1_test

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
)

func TestStaticIndexExternalBuilderValidatesManifest(t *testing.T) {
	pack := contributionv1.PackRef{ID: "helianthus.pack.test", Version: "1.0.0"}
	ref := func(id string) contributionv1.DefinitionRef {
		return contributionv1.DefinitionRef{Pack: pack, ID: id, Version: "1.0.0"}
	}
	field, service, capability := ref("field"), ref("service"), ref("capability")
	unit, operation, argument, effect := ref("unit"), ref("operation"), ref("argument"), ref("effect")
	contract := contributionv1.NativeContractRef{Owner: "helianthus", Contract: "test-native", Version: "1.0.0"}

	index := contributionv1.NewStaticIndex()
	index.AddPack(pack)
	for _, definition := range []contributionv1.DefinitionRef{field, service, capability, unit, operation, argument, effect} {
		index.AddDefinition(definition)
	}
	index.AddCanonicalUnit(field, unit)
	index.AddServiceCapability(service, capability)
	index.AddFieldRelation(field, service, capability)
	index.AddOperation(operation, capability, service, argument, effect)
	index.AddNativeMember(contract, "field", "native-field")

	manifest := contributionv1.Manifest{
		Contract: "helianthus.gateway.portal-contribution/v1", ManifestID: "portal.test", ManifestVersion: "1.0.0",
		Contributor: contributionv1.Contributor{DriverID: "test.driver", NativeContract: contract},
		Requires:    contributionv1.Requirements{SemanticKernel: "helianthus.semantic.kernel/v1", Packs: []contributionv1.PackRef{pack}},
		Groups:      []contributionv1.Group{{ID: "group", Label: contributionv1.Label{Key: "group", Default: "Group"}, ResourceContext: "resource", Order: 1}},
		Fields:      []contributionv1.Field{{ID: "field", Group: "group", Label: contributionv1.Label{Key: "field", Default: "Field"}, Ref: field, ServiceRef: service, CapabilityRef: capability, UnitRef: unit, Order: 1}},
		Views:       []contributionv1.View{{ID: "view", Group: "group", Label: contributionv1.Label{Key: "view", Default: "View"}, Renderer: "summary", Slot: "lens", FieldIDs: []string{"field"}, DiagnosticIDs: []string{"diagnostic"}, Order: 1}},
		Actions:     []contributionv1.Action{{ID: "action", Group: "group", Label: contributionv1.Label{Key: "action", Default: "Action"}, OperationRef: operation, CapabilityRef: capability, ServiceRef: service, ArgumentRef: argument, EffectRef: effect, Order: 1}},
		Diagnostics: []contributionv1.Diagnostic{{ID: "diagnostic", Group: "group", Label: contributionv1.Label{Key: "diagnostic", Default: "Diagnostic"}, MemberID: "native-field", Kind: "field", Order: 1}},
	}
	if err := contributionv1.Validate(manifest, index); err != nil {
		t.Fatalf("external StaticIndex builder did not validate manifest: %v", err)
	}
}

func TestStaticIndexExternalZeroValueAddIsSafe(t *testing.T) {
	pack := contributionv1.PackRef{ID: "helianthus.pack.zero", Version: "1.0.0"}
	var index contributionv1.StaticIndex
	index.AddPack(pack)
	if !index.HasPack(pack) {
		t.Fatal("zero-value StaticIndex did not retain added pack")
	}
}
