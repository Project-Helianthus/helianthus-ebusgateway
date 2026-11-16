#!/usr/bin/env python3
"""Adopt the helianthus-modbusreg vendor, detection and ledger packages.

The library moved its vendor families (Growatt, Tesla, Huawei, SunSpec, Fronius,
OutBack), profile detection and the attempt ledger out of its root package into
one package per owner. No alias is left in the root package, so each gateway
use of a moved name must name the package that declares it now.

This offline, standard-library script rewrites the gateway's Go files and the
listed documentation pages:

* each qualified use of a moved root name (``modbusreg.GrowattAggregateAttempt``)
  becomes the package-qualified current name (``growatt.AggregateAttempt``);
  RENAMED holds one table per moved family (previous name, current name), and
  QUALIFIER_ONLY lists the names that kept their spelling and changed only the
  package that declares them;
* a call of a registry method that became a function of a moved family is
  rewritten to that function with the receiver as the first argument;
* the sub-package imports that the rewritten code needs are added next to the
  root import, and the root import is dropped when no root name is left;
* source-text expectations in tests that quote a moved name are updated;
* backquoted identifiers of moved names in DOC_PAGES are rewritten;
* gofmt formats the rewritten Go files, because shorter qualifiers move the
  alignment columns of neighbouring lines.

Comments are rewritten with the code; string literals are never changed.
``--check`` rewrites nothing and fails when a moved name is left behind, a
replaced method call remains, a modbusreg import is not used by the code, or a
file that imports modbusreg is not gofmt-formatted.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

ROOT_IMPORT = "github.com/Project-Helianthus/helianthus-modbusreg"
DEFAULT_ROOT_NAME = "modbusreg"

# Registry methods that became functions of a moved family. A call
# ``receiver.Method(args)`` becomes ``modbusreg.Function(receiver, args)``;
# the name tables then move ``Function`` into its package.
METHOD_TO_FUNCTION: tuple[tuple[str, str], ...] = (
    ("EvaluateFroniusObservedFlavor", "FroniusEvaluateObservedFlavor"),
    ("EvaluateFroniusObservedFlavorV11", "FroniusEvaluateObservedFlavorV11"),
    ("EvaluateFroniusObservedFlavorV12", "FroniusEvaluateObservedFlavorV12"),
    ("SelectFroniusObservedFlavor", "FroniusSelectObservedFlavor"),
)

# Tests that read production source text and quote a moved call.
# (repository-relative path, previous text, current text)
SOURCE_TEXT_EXPECTATIONS: tuple[tuple[str, str, str], ...] = (
    (
        "internal/modbusadapter/sunspec_producer_test.go",
        '"SelectFroniusObservedFlavor",',
        '"fronius.SelectObservedFlavor",',
    ),
)

# Files where a local identifier already carries a family package name; the
# family package is imported under this name there, so the gateway identifier
# stays unchanged. (repository-relative path, family) -> import name
IMPORT_NAMES: dict[tuple[str, str], str] = {
    ("internal/modbusadapter/adapter_integration_test.go", "ledger"): "modbusledger",
}

# Documentation pages whose backquoted identifiers of moved names are rewritten.
DOC_PAGES: tuple[str, ...] = ("docs/architecture/tesla-activation-observation-run.md",)

SKIPPED_DIRECTORIES = frozenset({"node_modules", "vendor"})

# One table per moved family: (previous root name, current name in the family
# package), as listed in the library changelog.
RENAMED: dict[str, tuple[tuple[str, str], ...]] = {
    "growatt": (
        ("GrowattAggregateAttempt", "AggregateAttempt"),
        ("GrowattAggregateContext", "AggregateContext"),
        ("GrowattAggregateHistoryLimit", "AggregateHistoryLimit"),
        ("GrowattAggregateIdentity", "AggregateIdentity"),
        ("GrowattAggregateOfflineCycle", "AggregateOfflineCycle"),
        ("GrowattAggregateOfflineProvider", "AggregateOfflineProvider"),
        ("GrowattAggregateOfflineReader", "AggregateOfflineReader"),
        ("GrowattAggregateRetention", "AggregateRetention"),
        ("GrowattAggregateSnapshot", "AggregateSnapshot"),
        ("GrowattBMSCellVoltageGroup", "BMSCellVoltageGroup"),
        ("GrowattBMSDiagnosticRawSlice", "BMSDiagnosticRawSlice"),
        ("GrowattBMSMultiGroupDiagnostic", "BMSMultiGroupDiagnostic"),
        ("GrowattBMSMultiGroupDiagnosticInput", "BMSMultiGroupDiagnosticInput"),
        ("GrowattBMSNativeCellState", "BMSNativeCellState"),
        ("GrowattBMSNativeCellStateOff", "BMSNativeCellStateOff"),
        ("GrowattBMSNativeCellStateOn", "BMSNativeCellStateOn"),
        ("GrowattBMSOperatingState", "BMSOperatingState"),
        ("GrowattBMSRS485RTUObserver", "BMSRS485RTUObserver"),
        ("GrowattBMSRS485RTUObserverSession", "BMSRS485RTUObserverSession"),
        ("GrowattBMSReadOnlyInput", "BMSReadOnlyInput"),
        ("GrowattBMSReadOnlyObservation", "BMSReadOnlyObservation"),
        ("GrowattBMSReadOnlySlice", "BMSReadOnlySlice"),
        ("GrowattBMSRevisionTuple", "BMSRevisionTuple"),
        ("GrowattBMSStateCharging", "BMSStateCharging"),
        ("GrowattBMSStateDischarging", "BMSStateDischarging"),
        ("GrowattBMSStateSoftStarting", "BMSStateSoftStarting"),
        ("GrowattBMSStateStandby", "BMSStateStandby"),
        ("GrowattBMSTypedReadOnlyStatus", "BMSTypedReadOnlyStatus"),
        ("GrowattDiagnosticCommitBase", "DiagnosticCommitBase"),
        ("GrowattDiagnosticCommitCandidate", "DiagnosticCommitCandidate"),
        ("GrowattDiagnosticCommitContext", "DiagnosticCommitContext"),
        ("GrowattDiagnosticCommitContextMismatch", "DiagnosticCommitContextMismatch"),
        ("GrowattDiagnosticCommitIdentity", "DiagnosticCommitIdentity"),
        ("GrowattDiagnosticCommitInvalidBase", "DiagnosticCommitInvalidBase"),
        ("GrowattDiagnosticCommitInvalidContext", "DiagnosticCommitInvalidContext"),
        ("GrowattDiagnosticCommitInvalidDiagnostic", "DiagnosticCommitInvalidDiagnostic"),
        ("GrowattDiagnosticCommitLineageChanged", "DiagnosticCommitLineageChanged"),
        ("GrowattDiagnosticCommitNativeIdentity", "DiagnosticCommitNativeIdentity"),
        ("GrowattDiagnosticCommitRejection", "DiagnosticCommitRejection"),
        ("GrowattDiagnosticCommitTopologyChanged", "DiagnosticCommitTopologyChanged"),
        ("GrowattFC04FieldExtent", "FC04FieldExtent"),
        ("GrowattFC04Record", "FC04Record"),
        ("GrowattFC04SymbolicSchema", "FC04SymbolicSchema"),
        ("GrowattProtocolIIFC04Applicability", "ProtocolIIFC04Applicability"),
        ("GrowattProtocolIIFC04RTUObserver", "ProtocolIIFC04RTUObserver"),
        ("GrowattProtocolIIFC04RTUObserverSession", "ProtocolIIFC04RTUObserverSession"),
        ("GrowattProtocolIIFC04Slice", "ProtocolIIFC04Slice"),
        ("GrowattProtocolIIFC04Telemetry", "ProtocolIIFC04Telemetry"),
        ("GrowattProtocolIIIdentityInput", "ProtocolIIIdentityInput"),
        ("GrowattProtocolIIIdentityObservation", "ProtocolIIIdentityObservation"),
        ("GrowattProtocolIIIdentityProfile", "ProtocolIIIdentityProfile"),
        ("GrowattProtocolIIIdentityRTUObserver", "ProtocolIIIdentityRTUObserver"),
        ("GrowattProtocolIIIdentityRTUObserverSession", "ProtocolIIIdentityRTUObserverSession"),
        ("GrowattProtocolIIIdentitySlice", "ProtocolIIIdentitySlice"),
        ("GrowattProtocolIIInverterState", "ProtocolIIInverterState"),
        ("GrowattProtocolIIStateFault", "ProtocolIIStateFault"),
        ("GrowattProtocolIIStateNormal", "ProtocolIIStateNormal"),
        ("GrowattProtocolIIStateWaiting", "ProtocolIIStateWaiting"),
        ("NewGrowattAggregateOfflineProvider", "NewAggregateOfflineProvider"),
        ("NewGrowattAggregateRetention", "NewAggregateRetention"),
        ("NewGrowattBMSRS485RTUObserver", "NewBMSRS485RTUObserver"),
        ("NewGrowattDiagnosticCommitBase", "NewDiagnosticCommitBase"),
        ("NewGrowattProtocolIIFC04RTUObserver", "NewProtocolIIFC04RTUObserver"),
        ("NewGrowattProtocolIIIdentityRTUObserver", "NewProtocolIIIdentityRTUObserver"),
    ),
    "tesla": (
        ("NewTeslaFC100WCVitalsExecutor", "NewFC100WCVitalsExecutor"),
        ("NewTeslaFC100WCVitalsRTUSessionAdapter", "NewFC100WCVitalsRTUSessionAdapter"),
        ("NewTeslaGen3HSCNativeOperationRecord", "NewGen3HSCNativeOperationRecord"),
        ("NewTeslaGen3HSCProfile", "NewGen3HSCProfile"),
        ("NewTeslaGen3InteroperableCurrentLimit", "NewGen3InteroperableCurrentLimit"),
        ("NewTeslaGen3PersistentCurrentLimit", "NewGen3PersistentCurrentLimit"),
        ("NewTeslaGen3ProvisionalCurrentLimit", "NewGen3ProvisionalCurrentLimit"),
        ("NewTeslaGen3ReadAcquisition", "NewGen3ReadAcquisition"),
        ("NewTeslaHSCProfile", "NewHSCProfile"),
        ("NewTeslaHSCProvenance", "NewHSCProvenance"),
        ("NewTeslaHSCSyntacticRequest", "NewHSCSyntacticRequest"),
        ("NewTeslaLegacyFBE0Observation", "NewLegacyFBE0Observation"),
        ("NewTeslaLegacyFDE0Observation", "NewLegacyFDE0Observation"),
        ("NewTeslaLegacyWallConnectorProfile", "NewLegacyWallConnectorProfile"),
        ("NewTeslaTEDAPISemanticRegistry", "NewTEDAPISemanticRegistry"),
        ("TeslaFC100CommonSystemInfoIntermediate", "FC100CommonSystemInfoIntermediate"),
        ("TeslaFC100CommonSystemInfoReplay", "FC100CommonSystemInfoReplay"),
        ("TeslaFC100CommonSystemInfoReplayKind", "FC100CommonSystemInfoReplayKind"),
        ("TeslaFC100CommonSystemInfoTerminal", "FC100CommonSystemInfoTerminal"),
        ("TeslaFC100Operation", "FC100Operation"),
        ("TeslaFC100OperationApplicationError", "FC100OperationApplicationError"),
        ("TeslaFC100OperationCommonSystemInfo", "FC100OperationCommonSystemInfo"),
        ("TeslaFC100OperationIntermediate", "FC100OperationIntermediate"),
        ("TeslaFC100OperationNames", "FC100OperationNames"),
        ("TeslaFC100OperationResult", "FC100OperationResult"),
        ("TeslaFC100OperationResultKind", "FC100OperationResultKind"),
        ("TeslaFC100OperationTerminal", "FC100OperationTerminal"),
        ("TeslaFC100OperationWCConfigureSettings", "FC100OperationWCConfigureSettings"),
        ("TeslaFC100OperationWCGetLifetimeStats", "FC100OperationWCGetLifetimeStats"),
        ("TeslaFC100OperationWCGetLoadSharingState", "FC100OperationWCGetLoadSharingState"),
        ("TeslaFC100OperationWCGetProvisional", "FC100OperationWCGetProvisional"),
        ("TeslaFC100OperationWCGetSystemInfo", "FC100OperationWCGetSystemInfo"),
        ("TeslaFC100OperationWCGetVitals", "FC100OperationWCGetVitals"),
        ("TeslaFC100OperationWCSetProvisional", "FC100OperationWCSetProvisional"),
        ("TeslaFC100RecoveredNames", "FC100RecoveredNames"),
        ("TeslaFC100RequestMatrix", "FC100RequestMatrix"),
        ("TeslaFC100TEDAPI", "FC100TEDAPI"),
        ("TeslaFC100WCLifetimeIntermediate", "FC100WCLifetimeIntermediate"),
        ("TeslaFC100WCLifetimeReplay", "FC100WCLifetimeReplay"),
        ("TeslaFC100WCLifetimeReplayKind", "FC100WCLifetimeReplayKind"),
        ("TeslaFC100WCLifetimeTerminal", "FC100WCLifetimeTerminal"),
        ("TeslaFC100WCLoadSharingStateIntermediate", "FC100WCLoadSharingStateIntermediate"),
        ("TeslaFC100WCLoadSharingStateReplay", "FC100WCLoadSharingStateReplay"),
        ("TeslaFC100WCLoadSharingStateReplayKind", "FC100WCLoadSharingStateReplayKind"),
        ("TeslaFC100WCLoadSharingStateTerminal", "FC100WCLoadSharingStateTerminal"),
        ("TeslaFC100WCSystemInfoIntermediate", "FC100WCSystemInfoIntermediate"),
        ("TeslaFC100WCSystemInfoReplay", "FC100WCSystemInfoReplay"),
        ("TeslaFC100WCSystemInfoReplayKind", "FC100WCSystemInfoReplayKind"),
        ("TeslaFC100WCSystemInfoTerminal", "FC100WCSystemInfoTerminal"),
        ("TeslaFC100WCVitalsExecutor", "FC100WCVitalsExecutor"),
        ("TeslaFC100WCVitalsIntermediate", "FC100WCVitalsIntermediate"),
        ("TeslaFC100WCVitalsRTUSession", "FC100WCVitalsRTUSession"),
        ("TeslaFC100WCVitalsRTUSessionAdapter", "FC100WCVitalsRTUSessionAdapter"),
        ("TeslaFC100WCVitalsReplay", "FC100WCVitalsReplay"),
        ("TeslaFC100WCVitalsReplayKind", "FC100WCVitalsReplayKind"),
        ("TeslaFC100WCVitalsTerminal", "FC100WCVitalsTerminal"),
        ("TeslaFC100WireEntry", "FC100WireEntry"),
        ("TeslaGen3CurrentLimitOperationVersion24443", "Gen3CurrentLimitOperationVersion24443"),
        ("TeslaGen3HSCActivationRequest", "Gen3HSCActivationRequest"),
        ("TeslaGen3HSCActivationWitness", "Gen3HSCActivationWitness"),
        ("TeslaGen3HSCExceptionOutcome", "Gen3HSCExceptionOutcome"),
        ("TeslaGen3HSCNativeOperationDirection", "Gen3HSCNativeOperationDirection"),
        ("TeslaGen3HSCNativeOperationOutcome", "Gen3HSCNativeOperationOutcome"),
        ("TeslaGen3HSCNativeOperationRecord", "Gen3HSCNativeOperationRecord"),
        ("TeslaGen3HSCNativeOperationRecordConfig", "Gen3HSCNativeOperationRecordConfig"),
        ("TeslaGen3HSCNormalOutcome", "Gen3HSCNormalOutcome"),
        ("TeslaGen3HSCProfile", "Gen3HSCProfile"),
        ("TeslaGen3HSCProfileConfig", "Gen3HSCProfileConfig"),
        ("TeslaGen3HSCRequestDirection", "Gen3HSCRequestDirection"),
        ("TeslaGen3HSCResponseDirection", "Gen3HSCResponseDirection"),
        ("TeslaGen3HSCVersionCompatibleCandidate", "Gen3HSCVersionCompatibleCandidate"),
        ("TeslaGen3HSCVersionDisposition", "Gen3HSCVersionDisposition"),
        ("TeslaGen3HSCVersionKnownObservation", "Gen3HSCVersionKnownObservation"),
        ("TeslaGen3HSCVersionUnknown", "Gen3HSCVersionUnknown"),
        ("TeslaGen3InteroperableCurrentLimit", "Gen3InteroperableCurrentLimit"),
        ("TeslaGen3PersistentCurrentLimit", "Gen3PersistentCurrentLimit"),
        ("TeslaGen3PersistentCurrentLimitSpec", "Gen3PersistentCurrentLimitSpec"),
        ("TeslaGen3ProvisionalCurrentLimit", "Gen3ProvisionalCurrentLimit"),
        ("TeslaGen3ProvisionalCurrentLimitSpec", "Gen3ProvisionalCurrentLimitSpec"),
        ("TeslaGen3ReadAcquisition", "Gen3ReadAcquisition"),
        ("TeslaGen3ReadAcquisitionConfig", "Gen3ReadAcquisitionConfig"),
        ("TeslaGen3ReadEndpoint", "Gen3ReadEndpoint"),
        ("TeslaGen3ReadEndpointOpener", "Gen3ReadEndpointOpener"),
        ("TeslaGen3ReadEvidenceAck", "Gen3ReadEvidenceAck"),
        ("TeslaGen3ReadEvidenceIndeterminate", "Gen3ReadEvidenceIndeterminate"),
        ("TeslaGen3ReadEvidenceNoSend", "Gen3ReadEvidenceNoSend"),
        ("TeslaGen3ReadEvidenceRejected", "Gen3ReadEvidenceRejected"),
        ("TeslaGen3ReadEvidenceState", "Gen3ReadEvidenceState"),
        ("TeslaGen3ReadEvidenceTimeout", "Gen3ReadEvidenceTimeout"),
        ("TeslaGen3ReadLifecycleActivating", "Gen3ReadLifecycleActivating"),
        ("TeslaGen3ReadLifecycleClosed", "Gen3ReadLifecycleClosed"),
        ("TeslaGen3ReadLifecycleReady", "Gen3ReadLifecycleReady"),
        ("TeslaGen3ReadLifecycleRecovering", "Gen3ReadLifecycleRecovering"),
        ("TeslaGen3ReadLifecycleState", "Gen3ReadLifecycleState"),
        ("TeslaGen3ReadLifecycleStopped", "Gen3ReadLifecycleStopped"),
        ("TeslaGen3ReadLifecycleUnavailable", "Gen3ReadLifecycleUnavailable"),
        ("TeslaGen3ReadRecord", "Gen3ReadRecord"),
        ("TeslaGen3ReadRetained", "Gen3ReadRetained"),
        ("TeslaGen3ReadSourceRTUPrivateFunction", "Gen3ReadSourceRTUPrivateFunction"),
        ("TeslaGen3ReadWindow", "Gen3ReadWindow"),
        ("TeslaHSCCompatibilityV1", "HSCCompatibilityV1"),
        ("TeslaHSCDisabled", "HSCDisabled"),
        ("TeslaHSCDisposition", "HSCDisposition"),
        ("TeslaHSCEnvelope", "HSCEnvelope"),
        ("TeslaHSCException", "HSCException"),
        ("TeslaHSCExceptionCodecFailure", "HSCExceptionCodecFailure"),
        ("TeslaHSCExceptionUnknown", "HSCExceptionUnknown"),
        ("TeslaHSCExceptionUnknownFunction", "HSCExceptionUnknownFunction"),
        ("TeslaHSCFC100OperationCompatibilityV1", "HSCFC100OperationCompatibilityV1"),
        ("TeslaHSCFramingOnly", "HSCFramingOnly"),
        ("TeslaHSCNativeRecord", "HSCNativeRecord"),
        ("TeslaHSCOpaqueTerminalProvenance", "HSCOpaqueTerminalProvenance"),
        ("TeslaHSCProfile", "HSCProfile"),
        ("TeslaHSCProfileConfig", "HSCProfileConfig"),
        ("TeslaHSCProfileName", "HSCProfileName"),
        ("TeslaHSCProvenance", "HSCProvenance"),
        ("TeslaHSCQualifiedReadOnly", "HSCQualifiedReadOnly"),
        ("TeslaHSCResponse", "HSCResponse"),
        ("TeslaHSCSyntacticRequest", "HSCSyntacticRequest"),
        ("TeslaHSCSystemInfoCompatibilityV1", "HSCSystemInfoCompatibilityV1"),
        ("TeslaHSCWCLifetimeCompatibilityV1", "HSCWCLifetimeCompatibilityV1"),
        ("TeslaHSCWCLoadSharingStateCompatibilityV1", "HSCWCLoadSharingStateCompatibilityV1"),
        ("TeslaHSCWCSystemInfoCompatibilityV1", "HSCWCSystemInfoCompatibilityV1"),
        ("TeslaHSCWCVitalsCompatibilityV1", "HSCWCVitalsCompatibilityV1"),
        ("TeslaLegacyBuildConfirmed", "LegacyBuildConfirmed"),
        ("TeslaLegacyCommand", "LegacyCommand"),
        ("TeslaLegacyDirection", "LegacyDirection"),
        ("TeslaLegacyFBE0Observation", "LegacyFBE0Observation"),
        ("TeslaLegacyFBE0ObservationSpec", "LegacyFBE0ObservationSpec"),
        ("TeslaLegacyFDE0Observation", "LegacyFDE0Observation"),
        ("TeslaLegacyFDE0ObservationSpec", "LegacyFDE0ObservationSpec"),
        ("TeslaLegacyFDE0State", "LegacyFDE0State"),
        ("TeslaLegacyFDE0StateCharging", "LegacyFDE0StateCharging"),
        ("TeslaLegacyFDE0StatePreCharge", "LegacyFDE0StatePreCharge"),
        ("TeslaLegacyFamilyCompatible", "LegacyFamilyCompatible"),
        ("TeslaLegacyOperation", "LegacyOperation"),
        ("TeslaLegacyOperationPrimaryDiscovery", "LegacyOperationPrimaryDiscovery"),
        ("TeslaLegacyOperationPrimaryHeartbeat", "LegacyOperationPrimaryHeartbeat"),
        ("TeslaLegacyOperationSecondaryDiscovery", "LegacyOperationSecondaryDiscovery"),
        ("TeslaLegacyOperationSecondaryHeartbeat", "LegacyOperationSecondaryHeartbeat"),
        ("TeslaLegacyOperationUnknown", "LegacyOperationUnknown"),
        ("TeslaLegacyQualification", "LegacyQualification"),
        ("TeslaLegacyWallConnectorBuildConfirmed", "LegacyWallConnectorBuildConfirmed"),
        ("TeslaLegacyWallConnectorCompatibility", "LegacyWallConnectorCompatibility"),
        ("TeslaLegacyWallConnectorFamilyCompatible", "LegacyWallConnectorFamilyCompatible"),
        ("TeslaLegacyWallConnectorFrame", "LegacyWallConnectorFrame"),
        ("TeslaLegacyWallConnectorGeneration", "LegacyWallConnectorGeneration"),
        ("TeslaLegacyWallConnectorGenerationCandidate", "LegacyWallConnectorGenerationCandidate"),
        ("TeslaLegacyWallConnectorProfile", "LegacyWallConnectorProfile"),
        ("TeslaLegacyWallConnectorProfileConfig", "LegacyWallConnectorProfileConfig"),
        ("TeslaLegacyWallConnectorRecord", "LegacyWallConnectorRecord"),
        ("TeslaLegacyWallConnectorRequest", "LegacyWallConnectorRequest"),
        ("TeslaLegacyWallConnectorResponse", "LegacyWallConnectorResponse"),
        ("TeslaLegacyWallConnectorUnknown", "LegacyWallConnectorUnknown"),
        ("TeslaTEDAPIAdmissionAllowedCommonSystemInfo", "TEDAPIAdmissionAllowedCommonSystemInfo"),
        ("TeslaTEDAPIAdmissionAllowedWCLifetime", "TEDAPIAdmissionAllowedWCLifetime"),
        ("TeslaTEDAPIAdmissionAllowedWCLoadSharingState", "TEDAPIAdmissionAllowedWCLoadSharingState"),
        ("TeslaTEDAPIAdmissionAllowedWCSystemInfo", "TEDAPIAdmissionAllowedWCSystemInfo"),
        ("TeslaTEDAPIAdmissionAllowedWCVitals", "TEDAPIAdmissionAllowedWCVitals"),
        ("TeslaTEDAPIAdmissionBlockedNoAdmissibleOperation", "TEDAPIAdmissionBlockedNoAdmissibleOperation"),
        ("TeslaTEDAPIAdmissionBlockedProfile", "TEDAPIAdmissionBlockedProfile"),
        ("TeslaTEDAPIAdmissionState", "TEDAPIAdmissionState"),
        ("TeslaTEDAPIDirection", "TEDAPIDirection"),
        ("TeslaTEDAPIFramingOnly", "TEDAPIFramingOnly"),
        ("TeslaTEDAPINativeRecord", "TEDAPINativeRecord"),
        ("TeslaTEDAPIObservationSpec", "TEDAPIObservationSpec"),
        ("TeslaTEDAPIOpaqueQualified", "TEDAPIOpaqueQualified"),
        ("TeslaTEDAPIOperationAdmission", "TEDAPIOperationAdmission"),
        ("TeslaTEDAPIOperationCommonSystemInfoV1", "TEDAPIOperationCommonSystemInfoV1"),
        ("TeslaTEDAPIOperationWCLifetimeV1", "TEDAPIOperationWCLifetimeV1"),
        ("TeslaTEDAPIOperationWCLoadSharingStateV1", "TEDAPIOperationWCLoadSharingStateV1"),
        ("TeslaTEDAPIOperationWCSystemInfoV1", "TEDAPIOperationWCSystemInfoV1"),
        ("TeslaTEDAPIOperationWCVitalsV1", "TEDAPIOperationWCVitalsV1"),
        ("TeslaTEDAPIRequest", "TEDAPIRequest"),
        ("TeslaTEDAPIResponse", "TEDAPIResponse"),
        ("TeslaTEDAPISemanticRegistry", "TEDAPISemanticRegistry"),
        ("TeslaTEDAPISemanticState", "TEDAPISemanticState"),
    ),
    "huawei": (
        ("HuaweiDecimal", "Decimal"),
        ("HuaweiEMMACanonicalClass", "EMMACanonicalClass"),
        ("HuaweiEMMAObservedBasicReply", "EMMAObservedBasicReply"),
        ("HuaweiEMMAObservedChildReads", "EMMAObservedChildReads"),
        ("HuaweiEMMAObservedGatewayReads", "EMMAObservedGatewayReads"),
        ("HuaweiEMMAObservedInventoryPage", "EMMAObservedInventoryPage"),
        ("HuaweiEMMAObservedQualificationCard", "EMMAObservedQualificationCard"),
        ("HuaweiEMMAObservedQualificationCardID", "EMMAObservedQualificationCardID"),
        ("HuaweiEMMAObservedQualificationInput", "EMMAObservedQualificationInput"),
        ("HuaweiEMMAOfflineDecision", "EMMAOfflineDecision"),
        ("HuaweiEMMAOfflineFirmwareMismatch", "EMMAOfflineFirmwareMismatch"),
        ("HuaweiEMMAOfflineInventory", "EMMAOfflineInventory"),
        ("HuaweiEMMAOfflineInventoryInput", "EMMAOfflineInventoryInput"),
        ("HuaweiEMMAOfflineMatched", "EMMAOfflineMatched"),
        ("HuaweiEMMAOfflineModelMismatch", "EMMAOfflineModelMismatch"),
        ("HuaweiEMMAOfflineOfferingMismatch", "EMMAOfflineOfferingMismatch"),
        ("HuaweiEMMAOfflineReason", "EMMAOfflineReason"),
        ("HuaweiEMMAQualificationCard", "EMMAQualificationCard"),
        ("HuaweiEMMAQualificationCardID", "EMMAQualificationCardID"),
        ("HuaweiEMMAQualificationInput", "EMMAQualificationInput"),
        ("HuaweiEMMAReadOnlyProfileID", "EMMAReadOnlyProfileID"),
        ("HuaweiEditionBinding", "EditionBinding"),
        ("HuaweiEditionBindingLiveCheckedLayout", "EditionBindingLiveCheckedLayout"),
        ("HuaweiEditionBindingSupportedModelTable", "EditionBindingSupportedModelTable"),
        ("HuaweiEvidenceGrade", "EvidenceGrade"),
        ("HuaweiFactAvailability", "FactAvailability"),
        ("HuaweiFactUnknown", "FactUnknown"),
        ("HuaweiFactValue", "FactValue"),
        ("HuaweiGatewayAmbiguous", "GatewayAmbiguous"),
        ("HuaweiGatewayAtOrAboveFloor", "GatewayAtOrAboveFloor"),
        ("HuaweiGatewayBelowFloor", "GatewayBelowFloor"),
        ("HuaweiGatewayCatalogInvalid", "GatewayCatalogInvalid"),
        ("HuaweiGatewayEdition", "GatewayEdition"),
        ("HuaweiGatewayEndpoint", "GatewayEndpoint"),
        ("HuaweiGatewayEndpointSelf", "GatewayEndpointSelf"),
        ("HuaweiGatewayFamilyMismatch", "GatewayFamilyMismatch"),
        ("HuaweiGatewayFloor", "GatewayFloor"),
        ("HuaweiGatewayFloorBranch", "GatewayFloorBranch"),
        ("HuaweiGatewayFloorDomain", "GatewayFloorDomain"),
        ("HuaweiGatewayFloorLine", "GatewayFloorLine"),
        ("HuaweiGatewayFloorRelation", "GatewayFloorRelation"),
        ("HuaweiGatewayInventory", "GatewayInventory"),
        ("HuaweiGatewayInventoryChild", "GatewayInventoryChild"),
        ("HuaweiGatewayInventoryObject", "GatewayInventoryObject"),
        ("HuaweiGatewayInventoryPage", "GatewayInventoryPage"),
        ("HuaweiGatewayMultipleFloors", "GatewayMultipleFloors"),
        ("HuaweiGatewayNoDocumentedRelation", "GatewayNoDocumentedRelation"),
        ("HuaweiGatewayNotSelected", "GatewayNotSelected"),
        ("HuaweiGatewayOpaqueAttribute", "GatewayOpaqueAttribute"),
        ("HuaweiGatewayRelationAtOrAbove", "GatewayRelationAtOrAbove"),
        ("HuaweiGatewayRelationBelow", "GatewayRelationBelow"),
        ("HuaweiGatewayRelationNoDocumentedRelation", "GatewayRelationNoDocumentedRelation"),
        ("HuaweiGatewayRelationNotEvaluated", "GatewayRelationNotEvaluated"),
        ("HuaweiGatewayScope", "GatewayScope"),
        ("HuaweiGatewaySelected", "GatewaySelected"),
        ("HuaweiGatewaySelection", "GatewaySelection"),
        ("HuaweiGatewaySelectionInput", "GatewaySelectionInput"),
        ("HuaweiGatewaySelectionOutcome", "GatewaySelectionOutcome"),
        ("HuaweiGatewaySelectionReason", "GatewaySelectionReason"),
        ("HuaweiGatewaySoftwareMalformed", "GatewaySoftwareMalformed"),
        ("HuaweiGatewaySoftwareNotRead", "GatewaySoftwareNotRead"),
        ("HuaweiGradeHypothesis", "GradeHypothesis"),
        ("HuaweiGradeProven", "GradeProven"),
        ("HuaweiGradeUnknown", "GradeUnknown"),
        ("HuaweiIdentificationPage", "IdentificationPage"),
        ("HuaweiIdentificationRequest", "IdentificationRequest"),
        ("HuaweiInventoryPageAnomalies", "InventoryPageAnomalies"),
        ("HuaweiProviderAtDeadline", "ProviderAtDeadline"),
        ("HuaweiProviderAuthorityFieldSet", "ProviderAuthorityFieldSet"),
        ("HuaweiProviderChildDisposition", "ProviderChildDisposition"),
        ("HuaweiProviderChildObservation", "ProviderChildObservation"),
        ("HuaweiProviderChildProfileNotRecognised", "ProviderChildProfileNotRecognised"),
        ("HuaweiProviderConfig", "ProviderConfig"),
        ("HuaweiProviderCycle", "ProviderCycle"),
        ("HuaweiProviderCycleComplete", "ProviderCycleComplete"),
        ("HuaweiProviderCycleIncomplete", "ProviderCycleIncomplete"),
        ("HuaweiProviderCycleInsufficient", "ProviderCycleInsufficient"),
        ("HuaweiProviderCycleOutcome", "ProviderCycleOutcome"),
        ("HuaweiProviderDisposition", "ProviderDisposition"),
        ("HuaweiProviderElapsedClass", "ProviderElapsedClass"),
        ("HuaweiProviderEnabled", "ProviderEnabled"),
        ("HuaweiProviderEnablement", "ProviderEnablement"),
        ("HuaweiProviderEnablementReason", "ProviderEnablementReason"),
        ("HuaweiProviderEvidenceCurrent", "ProviderEvidenceCurrent"),
        ("HuaweiProviderEvidenceStale", "ProviderEvidenceStale"),
        ("HuaweiProviderEvidenceState", "ProviderEvidenceState"),
        ("HuaweiProviderEvidenceWithheld", "ProviderEvidenceWithheld"),
        ("HuaweiProviderExchange", "ProviderExchange"),
        ("HuaweiProviderExchangeAnswered", "ProviderExchangeAnswered"),
        ("HuaweiProviderExchangeException", "ProviderExchangeException"),
        ("HuaweiProviderExchangeMalformed", "ProviderExchangeMalformed"),
        ("HuaweiProviderExchangeOutcome", "ProviderExchangeOutcome"),
        ("HuaweiProviderExchangeTimeout", "ProviderExchangeTimeout"),
        ("HuaweiProviderExchangeTransport", "ProviderExchangeTransport"),
        ("HuaweiProviderFencedReading", "ProviderFencedReading"),
        ("HuaweiProviderFirstRequestTolerated", "ProviderFirstRequestTolerated"),
        ("HuaweiProviderGatewayIdentified", "ProviderGatewayIdentified"),
        ("HuaweiProviderGatewayIdentity", "ProviderGatewayIdentity"),
        ("HuaweiProviderGatewayInsufficient", "ProviderGatewayInsufficient"),
        ("HuaweiProviderGatewayNotIdentified", "ProviderGatewayNotIdentified"),
        ("HuaweiProviderGatewayState", "ProviderGatewayState"),
        ("HuaweiProviderGatewayUnconfirmed", "ProviderGatewayUnconfirmed"),
        ("HuaweiProviderGroupEvidence", "ProviderGroupEvidence"),
        ("HuaweiProviderIdentityNotAdmitted", "ProviderIdentityNotAdmitted"),
        ("HuaweiProviderLoss", "ProviderLoss"),
        ("HuaweiProviderLossBattery", "ProviderLossBattery"),
        ("HuaweiProviderLossKind", "ProviderLossKind"),
        ("HuaweiProviderLossMeter", "ProviderLossMeter"),
        ("HuaweiProviderLossPlannedGroup", "ProviderLossPlannedGroup"),
        ("HuaweiProviderLossRefusedGroup", "ProviderLossRefusedGroup"),
        ("HuaweiProviderLossUnconsumedWords", "ProviderLossUnconsumedWords"),
        ("HuaweiProviderLossWithheldChild", "ProviderLossWithheldChild"),
        ("HuaweiProviderNoAdmittedChildProfile", "ProviderNoAdmittedChildProfile"),
        ("HuaweiProviderPacing", "ProviderPacing"),
        ("HuaweiProviderPacingFor", "ProviderPacingFor"),
        ("HuaweiProviderPath", "ProviderPath"),
        ("HuaweiProviderPathEMMA", "ProviderPathEMMA"),
        ("HuaweiProviderPathSDongle", "ProviderPathSDongle"),
        ("HuaweiProviderPathSmartLogger", "ProviderPathSmartLogger"),
        ("HuaweiProviderReason", "ProviderReason"),
        ("HuaweiProviderReasonCatalogAmbiguous", "ProviderReasonCatalogAmbiguous"),
        ("HuaweiProviderReasonCatalogNotSelected", "ProviderReasonCatalogNotSelected"),
        ("HuaweiProviderReasonChildBackoff", "ProviderReasonChildBackoff"),
        ("HuaweiProviderReasonChildException", "ProviderReasonChildException"),
        ("HuaweiProviderReasonChildReadFailed", "ProviderReasonChildReadFailed"),
        ("HuaweiProviderReasonChildTimeout", "ProviderReasonChildTimeout"),
        ("HuaweiProviderReasonGatewayMismatch", "ProviderReasonGatewayMismatch"),
        ("HuaweiProviderReasonGatewayReadFail", "ProviderReasonGatewayReadFail"),
        ("HuaweiProviderReasonGuardChanged", "ProviderReasonGuardChanged"),
        ("HuaweiProviderReasonGuardFailed", "ProviderReasonGuardFailed"),
        ("HuaweiProviderReasonGuardHeld", "ProviderReasonGuardHeld"),
        ("HuaweiProviderReasonIdentified", "ProviderReasonIdentified"),
        ("HuaweiProviderReasonIdentityInsuff", "ProviderReasonIdentityInsuff"),
        ("HuaweiProviderReasonIdentityMismatch", "ProviderReasonIdentityMismatch"),
        ("HuaweiProviderReasonListingBackoff", "ProviderReasonListingBackoff"),
        ("HuaweiProviderReasonListingUnconfirmed", "ProviderReasonListingUnconfirmed"),
        ("HuaweiProviderReasonListingUnstable", "ProviderReasonListingUnstable"),
        ("HuaweiProviderReasonNoClassContract", "ProviderReasonNoClassContract"),
        ("HuaweiProviderReasonNoProfile", "ProviderReasonNoProfile"),
        ("HuaweiProviderReasonNotAllowlisted", "ProviderReasonNotAllowlisted"),
        ("HuaweiProviderReasonNotListed", "ProviderReasonNotListed"),
        ("HuaweiProviderReasonRead", "ProviderReasonRead"),
        ("HuaweiProviderRecordedStopInForce", "ProviderRecordedStopInForce"),
        ("HuaweiProviderSession", "ProviderSession"),
        ("HuaweiProviderUnknownPath", "ProviderUnknownPath"),
        ("HuaweiProviderWait", "ProviderWait"),
        ("HuaweiProviderWithinDeadline", "ProviderWithinDeadline"),
        ("HuaweiProviderWithinPromptBound", "ProviderWithinPromptBound"),
        ("HuaweiQualificationAmbiguous", "QualificationAmbiguous"),
        ("HuaweiQualificationBasicIdentityConflict", "QualificationBasicIdentityConflict"),
        ("HuaweiQualificationEvidenceBlocked", "QualificationEvidenceBlocked"),
        ("HuaweiQualificationExpectedResult", "QualificationExpectedResult"),
        ("HuaweiQualificationGatewayBehaviourMismatch", "QualificationGatewayBehaviourMismatch"),
        ("HuaweiQualificationIdentityInvalid", "QualificationIdentityInvalid"),
        ("HuaweiQualificationIdentityMatchedCapabilitiesMissing", "QualificationIdentityMatchedCapabilitiesMissing"),
        ("HuaweiQualificationInsufficientEvidence", "QualificationInsufficientEvidence"),
        ("HuaweiQualificationInventoryInvalid", "QualificationInventoryInvalid"),
        ("HuaweiQualificationInventoryLimits", "QualificationInventoryLimits"),
        ("HuaweiQualificationMatchedIdentityInventory", "QualificationMatchedIdentityInventory"),
        ("HuaweiQualificationMultipleClasses", "QualificationMultipleClasses"),
        ("HuaweiQualificationPackage", "QualificationPackage"),
        ("HuaweiQualificationPersistentNonResponse", "QualificationPersistentNonResponse"),
        ("HuaweiQualificationReadiness", "QualificationReadiness"),
        ("HuaweiQualificationReason", "QualificationReason"),
        ("HuaweiQualificationResult", "QualificationResult"),
        ("HuaweiQualificationStep", "QualificationStep"),
        ("HuaweiQualificationTestReady", "QualificationTestReady"),
        ("HuaweiRecordedProviderDisposition", "RecordedProviderDisposition"),
        ("HuaweiSDongleCanonicalClass", "SDongleCanonicalClass"),
        ("HuaweiSDongleObservedBasicOutcome", "SDongleObservedBasicOutcome"),
        ("HuaweiSDongleObservedBasicReply", "SDongleObservedBasicReply"),
        ("HuaweiSDongleObservedChildInventoryPage", "SDongleObservedChildInventoryPage"),
        ("HuaweiSDongleObservedChildReads", "SDongleObservedChildReads"),
        ("HuaweiSDongleObservedGatewayReads", "SDongleObservedGatewayReads"),
        ("HuaweiSDongleObservedQualificationCard", "SDongleObservedQualificationCard"),
        ("HuaweiSDongleObservedQualificationCardID", "SDongleObservedQualificationCardID"),
        ("HuaweiSDongleObservedQualificationInput", "SDongleObservedQualificationInput"),
        ("HuaweiSDongleOfflineCapacityMismatch", "SDongleOfflineCapacityMismatch"),
        ("HuaweiSDongleOfflineDecision", "SDongleOfflineDecision"),
        ("HuaweiSDongleOfflineFirmwareMismatch", "SDongleOfflineFirmwareMismatch"),
        ("HuaweiSDongleOfflineMatched", "SDongleOfflineMatched"),
        ("HuaweiSDongleOfflineModelMismatch", "SDongleOfflineModelMismatch"),
        ("HuaweiSDongleOfflineObservation", "SDongleOfflineObservation"),
        ("HuaweiSDongleOfflineProtocolMismatch", "SDongleOfflineProtocolMismatch"),
        ("HuaweiSDongleOfflineReason", "SDongleOfflineReason"),
        ("HuaweiSDongleOfflineSearchIncomplete", "SDongleOfflineSearchIncomplete"),
        ("HuaweiSDongleOfflineSequenceMismatch", "SDongleOfflineSequenceMismatch"),
        ("HuaweiSDongleOfflineUnitMismatch", "SDongleOfflineUnitMismatch"),
        ("HuaweiSDongleQualificationCard", "SDongleQualificationCard"),
        ("HuaweiSDongleQualificationCardID", "SDongleQualificationCardID"),
        ("HuaweiSDongleReadOnlyProfileID", "SDongleReadOnlyProfileID"),
        ("HuaweiSDongleReadOnlyProvider", "SDongleReadOnlyProvider"),
        ("HuaweiSDongleReadOnlyProviderID", "SDongleReadOnlyProviderID"),
        ("HuaweiSUN2000AmbiguousMultipleProfiles", "SUN2000AmbiguousMultipleProfiles"),
        ("HuaweiSUN2000BuildFirmwareText", "SUN2000BuildFirmwareText"),
        ("HuaweiSUN2000BuildNone", "SUN2000BuildNone"),
        ("HuaweiSUN2000BuildSoftwareText", "SUN2000BuildSoftwareText"),
        ("HuaweiSUN2000BuildSource", "SUN2000BuildSource"),
        ("HuaweiSUN2000CapabilityInput", "SUN2000CapabilityInput"),
        ("HuaweiSUN2000CapabilityOutcome", "SUN2000CapabilityOutcome"),
        ("HuaweiSUN2000CapabilityReason", "SUN2000CapabilityReason"),
        ("HuaweiSUN2000CapabilitySelection", "SUN2000CapabilitySelection"),
        ("HuaweiSUN2000CatalogAmbiguous", "SUN2000CatalogAmbiguous"),
        ("HuaweiSUN2000CatalogAtOrAboveFloor", "SUN2000CatalogAtOrAboveFloor"),
        ("HuaweiSUN2000CatalogBelowFloor", "SUN2000CatalogBelowFloor"),
        ("HuaweiSUN2000CatalogDecision", "SUN2000CatalogDecision"),
        ("HuaweiSUN2000CatalogEdition", "SUN2000CatalogEdition"),
        ("HuaweiSUN2000CatalogEqualOnCarriedParts", "SUN2000CatalogEqualOnCarriedParts"),
        ("HuaweiSUN2000CatalogFloorOutsideBranchForm", "SUN2000CatalogFloorOutsideBranchForm"),
        ("HuaweiSUN2000CatalogIdentifierAbsent", "SUN2000CatalogIdentifierAbsent"),
        ("HuaweiSUN2000CatalogIdentifierMismatch", "SUN2000CatalogIdentifierMismatch"),
        ("HuaweiSUN2000CatalogInput", "SUN2000CatalogInput"),
        ("HuaweiSUN2000CatalogInvalid", "SUN2000CatalogInvalid"),
        ("HuaweiSUN2000CatalogMatch", "SUN2000CatalogMatch"),
        ("HuaweiSUN2000CatalogMatchToken", "SUN2000CatalogMatchToken"),
        ("HuaweiSUN2000CatalogMatchWireAlias", "SUN2000CatalogMatchWireAlias"),
        ("HuaweiSUN2000CatalogModelTextInvalid", "SUN2000CatalogModelTextInvalid"),
        ("HuaweiSUN2000CatalogMultipleCandidateRows", "SUN2000CatalogMultipleCandidateRows"),
        ("HuaweiSUN2000CatalogNoCatalogRow", "SUN2000CatalogNoCatalogRow"),
        ("HuaweiSUN2000CatalogNoDocumentedRelation", "SUN2000CatalogNoDocumentedRelation"),
        ("HuaweiSUN2000CatalogNotSelected", "SUN2000CatalogNotSelected"),
        ("HuaweiSUN2000CatalogOutcome", "SUN2000CatalogOutcome"),
        ("HuaweiSUN2000CatalogReason", "SUN2000CatalogReason"),
        ("HuaweiSUN2000CatalogRow", "SUN2000CatalogRow"),
        ("HuaweiSUN2000CatalogScopeInvalid", "SUN2000CatalogScopeInvalid"),
        ("HuaweiSUN2000CatalogScopeMismatch", "SUN2000CatalogScopeMismatch"),
        ("HuaweiSUN2000CatalogSelected", "SUN2000CatalogSelected"),
        ("HuaweiSUN2000CatalogSoftwareAbsent", "SUN2000CatalogSoftwareAbsent"),
        ("HuaweiSUN2000CatalogSoftwareMalformed", "SUN2000CatalogSoftwareMalformed"),
        ("HuaweiSUN2000EndpointScope", "SUN2000EndpointScope"),
        ("HuaweiSUN2000Fact", "SUN2000Fact"),
        ("HuaweiSUN2000FloorAtOrAbove", "SUN2000FloorAtOrAbove"),
        ("HuaweiSUN2000FloorBelow", "SUN2000FloorBelow"),
        ("HuaweiSUN2000FloorEqualOnCarriedParts", "SUN2000FloorEqualOnCarriedParts"),
        ("HuaweiSUN2000FloorNoDocumentedRelation", "SUN2000FloorNoDocumentedRelation"),
        ("HuaweiSUN2000FloorNotEvaluated", "SUN2000FloorNotEvaluated"),
        ("HuaweiSUN2000FloorOutsideBranchForm", "SUN2000FloorOutsideBranchForm"),
        ("HuaweiSUN2000FloorRelation", "SUN2000FloorRelation"),
        ("HuaweiSUN2000GroupAnswered", "SUN2000GroupAnswered"),
        ("HuaweiSUN2000GroupException", "SUN2000GroupException"),
        ("HuaweiSUN2000GroupFailed", "SUN2000GroupFailed"),
        ("HuaweiSUN2000GroupNotRead", "SUN2000GroupNotRead"),
        ("HuaweiSUN2000GroupOutcome", "SUN2000GroupOutcome"),
        ("HuaweiSUN2000GroupReading", "SUN2000GroupReading"),
        ("HuaweiSUN2000InsufficientEvidence", "SUN2000InsufficientEvidence"),
        ("HuaweiSUN2000M1MonitoringProfileID", "SUN2000M1MonitoringProfileID"),
        ("HuaweiSUN2000M3MonitoringProfileID", "SUN2000M3MonitoringProfileID"),
        ("HuaweiSUN2000MonitoringObserver", "SUN2000MonitoringObserver"),
        ("HuaweiSUN2000MonitoringProfile", "SUN2000MonitoringProfile"),
        ("HuaweiSUN2000MonitoringProfiles", "SUN2000MonitoringProfiles"),
        ("HuaweiSUN2000MonitoringReading", "SUN2000MonitoringReading"),
        ("HuaweiSUN2000MonitoringSession", "SUN2000MonitoringSession"),
        ("HuaweiSUN2000NoAdmissibleProfile", "SUN2000NoAdmissibleProfile"),
        ("HuaweiSUN2000ProfileEvaluation", "SUN2000ProfileEvaluation"),
        ("HuaweiSUN2000ProfileSelected", "SUN2000ProfileSelected"),
        ("HuaweiSUN2000ReadStep", "SUN2000ReadStep"),
        ("HuaweiSUN2000ReasonCatalogAmbiguous", "SUN2000ReasonCatalogAmbiguous"),
        ("HuaweiSUN2000ReasonCatalogNotSelected", "SUN2000ReasonCatalogNotSelected"),
        ("HuaweiSUN2000ReasonCatalogRowNoProfile", "SUN2000ReasonCatalogRowNoProfile"),
        ("HuaweiSUN2000ReasonGatewayIdentityRejected", "SUN2000ReasonGatewayIdentityRejected"),
        ("HuaweiSUN2000ReasonIdentityWordsInvalid", "SUN2000ReasonIdentityWordsInvalid"),
        ("HuaweiSUN2000ReasonListingModelConflict", "SUN2000ReasonListingModelConflict"),
        ("HuaweiSUN2000ReasonMatched", "SUN2000ReasonMatched"),
        ("HuaweiSUN2000ReasonModelBlockInvalid", "SUN2000ReasonModelBlockInvalid"),
        ("HuaweiSUN2000ReasonMultipleProfilesSelected", "SUN2000ReasonMultipleProfilesSelected"),
        ("HuaweiSUN2000ReasonNoGatewayIdentity", "SUN2000ReasonNoGatewayIdentity"),
        ("HuaweiSUN2000ReasonUnitNotRouted", "SUN2000ReasonUnitNotRouted"),
        ("HuaweiSUN2000RefusedGroup", "SUN2000RefusedGroup"),
        ("HuaweiSUN2000RoutedIdentity", "SUN2000RoutedIdentity"),
        ("HuaweiSUN2000ScopeC81Map", "SUN2000ScopeC81Map"),
        ("HuaweiSUN2000ScopeGatewayNamespace", "SUN2000ScopeGatewayNamespace"),
        ("HuaweiSUN2000ScopeInverterMap", "SUN2000ScopeInverterMap"),
        ("HuaweiSmartLoggerCanonicalClass", "SmartLoggerCanonicalClass"),
        ("HuaweiSmartLoggerInventory", "SmartLoggerInventory"),
        ("HuaweiSmartLoggerInventoryChild", "SmartLoggerInventoryChild"),
        ("HuaweiSmartLoggerInventoryObject", "SmartLoggerInventoryObject"),
        ("HuaweiSmartLoggerInventoryPage", "SmartLoggerInventoryPage"),
        ("HuaweiSmartLoggerObservedBasicReply", "SmartLoggerObservedBasicReply"),
        ("HuaweiSmartLoggerObservedInventoryInput", "SmartLoggerObservedInventoryInput"),
        ("HuaweiSmartLoggerObservedInventoryPage", "SmartLoggerObservedInventoryPage"),
        ("HuaweiSmartLoggerObservedQualificationCard", "SmartLoggerObservedQualificationCard"),
        ("HuaweiSmartLoggerObservedQualificationCardID", "SmartLoggerObservedQualificationCardID"),
        ("HuaweiSmartLoggerObservedQualificationInput", "SmartLoggerObservedQualificationInput"),
        ("HuaweiSmartLoggerOfflineInventoryInput", "SmartLoggerOfflineInventoryInput"),
        ("HuaweiSmartLoggerQualificationCard", "SmartLoggerQualificationCard"),
        ("HuaweiSmartLoggerQualificationCardID", "SmartLoggerQualificationCardID"),
        ("HuaweiSmartLoggerQualificationInput", "SmartLoggerQualificationInput"),
        ("HuaweiSmartLoggerReadOnlyProvider", "SmartLoggerReadOnlyProvider"),
        ("HuaweiSmartLoggerReadOnlyProviderID", "SmartLoggerReadOnlyProviderID"),
        ("HuaweiVendorDocumentIdentity", "VendorDocumentIdentity"),
        ("NewHuaweiEMMAObservedQualificationCard", "NewEMMAObservedQualificationCard"),
        ("NewHuaweiEMMAQualificationCard", "NewEMMAQualificationCard"),
        ("NewHuaweiQualificationPackage", "NewQualificationPackage"),
        ("NewHuaweiSDongleObservedQualificationCard", "NewSDongleObservedQualificationCard"),
        ("NewHuaweiSDongleQualificationCard", "NewSDongleQualificationCard"),
        ("NewHuaweiSDongleReadOnlyProvider", "NewSDongleReadOnlyProvider"),
        ("NewHuaweiSUN2000MonitoringObserver", "NewSUN2000MonitoringObserver"),
        ("NewHuaweiSmartLoggerObservedQualificationCard", "NewSmartLoggerObservedQualificationCard"),
        ("NewHuaweiSmartLoggerQualificationCard", "NewSmartLoggerQualificationCard"),
        ("NewHuaweiSmartLoggerReadOnlyProvider", "NewSmartLoggerReadOnlyProvider"),
    ),
    "sunspec": (
        ("NewSunSpecChain", "NewChain"),
        ("NewSunSpecChainPlan", "NewChainPlan"),
        ("NewSunSpecChainSnapshot", "NewChainSnapshot"),
        ("NewSunSpecFactPath", "NewFactPath"),
        ("NewSunSpecOccurrenceSourceRange", "NewOccurrenceSourceRange"),
        ("NewSunSpecPhaseOneDecoder", "NewPhaseOneDecoder"),
        ("SunSpecCanonicalBitfield", "CanonicalBitfield"),
        ("SunSpecCanonicalEnum", "CanonicalEnum"),
        ("SunSpecCanonicalNumber", "CanonicalNumber"),
        ("SunSpecCanonicalValue", "CanonicalValue"),
        ("SunSpecCanonicalValueKind", "CanonicalValueKind"),
        ("SunSpecCapabilityDecision", "CapabilityDecision"),
        ("SunSpecCapabilityFact", "CapabilityFact"),
        ("SunSpecCapabilityReason", "CapabilityReason"),
        ("SunSpecCapabilityReasonAdmitted", "CapabilityReasonAdmitted"),
        ("SunSpecCapabilityReasonAmbiguousSource", "CapabilityReasonAmbiguousSource"),
        ("SunSpecCapabilityReasonInvalidChain", "CapabilityReasonInvalidChain"),
        ("SunSpecCapabilityReasonInvalidRequiredFact", "CapabilityReasonInvalidRequiredFact"),
        ("SunSpecCapabilityReasonSourceAbsent", "CapabilityReasonSourceAbsent"),
        ("SunSpecCapabilityReasonSourceUnsupported", "CapabilityReasonSourceUnsupported"),
        ("SunSpecChain", "Chain"),
        ("SunSpecChainDisposition", "ChainDisposition"),
        ("SunSpecChainDispositionAdmitted", "ChainDispositionAdmitted"),
        ("SunSpecChainDispositionUnknown", "ChainDispositionUnknown"),
        ("SunSpecChainDispositionUnknownModel", "ChainDispositionUnknownModel"),
        ("SunSpecChainDispositionUnsupportedLength", "ChainDispositionUnsupportedLength"),
        ("SunSpecChainLimits", "ChainLimits"),
        ("SunSpecChainPlan", "ChainPlan"),
        ("SunSpecChainPlanSpec", "ChainPlanSpec"),
        ("SunSpecChainSnapshot", "ChainSnapshot"),
        ("SunSpecCloneSourceViews", "CloneSourceViews"),
        ("SunSpecCommonModel", "CommonModel"),
        ("SunSpecDecimal", "Decimal"),
        ("SunSpecDecodedChain", "DecodedChain"),
        ("SunSpecDecodedModel", "DecodedModel"),
        ("SunSpecDecoderKey", "DecoderKey"),
        ("SunSpecDecoderRegistry", "DecoderRegistry"),
        ("SunSpecDiscoveryRequest", "DiscoveryRequest"),
        ("SunSpecEndModelID", "EndModelID"),
        ("SunSpecFact", "Fact"),
        ("SunSpecFactPath", "FactPath"),
        ("SunSpecFactPathSegment", "FactPathSegment"),
        ("SunSpecInverterModel", "InverterModel"),
        ("SunSpecModelOccurrence", "ModelOccurrence"),
        ("SunSpecModelsRevisionV1", "ModelsRevisionV1"),
        ("SunSpecModelsRevisionV2", "ModelsRevisionV2"),
        ("SunSpecOccurrence", "Occurrence"),
        ("SunSpecOccurrenceSourceRange", "OccurrenceSourceRange"),
        ("SunSpecPhaseOneActivation", "PhaseOneActivation"),
        ("SunSpecPhaseOneCapture", "PhaseOneCapture"),
        ("SunSpecPhaseOneChain", "PhaseOneChain"),
        ("SunSpecPhaseOneDecoder", "PhaseOneDecoder"),
        ("SunSpecPhaseOneModel", "PhaseOneModel"),
        ("SunSpecPhaseOneObservation", "PhaseOneObservation"),
        ("SunSpecPhaseOneVersions", "PhaseOneVersions"),
        ("SunSpecPointType", "PointType"),
        ("SunSpecReadHeader", "ReadHeader"),
        ("SunSpecReadPayload", "ReadPayload"),
        ("SunSpecReadPurpose", "ReadPurpose"),
        ("SunSpecReadRequest", "ReadRequest"),
        ("SunSpecReadSignature", "ReadSignature"),
        ("SunSpecSameProvenance", "SameProvenance"),
        ("SunSpecScaledValue", "ScaledValue"),
        ("SunSpecSchemaRevision", "SchemaRevision"),
        ("SunSpecSourceSpan", "SourceSpan"),
        ("SunSpecStructuralProjection", "StructuralProjection"),
        ("SunSpecThreePhaseMonitoringCapabilityID", "ThreePhaseMonitoringCapabilityID"),
        ("SunSpecThreePhaseMonitoringFieldIDs", "ThreePhaseMonitoringFieldIDs"),
        ("SunSpecTopology", "Topology"),
        ("SunSpecTopologyNone", "TopologyNone"),
        ("SunSpecTopologySinglePhase", "TopologySinglePhase"),
        ("SunSpecTopologySplitPhase", "TopologySplitPhase"),
        ("SunSpecTopologyThreePhase", "TopologyThreePhase"),
        ("SunSpecTypeAccumulator32", "TypeAccumulator32"),
        ("SunSpecTypeAccumulator64", "TypeAccumulator64"),
        ("SunSpecTypeBitfield16", "TypeBitfield16"),
        ("SunSpecTypeBitfield32", "TypeBitfield32"),
        ("SunSpecTypeCount", "TypeCount"),
        ("SunSpecTypeEnum16", "TypeEnum16"),
        ("SunSpecTypeFloat32", "TypeFloat32"),
        ("SunSpecTypeInt16", "TypeInt16"),
        ("SunSpecTypeInt32", "TypeInt32"),
        ("SunSpecTypePad", "TypePad"),
        ("SunSpecTypeScaleFactor", "TypeScaleFactor"),
        ("SunSpecTypeString", "TypeString"),
        ("SunSpecTypeUint16", "TypeUint16"),
        ("SunSpecTypeUint32", "TypeUint32"),
        ("SunSpecTypeUint64", "TypeUint64"),
        ("SunSpecUnsignedDecimal", "UnsignedDecimal"),
        ("SunSpecValue", "Value"),
        ("SunSpecValueInvalidEncoding", "ValueInvalidEncoding"),
        ("SunSpecValueNotAccumulated", "ValueNotAccumulated"),
        ("SunSpecValueNotImplemented", "ValueNotImplemented"),
        ("SunSpecValueState", "ValueState"),
        ("SunSpecValueValid", "ValueValid"),
        ("SunSpecVendorDecoderNamespace", "VendorDecoderNamespace"),
        ("SunSpecWireKey", "WireKey"),
    ),
    "fronius": (
        ("FroniusEvaluateObservedFlavor", "EvaluateObservedFlavor"),
        ("FroniusEvaluateObservedFlavorV11", "EvaluateObservedFlavorV11"),
        ("FroniusEvaluateObservedFlavorV12", "EvaluateObservedFlavorV12"),
        ("FroniusOfflineProvider", "OfflineProvider"),
        ("FroniusOfflineReadRequest", "OfflineReadRequest"),
        ("FroniusOfflineReadResult", "OfflineReadResult"),
        ("FroniusOfflineReader", "OfflineReader"),
        ("FroniusOfflineRequest", "OfflineRequest"),
        ("FroniusRetainedAttempt", "RetainedAttempt"),
        ("FroniusRetainedCancelled", "RetainedCancelled"),
        ("FroniusRetainedCommit", "RetainedCommit"),
        ("FroniusRetainedEnvelope", "RetainedEnvelope"),
        ("FroniusRetainedFailed", "RetainedFailed"),
        ("FroniusRetainedKey", "RetainedKey"),
        ("FroniusRetainedOutcome", "RetainedOutcome"),
        ("FroniusRetainedPending", "RetainedPending"),
        ("FroniusRetainedSucceeded", "RetainedSucceeded"),
        ("FroniusRetainedToken", "RetainedToken"),
        ("FroniusRetainedView", "RetainedView"),
        ("FroniusSelectObservedFlavor", "SelectObservedFlavor"),
    ),
    "outback": (
        ("NewOutBackAXSReadOnlyDecoder", "NewAXSReadOnlyDecoder"),
        ("NewOutBackDetachedRecord", "NewDetachedRecord"),
        ("NewOutBackOfflineProvider", "NewOfflineProvider"),
        ("OutBackAXSApplicationNoteRev5InterfaceCandidateWords", "AXSApplicationNoteRev5InterfaceCandidateWords"),
        ("OutBackAXSChargeModelWords", "AXSChargeModelWords"),
        ("OutBackAXSInterfaceModelWords", "AXSInterfaceModelWords"),
        ("OutBackAXSModelChargeControl", "AXSModelChargeControl"),
        ("OutBackAXSModelInterface", "AXSModelInterface"),
        ("OutBackAXSOwnerManualRevCInterfaceCandidateWords", "AXSOwnerManualRevCInterfaceCandidateWords"),
        ("OutBackAXSReadOnlyDecoder", "AXSReadOnlyDecoder"),
        ("OutBackAXSReadOnlyFlavorDecision", "AXSReadOnlyFlavorDecision"),
        ("OutBackAXSReadOnlyFlavorID", "AXSReadOnlyFlavorID"),
        ("OutBackAXSReadOnlyFlavorReason", "AXSReadOnlyFlavorReason"),
        ("OutBackAXSReadOnlyFlavorReasonCommonNotAdmitted", "AXSReadOnlyFlavorReasonCommonNotAdmitted"),
        ("OutBackAXSReadOnlyFlavorReasonInterfaceAbsent", "AXSReadOnlyFlavorReasonInterfaceAbsent"),
        ("OutBackAXSReadOnlyFlavorReasonInterfaceAmbiguous", "AXSReadOnlyFlavorReasonInterfaceAmbiguous"),
        ("OutBackAXSReadOnlyFlavorReasonInterfaceWrongLength", "AXSReadOnlyFlavorReasonInterfaceWrongLength"),
        ("OutBackAXSReadOnlyFlavorReasonInvalidChain", "AXSReadOnlyFlavorReasonInvalidChain"),
        ("OutBackAXSReadOnlyFlavorReasonMatched", "AXSReadOnlyFlavorReasonMatched"),
        ("OutBackAXSReadOnlyFlavorReasonSourceNotQualified", "AXSReadOnlyFlavorReasonSourceNotQualified"),
        ("OutBackAssociationCurrent", "AssociationCurrent"),
        ("OutBackAssociationDisposition", "AssociationDisposition"),
        ("OutBackAssociationHistorical", "AssociationHistorical"),
        ("OutBackDetachedOccurrence", "DetachedOccurrence"),
        ("OutBackDetachedRecord", "DetachedRecord"),
        ("OutBackDetachedRecordInput", "DetachedRecordInput"),
        ("OutBackDiagnosticSupplier", "DiagnosticSupplier"),
        ("OutBackOfflineAttempt", "OfflineAttempt"),
        ("OutBackOfflineCanceled", "OfflineCanceled"),
        ("OutBackOfflineFailed", "OfflineFailed"),
        ("OutBackOfflineIdle", "OfflineIdle"),
        ("OutBackOfflineImported", "OfflineImported"),
        ("OutBackOfflineIncomplete", "OfflineIncomplete"),
        ("OutBackOfflineProvider", "OfflineProvider"),
        ("OutBackOfflineRetired", "OfflineRetired"),
        ("OutBackOfflineRunning", "OfflineRunning"),
        ("OutBackOfflineSnapshot", "OfflineSnapshot"),
        ("OutBackOfflineStatus", "OfflineStatus"),
        ("OutBackSourceAssociation", "SourceAssociation"),
        ("OutBackWordSpan", "WordSpan"),
    ),
    "detection": (
        ("DetectionAmbiguous", "Ambiguous"),
        ("DetectionCandidate", "Candidate"),
        ("DetectionCandidateEvidence", "CandidateEvidence"),
        ("DetectionCandidateSpec", "CandidateSpec"),
        ("DetectionDecision", "Decision"),
        ("DetectionLimits", "Limits"),
        ("DetectionMatched", "Matched"),
        ("DetectionNoMatch", "NoMatch"),
        ("DetectionOptions", "Options"),
        ("DetectionOutcome", "Outcome"),
        ("DetectionReason", "Reason"),
        ("DetectionReasonCandidateDisabled", "ReasonCandidateDisabled"),
        ("DetectionReasonContextCancelled", "ReasonContextCancelled"),
        ("DetectionReasonEqualBest", "ReasonEqualBest"),
        ("DetectionReasonFirmwareMismatch", "ReasonFirmwareMismatch"),
        ("DetectionReasonFixtureOptInRequired", "ReasonFixtureOptInRequired"),
        ("DetectionReasonIdentityMismatch", "ReasonIdentityMismatch"),
        ("DetectionReasonIncompleteProbe", "ReasonIncompleteProbe"),
        ("DetectionReasonInvalidIdentity", "ReasonInvalidIdentity"),
        ("DetectionReasonInvalidProbeResult", "ReasonInvalidProbeResult"),
        ("DetectionReasonLowerScore", "ReasonLowerScore"),
        ("DetectionReasonMultipleMatches", "ReasonMultipleMatches"),
        ("DetectionReasonProbeException", "ReasonProbeException"),
        ("DetectionReasonProfileDefaultOff", "ReasonProfileDefaultOff"),
        ("DetectionReasonProfileRevoked", "ReasonProfileRevoked"),
        ("DetectionReasonProfileSuperseded", "ReasonProfileSuperseded"),
        ("DetectionReasonProfileUnqualified", "ReasonProfileUnqualified"),
        ("DetectionReasonReadError", "ReasonReadError"),
        ("DetectionReasonSelected", "ReasonSelected"),
        ("NewDetectionCandidate", "NewCandidate"),
    ),
    "ledger": (
        ("LedgerAuditAttempt", "AuditAttempt"),
        ("LedgerAuditClaim", "AuditClaim"),
        ("LedgerAuditObjectKind", "AuditObjectKind"),
        ("LedgerAuditTombstone", "AuditTombstone"),
        ("LedgerRestartState", "RestartState"),
        ("LedgerSnapshot", "Snapshot"),
    ),
}

# Names that kept their spelling and changed only the declaring package; each
# was declared in the root package before the move and is declared by exactly
# this family package after it.
QUALIFIER_ONLY: dict[str, tuple[str, ...]] = {
    "growatt": (
        "CheckGrowattDiagnosticCommit",
        "CompareGrowattFC04Record",
        "DecodeGrowattBMSMultiGroupDiagnostic",
        "DecodeGrowattBMSReadOnlyObservation",
        "DecodeGrowattBMSTypedReadOnlyStatus",
        "DecodeGrowattProtocolIIFC04Telemetry",
        "DecodeGrowattProtocolIIIdentity",
        "ErrGrowattBMSRS485RTUObserverInvalid",
        "ErrGrowattFC04Ambiguous",
        "ErrGrowattFC04Invalid",
        "ErrGrowattFC04Missing",
        "ErrGrowattProtocolIIFC04RTUObserverInvalid",
        "ErrGrowattProtocolIIIdentityRTUObserverInvalid",
        "I23GrowattAdapter",
        "I23GrowattDiagnostic",
        "I23GrowattLink",
        "I23GrowattProvider",
        "I23GrowattRequest",
        "I23GrowattResult",
        "I23GrowattSnapshot",
        "I23NewGrowattProvider",
        "ParseGrowattFC04Record",
        "ValidateGrowattFC04RecordAdmission",
    ),
    "tesla": (
        "BuildTeslaFC100OperationRequest",
        "BuildTeslaHSCNativeRequest",
        "ClassifyTeslaHSCException",
        "DecodeTeslaFC100CommonSystemInfoReplay",
        "DecodeTeslaFC100OperationSequence",
        "DecodeTeslaFC100TEDAPI",
        "DecodeTeslaFC100WCLifetimeReplay",
        "DecodeTeslaFC100WCLifetimeReplaySequence",
        "DecodeTeslaFC100WCLoadSharingStateReplay",
        "DecodeTeslaFC100WCLoadSharingStateReplaySequence",
        "DecodeTeslaFC100WCSystemInfoReplay",
        "DecodeTeslaFC100WCSystemInfoReplaySequence",
        "DecodeTeslaFC100WCVitalsReplay",
        "DecodeTeslaHSCEnvelope",
        "DecodeTeslaHSCExchange",
        "DecodeTeslaHSCNativeRecord",
        "DecodeTeslaHSCOpaqueTerminalProvenance",
        "DecodeTeslaHSCRequestEnvelope",
        "DecodeTeslaHSCResponse",
        "DecodeTeslaLegacyWallConnectorFrame",
        "EncodeTeslaLegacyWallConnectorFrame",
        "ErrTeslaGen3HSCActivationFailed",
        "ErrTeslaGen3ReadAcquisitionBusy",
        "ErrTeslaGen3ReadAcquisitionClosed",
        "ErrTeslaGen3ReadAcquisitionUnavailable",
        "OpenTeslaGen3ReadProductionEndpoint",
        "ValidateTeslaFC100RequestMatrix",
    ),
    "huawei": (
        "DecideHuaweiGatewaySelection",
        "DecideHuaweiSUN2000CatalogSelection",
        "DecideSDongleRoute",
        "EmmaCommitContextRejected",
        "EmmaCommitIdentityCommitted",
        "EmmaCommitIdentityMismatch",
        "EmmaCommitIdentityUnavailable",
        "EmmaCommitReason",
        "EmmaFC03Complete",
        "EmmaFC03Failed",
        "EmmaFC03Missing",
        "EmmaFC03Status",
        "EmmaIdentityAttempt",
        "EmmaIdentityContext",
        "EmmaIdentityInput",
        "EmmaIdentityResult",
        "EmmaInventoryBlocked",
        "EmmaInventoryNotRequested",
        "EmmaInventoryStatus",
        "EmmaMEIFailed",
        "EmmaMEINotRequested",
        "EmmaMEIStatus",
        "EmmaMEISucceeded",
        "EmmaOfflineBounds",
        "EmmaOfflineIdentityReader",
        "EmmaOfflineProvider",
        "EmmaRetainedIdentity",
        "ErrHuaweiProviderBusy",
        "ErrHuaweiProviderClosed",
        "ErrHuaweiProviderClosing",
        "ErrHuaweiProviderConfig",
        "ErrHuaweiProviderEarly",
        "ErrHuaweiProviderFenced",
        "ErrHuaweiProviderNotAdmitted",
        "ErrHuaweiProviderNotStarted",
        "ErrHuaweiProviderPacing",
        "ErrHuaweiProviderStarted",
        "ErrHuaweiSUN2000MonitoringObserverInvalid",
        "ErrI21Association",
        "ErrI21Busy",
        "ErrI21Retired",
        "ErrSDongleOfflineAssociation",
        "ErrSDongleOfflineInput",
        "ErrSmartloggerInventoryBusy",
        "ErrSmartloggerInventoryClosed",
        "ErrSmartloggerInventoryContext",
        "ErrSmartloggerOfflineProviderInput",
        "EvaluateHuaweiEMMAOfflineIdentity",
        "EvaluateHuaweiProviderEnablement",
        "EvaluateHuaweiSDongleOfflineObservation",
        "I13Evidence",
        "I13Identity",
        "I13NewProvider",
        "I13Port",
        "I13Provider",
        "I13Record",
        "I13Request",
        "I13Result",
        "I13Supplier",
        "I15LunaProvider",
        "I15LunaProviderPort",
        "I15LunaSupplier",
        "I15LunaView",
        "I15NewLunaProvider",
        "I17NewSmartloggerProvider",
        "I17SmartloggerProvider",
        "I17SmartloggerProviderAdapter",
        "I19EmmaProvider",
        "I19EmmaProviderConfig",
        "I19EmmaProviderPort",
        "I19NewEmmaProvider",
        "I21NewSDonglePort",
        "I21SDonglePort",
        "I21SDongleProvider",
        "LunaDocumentaryPresence",
        "LunaIdentityChanged",
        "LunaInventoryOnly",
        "LunaMalformedEvidence",
        "LunaNoObservation",
        "LunaOptionalBinding",
        "LunaOptionalEvent",
        "LunaOptionalEvidence",
        "LunaOptionalFailure",
        "LunaOptionalHistory",
        "LunaOptionalIdentity",
        "LunaOptionalInput",
        "LunaOptionalPolicy",
        "LunaOptionalState",
        "LunaOptionalStatus",
        "LunaPresentUnqualified",
        "LunaTemporarilyUnreadable",
        "LunaUnknown",
        "NewEmmaIdentityAttempt",
        "NewEmmaOfflineProvider",
        "NewSmartloggerInventoryCommitOwner",
        "NewSmartloggerOfflineProvider",
        "ParseHuaweiEMMAOfflineInventory",
        "ParseHuaweiSmartLoggerOfflineInventory",
        "ParseSun2000EvidenceInput",
        "ReduceLunaOptionalState",
        "ResolveHuaweiQualification",
        "RetainHuaweiSUN2000Monitoring",
        "RunSDongleOfflineProvider",
        "SDongleBasicMEI",
        "SDongleChild",
        "SDongleChildRoute",
        "SDongleExtendedMEI",
        "SDongleGateway",
        "SDongleGatewayFC03",
        "SDongleNoSend",
        "SDongleOfflineBlocked",
        "SDongleOfflineBounds",
        "SDongleOfflineDenied",
        "SDongleOfflineLookup",
        "SDongleOfflineReady",
        "SDongleOfflineRecord",
        "SDongleOfflineRequest",
        "SDongleOfflineResult",
        "SDongleOfflineState",
        "SDonglePlanned",
        "SDonglePrivateFunction",
        "SDongleRequestClass",
        "SDongleRouteBounds",
        "SDongleRouteDecision",
        "SDongleRouteEvidence",
        "SDongleRouteKey",
        "SDongleRouteOutcome",
        "SDongleRoutePlan",
        "SDongleRouteReference",
        "SDongleRouteRole",
        "SDongleRouteSelection",
        "SDongleWithheld",
        "SelectHuaweiSUN2000MonitoringProfile",
        "SmartloggerInventoryAttempt",
        "SmartloggerInventoryCancelled",
        "SmartloggerInventoryClosed",
        "SmartloggerInventoryCommitOwner",
        "SmartloggerInventoryComplete",
        "SmartloggerInventoryContext",
        "SmartloggerInventoryFailed",
        "SmartloggerInventoryIdentity",
        "SmartloggerInventoryOutcome",
        "SmartloggerInventoryPlan",
        "SmartloggerInventoryResult",
        "SmartloggerInventoryRetired",
        "SmartloggerInventorySnapshot",
        "SmartloggerOfflineInventoryLimits",
        "SmartloggerOfflineInventoryReader",
        "SmartloggerOfflineProvider",
        "Sun2000EvidenceAlternative",
        "Sun2000EvidenceCandidate",
        "Sun2000EvidenceInput",
        "Sun2000EvidenceInputContract",
        "Sun2000EvidenceInputLimits",
        "Sun2000EvidencePresence",
        "Sun2000EvidenceReference",
        "Sun2000MapEvidence",
        "Sun2000UnitEvidence",
    ),
    "sunspec": (
        "MaxSunSpecPhaseOneChainWords",
        "NewStandardSunSpecDecoderRegistry",
        "NewSunSpecOccurrenceSourceRangeFromSpans",
        "NewSunSpecPhaseOneProfile",
        "ProjectSunSpecStructuralFacts",
    ),
    "fronius": (
        "I25FroniusAdapter",
        "I25FroniusLifetime",
        "I25FroniusProvider",
        "I25FroniusView",
        "I25NewFroniusAdapter",
        "NewSunSpecFroniusQualificationCard",
        "NewSunSpecQualificationObservation",
        "SunSpecFroniusFlavorDecision",
        "SunSpecFroniusFlavorReason",
        "SunSpecFroniusFlavorReasonAmbiguousSource",
        "SunSpecFroniusFlavorReasonCapabilityNotAdmitted",
        "SunSpecFroniusFlavorReasonChainMismatch",
        "SunSpecFroniusFlavorReasonCommonIdentityMismatch",
        "SunSpecFroniusFlavorReasonFirmwareMismatch",
        "SunSpecFroniusFlavorReasonMatched",
        "SunSpecFroniusFlavorSelection",
        "SunSpecFroniusFlavorSelectionReason",
        "SunSpecFroniusFlavorSelectionReasonAmbiguousMatch",
        "SunSpecFroniusFlavorSelectionReasonMatched",
        "SunSpecFroniusFlavorSelectionReasonNoMatch",
        "SunSpecFroniusObservedFlavorID",
        "SunSpecFroniusObservedFlavorV11ID",
        "SunSpecFroniusObservedFlavorV12ID",
        "SunSpecFroniusQualificationCard",
        "SunSpecFroniusQualificationCardID",
        "SunSpecFroniusQualificationExpectedResult",
        "SunSpecQualificationObservation",
        "SunSpecQualificationReplay",
        "SunSpecQualificationSampleIdentity",
    ),
    "outback": (
        "ErrI27OutBackBusy",
        "ErrOutBackAXSSourceNotQualified",
        "ErrOutBackDetachedRecordBound",
        "ErrOutBackOfflineAssociation",
        "ErrOutBackOfflineInput",
        "ErrOutBackOfflineRetired",
        "ErrOutBackOfflineSuperseded",
        "I27NewOutBackAdapter",
        "I27OutBackAdapter",
        "I27OutBackConfig",
        "I27OutBackProvider",
    ),
    "detection": (
        "DefaultDetectionLimits",
        "ErrFixtureConformanceNondeterministic",
        "FirmwareGateSpec",
        "FixtureConformanceCorpus",
        "FixtureConformanceCorpusSpec",
        "FixtureConformanceLimits",
        "FixtureConformanceRecord",
        "FixtureConformanceRecordSpec",
        "FixtureConformanceReport",
        "FixtureDependencyFact",
        "FixtureDetectionCaseSpec",
        "FixtureDetectionEvidenceExpectation",
        "FixtureDetectionExpectation",
        "FixtureDetectionResult",
        "FixtureDetectorDeclarationSpec",
        "FixtureDetectorInput",
        "FixtureLogicalProvenance",
        "FixtureLogicalSlice",
        "FixtureMutationReason",
        "FixtureMutationReasonCaseFolded",
        "FixtureMutationReasonContradictory",
        "FixtureMutationReasonDuplicate",
        "FixtureMutationReasonMalformed",
        "FixtureMutationReasonMissing",
        "FixtureMutationReasonOversized",
        "FixtureMutationReasonUnknown",
        "FixtureMutationReasonUnsanitized",
        "FixtureProbeInput",
        "FixtureQualificationDisposition",
        "FixtureQualificationDispositionQualified",
        "FixtureQualificationDispositionRejected",
        "FixtureQualificationExpectation",
        "FixtureReplay",
        "FixtureReplayAccepted",
        "FixtureReplayActual",
        "FixtureReplayExpectation",
        "FixtureReplayOutcome",
        "FixtureReplayReason",
        "FixtureReplayReasonAccepted",
        "FixtureReplayReasonCoherenceMismatch",
        "FixtureReplayReasonDeadlineMismatch",
        "FixtureReplayReasonGenerationMismatch",
        "FixtureReplayReasonNormalizationMismatch",
        "FixtureReplayReasonObservationRejected",
        "FixtureReplayReasonSourceMismatch",
        "FixtureReplayReasonTableAccessMismatch",
        "FixtureReplayReasonTornRead",
        "FixtureReplayReasonUnitMismatch",
        "FixtureReplayRejected",
        "FixtureReplayResult",
        "FixtureReplayer",
        "FixtureSampleProvenance",
        "FixtureWireProvenance",
        "IdentityStringGateSpec",
        "IsFixtureMutationReason",
        "MarshalDetectionDecision",
        "MarshalFixtureConformanceCorpusSpec",
        "NewFixtureConformanceCorpus",
        "NewFixtureConformanceCorpusWithLimits",
        "NewFixtureReplayer",
        "NewProbePlan",
        "NewProbeReadResult",
        "NewProfileDetector",
        "ProbeDeclarationSpec",
        "ProbeFunction",
        "ProbeIdentityASCII",
        "ProbeIdentityEncoding",
        "ProbeIdentityField",
        "ProbeIdentityFirmware",
        "ProbeIdentityManufacturer",
        "ProbeIdentityModel",
        "ProbePlan",
        "ProbePlanSpec",
        "ProbeReadException",
        "ProbeReadHoldingRegisters",
        "ProbeReadInputRegisters",
        "ProbeReadRequest",
        "ProbeReadResult",
        "ProbeReadResultSpec",
        "ProbeReadStatus",
        "ProbeReadSucceeded",
        "ProbeReader",
        "ProfileDetector",
        "ProfileDetectorSpec",
        "SanitizedFixtureMetadata",
        "UnmarshalDetectionDecision",
        "UnmarshalFixtureConformanceCorpus",
    ),
    "ledger": (
        "AttemptCancelFailed",
        "AttemptCancelled",
        "AttemptCancelling",
        "AttemptOpen",
        "AttemptPhase",
        "AttemptPublishFailed",
        "AttemptPublished",
        "AttemptPublishing",
        "AttemptSealed",
        "CancellationAlreadyPublished",
        "CancellationCompleted",
        "CancellationFailed",
        "CancellationPublishFailed",
        "CancellationResult",
        "CapabilityCancelled",
        "CapabilityExpired",
        "CapabilityFailed",
        "ClaimAttemptCancelled",
        "ClaimOutcome",
        "ClaimRejectedTerminal",
        "ClaimSucceeded",
        "CompletionDisposition",
        "CompletionDispositionOverlayRequired",
        "CompletionDispositionStandardOnly",
        "CompletionEvidenceRefs",
        "CompletionRecord",
        "CompletionRecordSpec",
        "DefaultTerminalCommitTimeout",
        "EmptySampleLedgerState",
        "FMV3M303CompletionSchemaID",
        "FMV3M303CompletionSchemaVersion",
        "MarshalFMV3M303CompletionRecord",
        "MarshalSampleLedgerState",
        "NewCurrentFMV3M303CompletionRecord",
        "NewFMV3M303CompletionRecord",
        "NewObservationFactory",
        "NewObservationFactoryWithOptions",
        "NewSampleLedger",
        "NewSampleLedgerFromRestart",
        "ObservationAttempt",
        "ObservationFactory",
        "ObservationFactoryOptions",
        "PublicationCommitCancelled",
        "PublicationCommitCommitted",
        "PublicationCommitDecision",
        "PublicationCommitRequest",
        "PublicationCommitter",
        "PublishedAttemptV1",
        "RuntimeAttemptRequest",
        "RuntimeObservationFacts",
        "SampleLedger",
        "SampleLedgerState",
        "TerminalStateCommitRequest",
        "UnmarshalFMV3M303CompletionRecord",
        "UnmarshalSampleLedgerState",
    ),
}


class AdoptionError(Exception):
    """A table, file or import that the rewrite cannot handle safely."""


def build_mapping() -> dict[str, tuple[str, str]]:
    """Return previous root name -> (package, current name) for every table row."""
    mapping: dict[str, tuple[str, str]] = {}
    current_owner: dict[tuple[str, str], str] = {}
    families = set(RENAMED) | set(QUALIFIER_ONLY)
    for family in sorted(families):
        rows = list(RENAMED.get(family, ())) + [(name, name) for name in QUALIFIER_ONLY.get(family, ())]
        for previous, current in rows:
            if previous in mapping:
                raise AdoptionError(f"{previous} appears in two tables ({mapping[previous][0]}, {family})")
            key = (family, current)
            if key in current_owner:
                raise AdoptionError(f"{family}.{current} is the current name of {current_owner[key]} and {previous}")
            current_owner[key] = previous
            mapping[previous] = (family, current)
    return mapping


def current_names(mapping: dict[str, tuple[str, str]]) -> dict[str, frozenset[str]]:
    names: dict[str, set[str]] = {}
    for family, current in mapping.values():
        names.setdefault(family, set()).add(current)
    return {family: frozenset(values) for family, values in names.items()}


# ---------------------------------------------------------------------------
# Go source segmentation


def interpreted_literal_end(source: str, index: int) -> int:
    """End offset of the interpreted string or rune literal that starts at index."""
    quote = source[index]
    length = len(source)
    end = index + 1
    while end < length and source[end] != quote:
        if source[end] == "\\":
            end += 1
        elif source[end] == "\n":
            raise AdoptionError("newline in interpreted literal")
        end += 1
    if end >= length:
        raise AdoptionError("unterminated literal")
    return end + 1


def closing_end(source: str, index: int, opener: int, closer: str, problem: str) -> int:
    end = source.find(closer, index + opener)
    if end < 0:
        raise AdoptionError(problem)
    return end + len(closer)


def segment_at(source: str, index: int) -> tuple[str, int] | None:
    """Kind and end offset of a comment or literal starting at index, else None."""
    if source.startswith("//", index):
        end = source.find("\n", index)
        return "comment", len(source) if end < 0 else end
    if source.startswith("/*", index):
        return "comment", closing_end(source, index, 2, "*/", "unterminated block comment")
    char = source[index]
    if char == "`":
        return "string", closing_end(source, index, 1, "`", "unterminated raw string")
    if char in "\"'":
        return "string", interpreted_literal_end(source, index)
    return None


def segments(source: str) -> list[tuple[str, str]]:
    """Split Go source into ("code" | "comment" | "string", text) segments."""
    out: list[tuple[str, str]] = []
    code_start = 0
    index = 0
    length = len(source)
    while index < length:
        found = segment_at(source, index)
        if found is None:
            index += 1
            continue
        kind, end = found
        if index > code_start:
            out.append(("code", source[code_start:index]))
        out.append((kind, source[index:end]))
        index = end
        code_start = end
    if length > code_start:
        out.append(("code", source[code_start:length]))
    return out


def code_only(source: str) -> str:
    """Source with comments and literals blanked, offsets preserved per line."""
    parts = []
    for kind, text in segments(source):
        if kind == "code":
            parts.append(text)
        else:
            parts.append(re.sub(r"[^\n]", " ", text))
    return "".join(parts)


# ---------------------------------------------------------------------------
# Imports

IMPORT_SPEC = re.compile(r'^(?P<indent>[ \t]*)(?:(?P<name>[A-Za-z_][\w]*|\.)[ \t]+)?"(?P<path>[^"]+)"[ \t]*$')
SINGLE_IMPORT = re.compile(r'^import[ \t]+(?:(?P<name>[A-Za-z_][\w]*|\.)[ \t]+)?"(?P<path>[^"]+)"[ \t]*$', re.M)
BLOCK_IMPORT = re.compile(r"^import[ \t]*\(\n(?P<body>.*?)^\)", re.M | re.S)


def is_modbusreg_path(path: str) -> bool:
    return path == ROOT_IMPORT or path.startswith(ROOT_IMPORT + "/")


def find_imports(source: str) -> list[dict]:
    """Every modbusreg import spec with its local name and location."""
    blanked = code_only(source)
    found = []
    for spec in SINGLE_IMPORT.finditer(source):
        if not blanked.startswith("import", spec.start()):
            continue
        if is_modbusreg_path(spec.group("path")):
            found.append({"form": "single", "start": spec.start(), "end": spec.end(), "name": spec.group("name"), "path": spec.group("path")})
    for block in BLOCK_IMPORT.finditer(blanked):
        offset = block.start("body")
        for raw_line in source[offset : block.end("body")].splitlines(keepends=True):
            line = raw_line.rstrip("\n")
            spec = IMPORT_SPEC.match(line.split("//", 1)[0].rstrip()) if line.strip() else None
            if spec and is_modbusreg_path(spec.group("path")):
                found.append(
                    {
                        "form": "block",
                        "start": offset,
                        "end": offset + len(raw_line),
                        "name": spec.group("name"),
                        "path": spec.group("path"),
                        "indent": spec.group("indent"),
                        "block_start": block.start(),
                        "block_end": block.end(),
                    }
                )
            offset += len(raw_line)
    return found


def local_name(spec: dict) -> str:
    if spec["name"]:
        return spec["name"]
    if spec["path"] == ROOT_IMPORT:
        return DEFAULT_ROOT_NAME
    return spec["path"].rsplit("/", 1)[1]


# ---------------------------------------------------------------------------
# Rewrite of one Go file

IDENT = r"[A-Za-z_]\w*"
# Start of a qualified identifier: not inside a name or a selector chain, but
# allowed right after a variadic "...".
START = r"(?:(?<=\.\.\.)|(?<![\w.]))"


def root_import_name(imports: list[dict]) -> tuple[str | None, list[str]]:
    """Local name of the single root import, or the problems that block a rewrite."""
    roots = [spec for spec in imports if spec["path"] == ROOT_IMPORT]
    if not roots:
        return None, []
    if len(roots) > 1:
        return None, ["the root package is imported more than once"]
    root_name = local_name(roots[0])
    if root_name in {"_", "."}:
        return None, [f"the root package is imported as {root_name!r}"]
    return root_name, []


def method_rewriter(root_name: str):
    """Pattern and replacement turning a moved registry method into a function call."""
    pattern = re.compile(
        rf"{START}(?P<recv>{IDENT}(?:\.{IDENT})*)\.(?P<method>"
        + "|".join(re.escape(method) for method, _ in METHOD_TO_FUNCTION)
        + r")\((?P<space>\s*)(?P<close>\)?)"
    )
    functions = dict(METHOD_TO_FUNCTION)

    def method_call(match: re.Match) -> str:
        receiver = match.group("recv")
        if receiver.split(".", 1)[0] == root_name:
            return match.group(0)
        head = f"{root_name}.{functions[match.group('method')]}({receiver}"
        return head + (")" if match.group("close") else ", ")

    return pattern, method_call


def rewrite_segments(source: str, root_name: str, mapping: dict[str, tuple[str, str]], relative: str) -> tuple[str, set[str]]:
    """Rewrite qualifiers in code and comments; return the text and the families used in code."""
    method_pattern, method_call = method_rewriter(root_name)
    name_pattern = re.compile(rf"{START}{re.escape(root_name)}\.(?P<name>{IDENT})\b")
    introduced: set[str] = set()

    def qualified(match: re.Match, in_code: bool) -> str:
        target = mapping.get(match.group("name"))
        if target is None:
            return match.group(0)
        if in_code:
            introduced.add(target[0])
        return f"{IMPORT_NAMES.get((relative, target[0]), target[0])}.{target[1]}"

    rewritten = []
    for kind, text in segments(source):
        if kind == "code":
            text = method_pattern.sub(method_call, text)
            text = name_pattern.sub(lambda match: qualified(match, True), text)
        elif kind == "comment":
            text = name_pattern.sub(lambda match: qualified(match, False), text)
        rewritten.append(text)
    return "".join(rewritten), introduced


def rewrite_go(source: str, mapping: dict[str, tuple[str, str]], relative: str = "") -> tuple[str, list[str]]:
    """Return the rewritten source and the problems that block a safe rewrite."""
    imports = find_imports(source)
    root_name, problems = root_import_name(imports)
    if root_name is None:
        return source, problems
    result, introduced = rewrite_segments(source, root_name, mapping, relative)
    if result == source:
        return source, []

    blanked = code_only(result)
    root_used = re.search(rf"{START}{re.escape(root_name)}\.{IDENT}", blanked) is not None
    existing = {spec["path"] for spec in imports if spec["path"] != ROOT_IMPORT}
    wanted = sorted(existing | {f"{ROOT_IMPORT}/{family}" for family in introduced})
    return replace_imports(result, root_name, root_used, wanted, relative), []


def sub_import_spec(path: str, relative: str) -> str:
    name = IMPORT_NAMES.get((relative, path.rsplit("/", 1)[1]))
    return f'{name} "{path}"' if name else f'"{path}"'


def replace_imports(source: str, root_name: str, root_used: bool, sub_paths: list[str], relative: str = "") -> str:
    imports = find_imports(source)
    root = next(spec for spec in imports if spec["path"] == ROOT_IMPORT)
    root_line = f'{root_name} "{ROOT_IMPORT}"' if root["name"] else f'"{ROOT_IMPORT}"'
    specs = ([root_line] if root_used else []) + [sub_import_spec(path, relative) for path in sub_paths]
    if root["form"] == "single":
        if len(specs) == 1:
            replacement = f"import {specs[0]}"
        else:
            replacement = "import (\n" + "".join(f"\t{spec}\n" for spec in specs) + ")"
        return source[: root["start"]] + replacement + source[root["end"] :]
    in_block = [spec for spec in imports if spec["form"] == "block" and spec["block_start"] == root["block_start"]]
    indent = root["indent"]
    replacement = "".join(f"{indent}{spec}\n" for spec in specs)
    first = min(spec["start"] for spec in in_block)
    out = source
    for spec in sorted(in_block, key=lambda item: item["start"], reverse=True):
        insert = replacement if spec["start"] == first else ""
        out = out[: spec["start"]] + insert + out[spec["end"] :]
    return out


def check_go(source: str, mapping: dict[str, tuple[str, str]], names: dict[str, frozenset[str]], relative: str = "") -> list[str]:
    """Problems in an already rewritten file: moved names, old calls, unused imports."""
    rewritten, problems = rewrite_go(source, mapping, relative)
    if problems:
        return problems
    if rewritten != source:
        problems.append("a moved name or a replaced method call is left behind")
    blanked = code_only(source)
    imports = find_imports(source)
    if any(spec["path"] == ROOT_IMPORT for spec in imports):
        for method, _ in METHOD_TO_FUNCTION:
            if re.search(rf"\.{method}\(", blanked):
                problems.append(f"call of {method} remains")
    for spec in imports:
        name = local_name(spec)
        if spec["path"] == ROOT_IMPORT:
            used = re.search(rf"{START}{re.escape(name)}\.{IDENT}", blanked) is not None
        else:
            family = spec["path"].rsplit("/", 1)[1]
            declared = names.get(family, frozenset())
            used = any(match.group(1) in declared for match in re.finditer(rf"{START}{re.escape(name)}\.({IDENT})", blanked))
        if not used:
            problems.append(f'import "{spec["path"]}" is not used')
    return problems


# ---------------------------------------------------------------------------
# Documentation and source-text expectations


def rewrite_doc(text: str, mapping: dict[str, tuple[str, str]]) -> str:
    pattern = re.compile(rf"`(?:{re.escape(DEFAULT_ROOT_NAME)}\.)?(?P<name>{IDENT})`")

    def replace(match: re.Match) -> str:
        target = mapping.get(match.group("name"))
        return match.group(0) if target is None else f"`{target[0]}.{target[1]}`"

    return pattern.sub(replace, text)


def rewrite_expectations(relative: str, text: str) -> tuple[str, list[str]]:
    problems = []
    for path, previous, current in SOURCE_TEXT_EXPECTATIONS:
        if path != relative:
            continue
        if previous in text:
            text = text.replace(previous, current)
        elif current not in text:
            problems.append(f"expected source text {previous!r} not found")
    return text, problems


# ---------------------------------------------------------------------------
# Driver


def go_files(root: pathlib.Path) -> list[pathlib.Path]:
    found = []
    for path in sorted(root.rglob("*.go")):
        parts = path.relative_to(root).parts[:-1]
        if any(part.startswith(".") or part in SKIPPED_DIRECTORIES for part in parts):
            continue
        found.append(path)
    return found


def gofmt_paths(gofmt: str, flag: str, root: pathlib.Path, relatives: list[str]) -> list[str]:
    """Run gofmt with -w or -l on repository-relative paths; return what -l lists."""
    if not relatives:
        return []
    try:
        completed = subprocess.run([gofmt, flag, *relatives], cwd=root, capture_output=True, text=True, check=False)
    except OSError as error:
        raise AdoptionError(f"gofmt could not run: {error}") from error
    if completed.returncode != 0:
        raise AdoptionError(f"gofmt {flag} failed: {completed.stderr.strip()}")
    return [line for line in completed.stdout.splitlines() if line]


def check_go_file(relative: str, source: str, mapping: dict[str, tuple[str, str]], names: dict[str, frozenset[str]]) -> list[str]:
    """Problems of one already rewritten Go file, including stale source-text expectations."""
    problems = check_go(source, mapping, names, relative)
    expected, expectation_problems = rewrite_expectations(relative, source)
    if expected != source:
        problems.append("a source-text expectation quotes a moved call")
    return problems + expectation_problems


def rewrite_go_file(relative: str, source: str, mapping: dict[str, tuple[str, str]]) -> tuple[str, list[str]]:
    """Rewritten text of one Go file and the problems that block writing it."""
    rewritten, problems = rewrite_go(source, mapping, relative)
    rewritten, expectation_problems = rewrite_expectations(relative, rewritten)
    return rewritten, problems + expectation_problems


def process_go_files(root: pathlib.Path, check: bool, mapping: dict[str, tuple[str, str]]) -> tuple[list[str], list[str], list[str]]:
    """Return (changed or failing paths, problems, files importing modbusreg)."""
    names = current_names(mapping)
    changed: list[str] = []
    problems: list[str] = []
    importers: list[str] = []
    for path in go_files(root):
        relative = path.relative_to(root).as_posix()
        source = path.read_text(encoding="utf-8")
        if ROOT_IMPORT in source:
            importers.append(relative)
        try:
            if check:
                file_problems = check_go_file(relative, source, mapping, names)
                rewritten = source
            else:
                rewritten, file_problems = rewrite_go_file(relative, source, mapping)
        except AdoptionError as error:
            problems.append(f"{relative}: {error}")
            continue
        if file_problems:
            if check:
                changed.append(relative)
            problems += [f"{relative}: {problem}" for problem in file_problems]
            continue
        if rewritten != source:
            path.write_text(rewritten, encoding="utf-8")
            changed.append(relative)
    return changed, problems, importers


def format_go_files(gofmt: str, root: pathlib.Path, check: bool, changed: list[str], importers: list[str]) -> list[str]:
    """Check (-l) every importing file or format (-w) the rewritten ones; return problems."""
    if check:
        return [f"{relative}: not gofmt-formatted" for relative in gofmt_paths(gofmt, "-l", root, importers)]
    gofmt_paths(gofmt, "-w", root, [relative for relative in changed if relative.endswith(".go")])
    return []


def process_docs(root: pathlib.Path, check: bool, mapping: dict[str, tuple[str, str]]) -> tuple[list[str], list[str]]:
    """Rewrite or check the listed documentation pages; return (changed paths, problems)."""
    changed: list[str] = []
    problems: list[str] = []
    for relative in DOC_PAGES:
        path = root / relative
        if not path.is_file():
            problems.append(f"{relative}: documentation page not found")
            continue
        text = path.read_text(encoding="utf-8")
        rewritten = rewrite_doc(text, mapping)
        if rewritten == text:
            continue
        changed.append(relative)
        if check:
            problems.append(f"{relative}: a moved name is left behind")
        else:
            path.write_text(rewritten, encoding="utf-8")
    return changed, problems


def run(root: pathlib.Path, check: bool, gofmt: str | None = "gofmt") -> tuple[list[str], list[str]]:
    """Return (changed or failing paths, problems); gofmt None leaves formatting alone."""
    mapping = build_mapping()
    changed, problems, importers = process_go_files(root, check, mapping)
    if gofmt and not problems:
        problems += format_go_files(gofmt, root, check, changed, importers)
    doc_changed, doc_problems = process_docs(root, check, mapping)
    return changed + doc_changed, problems + doc_problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1], help="repository root")
    parser.add_argument("--check", action="store_true", help="rewrite nothing; fail when a moved name or unused import remains")
    parser.add_argument("--gofmt", default="gofmt", help="gofmt executable used to format and check the rewritten files")
    args = parser.parse_args(argv)
    try:
        changed, problems = run(args.root, args.check, args.gofmt)
    except AdoptionError as error:
        print(f"adopt_modbusreg_packages: {error}", file=sys.stderr)
        return 2
    for problem in problems:
        print(f"adopt_modbusreg_packages: {problem}", file=sys.stderr)
    if problems:
        return 1
    if args.check:
        print("adopt_modbusreg_packages: no moved name is left behind, every modbusreg import is used and gofmt-formatted")
    else:
        for relative in changed:
            print(f"rewritten {relative}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
