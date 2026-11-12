#!/usr/bin/env python3
"""Offline, token-aware adoption of the canbusreg provider export renames.

RENAMES is the exported subset of the canbusreg provider rename
(scripts/rename_provider_names.py in helianthus-canbusreg); only exported names
are reachable from the gateway. Identifier tokens and identifier words inside
comments are renamed; string, rune and raw-string literals stay unchanged.
Run without arguments to rewrite FILES; --check lists any remaining old name in
the repository's Go sources and fails.
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RENAMES = {
    "CX01ConfirmNativeSelection": "ConfirmNativeSelection",
    "CX01Decode": "DecodeLINKIdentity",
    "CX01FeatureGrade": "SelectionFeatureGrade",
    "CX01Grade": "SelectionGrade",
    "CX01Group": "SelectionGroup",
    "CX01LINKIdentityAssembler": "LINKIdentityAssembler",
    "CX01LINKIdentitySnapshot": "LINKIdentitySnapshot",
    "CX01LINKInput": "LINKInput",
    "CX01Layout": "SelectionLayout",
    "CX01NativeIndication": "SelectionNativeIndication",
    "CX01NewLINKIdentityAssembler": "NewLINKIdentityAssembler",
    "CX01SelectionSpec": "SelectionSpec",
    "CX01Snapshot": "SnapshotLINKIdentity",
    "CX01Spec": "SelectionSpecValue",
    "CX05DecodePCSStandard": "DecodePCSStandard",
    "CX05PCSInput": "PCSStandardPCSInput",
    "CX05PCSResult": "PCSStandardPCSResult",
    "CX05RetainAccepted": "RetainAccepted",
    "CX06DecodePCSGauge": "DecodePCSGauge",
    "CX06DecodePCSIdentity": "DecodePCSIdentity",
    "CX06PCSInput": "PCSIdentityPCSInput",
    "CX06SerialAssembler": "PCSIdentitySerialAssembler",
    "CX06SerialResult": "PCSIdentitySerialResult",
    "CX07DecodeDTC": "DecodeDTC",
    "CX07EncodeDTC": "EncodeDTC",
    "CX07ExplainLocalSelection": "ExplainLocalSelection",
    "CX07LocalSelection": "DTCLocalSelection",
    "CX08CounterInput": "CounterInput",
    "CX08DecodePCSSlots": "DecodePCSSlots",
    "CX08ExplainCounter": "ExplainCounter",
    "CX08ExplainTail": "ExplainTail",
    "CX08PCSInput": "PCSSlotPCSInput",
    "CX08RefineCounter": "RefineCounter",
    "CX09Decode": "DecodePCSExtendedFrame",
    "CX09DecodePCSExtended": "DecodePCSExtended",
    "CX09NewPCSExtendedDecoder": "NewPCSExtendedDecoder",
    "CX09PCSExtendedDecoder": "PCSExtendedDecoder",
    "CX09Snapshot": "SnapshotPCSExtended",
    "CX10Decode": "DecodeParallelIdentity",
    "CX10DecodeParallel": "ParallelIdentityDecodeParallel",
    "CX10NewParallelIdentityAssembler": "NewParallelIdentityAssembler",
    "CX10ParallelIdentityAssembler": "ParallelIdentityAssembler",
    "CX10ParallelIdentitySnapshot": "ParallelIdentitySnapshot",
    "CX10Snapshot": "SnapshotParallelIdentity",
    "CX11DecodeParallel": "ParallelReleaseDecodeParallel",
    "CX11ParallelContext": "ParallelContext",
    "CX12Decode": "DecodeLINKVector",
    "CX12DecodeLINKBalance": "DecodeLINKBalance",
    "CX12DecodeLINKCellMean": "DecodeLINKCellMean",
    "CX12DecodeLINKTemperatureSummary": "DecodeLINKTemperatureSummary",
    "CX12LINKVector": "LINKVector",
    "CX12MeasurementGate": "MeasurementGate",
    "CX12NewLINKCells": "NewLINKCells",
    "CX12NewLINKFixed16Cells": "NewLINKFixed16Cells",
    "CX12NewLINKTemperatures": "NewLINKTemperatures",
    "CX12Snapshot": "SnapshotLINKVector",
    "CX12SummaryGates": "SummaryGates",
    "CX12VectorSnapshot": "VectorSnapshot",
    "CX13DecodeLINKDiagnostics": "DecodeLINKDiagnostics",
    "CX13LabelCondition": "LabelCondition",
    "CX14DecodePCSSummaries": "DecodePCSSummaries",
    "CX14PCSInput": "PCSSummaryPCSInput",
    "CX14PCSResult": "PCSSummaryPCSResult",
    "CX14SummaryBit": "SummaryBit",
    "CX15CapabilityKey": "CapabilityKey",
    "CX15CapabilityState": "CapabilityState",
    "CX15ClassifyReceiver": "ClassifyReceiver",
    "CX15Observed": "ReceiverCapabilityObserved",
    "CX15OpaqueStore": "OpaqueStore",
    "CX15Qualified": "ReceiverCapabilityQualified",
    "CX15ReceiverContext": "ReceiverContext",
    "CX15ReceiverResult": "ReceiverResult",
    "CX15Unknown": "ReceiverCapabilityUnknown",
    "CX15Unsupported": "ReceiverCapabilityUnsupported",
    "CX16Diagnostic": "Diagnostic",
    "CX16Projection": "ReplayProjection",
    "CX16ReplayInput": "ReplayInput",
    "CX17NewLINKProvider": "NewLINKProvider",
    "CX17NewPCSProvider": "NewPCSProvider",
    "CX17Observation": "Observation",
    "CX17Provider": "Provider",
    "CX17Raw": "ProjectProviderRaw",
    "CX17Receive": "ProjectProviderReceive",
    "CX17ReceiveSource": "ReceiveSource",
    "CX17Snapshot": "ProjectProviderSnapshot",
    "O08Evidence": "Evidence",
    "O08Generation": "Generation",
    "O08GrowattProvider": "GrowattBMSProvider",
    "O08GrowattSnapshot": "GrowattBMSSnapshot",
    "O08HasRaw": "HasRaw",
    "O08NewGrowattProvider": "NewGrowattBMSProvider",
    "O08Observation": "BMSObservation",
    "O08Projection": "Projection",
    "O08ProjectionGeneration": "ProjectionGeneration",
    "O08Raw": "Raw",
    "O08RawGeneration": "RawGeneration",
    "O08Receive": "Receive",
    "O08ReceiveSource": "BMSReceiveSource",
    "O08RevisionV104": "BMSRevisionV104",
    "O08Snapshot": "Snapshot",
    "O08Start": "Start",
    "O08State": "State",
    "O08Stop": "Stop",
}
FILES = [
    "cmd/gateway/growatt_link_runtime.go",
    "cmd/gateway/growatt_link_runtime_test.go",
    "cmd/gateway/growatt_pcs_runtime.go",
    "cmd/gateway/growatt_pcs_runtime_test.go",
    "cmd/gateway/growatt_project_cli.go",
    "cmd/gateway/growatt_project_cli_test.go",
    "cmd/gateway/growatt_project_composition_test.go",
    "cmd/gateway/growatt_project_conformance_test.go",
    "cmd/gateway/growatt_project_provider.go",
    "cmd/gateway/growatt_project_provider_test.go",
    "growatt_project_config.go",
    "growatt_project_config_test.go",
]
TOKEN = re.compile(r"//[^\n]*|/\*[\s\S]*?\*/|\"(?:\\.|[^\"\\])*\"|`[^`]*`|'(?:\\.|[^'\\])*'|\b[A-Za-z_]\w*\b")
WORD = re.compile(r"\b[A-Za-z_]\w*\b")


def rename(match):
    return RENAMES.get(match[0], match[0])


def rewrite(source):
    def token(match):
        text = match[0]
        if text.startswith(("//", "/*")):
            return WORD.sub(rename, text)
        if text[0] in "\"'`":
            return text
        return rename(match)

    return TOKEN.sub(token, source)


def go_sources():
    for path in sorted(ROOT.rglob("*.go")):
        parts = path.relative_to(ROOT).parts
        if not any(part.startswith(".") or part == "node_modules" for part in parts):
            yield path


def remaining():
    for path in go_sources():
        for number, line in enumerate(path.read_text().splitlines(), 1):
            for match in WORD.finditer(line):
                if match[0] in RENAMES:
                    yield f"{path.relative_to(ROOT)}:{number}: {match[0]}"


def main(argv):
    if argv == ["--check"]:
        found = list(remaining())
        print("\n".join(found) if found else "no old canbusreg provider names remain")
        return 1 if found else 0
    if argv:
        raise SystemExit("usage: rename_can_provider_references.py [--check]")
    for name in FILES:
        path = ROOT / name
        source = rewrite(path.read_text())
        formatted = subprocess.run(["gofmt"], input=source, text=True, check=True,
                                   stdout=subprocess.PIPE).stdout
        path.write_text(formatted)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
