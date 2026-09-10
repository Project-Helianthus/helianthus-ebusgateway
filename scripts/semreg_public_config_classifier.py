#!/usr/bin/env python3
"""Fail closed unless config.go changes only the Storage public-read slice."""
from __future__ import annotations
import re
import subprocess
import sys
from pathlib import Path

DECLARATION = (
    "// PortalStorageConfig is an independently disabled, read-only BFF for the\n"
    "// versioned SemReg storage projection. It deliberately does not expose any\n"
    "// operation or native fallback fields.\n"
    "type PortalStorageConfig = PortalPVConfig\n"
)
FIELD = re.compile(r"(?m)^\s*PortalStorage\s+PortalStorageConfig\n")
FUNCTION = '''func (cfg Config) ValidatePortalStorage() error {
	if cfg.PortalStorage.RawReadEnabled {
		return errors.New("portal storage configuration does not permit raw reads")
	}
	copy := cfg
	copy.PortalPV = cfg.PortalStorage
	if err := copy.ValidatePortalPV(); err != nil {
		return err
	}
	producer := cfg.ModbusTCPConfig.GrowattBMSRS485
	if producer.Enabled && !cfg.M2MGraphQL.Disabled() {
		if !growattStorageOperationFitsDeadline(producer.MaxQuiescence, producer.ResponseTimeout, 10*time.Second, 750*time.Millisecond) {
			return errors.New("growatt storage GraphQL requires MaxQuiescence plus four reads plus 250ms processing and 500ms response headroom below the M2M server deadline")
		}
	}
	if !cfg.PortalStorage.SemanticEnabled {
		return nil
	}
	if !producer.Enabled || producer.AssetID != cfg.PortalStorage.AssetRef {
		return errors.New("portal storage semantic BFF requires the enabled matching Growatt BMS RS-485 producer")
	}
	if !growattStorageOperationFitsDeadline(producer.MaxQuiescence, producer.ResponseTimeout, 5*time.Second, 500*time.Millisecond) {
		return errors.New("portal storage semantic BFF requires MaxQuiescence plus four Growatt reads plus 500ms headroom below the M2M deadline")
	}
	return nil
}
'''
HELPER = '''// growattStorageOperationFitsDeadline checks the entire public RTU operation:
// recovery can consume one MaxQuiescence interval before its four serial reads.
// The subtraction/division formulation avoids overflowing time.Duration while
// preserving the strict response-deadline boundary.
func growattStorageOperationFitsDeadline(maxQuiescence, responseTimeout, deadline, headroom time.Duration) bool {
	if maxQuiescence < 0 || responseTimeout <= 0 || deadline <= headroom {
		return false
	}
	budget := deadline - headroom
	if maxQuiescence >= budget {
		return false
	}
	return responseTimeout <= (budget-maxQuiescence-time.Nanosecond)/4
}
'''

def show(ref: str) -> str:
    return subprocess.check_output(["git", "show", f"{ref}:config.go"], text=True)

def strip_storage(text: str) -> tuple[str, tuple[int, int, int, int]]:
    declaration_count = text.count(DECLARATION)
    if declaration_count > 1: raise ValueError("duplicate Storage config declaration")
    text = text.replace(DECLARATION, "")
    text, field_count = FIELD.subn("", text)
    if field_count > 1: raise ValueError("duplicate Storage config field")
    function_count = text.count(FUNCTION)
    if function_count > 1: raise ValueError("duplicate ValidatePortalStorage function")
    text = text.replace(FUNCTION, "")
    helper_count = text.count(HELPER)
    if helper_count > 1: raise ValueError("duplicate growattStorageOperationFitsDeadline helper")
    text = text.replace(HELPER, "")
    normalized = re.sub(r"\n{2,}", "\n", text).strip() + "\n"
    return normalized, (declaration_count, field_count, function_count, helper_count)

def main() -> int:
    try:
        base_rest, base_counts = strip_storage(show(sys.argv[1]))
        head_rest, head_counts = strip_storage(Path("config.go").read_text())
        if base_rest != head_rest: return 1
        if head_counts != (1, 1, 1, 1): return 1
        if any(count not in (0, 1) for count in base_counts): return 1
        return 0
    except Exception:
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
