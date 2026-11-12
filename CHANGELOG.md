# Changelog

## Site profile without a system controller

An offline acceptance scenario covers an eBUS that carries a heat source and the vendor's internet module but never a system controller. It runs the production startup scan, registry scan, semantic poller and poll schedule over a synthetic bus in simulated time and asserts the startup phases, the bounded B524 root discovery cadence, the published streams and the regulator-absence signal. The [reduced run sheet](docs/testing/site-without-controller-run-sheet.md) for the hardware session at such a site removes the controller rows and names the remaining ones. The gateway binary is unchanged.

## canbusreg provider behavior names

The canbusreg pin moves to the main-branch revision with the provider export renames, the Growatt BMS provider error texts and the exported API documentation. The Growatt project configuration, provider, LINK and PCS runtimes, CLI and their tests use the behavior names `SelectionSpec`, `SelectionSpecValue`, `SelectionGroup`, `ReplayInput`, `LINKInput`, `MeasurementGate`, `VectorSnapshot`, `Observation`, `Provider`, `NewLINKProvider`, `NewPCSProvider`, `ProjectProviderSnapshot`, `SelectionNativeIndication` and `ConfirmNativeSelection`; the gateway receive source implements `ProjectProviderReceive`. There is no alias or compatibility layer. `python3 scripts/rename_can_provider_references.py` reproduces the rename offline and its `--check` mode lists any remaining old name.

Native records, admission, field ages, retained last-good state and wire values are unchanged. The gateway binary changes, so affected release evidence must be renewed before the final bill of materials freeze.

## Coordinated public naming adoption

Owned GraphQL types, roots, fields and arguments use snake_case. CamelCase twins are removed. Zone observations use zone_room_temperature and zone_room_humidity. The selector-dependent target_temp_c remains provisional pending resolution of its manual, heating and cooling source selection.

MCP registered tool names join namespace tokens with underscores. The legacy ebus.invoke and ebus.devices routes are withdrawn. Pairing status remains registered. Versioned invoke intent, permission, dangerous-operation and replay requirements are preserved.

Circuit setters adopt the six selector-qualified names in the naming inventory. The unsupported frost-protection float writer is withdrawn: frostProtC and frost_prot_c perform no dispatch. Raw provisional observations remain. Other setter vocabulary without approved identity-qualified targets remains provisional.

SemReg thermal members adopt humidity, temperature_setpoint and dhw_temperature_setpoint. eeBUS structural output uses use_cases while opaque native evidence is preserved. Huawei status replaces device_status; energy_accumulated remains provisional. The bounded temperature metric names use constant celsius labels and semantic evidence age uses a constant seconds label. Numeric conversions and retained slot ownership remain unchanged.

Affected security, hardware and candidate evidence must be renewed before release. Full repository gates, affected transport/conformance, consumer coordination, the no-Matter build and independent review remain shipping requirements.
