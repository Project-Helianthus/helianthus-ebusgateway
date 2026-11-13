# Changelog

## canbusreg selection and native result names

The canbusreg pin moves to the main-branch revision that drops the remaining work-item prefixes from its exports. The Growatt project configuration, provider, LINK and PCS runtimes, CLI and their tests use `Selection`, `NewSelection` and `NativeField`. There is no alias or compatibility layer. `python3 scripts/rename_can_provider_references.py` now also carries `CXSelection`, `NewCXSelection`, `CXNativeField`, `CXNativeResult`, `CXDecodeLINKTelemetry`, `NewCX15OpaqueStore` and `NewCX16SelectedProfile`, reproduces the caller rename offline and its `--check` mode lists any remaining old name.

Native records, admission, field ages, retained last-good state, log and error texts and wire values are unchanged. The gateway binary changes, so affected release evidence must be renewed before the final bill of materials freeze.

## Site profile without a system controller

An offline acceptance scenario covers an eBUS that carries a heat source and the vendor's internet module but never a system controller. It runs the production startup scan, registry scan, B524 root probe, semantic poller, read scheduler and poll schedule over a recording synthetic bus on a simulated clock that starts at process start. It asserts the startup phases against the boot-live deadline, the root discovery frames and attempts, the native refresh spacing of the heat-source registers, the published streams and the regulator-absence signal. The [reduced run sheet](docs/testing/site-without-controller-run-sheet.md) for the hardware session at such a site removes the controller rows and names the remaining ones.

Runtime behaviour is unchanged. The semantic poller's root probe now sends through the poller's existing frame seam, its read-retry, post-read and probe back-off waits go through one pause hook with the same durations, and the semantic read scheduler gains `SetClock`, so the scenario runs the production probe and read cache on its clock. The gateway binary changes, so affected release evidence must be renewed before the final bill of materials freeze.

## Catalog spelling in the acceptance catalog and modbusreg adoption

The acceptance catalog package drops the work-item prefix and uses the catalog spelling: `Catalog`, `ReadCatalog`, `Signature`, `ReadSchema`, `ReadReport`, `ValidateReport`, `MaxBytes`, `Row`, `Category`, `Transition`, `Identity`, `Review`, `Record` and `Report`, whose version field is `CatalogVersion`. The report JSON key `catalogue_version` and the schema text are unchanged; they change with the acceptance report format. The catalog documents move to `docs/testing/hardware-acceptance-catalog.md` and `docs/testing/hardware-acceptance-catalog-v1.json` with unchanged catalog data, versions and ledger signatures. There is no alias or compatibility layer. `python3 scripts/rename_acceptance_catalog.py` reproduces the Go rename offline and its `--check` mode lists any remaining old name, spelling or file name.

The modbusreg pin moves to the main-branch revision with the catalog spelling. The Huawei PV publication admits a child only on the renamed `Catalog` decision with the `HuaweiSUN2000CatalogSelected` outcome. Withheld Huawei children now report the reasons `catalog_not_selected` and `catalog_ambiguous` in the Huawei instance status tools and the Portal instance diagnostics, and the Huawei observation evidence digest covers the `catalog` key of the native child observation. Admission, native records, retained last-good state and other wire values are unchanged. The gateway binary changes, so affected release evidence must be renewed before the final bill of materials freeze.

## canbusreg provider behavior names

The canbusreg pin moves to the main-branch revision with the provider export renames, the Growatt BMS provider error texts and the exported API documentation. The Growatt project configuration, provider, LINK and PCS runtimes, CLI and their tests use the behavior names `SelectionSpec`, `SelectionSpecValue`, `SelectionGroup`, `ReplayInput`, `LINKInput`, `MeasurementGate`, `VectorSnapshot`, `Observation`, `Provider`, `NewLINKProvider`, `NewPCSProvider`, `ProjectProviderSnapshot`, `SelectionNativeIndication` and `ConfirmNativeSelection`; the gateway receive source implements `ProjectProviderReceive`. There is no alias or compatibility layer. `python3 scripts/rename_can_provider_references.py` reproduces the rename offline and its `--check` mode lists any remaining old name.

Native records, admission, field ages, retained last-good state and wire values are unchanged. The gateway binary changes, so affected release evidence must be renewed before the final bill of materials freeze.

## Coordinated public naming adoption

Owned GraphQL types, roots, fields and arguments use snake_case. CamelCase twins are removed. Zone observations use zone_room_temperature and zone_room_humidity. The selector-dependent target_temp_c remains provisional pending resolution of its manual, heating and cooling source selection.

MCP registered tool names join namespace tokens with underscores. The legacy ebus.invoke and ebus.devices routes are withdrawn. Pairing status remains registered. Versioned invoke intent, permission, dangerous-operation and replay requirements are preserved.

Circuit setters adopt the six selector-qualified names in the naming inventory. The unsupported frost-protection float writer is withdrawn: frostProtC and frost_prot_c perform no dispatch. Raw provisional observations remain. Other setter vocabulary without approved identity-qualified targets remains provisional.

SemReg thermal members adopt humidity, temperature_setpoint and dhw_temperature_setpoint. eeBUS structural output uses use_cases while opaque native evidence is preserved. Huawei status replaces device_status; energy_accumulated remains provisional. The bounded temperature metric names use constant celsius labels and semantic evidence age uses a constant seconds label. Numeric conversions and retained slot ownership remain unchanged.

Affected security, hardware and candidate evidence must be renewed before release. Full repository gates, affected transport/conformance, consumer coordination, the no-Matter build and independent review remain shipping requirements.
