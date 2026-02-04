// Annotated register dump for Vaillant 15.720.tsp
// myVaillant correlations (2026-02-04T07-15-06Z dump + wire dump 09:19-09:23 local):
// - group=0x01 instance=0x01 addr=0x0400 (HwcTempDesired): tapping_setpoint (DHW target temperature)
// - group=0x01 instance=0x01 addr=0x0500 (HwcStorageTemp): current_dhw_temperature (DHW cylinder temperature)
// Notes:
// - These comments are intended for human review and do not affect parsing.
// - The included files provide the full register definitions and templates.

@include("https://raw.githubusercontent.com/burmistrzak/ebusd-configuration/refs/heads/add-720-series/src/vaillant/_templates.tsp")
@include("https://raw.githubusercontent.com/burmistrzak/ebusd-configuration/refs/heads/add-720-series/src/vaillant/15.720.tsp")
