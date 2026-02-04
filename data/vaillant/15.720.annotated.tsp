// Annotated register dump for Vaillant 15.720.tsp
// myVaillant correlations (first pass):
// - group=0x00 instance=0x01 addr=0x0400 (HwcTempDesired): DHW setpoint (myVaillant tapwater target temperature)
// - group=0x00 instance=0x01 addr=0x0500 (HwcStorageTemp): DHW cylinder temperature (myVaillant current tapwater temp)
// Notes:
// - These comments are intended for human review and do not affect parsing.
// - The included files provide the full register definitions and templates.

@include("https://raw.githubusercontent.com/burmistrzak/ebusd-configuration/refs/heads/add-720-series/src/vaillant/_templates.tsp")
@include("https://raw.githubusercontent.com/burmistrzak/ebusd-configuration/refs/heads/add-720-series/src/vaillant/15.720.tsp")
