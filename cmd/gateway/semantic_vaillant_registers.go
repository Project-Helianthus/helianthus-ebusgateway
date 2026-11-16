package main

const (
	vaillantExtRegisterPrimary   = byte(0xB5)
	vaillantExtRegisterSecondary = byte(0x24)
	vaillantB509Primary          = byte(0xB5)
	vaillantB509Secondary        = byte(0x09)
	vaillantB509OpcodeRead       = byte(0x0D)
	vaillantB509OpcodeWrite      = byte(0x0E)
	vaillantB524OpcodeRead       = byte(0x06)
	vaillantB524OpcodeLocal      = byte(0x02)
	vaillantB524OpRead           = byte(0x00)
	vaillantB524OpWrite          = byte(0x01)

	// --- Zone registers: GG=0x03 / OP=0x02 ---

	// STATE registers (read-only live values)
	zone_current_temp        = uint16(0x000F) // state.current_room_temperature
	zone_special_function    = uint16(0x000E) // state.current_special_function
	zone_valve_status        = uint16(0x0012) // state.valve_status
	zone_current_humidity    = uint16(0x0028) // state.current_room_humidity
	zone_quick_veto_end_time = uint16(0x001E) // state.quick_veto.end_time (HH:MM:SS)
	zone_quick_veto_end_date = uint16(0x0024) // state.quick_veto.end_date (DD.MM.YY)

	// CONFIG registers (user-writable settings)
	zone_heating_op_mode                   = uint16(0x0006) // configuration.heating.operation_mode
	zone_target_temp                       = uint16(0x0022) // configuration.heating.desired_setpoint
	zone_fallback_manual_temp              = uint16(0x0014) // configuration.heating.manual_mode_setpoint
	zone_room_temperature_zone_mapping_raw = uint16(0x0013) // configuration.room_temperature_zone_mapping
	zone_quick_veto_temp                   = uint16(0x0008) // configuration.quick_veto.temperature (f32 LE)
	zone_quick_veto_duration               = uint16(0x0026) // configuration.quick_veto.duration_hours (f32 LE)
	zone_holiday_start_date                = uint16(0x0003) // configuration.holiday.start_date (DD.MM.YY)
	zone_holiday_end_date                  = uint16(0x0004) // configuration.holiday.end_date (DD.MM.YY)
	zone_holiday_setpoint                  = uint16(0x0005) // configuration.holiday.setpoint (f32 LE)
	zone_holiday_end_time                  = uint16(0x0020) // configuration.holiday.end_time (HH:MM)
	zone_holiday_start_time                = uint16(0x0021) // configuration.holiday.start_time (HH:MM)

	// PARAMS registers (identification / metadata)
	zone_name        = uint16(0x0016)
	zone_name_prefix = uint16(0x0017)
	zone_name_suffix = uint16(0x0018)
	zone_index       = uint16(0x001C)

	// --- Circuit registers: GG=0x02 / OP=0x02 ---

	// STATE registers (read-only live values)
	circuit_flow_setpoint  = uint16(0x0007) // flow_setpoint
	circuit_flow_temp      = uint16(0x0008) // flow_temperature (VF[x])
	circuit_pump_status    = uint16(0x001B) // pump_status (observed Vaillant "Status Pump")
	circuit_circuit_state  = uint16(0x001E) // circuit_state (observed Vaillant "Status circuit"; 0=standby 1=heating 2=cooling 3=dhw)
	circuit_calc_flow_temp = uint16(0x0020) // legacy selector name; circuit_unknown_10 raw evidence, not a temperature
	circuit_mixer_position = uint16(0x0021) // mixer_position_pct
	circuit_humidity       = uint16(0x0022) // room_humidity_pct
	circuit_dew_point      = uint16(0x0023) // dew_point_temperature
	circuit_pump_hours     = uint16(0x0024) // pump_operating_hours
	circuit_pump_starts    = uint16(0x0025) // pump_starts

	// CONFIG registers (user-writable settings)
	circuit_type              = uint16(0x0002) // configuration.heating_circuit_type / mixer_circuit_type_external
	circuit_cooling_enabled   = uint16(0x0006) // cooling_enabled
	circuit_heating_curve     = uint16(0x000F) // heating_curve
	circuit_flow_temp_max     = uint16(0x0010) // flow_temperature_max
	circuit_flow_temp_min     = uint16(0x0012) // flow_temperature_min
	circuit_summer_limit      = uint16(0x0014) // summer_limit
	circuit_room_temp_control = uint16(0x0015) // room_temperature_control_mode
	// circuit_special_function_mode is one unsigned byte. The name is
	// provisional; the meaning and value table are unconfirmed on the wire,
	// so the raw byte is published and no write is offered.
	circuit_special_function_mode = uint16(0x001D)

	dhwPseudoCircuitInstance = byte(0x09)
	dhwPseudoCircuitTypeRaw  = uint16(0x0003)

	CircuitStateStandby = "standby"
	CircuitStateHeating = "heating"
	CircuitStateCooling = "cooling"
	CircuitStateDHW     = "dhw"

	// --- DHW registers: GG=0x01 / OP=0x02 ---

	// STATE registers (read-only live values)
	dhw_current_temp     = uint16(0x0005) // state.current_dhw_temperature
	dhw_special_function = uint16(0x000D) // state.current_special_function

	// CONFIG registers (user-writable settings)
	dhw_operation_mode     = uint16(0x0003) // configuration.domestic_hot_water.operation_mode
	dhw_target_temp        = uint16(0x0004) // configuration.domestic_hot_water.tapping_setpoint
	dhw_holiday_start_date = uint16(0x0009) // configuration.holiday.start_date (DD.MM.YY)
	dhw_holiday_end_date   = uint16(0x000A) // configuration.holiday.end_date (DD.MM.YY)

	dhwInstance = byte(0x00)

	// --- Device slot registers: OP=0x06 (remote) ---

	// STATE registers (read-only live values)
	device_slot_connected          = uint16(0x0001)
	device_slot_room_humidity      = uint16(0x0007)
	device_slot_room_temperature   = uint16(0x000F)
	device_slot_paired             = uint16(0x001E)
	device_slot_reception_strength = uint16(0x001F)

	// PARAMS registers (identification / metadata)
	device_slot_class_address          = uint16(0x0002)
	device_slot_firmware               = uint16(0x0004)
	device_slot_remote_control_address = uint16(0x0019)
	device_slot_hardware_identifier    = uint16(0x0023)
	device_slot_zone_assignment        = uint16(0x0025)

	solarInstance = byte(0x00)

	// --- Solar registers: GG=0x04 / OP=0x02 ---

	// STATE registers (read-only live values)
	solar_collector_temp = uint16(0x0003)
	solar_return_temp    = uint16(0x0007)
	solar_pump_active    = uint16(0x0008)
	solar_current_yield  = uint16(0x0009)
	solar_pump_hours     = uint16(0x000B)

	// CONFIG registers (user-writable settings)
	solar_enabled       = uint16(0x0001)
	solar_function_mode = uint16(0x0002)

	// --- Cylinder registers: GG=0x05 / OP=0x02 ---

	// STATE registers (read-only live values)
	cylinder_temperature = uint16(0x0004)

	// CONFIG registers (user-writable settings)
	cylinder_max_setpoint      = uint16(0x0001)
	cylinder_charge_hysteresis = uint16(0x0002)
	cylinder_charge_offset     = uint16(0x0003)
)

// b524GroupDef binds a B524 group number to its owning opcode.
// This eliminates the possibility of accidentally mixing OP/GG pairs.
// Public contract:
// https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/b524-namespace-invariants.md
type b524GroupDef struct {
	group  byte
	opcode byte
	name   string
}

// OP=0x02 (local/controller) groups.
var (
	localRegulator = b524GroupDef{group: 0x00, opcode: vaillantB524OpcodeLocal, name: "regulator_parameters"}
	localDHW       = b524GroupDef{group: 0x01, opcode: vaillantB524OpcodeLocal, name: "hot_water_circuit"}
	localCircuits  = b524GroupDef{group: 0x02, opcode: vaillantB524OpcodeLocal, name: "heating_circuits"}
	localZones     = b524GroupDef{group: 0x03, opcode: vaillantB524OpcodeLocal, name: "zones"}
	localSolar     = b524GroupDef{group: 0x04, opcode: vaillantB524OpcodeLocal, name: "solar_circuit"}
	localCylinders = b524GroupDef{group: 0x05, opcode: vaillantB524OpcodeLocal, name: "hot_water_cylinder"}
)

// OP=0x06 (remote/device) groups — all gated on device_connected (RR=0x0001).
var (
	remoteRegulators        = b524GroupDef{group: 0x09, opcode: vaillantB524OpcodeRead, name: "regulators"}
	remoteThermostats       = b524GroupDef{group: 0x0A, opcode: vaillantB524OpcodeRead, name: "thermostats"}
	remoteFunctionalModules = b524GroupDef{group: 0x0C, opcode: vaillantB524OpcodeRead, name: "functional_modules"}
)

// remoteDeviceGroups lists the OP=0x06 groups that the gateway actively polls.
var remoteDeviceGroups = []b524GroupDef{
	remoteRegulators,
	remoteThermostats,
	remoteFunctionalModules,
}

// B5.55 timer/schedule protocol constants.
const (
	vaillantB555Primary   = byte(0xB5)
	vaillantB555Secondary = byte(0x55)

	b555OpcodeConfigRead = byte(0xA3)
	b555OpcodeSlotsRead  = byte(0xA4)
	b555OpcodeTimerRead  = byte(0xA5)
	b555OpcodeTimerWrite = byte(0xA6)

	b555HCHeating = byte(0x00)
	b555HCCooling = byte(0x01)
	b555HCDHW     = byte(0x02)
	b555HCCC      = byte(0x03)
	b555HCSilent  = byte(0x04)
	b555ZoneDHW   = byte(0xFF) // zone-agnostic selector for DHW/CC/Silent
)

var b555WeekdayNames = [7]string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}

var b555HCNames = map[byte]string{
	b555HCHeating: "heating",
	b555HCCooling: "cooling",
	b555HCDHW:     "dhw",
	b555HCCC:      "circulation",
	b555HCSilent:  "silent",
}

// --- Boiler status ---

// B524 registers on the controller that mirror boiler operational data.
// Group 0x00 (regulator parameters), instance 0x00.
const (
	regulatorInstance = byte(0x00)

	// --- System registers: GG=0x00 / OP=0x02 ---

	// STATE registers (read-only live values)
	system_off                             = uint16(0x0007)
	system_water_pressure                  = uint16(0x0039)
	system_flow_temperature                = uint16(0x004B)
	system_outdoor_temperature             = uint16(0x0073)
	system_outdoor_temperature_avg_24h     = uint16(0x0095)
	system_maintenance_due                 = uint16(0x0096)
	system_hwc_cylinder_temperature_top    = uint16(0x009D)
	system_hwc_cylinder_temperature_bottom = uint16(0x009E)

	// CONFIG registers (user-writable settings)
	system_adaptive_heating_curve          = uint16(0x0014)
	system_alternative_point               = uint16(0x0022)
	system_heating_circuit_bivalence_point = uint16(0x0023)
	system_dhw_bivalence_point             = uint16(0x0001)
	system_hc_emergency_temperature        = uint16(0x0026)
	system_hwc_max_flow_temp_desired       = uint16(0x0046)
	system_max_room_humidity               = uint16(0x000E)
	system_maintenance_date                = uint16(0x002C)
	system_installer_name_1                = uint16(0x006C)
	system_installer_name_2                = uint16(0x006D)
	system_installer_phone_1               = uint16(0x006F)
	system_installer_phone_2               = uint16(0x0070)
	system_installer_menu_code             = uint16(0x0076)

	// PARAMS registers (system topology / identification)
	system_scheme                    = uint16(0x0036)
	system_module_configuration_vr71 = uint16(0x002F)

	// --- Boiler B509 direct registers on BAI00 ---
	boiler_b509_water_pressure            = uint16(0x0200)
	boiler_b509_flame_active              = uint16(0x0500)
	boiler_b509_partload_hc_kw            = uint16(0x0704)
	boiler_b509_partload_hwc_kw           = uint16(0x0804)
	boiler_b509_flowset_hc_max_c          = uint16(0x0E04)
	boiler_b509_flowset_hc_max_c_fallback = uint16(0xA500)
	boiler_b509_flowset_hwc_max_c         = uint16(0x0F04)
	boiler_b509_pump_hours                = uint16(0x1400)
	boiler_b509_flow_temperature          = uint16(0x1800)
	boiler_b509_fan_hours                 = uint16(0x1B00)
	boiler_b509_deactivations_ifc         = uint16(0x1F00)
	boiler_b509_hours_till_service        = uint16(0x2004)
	boiler_b509_deactivations_limit       = uint16(0x2000)
	boiler_b509_dhw_hours                 = uint16(0x2200)
	boiler_b509_dhw_starts                = uint16(0x2300)
	boiler_b509_target_fan_speed_rpm      = uint16(0x2400)
	boiler_b509_central_heating_hours     = uint16(0x2800)
	boiler_b509_central_heating_starts    = uint16(0x2900)
	boiler_b509_modulation_pct            = uint16(0x2E00)
	boiler_b509_flow_temp_desired_c       = uint16(0x3900)
	boiler_b509_external_pump_active      = uint16(0x3F00)
	boiler_b509_installer_menu_code       = uint16(0x4904)
	boiler_b509_central_heating_pump      = uint16(0x4400)
	boiler_b509_diverter_valve_position   = uint16(0x5400)
	boiler_b509_dhw_water_flow_lpm        = uint16(0x5500)
	boiler_b509_dhw_demand_active         = uint16(0x5800)
	boiler_b509_circulation_pump_active   = uint16(0x7B00)
	boiler_b509_phone_number              = uint16(0x8104)
	boiler_b509_fan_speed_rpm             = uint16(0x8300)
	boiler_b509_storage_load_pump_pct     = uint16(0x9E00)
	boiler_b509_ionisation_voltage_ua     = uint16(0xA400)
	boiler_b509_state_number              = uint16(0xAB00)
	boiler_b509_gas_valve_active          = uint16(0xBB00)
	boiler_b509_heating_switch_active     = uint16(0xF203)
	boiler_b509_dhw_temp_desired_c        = uint16(0xEA03)
	boiler_b509_primary_circuit_flow_lpm  = uint16(0xFB00)
)
