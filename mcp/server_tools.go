package mcp

func defaultServerTools() []Tool {
	return []Tool{
		{
			Name:        toolRuntimeStatusGetName,
			Description: "Get runtime daemon and adapter status.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticZonesGetName,
			Description: "Get semantic zones snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticCircuitsGetName,
			Description: "Get semantic heating circuits snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticRadioGetName,
			Description: "Get semantic remote-slot radio devices snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticFM5ModeGetName,
			Description: "Get semantic FM5 mode snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticFM5InterpretationGetName,
			Description: "Get the atomic semantic FM5 mode, degraded reason, and evidence revision.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSolarGetName,
			Description: "Get semantic solar snapshot (interpreted mode only).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticCylindersGetName,
			Description: "Get semantic cylinders snapshot (interpreted mode only).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticDHWGetName,
			Description: "Get semantic domestic hot water snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticEnergyGetName,
			Description: "Get semantic energy totals snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticBoilerGetName,
			Description: "Get semantic boiler status snapshot (flow/return temps, pump, diagnostics).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSystemGetName,
			Description: "Get semantic system status snapshot (outdoor temp, water pressure, flow temp, maintenance, config).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticAdapterInfoGetName,
			Description: "Get adapter hardware identity and telemetry (firmware, temperature, voltages, WiFi RSSI, reset info).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSchedulesGetName,
			Description: "Get semantic weekly timer schedules snapshot (B555 protocol).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSchedulesSetZoneName,
			Description: "Write a zone heating time program for a specific weekday (B555 protocol). Writes individual slots sequentially (SC=1 per slot for reliability).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"zone":    map[string]any{"type": "integer", "minimum": 0, "maximum": 2},
					"weekday": map[string]any{"type": "integer", "minimum": 0, "maximum": 6, "description": "0=Monday, 6=Sunday"},
					"slots": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"start_hour":    map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"start_minute":  map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"end_hour":      map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"end_minute":    map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"temperature_c": map[string]any{"type": "number"},
							},
							"required": []string{"start_hour", "start_minute", "end_hour", "end_minute", "temperature_c"},
						},
					},
				},
				"required":             []string{"zone", "weekday", "slots"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSchedulesSetDhwName,
			Description: "Write a DHW (domestic hot water) time program for a specific weekday (B555 protocol). Temperature is optional — omit to keep current B524 setpoint (0xFFFF sentinel).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"weekday": map[string]any{"type": "integer", "minimum": 0, "maximum": 6, "description": "0=Monday, 6=Sunday"},
					"slots": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"start_hour":    map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"start_minute":  map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"end_hour":      map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"end_minute":    map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"temperature_c": map[string]any{"type": "number"},
							},
							"required": []string{"start_hour", "start_minute", "end_hour", "end_minute"},
						},
					},
				},
				"required":             []string{"weekday", "slots"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSystemSetConfigName,
			Description: "Write a system configuration field (B524 controller). Accepts camelCase field names matching GraphQL mutation fields.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"field": map[string]any{"type": "string", "description": "camelCase field name (e.g. installerName1, maintenanceDate, installerMenuCode)"},
					"value": map[string]any{"type": "string", "description": "Value to write (string representation)"},
				},
				"required":             []string{"field", "value"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticBoilerSetConfigName,
			Description: "Write a boiler configuration field (B509 BAI00). Accepts camelCase field names matching GraphQL mutation fields.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"field": map[string]any{"type": "string", "description": "camelCase field name (e.g. installerMenuCode, phoneNumber)"},
					"value": map[string]any{"type": "string", "description": "Value to write (string representation)"},
				},
				"required":             []string{"field", "value"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSnapshotName,
			Description: "Get a consistent semantic snapshot across selected semantic planes.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"planes": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "string",
							"enum": []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status", "system", "circuits", "radio_devices", "fm5_mode", "solar", "cylinders", "schedules", "adapter_info"},
						},
					},
					"timeout_ms": map[string]any{"type": "integer", "minimum": 1},
					"allow_partial": map[string]any{
						"type": "boolean",
					},
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSnapshotCaptureName,
			Description: "Capture a read snapshot for deterministic MCP reads.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolSnapshotDropName,
			Description: "Drop a previously captured snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"snapshot_id": map[string]any{"type": "string"},
				},
				"required":             []string{"snapshot_id"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesV1Name,
			Description: "List devices discovered on the eBUS, including planes and methods.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDeviceGetV1Name,
			Description: "Get one device by address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"consistency": consistencyInputProperty(),
				},
				"required":             []string{"address"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolPlanesListV1Name,
			Description: "List registry planes for one device address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"consistency": consistencyInputProperty(),
				},
				"required":             []string{"address"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolMethodsListV1Name,
			Description: "List registry methods for a device address and plane.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":       map[string]any{"type": "string"},
					"consistency": consistencyInputProperty(),
				},
				"required":             []string{"address", "plane"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolInvokeV1Name,
			Description: "Invoke a plane method on a device.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":         map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":           map[string]any{"type": "string"},
					"method":          map[string]any{"type": "string"},
					"params":          map[string]any{"type": "object"},
					"intent":          map[string]any{"type": "string", "enum": []string{"READ_ONLY", "MUTATE"}},
					"allow_dangerous": map[string]any{"type": "boolean"},
					"idempotency_key": map[string]any{"type": "string"},
					"timeout_ms":      map[string]any{"type": "integer", "minimum": 1},
				},
				"required":             []string{"address", "plane", "method", "intent", "allow_dangerous"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesLegacyName,
			Description: "Compatibility alias for ebus.v1.registry.devices.list.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolInvokeLegacyName,
			Description: "Compatibility alias for ebus.v1.rpc.invoke.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":   map[string]any{"type": "string"},
					"method":  map[string]any{"type": "string"},
					"params":  map[string]any{"type": "object"},
				},
				"required":             []string{"address", "plane", "method"},
				"additionalProperties": false,
			},
		},
	}
}
