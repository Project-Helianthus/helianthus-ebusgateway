## Resolve adapter hardware info fields

`adapter_hardware_info` resolves each snake-case field from the matching field of the adapter hardware info value. A nil optional value stays null and the rest of the object stays in place. A missing value for a non-null field is that field's own error. The gateway binary changes.
