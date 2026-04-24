# Transport-Gate Evidence

This directory holds operator-captured evidence for the M7 live-bus acceptance window.

Capture procedure:

1. Run the gateway on real hardware for 5 minutes on the target transport.
2. Record the final startup-admission expvars and passive event count.
3. Hash the built gateway binary with `sha256sum`.
4. Copy `TEMPLATE.yaml` to `docs/transport-gate-evidence/<ISO8601-date>-<transport>.yaml`.
5. Fill every field and link the committed YAML file in the M7 PR description.

The YAML file is operator-provided evidence. This repo only ships the format and capture procedure.
