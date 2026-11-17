#!/usr/bin/env python3
"""Read-only smoke command of the adaptermux-v2 matrix suite.

matrix-runner runs this command once per executable cell with the case context
in MATRIX_* variables. It judges the invariant named by MATRIX_INVARIANT from
the gateway's own read surfaces: the bus observability snapshot, the
Prometheus text (AdapterMux diagnostics included), the expvar counters and,
for the semantic record, one GraphQL query and one MCP read tool. It proves
eBUS data flow on the selected southbound transport during the case window:
counters are read once at the start and again until the invariant's flow
requirements hold or the window ends.

The adapter-action cells AD04 and AD09 announce the operator action once the
first reading is taken; the operator then resets the adapter (AD04) or
disconnects and reconnects its host link (AD09) inside the case window.

It never sends a mutation, never calls a tool that writes and never invokes a
bus method. Every run writes one JSON object, smoke_evidence.json, into the
case directory.

Exit statuses: 0 pass, 1 fail, 2 usage error (missing or invalid context),
3 refused (an invariant this command cannot judge), 4 signal missing (the
gateway does not publish a series the invariant needs).
"""

from __future__ import annotations

import functools
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_USAGE = 2
EXIT_REFUSED = 3
EXIT_SIGNAL_MISSING = 4

EVIDENCE_FILE = "smoke_evidence.json"
EVIDENCE_SCHEMA = "adaptermux-v2-smoke/1"

READ_ONLY_REQUEST_TYPES = ("b509-read", "b524-read", "b555-read", "identification-0704")
EXTERNAL_WRITE_POLICIES = ("invalidate_only", "record_only", "record_and_invalidate")

# Transport class the gateway reports for each (composition, southbound) pair
# of an executable cell.
EXPECTED_TRANSPORT_CLASS = {
    ("mux", "ens"): "adapter-direct-ens",
    ("mux", "enh"): "adapter-direct",
    ("direct", "ens"): "ens",
    ("direct", "enh"): "enh",
    ("direct", "udp"): "udp-plain",
    ("direct", "tcp"): "tcp-plain",
    ("ebusd-tcp", "ebusd-tcp"): "ebusd-tcp",
}

ROW_TOKEN = {"ens": "ENS", "enh": "ENH", "udp": "UDP", "tcp": "TCP", "ebusd-tcp": "EBUSD"}

GT0 = "greater_than_zero"
EQ0 = "equal_to_zero"
EQ1 = "equal_to_one"

FLOW_ACTIVE = (("active_frames", GT0), ("mux_transactions_done", GT0))
CONTENTION = FLOW_ACTIVE + (("external_identification_frames", GT0),)

MUX_ENDINGS = "ebus_adaptermux_transaction_endings_total"
MUX_FOREIGN = "ebus_adaptermux_foreign_initiator_releases_total"
MUX_ESCAPE = "ebus_adaptermux_escape_pair_verify_failures_total"
MUX_HELD = "ebus_adaptermux_active_arbitration_held"
# Series AD04, AD07 and AD09 read that the gateway does not publish today.
# A check that needs one refuses with EXIT_SIGNAL_MISSING and names it.
MUX_SESSION_DISCONTINUITIES = "ebus_adaptermux_session_discontinuities_total"
MUX_UPSTREAM_RECONNECTS = "ebus_adaptermux_upstream_reconnects_total"
MUX_CONTENDED_SELECTIONS = "ebus_adaptermux_contended_selections_total"
MUX_RETAINED_BIDS = "ebus_adaptermux_retained_bids_total"
PASSIVE_COMPLETED = "ebus_passive_completed_transactions_total"
WRITE_POLICY_STATE = "external_write_policy_state"
DEDUP_ADJUDICATIONS = "observe_first_dedup_adjudications_total"

SIGNAL_MEANING = {
    MUX_HELD: "the out-of-step active-arbitration hold gauge",
    MUX_SESSION_DISCONTINUITIES: "discontinuity boundaries delivered to each consumer session of the mux (label session)",
    MUX_UPSTREAM_RECONNECTS: "completed upstream reconnects of the mux",
    MUX_CONTENDED_SELECTIONS: "SYN boundaries at which a gateway bid and an external bid competed, by winner (label winner: gateway or external)",
    MUX_RETAINED_BIDS: "losing bids kept for a later boundary, by how they ended (label result: granted or another ending)",
}

# AD04 and AD09: every consumer session observes the discontinuity, the mux
# reconnects once, gateway transactions and passive reconstruction resume
# after the discontinuity, and no out-of-step hold is raised.
ADAPTER_ACTION_SIGNALS = (MUX_HELD, MUX_SESSION_DISCONTINUITIES, MUX_UPSTREAM_RECONNECTS)
ADAPTER_ACTION_REQUIRES = (
    ("consumer_sessions_observing_discontinuity", GT0),
    ("consumer_sessions_missing_discontinuity", EQ0),
    ("mux_upstream_reconnects", EQ1),
    ("resumed_active_frames", GT0),
    ("resumed_mux_transactions_done", GT0),
    ("resumed_passive_completed_transactions", GT0),
    ("out_of_step_hold_readings", EQ0),
)

# AD07: with no owner and an idle bus, the lowest initiator low nibble wins,
# then the lowest high nibble (docs/architecture/adapter-arbitration-decision.md);
# the losing bid is kept and completes at a later boundary; neither session
# is starved.
PRIORITY_SIGNALS = (MUX_CONTENDED_SELECTIONS, MUX_RETAINED_BIDS)
PRIORITY_REQUIRES = CONTENTION + (
    ("contended_selections_won_by_priority", GT0),
    ("contended_selections_won_against_priority", EQ0),
    ("retained_bids_granted", GT0),
    ("retained_bids_not_granted", EQ0),
)

# One entry per executable invariant: the composition it runs on, the
# southbound transports it may run on, its flow requirements and whether it
# captures the semantic record that AD10 compares.
CHECKS = {
    "AD01": {"composition": "mux", "rows": ("ens", "enh"), "requires": FLOW_ACTIVE},
    "AD02": {"composition": "mux", "rows": ("ens", "enh"), "passive_tap": True,
             "requires": (("active_frames", GT0), ("passive_frames", GT0),
                          ("dedup_matched_active", GT0), ("self_echo_frames", EQ0))},
    "AD03": {"composition": "mux", "rows": ("ens", "enh"), "passive_tap": True,
             "requires": (("passive_third_party_frames", GT0),
                          ("passive_completed_transactions", GT0))},
    "AD04": {"composition": "mux", "rows": ("ens", "enh"), "passive_tap": True,
             "signals": ADAPTER_ACTION_SIGNALS, "discontinuity": True,
             "operator_action": "reset the adapter",
             "requires": ADAPTER_ACTION_REQUIRES},
    "AD06": {"composition": "mux", "rows": ("ens", "enh"), "requires": CONTENTION},
    "AD07": {"composition": "mux", "rows": ("ens", "enh"), "signals": PRIORITY_SIGNALS,
             "priority": True, "requires": PRIORITY_REQUIRES},
    "AD09": {"composition": "mux", "rows": ("ens", "enh"), "passive_tap": True,
             "signals": ADAPTER_ACTION_SIGNALS, "discontinuity": True,
             "operator_action": "disconnect the adapter's host link and reconnect it",
             "requires": ADAPTER_ACTION_REQUIRES},
    "AD10": {"composition": "mux", "rows": ("ens", "enh"), "requires": FLOW_ACTIVE,
             "semantic": True, "paired": True},
    "PX01": {"composition": "mux", "rows": ("ens", "enh"),
             "requires": CONTENTION + (("mux_endings_max_ownership", EQ0),)},
    "PX02": {"composition": "mux", "rows": ("ens", "enh"),
             "requires": CONTENTION + (("mux_endings_max_ownership", EQ0),)},
    "PX03": {"composition": "mux", "rows": ("ens", "enh"),
             "requires": CONTENTION + (("mux_endings_syn_timeout", EQ0),)},
    "PX04": {"composition": "mux", "rows": ("ens", "enh"),
             "requires": CONTENTION + (("mux_endings_active_read_timeout", EQ0),)},
    "PX12": {"composition": "mux", "rows": ("ens", "enh"),
             "requires": CONTENTION + (("mux_foreign_initiator_releases", EQ0),)},
    "CX01": {"composition": "mux", "rows": ("ens", "enh"),
             "requires": CONTENTION + (("mux_escape_pair_verify_failures", EQ0),)},
    "DS01": {"composition": "direct", "rows": ("ens", "enh", "udp", "tcp"),
             "requires": (("active_frames", GT0),), "semantic": True},
    "ES01": {"composition": "ebusd-tcp", "rows": ("ebusd-tcp",),
             "requires": (("active_frames", GT0),), "semantic": True},
}

# Cells of the inventory that are never executed; asking for them is refused.
RESERVED = {
    "PX05": "fifo_order_superseded_by_address_priority",
    "PX06": "fifo_order_superseded_by_address_priority",
    "PX07": "fifo_order_superseded_by_address_priority",
    "PX08": "fifo_order_superseded_by_address_priority",
    "AD05": "mux_raw_symbol_forwarding_not_implemented",
    "AD08": "mux_raw_symbol_forwarding_not_implemented",
    "AD12": "mux_raw_symbol_forwarding_not_implemented",
    "PX09": "mux_local_responder_not_implemented",
    "PX10": "mux_local_responder_not_implemented",
    "PX11": "mux_local_responder_not_implemented",
    "AD11": "invariant_retired",
}

RESERVED_ROWS = {
    "udp": "mux_southbound_plain_not_implemented",
    "tcp": "mux_southbound_plain_not_implemented",
    "uart-ens": "uart_direct_hardware_absent",
    "uart-enh": "uart_direct_hardware_absent",
    "uart-raw": "uart_direct_hardware_absent",
}

# The semantic record: one GraphQL query and the MCP read tool of the same
# plane. Units are those of the semantic contract for each field.
SEMANTIC_PLANE = "system"
SEMANTIC_FIELDS = {
    "system_off": {"unit": "", "tolerance": None},
    "maintenance_active": {"unit": "", "tolerance": None},
    "system_water_pressure": {"unit": "bar", "tolerance": 0.2},
    "system_flow_temperature": {"unit": "degC", "tolerance": 2.0},
    "outdoor_temperature": {"unit": "degC", "tolerance": 2.0},
    "outdoor_temperature_daily_average": {"unit": "degC", "tolerance": 1.0},
}
GRAPHQL_SEMANTIC_QUERY = "query { system { state { " + " ".join(SEMANTIC_FIELDS) + " } } }"
MCP_READ_TOOLS = ("ebus_v1_semantic_system_get",)
MCP_SEMANTIC_TOOL = "ebus_v1_semantic_system_get"

CASE_ID_PATTERN = re.compile(r"^[A-Z]+-[A-Z]{2}[0-9]{2}$")
PROM_SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")
PROM_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


class RefusalError(Exception):
    """The invariant cannot be judged by this command."""


class UsageError(Exception):
    """The case context is missing or invalid."""


class SurfaceError(Exception):
    """A gateway read surface did not answer as expected."""


class SignalMissingError(Exception):
    """The gateway does not publish a series the invariant needs."""

    def __init__(self, names):
        super().__init__(", ".join(names))
        self.names = list(names)


# ---------------------------------------------------------------- context


def env_value(environ, name):
    return (environ.get(name) or "").strip()


def read_context(environ):
    context = {name.lower(): env_value(environ, "MATRIX_" + name) for name in (
        "CASE_ID", "INVARIANT", "SOUTHBOUND_TRANSPORT", "COMPOSITION", "CASE_DIR",
        "OUTPUT_DIR", "SESSION_ID", "CANDIDATE", "HARDWARE_SETUP", "CELL_CLASS",
        "AD10_PAIRED_CASES", "EXTERNAL_WRITE_POLICY", "CONFIG_DIR")}
    context["invariant"] = context["invariant"].upper()
    context["case_id"] = context["case_id"].upper()
    context["southbound_transport"] = context["southbound_transport"].lower()
    context["composition"] = context["composition"].lower()
    for required in ("invariant", "case_id", "southbound_transport", "composition", "case_dir"):
        if not context[required]:
            raise UsageError("MATRIX_" + required.upper() + " is required")
    if not context["config_dir"]:
        context["config_dir"] = os.path.join(context["case_dir"], "configs")
    return context


def resolve_check(context):
    """Return the check of an executable cell, or raise RefusalError."""
    invariant = context["invariant"]
    southbound = context["southbound_transport"]
    if invariant in RESERVED:
        raise RefusalError(invariant + " is not executable: " + RESERVED[invariant])
    check = CHECKS.get(invariant)
    if check is None:
        raise RefusalError("unknown invariant " + invariant)
    if invariant not in ("DS01", "ES01") and southbound in RESERVED_ROWS:
        raise RefusalError(invariant + " on " + southbound + " is not executable: " + RESERVED_ROWS[southbound])
    if southbound not in check["rows"]:
        raise RefusalError(invariant + " does not run on southbound transport " + southbound)
    if context["composition"] != check["composition"]:
        raise RefusalError(invariant + " runs on composition " + check["composition"]
                      + ", not " + context["composition"])
    expected_id = ROW_TOKEN[southbound] + "-" + invariant
    if context["case_id"] != expected_id:
        raise RefusalError("case " + context["case_id"] + " does not match " + expected_id)
    if context["cell_class"] == "-":
        raise RefusalError(expected_id + " has cell class '-' and is never executed")
    return check


def surface_urls(environ):
    base = env_value(environ, "MATRIX_GATEWAY_BASE_URL").rstrip("/")
    defaults = {
        "observability": "/portal/api/v1/bus/observability",
        "metrics": "/metrics",
        "vars": "/debug/vars",
        "graphql": "/graphql",
        "mcp": "/mcp",
    }
    names = {"observability": "MATRIX_BUS_OBSERVABILITY_URL", "metrics": "MATRIX_METRICS_URL",
             "vars": "MATRIX_DEBUG_VARS_URL", "graphql": "MATRIX_GRAPHQL_URL",
             "mcp": "MATRIX_MCP_URL"}
    urls = {}
    for key, path in defaults.items():
        urls[key] = env_value(environ, names[key]) or (base + path if base else "")
        if not urls[key]:
            raise UsageError(names[key] + " or MATRIX_GATEWAY_BASE_URL is required")
    return urls


def float_setting(environ, name, default):
    raw = env_value(environ, name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise UsageError(name + " is not a number") from error
    if value <= 0:
        raise UsageError(name + " must be positive")
    return value


def timing(environ):
    return {
        "window": float_setting(environ, "MATRIX_SMOKE_WINDOW_SEC", 60.0),
        "poll": float_setting(environ, "MATRIX_SMOKE_POLL_SEC", 3.0),
        "http_timeout": float_setting(environ, "MATRIX_SMOKE_HTTP_TIMEOUT_SEC", 10.0),
    }


# ---------------------------------------------------------------- reads


def guard_graphql(document):
    """Allow only query documents; the smoke command never mutates."""
    stripped = re.sub(r"#[^\n]*", "", document).strip()
    if not stripped.startswith("query") and not stripped.startswith("{"):
        raise ValueError("only GraphQL query documents are allowed")
    if re.search(r"\b(mutation|subscription)\b", stripped):
        raise ValueError("GraphQL mutation and subscription documents are refused")
    return document


def guard_mcp_tool(name):
    """Allow only the read tools the semantic record uses."""
    if name not in MCP_READ_TOOLS:
        raise ValueError("MCP tool " + name + " is not an allowed read tool")
    return name


def http_get(url, timeout):
    request = urllib.request.Request(url, method="GET")
    return http_send(request, timeout)


def http_post_json(url, payload, timeout):
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
    return http_send(request, timeout)


def http_send(request, timeout):
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        raise SurfaceError("HTTP status " + str(error.code)) from error
    except (urllib.error.URLError, OSError, ValueError) as error:
        raise SurfaceError("no answer (" + type(error).__name__ + ")") from error


def parse_prometheus(text):
    samples = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = PROM_SAMPLE.match(line)
        if not match:
            continue
        labels = tuple(sorted(PROM_LABEL.findall(match.group(2) or "")))
        try:
            samples[(match.group(1), labels)] = float(match.group(3))
        except ValueError:
            continue
    return samples


def read_json(text, surface):
    try:
        return json.loads(text)
    except ValueError as error:
        raise SurfaceError(surface + " did not answer JSON") from error


def take_sample(urls, settings):
    timeout = settings["http_timeout"]
    sample = {}
    try:
        sample["observability"] = read_json(http_get(urls["observability"], timeout), "bus observability")
        sample["metrics"] = parse_prometheus(http_get(urls["metrics"], timeout))
        sample["vars"] = read_json(http_get(urls["vars"], timeout), "expvar")
    except SurfaceError as error:
        raise SurfaceError("gateway read surface: " + str(error)) from error
    return sample


# ---------------------------------------------------------------- measures


def status_of(sample):
    return sample["observability"].get("status") or {}


def own_source(sample):
    # The admission record nests the selected address; it has no flat source key.
    admission = status_of(sample).get("bus_admission") or {}
    source = (admission.get("source_selection") or {}).get("selected_source")
    if isinstance(source, bool) or not isinstance(source, int) or source <= 0 or source > 0xFF:
        return None
    return "0x%02x" % source


def frame_series(sample):
    series = {}
    for item in sample["observability"].get("frames") or []:
        key = (item.get("scope"), item.get("source"), item.get("target"),
               item.get("family"), item.get("frame_type"))
        series[key] = float(item.get("count") or 0)
    return series


def frame_delta(baseline, current, keep):
    before = frame_series(baseline)
    total = 0.0
    for key, value in frame_series(current).items():
        if key[4] != "abandoned_partial" and keep(key):
            total += value - before.get(key, 0.0)
    return total


def metric_sum(sample, name, labels=None):
    total = 0.0
    for (metric, sample_labels), value in sample["metrics"].items():
        if metric == name and (labels is None or set(labels) <= set(sample_labels)):
            total += value
    return total


def metric_delta(baseline, current, name, labels=None):
    return metric_sum(current, name, labels) - metric_sum(baseline, name, labels)


def dedup_count(sample, disposition):
    adjudications = sample["vars"].get(DEDUP_ADJUDICATIONS) or {}
    value = adjudications.get(disposition, 0) if isinstance(adjudications, dict) else 0
    return float(value or 0)


def frame_measures(baseline, current, own):
    measures = {
        "active_frames": frame_delta(baseline, current, lambda key: key[0] == "active"),
        "passive_frames": frame_delta(baseline, current, lambda key: key[0] == "passive"),
    }
    if own is None:
        return measures
    measures["passive_own_frames"] = frame_delta(
        baseline, current, lambda key: key[0] == "passive" and key[1] == own)
    measures["passive_third_party_frames"] = frame_delta(
        baseline, current, lambda key: key[0] == "passive" and key[1] != own)
    measures["external_identification_frames"] = frame_delta(
        baseline, current, lambda key: key[0] == "passive" and key[1] != own and key[3] == "0704")
    return measures


def counter_measures(baseline, current):
    measures = {
        "passive_completed_transactions": metric_delta(baseline, current, PASSIVE_COMPLETED),
        "mux_transactions_done": metric_delta(baseline, current, MUX_ENDINGS, [("reason", "transaction_done")]),
        "mux_foreign_initiator_releases": metric_delta(baseline, current, MUX_FOREIGN),
        "mux_escape_pair_verify_failures": metric_delta(baseline, current, MUX_ESCAPE),
        "dedup_matched_active": dedup_count(current, "matched_active_duplicate")
        - dedup_count(baseline, "matched_active_duplicate"),
    }
    for reason in ("max_ownership", "syn_timeout", "active_read_timeout"):
        measures["mux_endings_" + reason] = metric_delta(baseline, current, MUX_ENDINGS, [("reason", reason)])
    return measures


def compute_measures(baseline, current, own):
    measures = frame_measures(baseline, current, own)
    measures.update(counter_measures(baseline, current))
    if "passive_own_frames" in measures:
        measures["self_echo_frames"] = max(0.0, measures["passive_own_frames"] - measures["dedup_matched_active"])
    return measures


def series_values(sample, name):
    """Return {labels: value} of every sample of one series."""
    return {labels: value for (metric, labels), value in sample["metrics"].items() if metric == name}


def label_value(labels, key):
    return dict(labels).get(key)


def discontinuity_measures(readings):
    """Measures of an adapter-action cell over every judged reading.

    The consumer sessions are those the mux reports at the first reading. The
    discontinuity reading is the first one in which any of them moved; the
    resumption is measured from that reading to the last one.
    """
    baseline, final = readings[0], readings[-1]
    sessions = series_values(baseline, MUX_SESSION_DISCONTINUITIES)
    after = series_values(final, MUX_SESSION_DISCONTINUITIES)
    observed = sum(1 for labels, value in sessions.items() if after.get(labels, 0.0) - value >= 1)
    measures = {
        "consumer_sessions_observing_discontinuity": float(observed),
        "consumer_sessions_missing_discontinuity": float(len(sessions) - observed),
        "mux_upstream_reconnects": metric_delta(baseline, final, MUX_UPSTREAM_RECONNECTS),
        "out_of_step_hold_readings": float(sum(1 for reading in readings[1:] if metric_sum(reading, MUX_HELD) >= 1)),
    }
    boundary = None
    for index, reading in enumerate(readings[1:], start=1):
        current = series_values(reading, MUX_SESSION_DISCONTINUITIES)
        if any(current.get(labels, 0.0) > value for labels, value in sessions.items()):
            boundary = index
            break
    unmeasured = {}
    resumed = ("resumed_active_frames", "resumed_mux_transactions_done", "resumed_passive_completed_transactions")
    if boundary is None:
        for name in resumed:
            measures[name] = None
            unmeasured[name] = "no consumer session observed a discontinuity during the case window"
        return measures, unmeasured, None
    reading = readings[boundary]
    measures["resumed_active_frames"] = frame_delta(reading, final, lambda key: key[0] == "active")
    measures["resumed_mux_transactions_done"] = metric_delta(
        reading, final, MUX_ENDINGS, [("reason", "transaction_done")])
    measures["resumed_passive_completed_transactions"] = metric_delta(reading, final, PASSIVE_COMPLETED)
    return measures, unmeasured, boundary


def priority_key(address):
    """Selection order of an initiator: lowest low nibble, then lowest high nibble."""
    return (address & 0x0F, address >> 4)


def priority_winner(gateway, external):
    """The side whose bid wins a contended boundary under initiator priority."""
    return "gateway" if priority_key(gateway) < priority_key(external) else "external"


def external_initiators(baseline, current, own):
    before = frame_series(baseline)
    sources = set()
    for key, value in frame_series(current).items():
        if key[0] == "passive" and key[1] != own and key[3] == "0704" and value - before.get(key, 0.0) > 0:
            sources.add(key[1])
    return sorted(str(source) for source in sources)


def parse_address(text):
    try:
        value = int(str(text), 16)
    except ValueError:
        return None
    return value if 0 < value <= 0xFF else None


def priority_measures(baseline, current, own):
    """Measures of the address-priority cell and the priority it expects."""
    names = ("contended_selections_won_by_priority", "contended_selections_won_against_priority")
    external = external_initiators(baseline, current, own)
    gateway = parse_address(own) if own else None
    evidence = {"gateway_source": own, "external_initiators": external, "expected_winner": None}
    measures, unmeasured = {}, {}
    if gateway is None or len(external) != 1 or parse_address(external[0]) is None:
        if gateway is None:
            why = "the gateway reports no own source address"
        elif not external:
            why = ("the external session's initiator is not one address (none); frames of an external session "
                   "that uses the gateway's own initiator cannot be told apart, so the external session must "
                   "use a different initiator")
        else:
            why = "the external session's initiator is not one address (" + ", ".join(external) + ")"
        for name in names:
            measures[name] = None
            unmeasured[name] = why
    else:
        winner = priority_winner(gateway, parse_address(external[0]))
        loser = "external" if winner == "gateway" else "gateway"
        evidence["expected_winner"] = winner
        measures[names[0]] = metric_delta(baseline, current, MUX_CONTENDED_SELECTIONS, [("winner", winner)])
        measures[names[1]] = metric_delta(baseline, current, MUX_CONTENDED_SELECTIONS, [("winner", loser)])
    before = series_values(baseline, MUX_RETAINED_BIDS)
    granted = not_granted = 0.0
    for labels, value in series_values(current, MUX_RETAINED_BIDS).items():
        delta = value - before.get(labels, 0.0)
        if label_value(labels, "result") == "granted":
            granted += delta
        else:
            not_granted += delta
    measures["retained_bids_granted"] = granted
    measures["retained_bids_not_granted"] = not_granted
    return measures, unmeasured, evidence


def missing_signals(check, sample):
    """Series the check needs that the gateway does not publish.

    Without any AdapterMux diagnostics the composition is wrong, which the
    transport check fails; only a mux that runs but lacks a series is a
    missing signal.
    """
    published = {name for name, _ in sample["metrics"]}
    if MUX_ENDINGS not in published:
        return []
    return [name for name in check.get("signals", ()) if name not in published]


def tracked_counters(sample):
    counters = {"frames " + "/".join(str(part) for part in key): value
                for key, value in frame_series(sample).items()}
    for (name, labels), value in sample["metrics"].items():
        # Only monotonic series: gauges such as the held-arbitration flag may fall.
        if (name.startswith("ebus_adaptermux_") and name.endswith("_total")) or name == PASSIVE_COMPLETED:
            counters[name + str(dict(labels))] = value
    adjudications = sample["vars"].get(DEDUP_ADJUDICATIONS) or {}
    if isinstance(adjudications, dict):
        for key, value in adjudications.items():
            counters[DEDUP_ADJUDICATIONS + "/" + str(key)] = float(value or 0)
    return counters


def backward_counters(baseline, current):
    before = tracked_counters(baseline)
    after = tracked_counters(current)
    return sorted(name for name, value in before.items() if after.get(name, 0.0) < value)


# ---------------------------------------------------------------- static checks


def check_transport(context, sample, check):
    expected = EXPECTED_TRANSPORT_CLASS[(check["composition"], context["southbound_transport"])]
    reported = status_of(sample).get("transport_class")
    reasons = []
    if reported != expected:
        reasons.append("gateway reports transport class " + str(reported) + ", the cell needs " + expected)
    mux_present = any(name == MUX_ENDINGS for name, _ in sample["metrics"])
    if check["composition"] == "mux" and not mux_present:
        reasons.append("AdapterMux diagnostics are absent: the gateway does not run the mux")
    if check["composition"] != "mux" and mux_present:
        reasons.append("AdapterMux diagnostics are present: the composition must bypass the mux")
    return {"reported": reported, "expected": expected, "mux_diagnostics": mux_present}, reasons


def read_workload(context):
    path = os.path.join(context["config_dir"], "workload.json")
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def check_write_guard(context, sample):
    declared = context["external_write_policy"]
    reported = (status_of(sample).get("feature_flags") or {}).get("external_write_policy")
    state = metric_sum(sample, WRITE_POLICY_STATE, [("policy", declared)]) if declared else 0.0
    workload = read_workload(context) or {}
    allowlist = sorted(workload.get("allowlisted_request_types") or [])
    evidence = {"declared_policy": declared, "gateway_policy": reported,
                "gateway_policy_state": state, "client_allowlist": allowlist}
    reasons = []
    if declared not in EXTERNAL_WRITE_POLICIES:
        reasons.append("MATRIX_EXTERNAL_WRITE_POLICY is not a declared policy")
    elif reported != declared or state != 1.0:
        reasons.append("gateway write policy " + str(reported) + " does not confirm the declared " + declared)
    if allowlist != sorted(READ_ONLY_REQUEST_TYPES):
        reasons.append("configs/workload.json does not carry the read-only client allowlist")
    evidence["confirmed"] = not reasons
    return evidence, ["write guard not confirmed: " + reason for reason in reasons]


def check_passive_tap(sample):
    capability = status_of(sample).get("capability") or {}
    if capability.get("passive_available") is True and capability.get("tap_connected") is True:
        return []
    return ["passive path unavailable: passive_available or tap_connected is false"]


# ---------------------------------------------------------------- judgement


def rule_met(rule, value):
    if value is None:
        return False
    if rule == GT0:
        return value > 0
    return value == (1 if rule == EQ1 else 0)


def judge_requirements(check, measures, unmeasured=None):
    """Return one row per requirement and the reasons of the unmet ones."""
    rows, reasons = [], []
    for name, rule in check["requires"]:
        value = measures.get(name)
        met = rule_met(rule, value)
        rows.append({"measure": name, "rule": rule, "value": value, "met": met})
        if met:
            continue
        if value is None:
            why = (unmeasured or {}).get(name, "the gateway reports no own source address")
            reasons.append(name + " cannot be measured: " + why)
        elif rule == EQ1:
            reasons.append(name + " is " + format_number(value) + " during the case window; it must be exactly 1")
        elif rule == EQ0:
            reasons.append(name + " moved by " + format_number(value) + " during the case window; it must stay")
        else:
            reasons.append(name + " did not move during the case window (no bus flow)")
    return rows, reasons


def format_number(value):
    return str(int(value)) if float(value).is_integer() else str(value)


def flow_ready(check, measures):
    for name, rule in check["requires"]:
        value = measures.get(name)
        if rule == GT0 and (value is None or value <= 0):
            return False
    return True


def observe_window(urls, settings, check, clock, announce=None):
    """Sample until the flow requirements hold or the window ends.

    Every consecutive pair of samples is compared for a counter moving
    backwards, so a reset that recovers before the last sample is still seen.
    A check that forbids a counter moving observes the whole window: the
    concurrent client finishes only after this command returns, so a late
    movement must still be seen. Once the requirements hold, one more sample
    confirms that no counter fell. A sample whose sleep or reads complete
    after the window ends may count traffic outside the window, so it is
    discarded and the last sample completed inside the window is judged.
    A check whose series the gateway does not publish stops after the first
    sample, before any operator action is announced.
    """
    full_window = any(rule in (EQ0, EQ1) for _, rule in check["requires"])
    baseline = take_sample(urls, settings)
    missing = missing_signals(check, baseline)
    if missing:
        raise SignalMissingError(missing)
    if announce is not None:
        announce()
    deadline = clock.monotonic() + settings["window"]
    window = {"samples": 1, "discarded_after_window": 0}
    final = baseline
    readings = [baseline]
    regressions = set(())
    ready = False
    while True:
        remaining = deadline - clock.monotonic()
        if remaining <= 0:
            break
        clock.sleep(min(settings["poll"], remaining))
        current = take_sample(urls, settings)
        if clock.monotonic() > deadline:
            window["discarded_after_window"] += 1
            break
        window["samples"] += 1
        regressions.update(backward_counters(final, current))
        final = current
        readings.append(current)
        if ready:
            break
        own = own_source(baseline) or own_source(final)
        ready = not full_window and flow_ready(check, compute_measures(baseline, final, own))
    return readings, own_source(baseline) or own_source(final), window, sorted(regressions)


# ---------------------------------------------------------------- semantic record


def graphql_semantic(urls, settings):
    payload = {"query": guard_graphql(GRAPHQL_SEMANTIC_QUERY)}
    answer = read_json(http_post_json(urls["graphql"], payload, settings["http_timeout"]), "GraphQL")
    if answer.get("errors"):
        raise SurfaceError("GraphQL answered errors")
    state = (((answer.get("data") or {}).get(SEMANTIC_PLANE) or {}).get("state")) or {}
    return state


def mcp_semantic(urls, settings):
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": guard_mcp_tool(MCP_SEMANTIC_TOOL), "arguments": {}}}
    answer = read_json(http_post_json(urls["mcp"], payload, settings["http_timeout"]), "MCP")
    result = answer.get("result") or {}
    content = result.get("content") or []
    if result.get("isError") or not content:
        raise SurfaceError("MCP read tool answered an error")
    envelope = read_json(content[0].get("text") or "", "MCP tool")
    data = envelope.get("data") or {}
    # The contract is the lineage AD10 compares; a blank one proves nothing.
    contract = well_formed_contract((envelope.get("meta") or {}).get("contract"))
    if contract is None:
        raise SurfaceError("MCP read tool answered without a meta.contract")
    return (data.get("state") or {}), contract


def well_formed_contract(contract):
    """Return the MCP contract object (name, major, minor) or None when malformed."""
    if not isinstance(contract, dict):
        return None
    name = contract.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    version = {}
    for key in ("major", "minor"):
        number = contract.get(key)
        if isinstance(number, bool) or not isinstance(number, int) or number < 0:
            return None
        version[key] = number
    return {"name": name, "major": version["major"], "minor": version["minor"]}


def json_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    return "string" if isinstance(value, str) else "object"


def semantic_record(urls, settings):
    graphql_state = graphql_semantic(urls, settings)
    mcp_state, contract = mcp_semantic(urls, settings)
    fields, parity = {}, []
    for name, spec in SEMANTIC_FIELDS.items():
        value = graphql_state.get(name)
        fields[name] = {"present": value is not None, "type": json_type(value), "value": value,
                        "unit": spec["unit"],
                        "lineage": {"plane": SEMANTIC_PLANE, "path": "state." + name, "mcp_contract": contract}}
        # An omitted MCP key and a GraphQL null are the same absence.
        if mcp_state.get(name) != value:
            parity.append(name)
    return {"plane": SEMANTIC_PLANE, "fields": fields, "parity_mismatches": parity}


# ---------------------------------------------------------------- AD10


def values_equivalent(name, left, right):
    tolerance = SEMANTIC_FIELDS[name]["tolerance"]
    if tolerance is None or json_type(left) != "number" or json_type(right) != "number":
        return left == right
    return abs(float(left) - float(right)) <= tolerance


def compare_records(mux, replaced):
    """Compare two semantic records; each aspect is a separate decision."""
    aspects = {"presence": [], "types": [], "units": [], "values": [], "lineage": []}
    for name in SEMANTIC_FIELDS:
        left = (mux.get("fields") or {}).get(name) or {}
        right = (replaced.get("fields") or {}).get(name) or {}
        for aspect, key in (("presence", "present"), ("types", "type"), ("units", "unit"), ("lineage", "lineage")):
            if left.get(key) != right.get(key):
                aspects[aspect].append(name)
        if not values_equivalent(name, left.get("value"), right.get("value")):
            aspects["values"].append(name)
    parity = sorted(set(mux.get("parity_mismatches") or []) | set(replaced.get("parity_mismatches") or []))
    aspects["mcp_graphql_parity"] = parity
    return {aspect: {"equivalent": not names, "fields": names} for aspect, names in aspects.items()}


def load_json_file(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def paired_case_reasons(context, case_id, verdict, evidence):
    if verdict is None:
        return ["paired case " + case_id + " has no verdict in the output directory"]
    reasons = []
    if verdict.get("outcome") != "pass":
        reasons.append("paired case " + case_id + " did not pass (outcome " + str(verdict.get("outcome")) + ")")
    for key in ("session_id", "candidate", "hardware_setup"):
        if verdict.get(key) != context[key]:
            reasons.append("paired case " + case_id + " belongs to another " + key)
    if evidence is None or evidence.get("result") != "pass" or not evidence.get("semantic_record"):
        reasons.append("paired case " + case_id + " has no passing smoke evidence with a semantic record")
    return reasons


def compare_pairs(context, mux_record):
    names = [name.strip().upper() for name in context["ad10_paired_cases"].split(",") if name.strip()]
    if not names:
        return {"pairs": []}, ["MATRIX_AD10_PAIRED_CASES names no paired case"]
    pairs, reasons = [], []
    for case_id in names:
        if not CASE_ID_PATTERN.match(case_id):
            reasons.append("paired case name " + case_id + " is not a case identifier")
            continue
        directory = os.path.join(context["output_dir"], case_id)
        verdict = load_json_file(os.path.join(directory, "verdict.json"))
        evidence = load_json_file(os.path.join(directory, EVIDENCE_FILE))
        problems = paired_case_reasons(context, case_id, verdict, evidence)
        if problems:
            reasons.extend(problems)
            pairs.append({"case_id": case_id, "available": False})
            continue
        decisions = compare_records(mux_record, evidence["semantic_record"])
        pairs.append({"case_id": case_id, "available": True, "decisions": decisions})
        for aspect, decision in decisions.items():
            if not decision["equivalent"]:
                reasons.append(case_id + " " + aspect + " not equivalent: " + ", ".join(decision["fields"]))
    return {"pairs": pairs}, reasons


# ---------------------------------------------------------------- run


def operator_action_text(context, check, settings):
    return ("operator action for " + context["case_id"] + ": " + check["operator_action"]
            + " now, inside the " + format_number(settings["window"]) + " s case window")


OPERATOR_CHANNEL = "/dev/tty"


def announce_to_operator(text, stream):
    """Print the cue to the log stream and, when there is one, the operator's terminal.

    The runner captures this command's output in the case log, so the cue is
    also written to the controlling terminal the operator started the run on.
    """
    print(text, file=stream, flush=True)
    try:
        with open(OPERATOR_CHANNEL, "w", encoding="utf-8") as channel:
            channel.write(text + "\n")
    except OSError:
        pass


def judge(context, check, urls, settings, clock, stream=None):
    evidence = {}
    announce = None
    if check.get("operator_action"):
        evidence["operator_action"] = check["operator_action"]
        announce = functools.partial(announce_to_operator, operator_action_text(context, check, settings), stream)
    readings, own, window, backwards = observe_window(urls, settings, check, clock, announce)
    baseline, current = readings[0], readings[-1]
    evidence["window"] = dict(window, window_seconds=settings["window"])
    evidence["gateway_source"] = own
    evidence["transport"], reasons = check_transport(context, current, check)
    evidence["write_guard"], guard_reasons = check_write_guard(context, current)
    reasons += guard_reasons
    if check.get("passive_tap"):
        reasons += check_passive_tap(current)
    if backwards:
        reasons.append("counters moved backwards during the case window: " + ", ".join(backwards))
    measures = compute_measures(baseline, current, own)
    unmeasured = {}
    if check.get("discontinuity"):
        extra, unmeasured, boundary = discontinuity_measures(readings)
        measures.update(extra)
        evidence["discontinuity_reading"] = boundary
    if check.get("priority"):
        extra, unmeasured, evidence["priority"] = priority_measures(baseline, current, own)
        measures.update(extra)
    evidence["measures"] = measures
    evidence["requirements"], flow_reasons = judge_requirements(check, measures, unmeasured)
    reasons += flow_reasons
    if check.get("semantic") and not reasons:
        reasons += attach_semantic(context, check, urls, settings, evidence)
    return evidence, reasons


def attach_semantic(context, check, urls, settings, evidence):
    try:
        evidence["semantic_record"] = semantic_record(urls, settings)
    except SurfaceError as error:
        return ["semantic record: " + str(error)]
    reasons = []
    if evidence["semantic_record"]["parity_mismatches"]:
        reasons.append("MCP/GraphQL parity differs for " + ", ".join(evidence["semantic_record"]["parity_mismatches"]))
    if check.get("paired"):
        evidence["ad10"], pair_reasons = compare_pairs(context, evidence["semantic_record"])
        reasons += pair_reasons
    return reasons


def base_evidence(context):
    keys = ("case_id", "invariant", "southbound_transport", "composition", "session_id",
            "candidate", "hardware_setup", "cell_class")
    evidence = {"schema": EVIDENCE_SCHEMA}
    evidence.update({key: context.get(key, "") for key in keys})
    return evidence


def write_evidence(case_dir, evidence):
    if not case_dir:
        return
    os.makedirs(case_dir, exist_ok=True)
    path = os.path.join(case_dir, EVIDENCE_FILE)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(evidence, handle, indent=2, sort_keys=True)
        handle.write("\n")


RESULT_NAMES = {EXIT_PASS: "pass", EXIT_FAIL: "fail", EXIT_USAGE: "usage_error", EXIT_REFUSED: "refused",
                EXIT_SIGNAL_MISSING: "signal_missing"}


def finish(context, evidence, status, reasons, stream):
    evidence["result"] = RESULT_NAMES[status]
    evidence["reasons"] = reasons
    write_evidence(context["case_dir"], evidence)
    print("adaptermux-v2 smoke " + (context["case_id"] or "case") + ": " + evidence["result"], file=stream)
    for reason in reasons:
        print("  " + reason, file=stream)
    return status


def run(environ, stream=sys.stdout, clock=time):
    try:
        context = read_context(environ)
    except UsageError as error:
        print("adaptermux-v2 smoke: usage_error: " + str(error), file=stream)
        return EXIT_USAGE
    evidence = base_evidence(context)
    try:
        check = resolve_check(context)
    except RefusalError as error:
        return finish(context, evidence, EXIT_REFUSED, ["refused: " + str(error)], stream)
    try:
        urls, settings = surface_urls(environ), timing(environ)
    except UsageError as error:
        return finish(context, evidence, EXIT_USAGE, [str(error)], stream)
    try:
        judged, reasons = judge(context, check, urls, settings, clock, stream)
    except SurfaceError as error:
        return finish(context, evidence, EXIT_FAIL, [str(error)], stream)
    except SignalMissingError as error:
        evidence["missing_signals"] = error.names
        reasons = ["signal missing: the gateway does not publish " + name + " (" + SIGNAL_MEANING[name]
                   + "), which " + context["invariant"] + " needs" for name in error.names]
        return finish(context, evidence, EXIT_SIGNAL_MISSING, reasons, stream)
    evidence.update(judged)
    return finish(context, evidence, EXIT_FAIL if reasons else EXIT_PASS, reasons, stream)


def main():
    return run(os.environ)


if __name__ == "__main__":
    sys.exit(main())
