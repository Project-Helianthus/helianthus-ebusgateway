#!/usr/bin/env python3
"""Offline tests of the adaptermux-v2 smoke command against recorded responses.

A local stub serves the synthetic answers of
scripts/testdata/adaptermux_v2_smoke/recorded_responses.json: the first read
of each surface is the baseline, later reads the case-window state.
"""

from __future__ import annotations

import copy
import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCRIPT = pathlib.Path(__file__).with_name("adaptermux_v2_smoke.py")
RECORDED = pathlib.Path(__file__).with_name("testdata") / "adaptermux_v2_smoke" / "recorded_responses.json"
SPEC = importlib.util.spec_from_file_location("adaptermux_v2_smoke", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)

DATA = json.loads(RECORDED.read_text(encoding="utf-8"))

GRID_INVARIANTS = ("AD01", "AD02", "AD03", "AD06", "AD10", "PX01", "PX02", "PX03", "PX04", "PX12", "CX01")
# Invariants whose checks read AdapterMux series beyond the common surfaces.
ADAPTER_ACTION_INVARIANTS = ("AD04", "AD09")
PRIORITY_INVARIANTS = ("AD07",)
FIFO_ORDER_INVARIANTS = ("PX05", "PX06", "PX07", "PX08")
ROWS = {"ens": "ENS", "enh": "ENH", "udp": "UDP", "tcp": "TCP"}
SESSION = {"MATRIX_SESSION_ID": "session-a", "MATRIX_CANDIDATE": "candidate-a"}

# Cells of every executable invariant: (invariant, composition, southbound).
EXECUTABLE_CELLS = (
    [(invariant, "mux", row) for invariant in GRID_INVARIANTS for row in ("ens", "enh")]
    + [("DS01", "direct", row) for row in ("ens", "enh", "udp", "tcp")]
    + [("ES01", "ebusd-tcp", "ebusd-tcp")]
)

# The counter an EQ0 rule watches, as (metric, labels) in the recorded metrics.
STAY_COUNTERS = {
    "PX01": ("ebus_adaptermux_transaction_endings_total", {"reason": "max_ownership"}),
    "PX02": ("ebus_adaptermux_transaction_endings_total", {"reason": "max_ownership"}),
    "PX03": ("ebus_adaptermux_transaction_endings_total", {"reason": "syn_timeout"}),
    "PX04": ("ebus_adaptermux_transaction_endings_total", {"reason": "active_read_timeout"}),
    "PX12": ("ebus_adaptermux_foreign_initiator_releases_total", {}),
    "CX01": ("ebus_adaptermux_escape_pair_verify_failures_total", {"step": "suffix"}),
}


def case_id(invariant, southbound):
    return ("EBUSD" if southbound == "ebusd-tcp" else ROWS[southbound]) + "-" + invariant


def render_metrics(rows):
    lines = []
    for name, labels, value in rows:
        rendered = ",".join('%s="%s"' % (key, labels[key]) for key in sorted(labels))
        lines.append("%s%s %s" % (name, "{" + rendered + "}" if rendered else "", value))
    return "\n".join(lines) + "\n"


class Phase:
    """The surfaces at one point of the case window, before rendering."""

    def __init__(self, name, composition, southbound, signals=None):
        mux = composition == "mux"
        self.status = copy.deepcopy(DATA["status"])
        self.status["transport_class"] = SMOKE.EXPECTED_TRANSPORT_CLASS[(composition, southbound)]
        self.frames = copy.deepcopy(DATA["frames"][name])
        metrics = DATA["metrics"][name]
        self.metrics = copy.deepcopy(metrics["common"] + (metrics["mux"] if mux else []))
        if mux and signals is not None:
            self.metrics += copy.deepcopy(DATA["adaptermux_signals"][signals])
        self.vars = copy.deepcopy(DATA["vars"][name])

    def row(self, metric, labels):
        for row in self.metrics:
            if row[0] == metric and row[1] == labels:
                return row
        raise AssertionError("no recorded series " + metric)

    def bump(self, metric, labels, amount):
        self.row(metric, labels)[2] += amount

    def set(self, metric, labels, value):
        self.row(metric, labels)[2] = value

    def drop(self, metric):
        self.metrics = [row for row in self.metrics if row[0] != metric]

    def bodies(self):
        observability = {"status": self.status, "frames": self.frames}
        return {
            "/portal/api/v1/bus/observability": json.dumps(observability),
            "/metrics": render_metrics(self.metrics),
            "/debug/vars": json.dumps(self.vars),
        }


def graphql_body(state):
    return json.dumps({"data": {"system": {"state": state}}})


def mcp_body(state, contract):
    # The gateway omits nil fields of the MCP state.
    present = {key: value for key, value in state.items() if value is not None}
    envelope = {"meta": {"contract": contract}, "data": {"state": present}, "error": None}
    return json.dumps({"jsonrpc": "2.0", "id": 1,
                       "result": {"content": [{"type": "text", "text": json.dumps(envelope)}], "isError": False}})


class StubGateway:
    """Serves one scenario: per path, a sequence of answers whose last repeats."""

    def __init__(self):
        self.requests = []
        self.answers = {}
        self.cursor = {}
        self.on_request = None
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def answer_get(self):
                stub.serve(self, b"")

            def answer_post(self):
                length = int(self.headers.get("Content-Length") or 0)
                stub.serve(self, self.rfile.read(length))

            def log_message(self, *args):
                return

        # BaseHTTPRequestHandler dispatches on do_<METHOD> attribute names.
        setattr(Handler, "do_GET", Handler.answer_get)
        setattr(Handler, "do_POST", Handler.answer_post)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = "http://127.0.0.1:%d" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def load(self, phases, semantic_state=None, contract=None):
        self.answers, self.cursor = {}, {}
        for phase in phases:
            for path, body in phase.bodies().items():
                self.answers.setdefault(path, []).append(body)
        state = DATA["semantic"]["state"] if semantic_state is None else semantic_state
        self.answers["/graphql"] = [graphql_body(state)]
        self.answers["/mcp"] = [mcp_body(state, contract or DATA["semantic"]["mcp_contract"])]

    def serve(self, handler, body):
        path = handler.path
        self.requests.append((handler.command, path, body.decode("utf-8")))
        if self.on_request is not None:
            self.on_request()
        answers = self.answers.get(path)
        if not answers:
            handler.send_response(404)
            handler.end_headers()
            return
        index = self.cursor.get(path, 0)
        self.cursor[path] = index + 1
        payload = answers[min(index, len(answers) - 1)].encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)


class SteppedClock:
    """A monotonic clock that sleeps exactly and charges every gateway read."""

    def __init__(self, read_seconds):
        self.now = 0.0
        self.read_seconds = read_seconds
        self.lock = threading.Lock()

    def monotonic(self):
        with self.lock:
            return self.now

    def sleep(self, seconds):
        with self.lock:
            self.now += seconds

    def charge_read(self):
        with self.lock:
            self.now += self.read_seconds


class SmokeTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stub = StubGateway()

    @classmethod
    def tearDownClass(cls):
        cls.stub.close()

    def setUp(self):
        self.stub.requests.clear()
        self.stub.on_request = None
        self.temp = tempfile.TemporaryDirectory()
        self.output_dir = pathlib.Path(self.temp.name)
        self.operator_channel = self.output_dir / "operator_channel.txt"
        self.operator_channel.write_text("", encoding="utf-8")
        self.saved_channel = SMOKE.OPERATOR_CHANNEL
        SMOKE.OPERATOR_CHANNEL = str(self.operator_channel)

    def tearDown(self):
        SMOKE.OPERATOR_CHANNEL = self.saved_channel
        self.temp.cleanup()

    def environ(self, invariant, composition, southbound, **overrides):
        identifier = case_id(invariant, southbound)
        case_dir = self.output_dir / identifier
        (case_dir / "configs").mkdir(parents=True, exist_ok=True)
        workload = {"case_id": identifier, "allowlisted_request_types": list(SMOKE.READ_ONLY_REQUEST_TYPES)}
        (case_dir / "configs" / "workload.json").write_text(json.dumps(workload), encoding="utf-8")
        environ = {
            "MATRIX_CASE_ID": identifier, "MATRIX_INVARIANT": invariant,
            "MATRIX_SOUTHBOUND_TRANSPORT": southbound, "MATRIX_COMPOSITION": composition,
            "MATRIX_CASE_DIR": str(case_dir), "MATRIX_CONFIG_DIR": str(case_dir / "configs"),
            "MATRIX_OUTPUT_DIR": str(self.output_dir), "MATRIX_HARDWARE_SETUP": "setup-" + southbound,
            "MATRIX_CELL_CLASS": "Q", "MATRIX_EXTERNAL_WRITE_POLICY": "record_only",
            "MATRIX_GATEWAY_BASE_URL": self.stub.base_url,
            "MATRIX_SMOKE_WINDOW_SEC": "0.3", "MATRIX_SMOKE_POLL_SEC": "0.01",
            "MATRIX_SMOKE_HTTP_TIMEOUT_SEC": "5",
        }
        environ.update(SESSION)
        if invariant == "AD10":
            environ["MATRIX_AD10_PAIRED_CASES"] = "ENS-DS01" if southbound == "ens" else "ENH-DS01,EBUSD-ES01"
        environ.update(overrides)
        return environ

    def run_smoke(self, environ, clock=None):
        stream = io.StringIO()
        if clock is None:
            status = SMOKE.run(environ, stream=stream)
        else:
            self.stub.on_request = clock.charge_read
            try:
                status = SMOKE.run(environ, stream=stream, clock=clock)
            finally:
                self.stub.on_request = None
        self.output = stream.getvalue()
        evidence_path = pathlib.Path(environ["MATRIX_CASE_DIR"]) / SMOKE.EVIDENCE_FILE
        evidence = json.loads(evidence_path.read_text(encoding="utf-8")) if evidence_path.exists() else None
        return status, evidence

    def phases(self, composition, southbound):
        return Phase("baseline", composition, southbound), Phase("flow", composition, southbound)

    def action_phases(self, southbound):
        """AD04/AD09: before the action, at the discontinuity, after the resumption."""
        baseline = Phase("baseline", "mux", southbound, signals="baseline")
        discontinuity = Phase("baseline", "mux", southbound, signals="discontinuity")
        discontinuity.set(SMOKE.MUX_HELD, {}, 0)
        resumed = Phase("flow", "mux", southbound, signals="flow")
        return [baseline, discontinuity, resumed]

    def priority_phases(self, southbound):
        """AD07: before and after the contended window."""
        return [Phase("baseline", "mux", southbound, signals="baseline"),
                Phase("flow", "mux", southbound, signals="flow")]

    def respecified_phases(self, invariant, southbound):
        if invariant in ADAPTER_ACTION_INVARIANTS:
            return self.action_phases(southbound)
        return self.priority_phases(southbound)

    def reasons(self, evidence):
        return "\n".join(evidence["reasons"])

    def prepare_pairs(self, southbound, replaced_state=None, replaced_contract=None):
        """Run the replaced compositions of an AD10 pair and record their verdicts."""
        pairs = [("DS01", "direct", southbound)]
        if southbound == "enh":
            pairs.append(("ES01", "ebusd-tcp", "ebusd-tcp"))
        for invariant, composition, row in pairs:
            self.stub.load(self.phases(composition, row), replaced_state, replaced_contract)
            environ = self.environ(invariant, composition, row, MATRIX_HARDWARE_SETUP="setup-" + southbound)
            status, _ = self.run_smoke(environ)
            self.assertEqual(status, SMOKE.EXIT_PASS)
            self.write_verdict(case_id(invariant, row), southbound)

    def write_verdict(self, identifier, southbound, outcome="pass", **overrides):
        verdict = {"case_id": identifier, "suite": "adaptermux-v2", "outcome": outcome,
                   "status": "passed" if outcome == "pass" else "failed",
                   "session_id": "session-a", "candidate": "candidate-a",
                   "hardware_setup": "setup-" + southbound}
        verdict.update(overrides)
        path = self.output_dir / identifier / "verdict.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(verdict), encoding="utf-8")


class ExecutableInvariantTests(SmokeTestCase):
    def check_pass(self, invariant, composition, southbound):
        if invariant == "AD10":
            self.prepare_pairs(southbound)
        self.stub.load(self.phases(composition, southbound))
        status, evidence = self.run_smoke(self.environ(invariant, composition, southbound))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence and evidence["reasons"])
        self.assertEqual(evidence["result"], "pass")
        self.assertTrue(evidence["write_guard"]["confirmed"])
        self.assertTrue(all(row["met"] for row in evidence["requirements"]))
        self.assertGreater(evidence["measures"]["active_frames"], 0)

    def test_each_executable_cell_passes_with_bus_flow(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                self.check_pass(invariant, composition, southbound)

    def test_surface_answering_without_bus_flow_fails(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                baseline = Phase("baseline", composition, southbound)
                self.stub.load([baseline, Phase("baseline", composition, southbound)])
                status, evidence = self.run_smoke(self.environ(invariant, composition, southbound))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("did not move during the case window", self.reasons(evidence))
                self.assertGreater(evidence["window"]["samples"], 2)

    # Window 1 s after a baseline read of 0.3 s; each later sample sleeps
    # 0.25 s and reads for 0.3 s, so the second one completes past the window.
    STEPPED_WINDOW = {"MATRIX_SMOKE_WINDOW_SEC": "1.0", "MATRIX_SMOKE_POLL_SEC": "0.25"}

    def test_flow_seen_only_after_the_window_ends_fails(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                baseline, flow = self.phases(composition, southbound)
                self.stub.load([baseline, Phase("baseline", composition, southbound), flow])
                environ = self.environ(invariant, composition, southbound, **self.STEPPED_WINDOW)
                status, evidence = self.run_smoke(environ, clock=SteppedClock(0.1))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("did not move during the case window", self.reasons(evidence))
                self.assertEqual(evidence["window"]["samples"], 2)
                self.assertEqual(evidence["window"]["discarded_after_window"], 1)

    def test_flow_inside_the_window_passes_when_the_next_sample_ends_late(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                if invariant == "AD10":
                    self.prepare_pairs(southbound)
                self.stub.load(self.phases(composition, southbound))
                environ = self.environ(invariant, composition, southbound, **self.STEPPED_WINDOW)
                status, evidence = self.run_smoke(environ, clock=SteppedClock(0.1))
                self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
                self.assertEqual(evidence["window"]["samples"], 2)
                self.assertEqual(evidence["window"]["discarded_after_window"], 1)

    def test_counter_moving_backwards_fails(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                baseline, flow = self.phases(composition, southbound)
                flow.bump("ebus_passive_completed_transactions_total", {}, -400)
                self.stub.load([baseline, flow])
                status, evidence = self.run_smoke(self.environ(invariant, composition, southbound))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("counters moved backwards", self.reasons(evidence))

    def test_counter_reset_that_recovers_before_the_last_sample_fails(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                baseline, flow = self.phases(composition, southbound)
                dipped = self.phases(composition, southbound)[1]
                dipped.bump("ebus_passive_completed_transactions_total", {}, -400)
                self.stub.load([baseline, dipped, flow])
                status, evidence = self.run_smoke(self.environ(invariant, composition, southbound))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("counters moved backwards", self.reasons(evidence))

    def test_counter_that_must_stay_moving_fails(self):
        for invariant, (metric, labels) in STAY_COUNTERS.items():
            for southbound in ("ens", "enh"):
                with self.subTest(case=case_id(invariant, southbound)):
                    baseline, flow = self.phases("mux", southbound)
                    flow.bump(metric, labels, 1)
                    self.stub.load([baseline, flow])
                    status, evidence = self.run_smoke(self.environ(invariant, "mux", southbound))
                    self.assertEqual(status, SMOKE.EXIT_FAIL)
                    self.assertIn("during the case window; it must stay", self.reasons(evidence))

    def test_gauge_falling_is_not_a_backward_counter(self):
        baseline, flow = self.phases("mux", "enh")
        self.assertEqual(baseline.metrics[-1][0], "ebus_adaptermux_active_arbitration_held")
        self.assertGreater(baseline.metrics[-1][2], flow.metrics[-1][2])
        self.stub.load([baseline, flow])
        status, evidence = self.run_smoke(self.environ("PX03", "mux", "enh"))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])

    def test_movement_after_the_flow_requirements_hold_still_fails(self):
        baseline, flow = self.phases("mux", "enh")
        late = self.phases("mux", "enh")[1]
        late.bump("ebus_adaptermux_transaction_endings_total", {"reason": "syn_timeout"}, 1)
        self.stub.load([baseline, flow, late])
        status, evidence = self.run_smoke(self.environ("PX03", "mux", "enh"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("during the case window; it must stay", self.reasons(evidence))

    def test_field_absent_on_both_surfaces_is_parity(self):
        state = dict(DATA["semantic"]["state"], system_flow_temperature=None)
        baseline, flow = self.phases("direct", "ens")
        self.stub.load([baseline, flow], semantic_state=state)
        status, evidence = self.run_smoke(self.environ("DS01", "direct", "ens"))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
        self.assertEqual(evidence["semantic_record"]["parity_mismatches"], [])

    def test_self_echo_on_the_passive_path_fails(self):
        baseline, flow = self.phases("mux", "enh")
        flow.vars["observe_first_dedup_adjudications_total"]["matched_active_duplicate"] -= 2
        self.stub.load([baseline, flow])
        status, evidence = self.run_smoke(self.environ("AD02", "mux", "enh"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertEqual(evidence["measures"]["self_echo_frames"], 2)

    def test_passive_reconstruction_without_third_party_frames_fails(self):
        baseline, flow = self.phases("mux", "ens")
        flow.frames = [item for item in flow.frames if item["source"] == "0x71"] + [
            item for item in baseline.frames if item["source"] != "0x71"]
        self.stub.load([baseline, flow])
        status, evidence = self.run_smoke(self.environ("AD03", "mux", "ens"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("passive_third_party_frames did not move", self.reasons(evidence))

    def test_disconnected_passive_tap_fails(self):
        for invariant in ("AD02", "AD03"):
            with self.subTest(invariant=invariant):
                baseline, flow = self.phases("mux", "enh")
                flow.status["capability"]["tap_connected"] = False
                self.stub.load([baseline, flow])
                status, evidence = self.run_smoke(self.environ(invariant, "mux", "enh"))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("passive path unavailable", self.reasons(evidence))

    def test_contention_without_external_identification_frames_fails(self):
        for invariant in ("AD06", "PX01", "PX02", "PX03", "PX04", "PX12", "CX01"):
            with self.subTest(invariant=invariant):
                baseline, flow = self.phases("mux", "enh")
                flow.frames = [item for item in flow.frames if item["family"] != "0704"]
                self.stub.load([baseline, flow])
                status, evidence = self.run_smoke(self.environ(invariant, "mux", "enh"))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("external_identification_frames did not move", self.reasons(evidence))

    def test_own_source_is_read_from_the_nested_source_selection(self):
        self.stub.load(self.phases("mux", "enh"))
        status, evidence = self.run_smoke(self.environ("AD06", "mux", "enh"))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence and evidence["reasons"])
        self.assertEqual(evidence["gateway_source"], "0x%02x" % DATA["gateway_source"])

    def test_flat_admission_source_without_source_selection_cannot_measure_own_frames(self):
        for invariant in ("AD02", "AD03", "AD06", "PX01", "PX12", "CX01"):
            with self.subTest(invariant=invariant):
                phases = self.phases("mux", "enh")
                for phase in phases:
                    phase.status["bus_admission"] = {"state": "active", "source": DATA["gateway_source"]}
                self.stub.load(phases)
                status, evidence = self.run_smoke(self.environ(invariant, "mux", "enh"))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("the gateway reports no own source address", self.reasons(evidence))


class WriteGuardAndTransportTests(SmokeTestCase):
    def test_write_policy_not_confirmed_by_the_gateway_fails(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                self.stub.load(self.phases(composition, southbound))
                environ = self.environ(invariant, composition, southbound,
                                       MATRIX_EXTERNAL_WRITE_POLICY="record_and_invalidate")
                status, evidence = self.run_smoke(environ)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("write guard not confirmed", self.reasons(evidence))
                self.assertFalse(evidence["write_guard"]["confirmed"])

    def test_undeclared_write_policy_fails(self):
        self.stub.load(self.phases("mux", "ens"))
        status, evidence = self.run_smoke(self.environ("AD01", "mux", "ens", MATRIX_EXTERNAL_WRITE_POLICY=""))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("MATRIX_EXTERNAL_WRITE_POLICY is not a declared policy", self.reasons(evidence))

    def test_client_allowlist_with_a_write_request_fails(self):
        self.stub.load(self.phases("mux", "enh"))
        environ = self.environ("CX01", "mux", "enh")
        workload = pathlib.Path(environ["MATRIX_CONFIG_DIR"]) / "workload.json"
        workload.write_text(json.dumps({"allowlisted_request_types": list(SMOKE.READ_ONLY_REQUEST_TYPES)
                                        + ["b524-write"]}), encoding="utf-8")
        status, evidence = self.run_smoke(environ)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("read-only client allowlist", self.reasons(evidence))

    def test_other_transport_class_fails(self):
        baseline, flow = self.phases("mux", "ens")
        for phase in (baseline, flow):
            phase.status["transport_class"] = "adapter-direct"
        self.stub.load([baseline, flow])
        status, evidence = self.run_smoke(self.environ("AD01", "mux", "ens"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("the cell needs adapter-direct-ens", self.reasons(evidence))

    def test_mux_cell_without_adaptermux_diagnostics_fails(self):
        baseline, flow = self.phases("direct", "enh")
        for phase in (baseline, flow):
            phase.status["transport_class"] = "adapter-direct"
        self.stub.load([baseline, flow])
        status, evidence = self.run_smoke(self.environ("AD01", "mux", "enh"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("AdapterMux diagnostics are absent", self.reasons(evidence))

    def test_direct_cell_through_the_mux_fails(self):
        baseline, flow = self.phases("mux", "enh")
        for phase in (baseline, flow):
            phase.status["transport_class"] = "enh"
        self.stub.load([baseline, flow])
        status, evidence = self.run_smoke(self.environ("DS01", "direct", "enh"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("composition must bypass the mux", self.reasons(evidence))

    def test_unreachable_gateway_fails(self):
        environ = self.environ("AD01", "mux", "ens", MATRIX_GATEWAY_BASE_URL="http://127.0.0.1:9")
        status, evidence = self.run_smoke(environ)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("gateway read surface", self.reasons(evidence))


class RefusalTests(SmokeTestCase):
    def assert_refused(self, environ, fragment):
        status, evidence = self.run_smoke(environ)
        self.assertEqual(status, SMOKE.EXIT_REFUSED)
        self.assertEqual(evidence["result"], "refused")
        self.assertIn(fragment, self.reasons(evidence))
        self.assertEqual(self.stub.requests, [])

    def test_reserved_invariants_are_refused(self):
        for invariant, reason in SMOKE.RESERVED.items():
            for southbound in ("ens", "enh"):
                with self.subTest(case=case_id(invariant, southbound)):
                    self.assert_refused(self.environ(invariant, "mux", southbound), reason)

    def test_fifo_order_cells_are_refused_under_pl04(self):
        for invariant in FIFO_ORDER_INVARIANTS:
            self.assertEqual(SMOKE.RESERVED[invariant], "fifo_order_superseded_by_address_priority")
            for southbound in ("ens", "enh"):
                with self.subTest(case=case_id(invariant, southbound)):
                    self.assert_refused(self.environ(invariant, "mux", southbound),
                                        invariant + " is not executable: fifo_order_superseded_by_address_priority")
        for invariant in ADAPTER_ACTION_INVARIANTS + PRIORITY_INVARIANTS:
            self.assertNotIn(invariant, SMOKE.RESERVED)
        self.assertNotIn("cell_awaiting_respecification", SMOKE.RESERVED.values())

    def test_planned_and_blocked_rows_are_refused(self):
        for southbound, token in (("udp", "UDP"), ("tcp", "TCP"), ("uart-ens", "UARTENS"),
                                  ("uart-enh", "UARTENH"), ("uart-raw", "UARTRAW")):
            with self.subTest(row=token):
                environ = self.environ("AD01", "mux", "ens", MATRIX_SOUTHBOUND_TRANSPORT=southbound,
                                       MATRIX_CASE_ID=token + "-AD01")
                self.assert_refused(environ, SMOKE.RESERVED_ROWS[southbound])

    def test_unknown_invariant_is_refused(self):
        for invariant in ("ZZ99", "AD13", "PX13"):
            with self.subTest(invariant=invariant):
                self.assert_refused(self.environ(invariant, "mux", "ens"), "unknown invariant " + invariant)

    def test_mismatched_cell_context_is_refused(self):
        self.assert_refused(self.environ("AD01", "direct", "ens"), "runs on composition mux")
        self.assert_refused(self.environ("AD01", "mux", "ens", MATRIX_CASE_ID="ENH-AD01"), "does not match ENS-AD01")
        self.assert_refused(self.environ("ES01", "ebusd-tcp", "enh", MATRIX_CASE_ID="ENH-ES01"),
                            "does not run on southbound transport enh")
        self.assert_refused(self.environ("AD01", "mux", "ens", MATRIX_CELL_CLASS="-"), "never executed")

    def test_missing_context_is_a_usage_error(self):
        environ = self.environ("AD01", "mux", "ens")
        del environ["MATRIX_INVARIANT"]
        self.assertEqual(SMOKE.run(environ, stream=io.StringIO()), SMOKE.EXIT_USAGE)
        for override in ({"MATRIX_GATEWAY_BASE_URL": ""}, {"MATRIX_SMOKE_WINDOW_SEC": "soon"},
                         {"MATRIX_SMOKE_POLL_SEC": "0"}):
            with self.subTest(override=override):
                status, evidence = self.run_smoke(self.environ("AD01", "mux", "ens", **override))
                self.assertEqual(status, SMOKE.EXIT_USAGE)
                self.assertEqual(evidence["result"], "usage_error")
        self.assertEqual(self.stub.requests, [])

    def test_command_line_exit_status_of_a_refusal(self):
        environ = self.environ("AD12", "mux", "enh")
        environ["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run([sys.executable, str(SCRIPT)], env=environ, capture_output=True,
                                text=True, timeout=60, check=False)
        self.assertEqual(result.returncode, SMOKE.EXIT_REFUSED)
        self.assertIn("refused", result.stdout)


class AdapterActionTests(SmokeTestCase):
    """AD04 (adapter reset) and AD09 (host-link disconnect and reconnect)."""

    def run_action(self, invariant, southbound, phases, typed=None):
        self.stub.load(phases)
        confirmation = case_id(invariant, southbound) if typed is None else typed
        self.operator_channel.write_text(confirmation + "\n", encoding="utf-8")
        return self.run_smoke(self.environ(invariant, "mux", southbound, MATRIX_CELL_CLASS="W"))

    def each_cell(self):
        for invariant in ADAPTER_ACTION_INVARIANTS:
            for southbound in ("ens", "enh"):
                yield invariant, southbound

    def assert_records_the_unprovable_hold_and_passes(self, status, evidence):
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence.get("reasons"))
        self.assertEqual(evidence["reasons"], [])
        self.assertEqual(evidence["limits"]["out_of_step_holds_raised"], SMOKE.HOLD_NOT_PROVABLE)
        self.assertIsNone(evidence["measures"]["out_of_step_holds_raised"])
        names = [row["measure"] for row in evidence["requirements"]]
        self.assertNotIn("out_of_step_holds_raised", names)
        unmet = [row["measure"] for row in evidence["requirements"] if not row["met"]]
        self.assertEqual(unmet, [])

    def test_discontinuity_seen_by_every_session_with_one_reconnect_and_resumption_meets_every_sampled_rule(self):
        # Every requirement a reading can prove holds. The unseen hold is
        # recorded as a limit and is not a failure.
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                status, evidence = self.run_action(invariant, southbound, self.action_phases(southbound))
                self.assert_records_the_unprovable_hold_and_passes(status, evidence)
                measures = evidence["measures"]
                self.assertEqual(measures["consumer_sessions_observing_discontinuity"], 3)
                self.assertEqual(measures["consumer_sessions_missing_discontinuity"], 0)
                self.assertEqual(measures["mux_upstream_reconnects"], 1)
                self.assertGreater(measures["resumed_active_frames"], 0)
                self.assertGreater(measures["resumed_mux_transactions_done"], 0)
                self.assertGreater(measures["resumed_passive_completed_transactions"], 0)
                self.assertEqual(measures["out_of_step_hold_readings"], 0)
                self.assertEqual(evidence["discontinuity_reading"], 1)
                self.assertEqual(evidence["operator_action"], SMOKE.CHECKS[invariant]["operator_action"])
                self.assertIn("operator action for " + case_id(invariant, southbound), self.output)
                self.assertIn("operator action for " + case_id(invariant, southbound),
                              self.operator_channel.read_text(encoding="utf-8"))
                self.assertEqual(evidence["operator_cue"], SMOKE.CUE_TERMINAL)
                self.assertTrue(evidence["operator_confirmed"])
                self.assertIn("Type " + case_id(invariant, southbound) + " to confirm",
                              self.operator_channel.read_text(encoding="utf-8"))

    def test_action_without_a_fresh_typed_confirmation_fails(self):
        # The confirmation is asked at action time, after the baseline sample.
        for typed in ("", "skip"):
            for invariant, southbound in self.each_cell():
                with self.subTest(case=case_id(invariant, southbound), typed=typed):
                    status, evidence = self.run_action(
                        invariant, southbound, self.action_phases(southbound), typed=typed)
                    self.assertEqual(status, SMOKE.EXIT_FAIL)
                    self.assertFalse(evidence["operator_confirmed"])
                    self.assertIn("did not type the case identifier", self.reasons(evidence))

    def test_cue_without_a_terminal_is_recorded_and_named_when_the_cell_fails(self):
        # Without a controlling terminal the cue reaches only the case log;
        # a failing cell then says so, so the operator learns why.
        SMOKE.OPERATOR_CHANNEL = str(self.output_dir / "absent-directory" / "tty")
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                status, evidence = self.run_action(invariant, southbound, self.action_phases(southbound)[:2])
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertEqual(evidence["operator_cue"], SMOKE.CUE_CASE_LOG_ONLY)
                self.assertIn("operator action for " + case_id(invariant, southbound), self.output)
                self.assertIn("the operator cue reached only the case log", self.reasons(evidence))

    def test_cue_without_a_terminal_cannot_be_confirmed_and_fails_a_passing_cell(self):
        SMOKE.OPERATOR_CHANNEL = str(self.output_dir / "absent-directory" / "tty")
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                status, evidence = self.run_action(invariant, southbound, self.action_phases(southbound))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertEqual(evidence["operator_cue"], SMOKE.CUE_CASE_LOG_ONLY)
                self.assertFalse(evidence["operator_confirmed"])
                self.assertIn("no terminal was available", self.reasons(evidence))

    def test_no_discontinuity_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                for phase in phases[1:]:
                    phase.metrics = [row for row in phase.metrics if row[0] != SMOKE.MUX_SESSION_DISCONTINUITIES]
                    phase.metrics += copy.deepcopy([row for row in phases[0].metrics
                                                    if row[0] == SMOKE.MUX_SESSION_DISCONTINUITIES])
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("no consumer session observed a discontinuity", self.reasons(evidence))

    def test_session_that_misses_the_discontinuity_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                for phase in phases[1:]:
                    phase.set(SMOKE.MUX_SESSION_DISCONTINUITIES, {"session": "external-a"}, 1)
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("consumer_sessions_missing_discontinuity moved by 1", self.reasons(evidence))

    def test_second_reconnect_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                phases[2].bump(SMOKE.MUX_UPSTREAM_RECONNECTS, {}, 1)
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("mux_upstream_reconnects is 2 during the case window; it must be exactly 1",
                              self.reasons(evidence))

    def test_no_reconnect_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                for phase in phases[1:]:
                    phase.set(SMOKE.MUX_UPSTREAM_RECONNECTS, {}, 3)
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("mux_upstream_reconnects is 0", self.reasons(evidence))

    def test_no_resumption_after_the_discontinuity_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                status, evidence = self.run_action(invariant, southbound, phases[:2])
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                reasons = self.reasons(evidence)
                for name in ("resumed_active_frames", "resumed_mux_transactions_done",
                             "resumed_passive_completed_transactions"):
                    self.assertIn(name + " did not move", reasons)

    def test_flow_before_the_discontinuity_only_is_not_resumption(self):
        # Traffic moved before the action, then the discontinuity, then silence.
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                baseline = Phase("baseline", "mux", southbound, signals="baseline")
                flowed = Phase("flow", "mux", southbound, signals="baseline")
                flowed.set(SMOKE.MUX_HELD, {}, 0)
                after = Phase("flow", "mux", southbound, signals="flow")
                after.set(SMOKE.MUX_HELD, {}, 0)
                status, evidence = self.run_action(invariant, southbound, [baseline, flowed, after])
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertGreater(evidence["measures"]["active_frames"], 0)
                self.assertIn("resumed_mux_transactions_done did not move", self.reasons(evidence))

    def test_hold_raised_and_cleared_between_readings_is_not_provable(self):
        # The gauge reads 0 at every reading; a hold that was raised and
        # cleared between two of them would look exactly the same, so the
        # check must not count the zero readings as proof.
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                for phase in phases:
                    phase.set(SMOKE.MUX_HELD, {}, 0)
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(evidence["measures"]["out_of_step_hold_readings"], 0)
                self.assertIsNone(evidence["measures"]["out_of_step_holds_raised"])
                self.assertIn("not provable with a sampled gauge",
                              evidence["limits"]["out_of_step_holds_raised"])
                self.assertNotIn("out_of_step_holds_raised", self.reasons(evidence))
                self.assert_records_the_unprovable_hold_and_passes(status, evidence)

    def test_recovery_between_the_same_two_readings_as_the_discontinuity_counts(self):
        # The discontinuity and the first resumed transactions fall between
        # the baseline and the next reading: the detecting reading already
        # holds the recovery, so the resumption is measured from the reading
        # before it.
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                baseline = Phase("baseline", "mux", southbound, signals="baseline")
                recovered = Phase("flow", "mux", southbound, signals="discontinuity")
                status, evidence = self.run_action(invariant, southbound, [baseline, recovered])
                self.assertEqual(evidence["discontinuity_reading"], 1)
                measures = evidence["measures"]
                self.assertGreater(measures["resumed_active_frames"], 0)
                self.assertGreater(measures["resumed_mux_transactions_done"], 0)
                self.assertGreater(measures["resumed_passive_completed_transactions"], 0)
                self.assert_records_the_unprovable_hold_and_passes(status, evidence)

    def test_out_of_step_hold_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                phases[1].set(SMOKE.MUX_HELD, {}, 1)
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("out_of_step_hold_readings moved by 1", self.reasons(evidence))
                self.assertNotIn("out_of_step_holds_raised cannot be measured", self.reasons(evidence))
                self.assertEqual(evidence["limits"]["out_of_step_holds_raised"], SMOKE.HOLD_NOT_PROVABLE)

    def test_disconnected_passive_tap_fails(self):
        for invariant, southbound in self.each_cell():
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.action_phases(southbound)
                phases[2].status["capability"]["tap_connected"] = False
                status, evidence = self.run_action(invariant, southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("passive path unavailable", self.reasons(evidence))


class AddressPriorityTests(SmokeTestCase):
    """AD07: address-priority selection between the gateway and ebusd."""

    def run_priority(self, southbound, phases):
        self.stub.load(phases)
        return self.run_smoke(self.environ("AD07", "mux", southbound))

    def test_priority_winner_and_kept_losing_bid_pass(self):
        for southbound in ("ens", "enh"):
            with self.subTest(row=southbound):
                status, evidence = self.run_priority(southbound, self.priority_phases(southbound))
                self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
                self.assertEqual(evidence["priority"], {"gateway_source": "0x71", "external_initiators": ["0xf7"],
                                                        "expected_winner": "gateway"})
                self.assertEqual(evidence["measures"]["contended_selections_won_by_priority"], 6)
                self.assertEqual(evidence["measures"]["retained_bids_granted"], 6)
                self.assertNotIn("operator action", self.output)

    def test_selection_against_priority_fails(self):
        for southbound in ("ens", "enh"):
            with self.subTest(row=southbound):
                phases = self.priority_phases(southbound)
                phases[1].bump(SMOKE.MUX_CONTENDED_SELECTIONS, {"winner": "external"}, 1)
                status, evidence = self.run_priority(southbound, phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("contended_selections_won_against_priority moved by 1", self.reasons(evidence))

    def test_expected_winner_follows_the_addresses(self):
        # A gateway source whose low nibble is higher than the external one's
        # makes the external bid the priority winner; the recorded counts,
        # where the gateway won, then fail in both directions.
        phases = self.priority_phases("enh")
        for phase in phases:
            phase.status["bus_admission"]["source_selection"]["selected_source"] = 0x7F
        status, evidence = self.run_priority("enh", phases)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertEqual(evidence["priority"]["expected_winner"], "external")
        reasons = self.reasons(evidence)
        self.assertIn("contended_selections_won_by_priority did not move", reasons)
        self.assertIn("contended_selections_won_against_priority moved by 6", reasons)

    def test_losing_bid_not_completed_fails(self):
        for result in ("failed", "withdrawn"):
            with self.subTest(result=result):
                phases = self.priority_phases("ens")
                phases[1].bump(SMOKE.MUX_RETAINED_BIDS, {"result": result}, 1)
                status, evidence = self.run_priority("ens", phases)
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("retained_bids_not_granted moved by 1", self.reasons(evidence))

    def test_no_losing_bid_completed_fails(self):
        phases = self.priority_phases("enh")
        phases[1].set(SMOKE.MUX_RETAINED_BIDS, {"result": "granted"}, 20)
        status, evidence = self.run_priority("enh", phases)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("retained_bids_granted did not move", self.reasons(evidence))

    def test_starved_external_session_fails(self):
        phases = self.priority_phases("enh")
        phases[1].frames = [item for item in phases[1].frames if item["family"] != "0704"]
        status, evidence = self.run_priority("enh", phases)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        reasons = self.reasons(evidence)
        self.assertIn("external_identification_frames did not move", reasons)
        self.assertIn("the external session's initiator is not one address (none)", reasons)
        self.assertIn("must use a different initiator", reasons)

    def test_starved_gateway_session_fails(self):
        phases = self.priority_phases("ens")
        phases[1].frames = [item for item in phases[1].frames if item["scope"] != "active"] + [
            item for item in phases[0].frames if item["scope"] == "active"]
        status, evidence = self.run_priority("ens", phases)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("active_frames did not move", self.reasons(evidence))

    def test_two_external_initiators_cannot_be_judged(self):
        phases = self.priority_phases("enh")
        phases[1].frames.append({"scope": "passive", "source": "0x37", "target": "0x15", "family": "0704",
                                 "frame_type": "initiator_target", "count": 2})
        status, evidence = self.run_priority("enh", phases)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("is not one address (0x37, 0xf7)", self.reasons(evidence))

    def test_priority_rule_orders_low_nibble_then_high_nibble(self):
        cases = (
            (0x71, 0xF7, "gateway"),   # low nibble 1 before 7
            (0xF7, 0x71, "external"),
            (0x31, 0xF1, "gateway"),   # equal low nibble, high nibble 3 before F
            (0xF1, 0x31, "external"),
            (0x10, 0x03, "gateway"),   # low nibble decides before the high nibble
            (0x03, 0x10, "external"),
        )
        for gateway, external, winner in cases:
            with self.subTest(gateway=hex(gateway), external=hex(external)):
                self.assertEqual(SMOKE.priority_winner(gateway, external), winner)

    def test_mux_absent_fails_as_a_composition_error(self):
        phases = [Phase("baseline", "direct", "enh"), Phase("flow", "direct", "enh")]
        for phase in phases:
            phase.status["transport_class"] = "adapter-direct"
        status, evidence = self.run_priority("enh", phases)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("AdapterMux diagnostics are absent", self.reasons(evidence))


class MissingSignalTests(SmokeTestCase):
    def test_each_unpublished_series_refuses_with_its_own_status(self):
        for invariant in ADAPTER_ACTION_INVARIANTS + PRIORITY_INVARIANTS:
            for signal in SMOKE.CHECKS[invariant]["signals"]:
                with self.subTest(invariant=invariant, signal=signal):
                    self.stub.requests.clear()
                    phases = self.respecified_phases(invariant, "enh")
                    for phase in phases:
                        phase.drop(signal)
                    self.stub.load(phases)
                    status, evidence = self.run_smoke(self.environ(invariant, "mux", "enh"))
                    self.assertEqual(status, SMOKE.EXIT_SIGNAL_MISSING)
                    self.assertEqual(evidence["result"], "signal_missing")
                    self.assertEqual(evidence["missing_signals"], [signal])
                    self.assertIn("the gateway does not publish " + signal, self.reasons(evidence))
                    self.assertNotIn("operator action", self.output)
                    self.assertEqual(sorted(path for _, path, _ in self.stub.requests),
                                     ["/debug/vars", "/metrics", "/portal/api/v1/bus/observability"])

    def test_mux_before_its_first_transaction_ending_still_refuses_missing_series(self):
        # The endings family is empty until a transaction ends; the always
        # rendered mux series still show that the mux runs.
        expected = {
            "AD04": [SMOKE.MUX_SESSION_DISCONTINUITIES, SMOKE.MUX_UPSTREAM_RECONNECTS],
            "AD09": [SMOKE.MUX_SESSION_DISCONTINUITIES, SMOKE.MUX_UPSTREAM_RECONNECTS],
            "AD07": [SMOKE.MUX_CONTENDED_SELECTIONS, SMOKE.MUX_RETAINED_BIDS],
        }
        for invariant in ADAPTER_ACTION_INVARIANTS + PRIORITY_INVARIANTS:
            with self.subTest(invariant=invariant):
                phases = [Phase("baseline", "mux", "ens"), Phase("flow", "mux", "ens")]
                for phase in phases:
                    phase.drop(SMOKE.MUX_ENDINGS)
                self.stub.load(phases)
                status, evidence = self.run_smoke(self.environ(invariant, "mux", "ens"))
                self.assertEqual(status, SMOKE.EXIT_SIGNAL_MISSING)
                self.assertEqual(evidence["missing_signals"], expected[invariant])
                self.assertNotIn("operator action", self.output)

    def test_mux_presence_needs_no_transaction_ending(self):
        sample = {"metrics": {(SMOKE.MUX_HELD, ()): 0.0, (SMOKE.MUX_FOREIGN, ()): 0.0}}
        self.assertTrue(SMOKE.mux_diagnostics_present(sample))
        self.assertFalse(SMOKE.mux_diagnostics_present({"metrics": {(SMOKE.PASSIVE_COMPLETED, ()): 1.0}}))
        context = {"southbound_transport": "ens"}
        evidence, reasons = SMOKE.check_transport(context, dict(sample, observability={}), {"composition": "mux"})
        self.assertTrue(evidence["mux_diagnostics"])
        self.assertNotIn("AdapterMux diagnostics are absent", "\n".join(reasons))

    def test_idle_mux_is_seen_from_any_family_it_renders(self):
        # An idle mux renders the endings family with no sample yet; any mux
        # family, declared or sampled, shows the mux runs.
        for family in ("ebus_adaptermux_transaction_endings_total", "ebus_adaptermux_syn_suppressed_pre_echo_total",
                       "ebus_adaptermux_stale_active_bytes_discarded_total"):
            with self.subTest(family=family):
                declared = SMOKE.declared_families("# HELP %s text\n# TYPE %s counter\n" % (family, family))
                sample = {"metrics": {(SMOKE.PASSIVE_COMPLETED, ()): 1.0}, "families": sorted(declared)}
                self.assertTrue(SMOKE.mux_diagnostics_present(sample))
                sampled = {"metrics": {(family, ()): 0.0}}
                self.assertTrue(SMOKE.mux_diagnostics_present(sampled))
        self.assertFalse(SMOKE.mux_diagnostics_present({"metrics": {}, "families": ["ebus_passive_completed_transactions_total"]}))

    def test_compatibility_cell_with_an_idle_mux_fails(self):
        # Only HELP and TYPE lines of the mux endings family: no mux sample at all.
        for invariant, composition, southbound in (("DS01", "direct", "ens"), ("ES01", "ebusd-tcp", "ebusd-tcp")):
            with self.subTest(case=case_id(invariant, southbound)):
                phases = self.phases(composition, southbound)
                self.stub.load(phases)
                declaration = ("# HELP ebus_adaptermux_transaction_endings_total endings\n"
                               "# TYPE ebus_adaptermux_transaction_endings_total counter\n")
                self.stub.answers["/metrics"] = [declaration + body for body in self.stub.answers["/metrics"]]
                status, evidence = self.run_smoke(self.environ(invariant, composition, southbound))
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("AdapterMux diagnostics are present", self.reasons(evidence))
                self.assertEqual(evidence["transport"]["mux_families"], ["ebus_adaptermux_transaction_endings_total"])

    def test_gateway_without_any_new_series_names_every_missing_one(self):
        self.stub.load(self.phases("mux", "ens"))
        status, evidence = self.run_smoke(self.environ("AD04", "mux", "ens"))
        self.assertEqual(status, SMOKE.EXIT_SIGNAL_MISSING)
        self.assertEqual(evidence["missing_signals"],
                         [SMOKE.MUX_SESSION_DISCONTINUITIES, SMOKE.MUX_UPSTREAM_RECONNECTS])
        self.assertEqual(SMOKE.EXIT_SIGNAL_MISSING, 4)
        self.assertNotIn(SMOKE.EXIT_SIGNAL_MISSING, (SMOKE.EXIT_PASS, SMOKE.EXIT_FAIL, SMOKE.EXIT_USAGE,
                                                      SMOKE.EXIT_REFUSED))


class MigrationEquivalenceTests(SmokeTestCase):
    def run_ad10(self, southbound):
        self.stub.load(self.phases("mux", southbound))
        return self.run_smoke(self.environ("AD10", "mux", southbound))

    def test_equal_pairs_pass_with_every_decision_recorded(self):
        for southbound in ("ens", "enh"):
            with self.subTest(row=southbound):
                self.prepare_pairs(southbound)
                status, evidence = self.run_ad10(southbound)
                self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
                pairs = evidence["ad10"]["pairs"]
                self.assertEqual(len(pairs), 1 if southbound == "ens" else 2)
                for pair in pairs:
                    self.assertEqual(sorted(pair["decisions"]), ["lineage", "mcp_graphql_parity", "presence",
                                                                 "types", "units", "values"])
                    self.assertTrue(all(item["equivalent"] for item in pair["decisions"].values()))

    def test_different_value_fails_the_values_decision(self):
        self.prepare_pairs("enh", replaced_state=DATA["semantic"]["drifted_state"])
        status, evidence = self.run_ad10("enh")
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("ENH-DS01 values not equivalent: outdoor_temperature", self.reasons(evidence))
        self.assertIn("EBUSD-ES01 values not equivalent: outdoor_temperature", self.reasons(evidence))
        decisions = evidence["ad10"]["pairs"][0]["decisions"]
        self.assertTrue(decisions["presence"]["equivalent"])
        self.assertTrue(decisions["lineage"]["equivalent"])

    def test_value_inside_tolerance_is_equivalent(self):
        state = dict(DATA["semantic"]["state"], outdoor_temperature=8.5)
        self.prepare_pairs("ens", replaced_state=state)
        status, evidence = self.run_ad10("ens")
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])

    def test_missing_value_fails_presence_and_types(self):
        state = dict(DATA["semantic"]["state"], system_water_pressure=None)
        self.prepare_pairs("ens", replaced_state=state)
        status, evidence = self.run_ad10("ens")
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("ENS-DS01 presence not equivalent: system_water_pressure", self.reasons(evidence))
        self.assertIn("ENS-DS01 types not equivalent: system_water_pressure", self.reasons(evidence))

    def test_equal_values_with_other_lineage_fail(self):
        self.prepare_pairs("ens", replaced_contract=DATA["semantic"]["other_mcp_contract"])
        status, evidence = self.run_ad10("ens")
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        decisions = evidence["ad10"]["pairs"][0]["decisions"]
        self.assertTrue(decisions["values"]["equivalent"])
        self.assertFalse(decisions["lineage"]["equivalent"])

    def test_missing_paired_case_fails(self):
        self.prepare_pairs("ens")
        (self.output_dir / "ENS-DS01" / "verdict.json").unlink()
        status, evidence = self.run_ad10("ens")
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("paired case ENS-DS01 has no verdict in the output directory", self.reasons(evidence))

    def test_one_missing_pair_of_two_fails(self):
        self.prepare_pairs("enh")
        (self.output_dir / "EBUSD-ES01" / "verdict.json").unlink()
        status, evidence = self.run_ad10("enh")
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("paired case EBUSD-ES01 has no verdict", self.reasons(evidence))
        self.assertNotIn("ENH-DS01", self.reasons(evidence))

    def test_paired_case_that_did_not_pass_fails(self):
        self.prepare_pairs("ens")
        self.write_verdict("ENS-DS01", "ens", outcome="fail")
        status, evidence = self.run_ad10("ens")
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("paired case ENS-DS01 did not pass", self.reasons(evidence))

    def test_paired_case_identity_must_match(self):
        for key, value in (("session_id", "session-b"), ("candidate", "candidate-b"),
                           ("hardware_setup", "setup-other")):
            with self.subTest(field=key):
                self.prepare_pairs("ens")
                self.write_verdict("ENS-DS01", "ens", **{key: value})
                status, evidence = self.run_ad10("ens")
                self.assertEqual(status, SMOKE.EXIT_FAIL)
                self.assertIn("paired case ENS-DS01 belongs to another " + key, self.reasons(evidence))

    def test_no_paired_case_named_fails(self):
        self.stub.load(self.phases("mux", "ens"))
        status, evidence = self.run_smoke(self.environ("AD10", "mux", "ens", MATRIX_AD10_PAIRED_CASES=""))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("names no paired case", self.reasons(evidence))

    def test_paired_case_name_outside_the_output_directory_fails(self):
        self.stub.load(self.phases("mux", "ens"))
        environ = self.environ("AD10", "mux", "ens", MATRIX_AD10_PAIRED_CASES="../ENS-DS01")
        status, evidence = self.run_smoke(environ)
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("is not a case identifier", self.reasons(evidence))

    def test_mcp_graphql_parity_difference_fails(self):
        self.stub.load(self.phases("direct", "ens"))
        self.stub.answers["/mcp"] = [mcp_body(DATA["semantic"]["drifted_state"], DATA["semantic"]["mcp_contract"])]
        status, evidence = self.run_smoke(self.environ("DS01", "direct", "ens"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertEqual(evidence["semantic_record"]["parity_mismatches"], ["outdoor_temperature"])

    def test_value_arriving_between_the_two_requests_is_not_drift(self):
        # GraphQL answers the old value first, then the new one; MCP already
        # answers the new one. The pair is repeated until GraphQL is stable.
        drifted = DATA["semantic"]["drifted_state"]
        self.stub.load(self.phases("direct", "ens"))
        self.stub.answers["/graphql"] = [graphql_body(DATA["semantic"]["state"]), graphql_body(drifted)]
        self.stub.answers["/mcp"] = [mcp_body(drifted, DATA["semantic"]["mcp_contract"])]
        status, evidence = self.run_smoke(self.environ("DS01", "direct", "ens"))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
        self.assertEqual(evidence["semantic_record"]["parity_mismatches"], [])
        self.assertEqual(evidence["semantic_record"]["parity_attempts"], 2)

    def test_state_changing_around_every_mcp_read_fails(self):
        states = [DATA["semantic"]["state"], DATA["semantic"]["drifted_state"]]
        self.stub.load(self.phases("direct", "ens"))
        self.stub.answers["/graphql"] = [graphql_body(states[index % 2]) for index in range(SMOKE.PARITY_ATTEMPTS + 1)]
        status, evidence = self.run_smoke(self.environ("DS01", "direct", "ens"))
        self.assertEqual(status, SMOKE.EXIT_FAIL)
        self.assertIn("MCP/GraphQL parity has no consistent snapshot", self.reasons(evidence))

    def test_missing_mcp_contract_fails_the_semantic_record(self):
        state = DATA["semantic"]["state"]
        missing = json.loads(json.loads(mcp_body(state, "x"))["result"]["content"][0]["text"])
        missing["meta"] = {}
        absent = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {
            "content": [{"type": "text", "text": json.dumps(missing)}], "isError": False}})
        contract = DATA["semantic"]["mcp_contract"]
        malformed = (
            ("absent", absent), ("empty", mcp_body(state, "")), ("blank", mcp_body(state, "  ")),
            ("bare string", mcp_body(state, contract["name"])),
            ("empty object", mcp_body(state, {})),
            ("blank name", mcp_body(state, dict(contract, name="  "))),
            ("no major", mcp_body(state, {"name": contract["name"], "minor": 0})),
            ("textual minor", mcp_body(state, dict(contract, minor="0"))),
        )
        for label, body in malformed:
            for invariant, composition in (("DS01", "direct"), ("AD10", "mux")):
                with self.subTest(contract=label, invariant=invariant):
                    if invariant == "AD10":
                        self.prepare_pairs("ens")
                    self.stub.load(self.phases(composition, "ens"))
                    self.stub.answers["/mcp"] = [body]
                    status, evidence = self.run_smoke(self.environ(invariant, composition, "ens"))
                    self.assertEqual(status, SMOKE.EXIT_FAIL)
                    self.assertIn("MCP read tool answered without a meta.contract", self.reasons(evidence))
                    self.assertNotIn("ad10", evidence)


    def test_gateway_contract_object_is_recorded_as_lineage(self):
        self.stub.load(self.phases("direct", "ens"))
        status, evidence = self.run_smoke(self.environ("DS01", "direct", "ens"))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence and evidence["reasons"])
        for name, field in evidence["semantic_record"]["fields"].items():
            with self.subTest(field=name):
                self.assertEqual(field["lineage"]["mcp_contract"], DATA["semantic"]["mcp_contract"])


class ReadinessTests(SmokeTestCase):
    def test_ready_file_is_created_after_the_baseline_reading(self):
        ready = self.output_dir / "ready"
        seen = []

        def observe():
            seen.append(ready.exists())

        self.stub.load(self.phases("mux", "enh"))
        self.stub.on_request = observe
        status, evidence = self.run_smoke(self.environ("PX01", "mux", "enh", MATRIX_SMOKE_READY_FILE=str(ready)))
        self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
        self.assertTrue(ready.exists())
        # The three baseline reads happen before the file exists, later reads after.
        self.assertEqual(seen[:3], [False, False, False])
        self.assertTrue(all(seen[3:]))

    def test_no_ready_file_when_a_signal_is_missing_or_the_cell_is_refused(self):
        ready = self.output_dir / "ready"
        self.stub.load(self.phases("mux", "ens"))
        status, _ = self.run_smoke(self.environ("AD07", "mux", "ens", MATRIX_SMOKE_READY_FILE=str(ready)))
        self.assertEqual(status, SMOKE.EXIT_SIGNAL_MISSING)
        self.assertFalse(ready.exists())
        status, _ = self.run_smoke(self.environ("PX05", "mux", "ens", MATRIX_SMOKE_READY_FILE=str(ready)))
        self.assertEqual(status, SMOKE.EXIT_REFUSED)
        self.assertFalse(ready.exists())


class ReadOnlyTests(SmokeTestCase):
    def test_every_request_is_a_read(self):
        self.prepare_pairs("enh")
        for invariant in GRID_INVARIANTS:
            self.stub.load(self.phases("mux", "enh"))
            self.run_smoke(self.environ(invariant, "mux", "enh"))
        for invariant in ADAPTER_ACTION_INVARIANTS + PRIORITY_INVARIANTS:
            self.stub.load(self.respecified_phases(invariant, "enh"))
            self.operator_channel.write_text(case_id(invariant, "enh") + "\n", encoding="utf-8")
            status, evidence = self.run_smoke(self.environ(invariant, "mux", "enh"))
            if invariant in ADAPTER_ACTION_INVARIANTS:
                self.assertEqual(status, SMOKE.EXIT_PASS, evidence["reasons"])
                self.assertEqual(evidence["reasons"], [])
                self.assertEqual(evidence["limits"]["out_of_step_holds_raised"], SMOKE.HOLD_NOT_PROVABLE)
                self.assertIsNone(evidence["measures"]["out_of_step_holds_raised"])
            else:
                self.assertEqual(status, SMOKE.EXIT_PASS)
        methods = {(method, path) for method, path, _ in self.stub.requests}
        self.assertEqual(methods, {("GET", "/portal/api/v1/bus/observability"), ("GET", "/metrics"),
                                   ("GET", "/debug/vars"), ("POST", "/graphql"), ("POST", "/mcp")})
        for method, path, body in self.stub.requests:
            if path == "/graphql":
                self.assertNotIn("mutation", json.loads(body)["query"])
            if path == "/mcp":
                call = json.loads(body)
                self.assertEqual(call["method"], "tools/call")
                self.assertIn(call["params"]["name"], SMOKE.MCP_READ_TOOLS)

    def test_guards_refuse_writes(self):
        for document in ("mutation { setSystemConfig(field: \"x\", value: \"1\") { ok } }",
                         "subscription { busSummary { status } }",
                         "query { a } mutation { b }"):
            with self.subTest(document=document):
                with self.assertRaises(ValueError):
                    SMOKE.guard_graphql(document)
        for tool in ("ebus_v1_semantic_system_set_config", "ebus_v1_rpc_invoke",
                     "ebus_v1_semantic_schedules_set_zone_time_program"):
            with self.subTest(tool=tool):
                with self.assertRaises(ValueError):
                    SMOKE.guard_mcp_tool(tool)
        self.assertEqual(SMOKE.guard_graphql(SMOKE.GRAPHQL_SEMANTIC_QUERY), SMOKE.GRAPHQL_SEMANTIC_QUERY)

    def test_checks_cover_exactly_the_executable_invariants(self):
        self.assertEqual(sorted(SMOKE.CHECKS), sorted(GRID_INVARIANTS + ADAPTER_ACTION_INVARIANTS
                                                      + PRIORITY_INVARIANTS + ("DS01", "ES01")))
        self.assertFalse(set(SMOKE.CHECKS) & set(SMOKE.RESERVED))


if __name__ == "__main__":
    unittest.main()
