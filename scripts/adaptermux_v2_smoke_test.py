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

    def __init__(self, name, composition, southbound):
        mux = composition == "mux"
        self.status = copy.deepcopy(DATA["status"])
        self.status["transport_class"] = SMOKE.EXPECTED_TRANSPORT_CLASS[(composition, southbound)]
        self.frames = copy.deepcopy(DATA["frames"][name])
        metrics = DATA["metrics"][name]
        self.metrics = copy.deepcopy(metrics["common"] + (metrics["mux"] if mux else []))
        self.vars = copy.deepcopy(DATA["vars"][name])

    def bump(self, metric, labels, amount):
        for row in self.metrics:
            if row[0] == metric and row[1] == labels:
                row[2] += amount
                return
        raise AssertionError("no recorded series " + metric)

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


class SmokeTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stub = StubGateway()

    @classmethod
    def tearDownClass(cls):
        cls.stub.close()

    def setUp(self):
        self.stub.requests.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.output_dir = pathlib.Path(self.temp.name)

    def tearDown(self):
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

    def run_smoke(self, environ):
        status = SMOKE.run(environ, stream=io.StringIO())
        evidence_path = pathlib.Path(environ["MATRIX_CASE_DIR"]) / SMOKE.EVIDENCE_FILE
        evidence = json.loads(evidence_path.read_text(encoding="utf-8")) if evidence_path.exists() else None
        return status, evidence

    def phases(self, composition, southbound):
        return Phase("baseline", composition, southbound), Phase("flow", composition, southbound)

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

    def test_counter_moving_backwards_fails(self):
        for invariant, composition, southbound in EXECUTABLE_CELLS:
            with self.subTest(case=case_id(invariant, southbound)):
                baseline, flow = self.phases(composition, southbound)
                flow.bump("ebus_passive_completed_transactions_total", {}, -400)
                self.stub.load([baseline, flow])
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


class ReadOnlyTests(SmokeTestCase):
    def test_every_request_is_a_read(self):
        self.prepare_pairs("enh")
        for invariant in GRID_INVARIANTS:
            self.stub.load(self.phases("mux", "enh"))
            self.run_smoke(self.environ(invariant, "mux", "enh"))
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
        self.assertEqual(sorted(SMOKE.CHECKS), sorted(GRID_INVARIANTS + ("DS01", "ES01")))
        self.assertFalse(set(SMOKE.CHECKS) & set(SMOKE.RESERVED))


if __name__ == "__main__":
    unittest.main()
