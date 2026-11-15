#!/usr/bin/env python3
"""Offline tests of the verify-only add-on target commands of adaptermux-v2.

A local HTTP stub plays the gateway's read surfaces and a local TCP stub
plays the ebusd command port. Every answer is synthetic.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCRIPTS = pathlib.Path(__file__).resolve().parent
GATEWAY_STARTED = SCRIPTS / "adaptermux_v2_addon_gateway_started.sh"
CLIENT_STARTED = SCRIPTS / "adaptermux_v2_addon_client_started.sh"
STOP = SCRIPTS / "adaptermux_v2_addon_stop.sh"
COMMANDS = (GATEWAY_STARTED, CLIENT_STARTED, STOP)

HEALTH = "/portal/api/v1/health"
OBSERVABILITY = "/portal/api/v1/bus/observability"
VARS = "/debug/vars"
METRICS = "/metrics"
READ_PATHS = {HEALTH, OBSERVABILITY, VARS, METRICS}

# (composition, southbound) of every executable cell and the transport class
# the gateway reports for it.
CELL_TRANSPORT = {
    ("mux", "ens"): "adapter-direct-ens",
    ("mux", "enh"): "adapter-direct",
    ("direct", "ens"): "ens",
    ("direct", "enh"): "enh",
    ("direct", "udp"): "udp-plain",
    ("direct", "tcp"): "tcp-plain",
    ("ebusd-tcp", "ebusd-tcp"): "ebusd-tcp",
}

NON_FINITE_SETTINGS = ("MATRIX_ADDON_WAIT_SEC", "MATRIX_ADDON_POLL_SEC", "MATRIX_ADDON_HTTP_TIMEOUT_SEC")
NON_FINITE_VALUES = ("nan", "inf", "Infinity")
EBUSD_SIGNAL = "signal acquired, 40 symbols/sec, 80 max symbols/sec\n\n"
UNREACHABLE_PROXY = "http://127.0.0.1:9"


def health_body(proxy="READY"):
    return json.dumps({"status": "ok", "readiness": {
        "process_readiness": "READY", "http_readiness": "READY", "proxy_readiness": proxy}})


def observability_body(transport_class, phase="LIVE_READY"):
    status = {"transport_class": transport_class}
    if phase is not None:
        status["startup"] = {"phase": phase, "cache_epoch": 1, "live_epoch": 1}
    return json.dumps({"status": status, "counters": {"series_budget_overflow_total": 0}})


def vars_body(session_frames):
    variables = {"cmdline": ["gateway", "-synthetic-setting"], "memstats": {"Alloc": 1}}
    if session_frames is not None:
        variables["adaptermux_session_frame_latency_us_bucket_total"] = {
            "le_1000": session_frames, "le_5000": session_frames,
            "le_25000": session_frames, "le_100000": session_frames, "gt_100000": 1}
    return json.dumps(variables)


class Truncated:
    """An answer whose Content-Length announces more bytes than are sent."""

    def __init__(self, body):
        self.body = body


def closed_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class StubGateway:
    """Serves, per path, a sequence of answers whose last one repeats."""

    def __init__(self):
        self.requests = []
        self.answers = {}
        self.cursor = {}
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def answer(self):
                stub.serve(self)

            def log_message(self, *args):
                return

        # BaseHTTPRequestHandler dispatches on do_<METHOD> attribute names.
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
            setattr(Handler, "do_" + method, Handler.answer)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = "http://127.0.0.1:%d" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def load(self, answers):
        with self.lock:
            self.requests.clear()
            self.answers = {path: list(bodies) for path, bodies in answers.items()}
            self.cursor = {}

    def serve(self, handler):
        with self.lock:
            self.requests.append((handler.command, handler.path, dict(handler.headers)))
            bodies = self.answers.get(handler.path)
            index = self.cursor.get(handler.path, 0)
            self.cursor[handler.path] = index + 1
        if handler.command != "GET" or not bodies:
            handler.send_response(404)
            handler.end_headers()
            return
        body = bodies[min(index, len(bodies) - 1)]
        truncated = isinstance(body, Truncated)
        payload = (body.body if truncated else body).encode("utf-8")
        handler.send_response(200)
        # A truncated answer announces more bytes than it sends and the
        # connection closes, as when the gateway restarts mid-answer.
        handler.send_header("Content-Length", str(len(payload) + (64 if truncated else 0)))
        handler.end_headers()
        handler.wfile.write(payload)
        if truncated:
            handler.close_connection = True

    def paths(self):
        with self.lock:
            return [path for _, path, _ in self.requests]


class StubEbusd:
    """Plays the ebusd command port: records each line, answers one text."""

    def __init__(self):
        self.lines = []
        self.answer = EBUSD_SIGNAL
        stub = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                line = self.rfile.readline().decode("utf-8")
                stub.lines.append(line)
                self.wfile.write(stub.answer.encode("utf-8"))

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.address = "127.0.0.1:%d" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class CommandTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gateway = StubGateway()
        cls.ebusd = StubEbusd()

    @classmethod
    def tearDownClass(cls):
        cls.gateway.close()
        cls.ebusd.close()

    def setUp(self):
        self.gateway.load({})
        self.ebusd.lines.clear()
        self.ebusd.answer = EBUSD_SIGNAL
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.case_dir = self.root / "results" / "ENH-AD06"
        self.case_dir.mkdir(parents=True)
        self.work_dir = self.root / "work"
        self.work_dir.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def run_command(self, command, matrix):
        # Proxy variables point nowhere: a command that honoured them could
        # not reach the stub. Only PATH and MATRIX_* reach the command.
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C",
               "http_proxy": UNREACHABLE_PROXY, "HTTP_PROXY": UNREACHABLE_PROXY}
        env.update(matrix)
        result = subprocess.run(["sh", str(command)], env=env, cwd=self.work_dir,
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(list(self.work_dir.iterdir()), [], "a command wrote outside the case directory")
        for method, path, headers in self.gateway.requests:
            self.assertEqual(method, "GET")
            self.assertIn(path, READ_PATHS)
            lowered = {name.lower() for name in headers}
            self.assertFalse(lowered & {"authorization", "cookie", "proxy-authorization"})
        return result

    def matrix(self, composition, southbound, **extra):
        values = {
            "MATRIX_CASE_ID": "ENH-AD06",
            "MATRIX_CASE_DIR": str(self.case_dir),
            "MATRIX_COMPOSITION": composition,
            "MATRIX_SOUTHBOUND_TRANSPORT": southbound,
            "MATRIX_GATEWAY_BASE_URL": self.gateway.base_url,
            "MATRIX_ADDON_WAIT_SEC": "0",
            "MATRIX_ADDON_POLL_SEC": "0.05",
            "MATRIX_ADDON_HTTP_TIMEOUT_SEC": "5",
        }
        values.update(extra)
        return values


class GatewayStartedTest(CommandTestCase):
    def test_ready_for_every_executable_cell(self):
        for (composition, southbound), transport_class in CELL_TRANSPORT.items():
            with self.subTest(composition=composition, southbound=southbound):
                self.gateway.load({HEALTH: [health_body()], OBSERVABILITY: [observability_body(transport_class)]})
                result = self.run_command(GATEWAY_STARTED, self.matrix(composition, southbound))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("ready", result.stdout)
                self.assertNotIn("127.0.0.1", result.stdout + result.stderr)
                expected_paths = {HEALTH, OBSERVABILITY} if composition == "mux" else {OBSERVABILITY}
                self.assertEqual(set(self.gateway.paths()), expected_paths)

    def test_wrong_transport_for_every_executable_cell(self):
        for (composition, southbound), expected in CELL_TRANSPORT.items():
            for reported in sorted(set(CELL_TRANSPORT.values()) - {expected}) + [""]:
                with self.subTest(composition=composition, southbound=southbound, reported=reported):
                    self.gateway.load({HEALTH: [health_body()], OBSERVABILITY: [observability_body(reported)]})
                    result = self.run_command(GATEWAY_STARTED, self.matrix(composition, southbound))
                    self.assertEqual(result.returncode, 11, result.stderr)
                    self.assertIn("the cell expects " + expected, result.stderr)

    def test_startup_phase_not_ready(self):
        for phase in ("BOOT_INIT", "CACHE_LOADED_STALE", "LIVE_WARMUP", "DEGRADED", None):
            with self.subTest(phase=phase):
                self.gateway.load({HEALTH: [health_body()],
                                   OBSERVABILITY: [observability_body("adapter-direct", phase)]})
                result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "enh"))
                self.assertEqual(result.returncode, 13, result.stderr)
                self.assertIn("startup phase is " + (phase or "absent"), result.stderr)

    def test_proxy_listener_not_ready_on_mux_cells(self):
        for proxy in ("DEGRADED", "DISABLED"):
            with self.subTest(proxy=proxy):
                self.gateway.load({HEALTH: [health_body(proxy)],
                                   OBSERVABILITY: [observability_body("adapter-direct-ens")]})
                result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "ens"))
                self.assertEqual(result.returncode, 12, result.stderr)
                self.assertIn("proxy listener is " + proxy, result.stderr)

    def test_compatibility_cell_does_not_need_the_proxy_listener(self):
        self.gateway.load({HEALTH: [health_body("DISABLED")], OBSERVABILITY: [observability_body("enh")]})
        result = self.run_command(GATEWAY_STARTED, self.matrix("direct", "enh"))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_waits_until_the_gateway_is_ready(self):
        self.gateway.load({HEALTH: [health_body("DEGRADED"), health_body()],
                           OBSERVABILITY: [observability_body("adapter-direct", "LIVE_WARMUP"),
                                           observability_body("adapter-direct", "LIVE_WARMUP"),
                                           observability_body("adapter-direct")]})
        result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "enh", MATRIX_ADDON_WAIT_SEC="20"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.gateway.paths().count(OBSERVABILITY), 3)

    def test_gateway_absent(self):
        url = "http://127.0.0.1:%d" % closed_port()
        result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "enh", MATRIX_GATEWAY_BASE_URL=url))
        self.assertEqual(result.returncode, 10, result.stderr)
        self.assertNotIn("127.0.0.1", result.stdout + result.stderr)

    def test_truncated_answer_is_no_answer(self):
        truncated = Truncated(observability_body("adapter-direct"))
        self.gateway.load({HEALTH: [health_body()], OBSERVABILITY: [truncated]})
        result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "enh"))
        self.assertEqual(result.returncode, 10, result.stderr)
        self.assertIn("did not answer (IncompleteRead)", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_waits_through_a_truncated_answer(self):
        self.gateway.load({HEALTH: [Truncated(health_body()), health_body()],
                           OBSERVABILITY: [observability_body("adapter-direct")]})
        result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "enh", MATRIX_ADDON_WAIT_SEC="20"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.gateway.paths().count(HEALTH), 2)

    def test_usage_errors_contact_nothing(self):
        cases = {
            "no gateway": {"MATRIX_GATEWAY_BASE_URL": ""},
            "credentials in the URL": {"MATRIX_GATEWAY_BASE_URL": "http://user:pw@127.0.0.1:1"},
            "not http": {"MATRIX_GATEWAY_BASE_URL": "file:///tmp"},
            "unclosed IPv6 bracket": {"MATRIX_GATEWAY_BASE_URL": "http://[::1"},
            "port not a number": {"MATRIX_GATEWAY_BASE_URL": "http://127.0.0.1:port"},
            "no executable cell": {"MATRIX_COMPOSITION": "mux", "MATRIX_SOUTHBOUND_TRANSPORT": "udp"},
            "no composition": {"MATRIX_COMPOSITION": ""},
            "bad wait": {"MATRIX_ADDON_WAIT_SEC": "soon"},
        }
        for setting_name in NON_FINITE_SETTINGS:
            for value in NON_FINITE_VALUES:
                cases["%s=%s" % (setting_name, value)] = {setting_name: value}
        for name, override in cases.items():
            with self.subTest(case=name):
                self.gateway.load({})
                result = self.run_command(GATEWAY_STARTED, self.matrix("mux", "enh", **override))
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertEqual(self.gateway.requests, [])


class ClientStartedTest(CommandTestCase):
    def client_matrix(self, client="ebusd", composition="mux", **extra):
        values = {"MATRIX_NORTHBOUND_CLIENT": client, "MATRIX_EBUSD_COMMAND_ADDR": self.ebusd.address,
                  "MATRIX_ADDON_WAIT_SEC": "0.3", "MATRIX_ADDON_POLL_SEC": "0.1"}
        values.update(extra)
        southbound = "ebusd-tcp" if composition == "ebusd-tcp" else "enh"
        return self.matrix(composition, southbound, **values)

    def test_ebusd_client_connected_through_the_proxy_listener(self):
        for client in ("ebusd", "vrc-explorer"):
            with self.subTest(client=client):
                self.ebusd.lines.clear()
                self.gateway.load({HEALTH: [health_body()], VARS: [vars_body(10), vars_body(25)]})
                result = self.run_command(CLIENT_STARTED, self.client_matrix(client))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("external session on the proxy listener", result.stdout)
                self.assertEqual(self.ebusd.lines, ["state\n"])
                self.assertNotIn("127.0.0.1", result.stdout + result.stderr)

    def test_client_absent_when_no_session_receives_symbols(self):
        for frames in (10, None):
            with self.subTest(frames=frames):
                self.ebusd.lines.clear()
                self.gateway.load({HEALTH: [health_body()], VARS: [vars_body(frames)]})
                result = self.run_command(CLIENT_STARTED, self.client_matrix())
                self.assertEqual(result.returncode, 14, result.stderr)
                self.assertGreaterEqual(self.gateway.paths().count(VARS), 2)
                self.assertEqual(self.ebusd.lines, [])

    def test_proxy_listener_not_ready(self):
        self.gateway.load({HEALTH: [health_body("DEGRADED")], VARS: [vars_body(10), vars_body(25)]})
        result = self.run_command(CLIENT_STARTED, self.client_matrix())
        self.assertEqual(result.returncode, 12, result.stderr)
        self.assertNotIn(VARS, self.gateway.paths())

    def test_truncated_gateway_answer_is_no_answer(self):
        self.gateway.load({HEALTH: [Truncated(health_body())], VARS: [vars_body(10), vars_body(25)]})
        result = self.run_command(CLIENT_STARTED, self.client_matrix())
        self.assertEqual(result.returncode, 10, result.stderr)
        self.assertIn("did not answer (IncompleteRead)", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(self.ebusd.lines, [])

    def test_ebusd_command_port_absent(self):
        self.gateway.load({HEALTH: [health_body()], VARS: [vars_body(10), vars_body(25)]})
        address = "127.0.0.1:%d" % closed_port()
        result = self.run_command(CLIENT_STARTED, self.client_matrix(MATRIX_EBUSD_COMMAND_ADDR=address))
        self.assertEqual(result.returncode, 15, result.stderr)
        self.assertNotIn("127.0.0.1", result.stdout + result.stderr)

    def test_ebusd_answers_without_signal_or_with_an_error(self):
        for answer, code in (("no signal\n\n", 16), ("ERR: element not found\n\n", 15)):
            with self.subTest(answer=answer):
                self.ebusd.answer = answer
                self.gateway.load({HEALTH: [health_body()], VARS: [vars_body(10), vars_body(25)]})
                result = self.run_command(CLIENT_STARTED, self.client_matrix())
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertTrue(self.ebusd.lines)
                self.assertEqual(set(self.ebusd.lines), {"state\n"})

    def test_ebusd_tcp_cell_checks_only_ebusd(self):
        result = self.run_command(CLIENT_STARTED, self.client_matrix(
            client="none", composition="ebusd-tcp", MATRIX_GATEWAY_BASE_URL=""))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.gateway.requests, [])
        self.assertEqual(self.ebusd.lines, ["state\n"])

    def test_refused_when_the_cell_has_no_external_client(self):
        for client, composition in (("synthetic-enh", "mux"), ("none", "mux"), ("none", "direct")):
            with self.subTest(client=client, composition=composition):
                result = self.run_command(CLIENT_STARTED, self.client_matrix(client, composition))
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assertEqual(self.gateway.requests, [])
                self.assertEqual(self.ebusd.lines, [])

    def test_usage_errors(self):
        cases = {
            "no ebusd port": {"MATRIX_EBUSD_COMMAND_ADDR": ""},
            "ebusd port without port": {"MATRIX_EBUSD_COMMAND_ADDR": "localhost"},
            "single reading": {"MATRIX_ADDON_WAIT_SEC": "0"},
            "credentials in the URL": {"MATRIX_GATEWAY_BASE_URL": "http://user:pw@127.0.0.1:1"},
            "unclosed IPv6 bracket": {"MATRIX_GATEWAY_BASE_URL": "http://[::1"},
            "port not a number": {"MATRIX_GATEWAY_BASE_URL": "http://127.0.0.1:port"},
        }
        for setting_name in NON_FINITE_SETTINGS:
            for value in NON_FINITE_VALUES:
                cases["%s=%s" % (setting_name, value)] = {setting_name: value}
        for name, override in cases.items():
            with self.subTest(case=name):
                self.gateway.load({})
                result = self.run_command(CLIENT_STARTED, self.client_matrix(**override))
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertEqual(self.gateway.requests, [])
                self.assertEqual(self.ebusd.lines, [])


METRICS_TEXT = "\n".join([
    "# TYPE ebus_adaptermux_transaction_endings_total counter",
    'ebus_adaptermux_transaction_endings_total{reason="transaction_done"} 42',
    'ebus_adaptermux_transaction_endings_total{reason="syn_timeout"} 1',
    "ebus_adaptermux_active_arbitration_held 0",
    "ebus_passive_completed_transactions_total 7",
]) + "\n"


class StopTest(CommandTestCase):
    def record(self):
        return json.loads((self.case_dir / "gateway_counters_end.json").read_text(encoding="utf-8"))

    def test_records_the_counters_and_stops_nothing(self):
        observability = json.loads(observability_body("adapter-direct"))
        observability["frames"] = [
            {"scope": "active", "source": "31", "target": "15", "family": "07", "frame_type": "md", "count": 3},
            {"scope": "active", "source": "31", "target": "08", "family": "b5", "frame_type": "md", "count": 4},
            {"scope": "passive", "source": "10", "target": "08", "family": "b5", "frame_type": "md", "count": 5},
        ]
        self.gateway.load({METRICS: [METRICS_TEXT], VARS: [vars_body(25)], OBSERVABILITY: [json.dumps(observability)]})
        result = self.run_command(STOP, self.matrix("mux", "enh"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(path.name for path in self.case_dir.iterdir()), ["gateway_counters_end.json"])
        record = self.record()
        self.assertEqual(record["case_id"], "ENH-AD06")
        metrics = record["surfaces"]["metrics"]["counters"]
        self.assertEqual([(sample["name"], sample["labels"].get("reason"), sample["value"]) for sample in metrics], [
            ("ebus_adaptermux_transaction_endings_total", "syn_timeout", 1.0),
            ("ebus_adaptermux_transaction_endings_total", "transaction_done", 42.0),
            ("ebus_passive_completed_transactions_total", None, 7.0),
        ])
        variables = record["surfaces"]["vars"]["counters"]
        self.assertEqual(sorted(variables), ["adaptermux_session_frame_latency_us_bucket_total"])
        frames = record["surfaces"]["observability"]["counters"]["frame_counts"]
        self.assertEqual(frames, {"active/md": 7, "passive/md": 5})
        text = json.dumps(record)
        for absent in ("127.0.0.1", "http", "synthetic-setting"):
            self.assertNotIn(absent, text)
        self.assertEqual(self.gateway.paths().count(METRICS), 1)

    def test_gateway_absent_still_exits_zero(self):
        url = "http://127.0.0.1:%d" % closed_port()
        result = self.run_command(STOP, self.matrix("mux", "enh", MATRIX_GATEWAY_BASE_URL=url))
        self.assertEqual(result.returncode, 0, result.stderr)
        surfaces = self.record()["surfaces"]
        self.assertEqual(sorted(surfaces), ["metrics", "observability", "vars"])
        self.assertTrue(all(surface["read"] is False for surface in surfaces.values()))
        self.assertNotIn("127.0.0.1", json.dumps(surfaces))

    def test_unanswered_surfaces_are_read_together_within_one_deadline(self):
        silent = socket.socket()
        silent.bind(("127.0.0.1", 0))
        silent.listen(8)
        self.addCleanup(silent.close)
        url = "http://127.0.0.1:%d" % silent.getsockname()[1]
        started = time.monotonic()
        result = self.run_command(STOP, self.matrix(
            "mux", "enh", MATRIX_GATEWAY_BASE_URL=url, MATRIX_ADDON_HTTP_TIMEOUT_SEC="2"))
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(elapsed, 4.5, "the three reads must not add their timeouts")
        surfaces = self.record()["surfaces"]
        self.assertTrue(all(surface["read"] is False for surface in surfaces.values()))

    def test_truncated_answer_is_recorded_as_not_read(self):
        self.gateway.load({METRICS: [Truncated(METRICS_TEXT)], VARS: [vars_body(25)],
                           OBSERVABILITY: [observability_body("adapter-direct")]})
        result = self.run_command(STOP, self.matrix("mux", "enh"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        surfaces = self.record()["surfaces"]
        self.assertEqual(surfaces["metrics"], {"read": False, "reason": "metrics did not answer (IncompleteRead)"})
        self.assertTrue(surfaces["vars"]["read"])
        self.assertTrue(surfaces["observability"]["read"])

    def test_non_finite_timeout_is_recorded_as_usage(self):
        for value in NON_FINITE_VALUES:
            with self.subTest(value=value):
                self.gateway.load({})
                result = self.run_command(STOP, self.matrix("mux", "enh", MATRIX_ADDON_HTTP_TIMEOUT_SEC=value))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertEqual(self.gateway.requests, [])
                surfaces = self.record()["surfaces"]
                self.assertEqual(sorted(surfaces), ["metrics", "observability", "vars"])
                for surface in surfaces.values():
                    self.assertEqual(surface, {"read": False, "reason":
                                               "usage: MATRIX_ADDON_HTTP_TIMEOUT_SEC must be a finite number"})

    def test_malformed_url_is_recorded_as_usage(self):
        for value in ("http://[::1", "http://127.0.0.1:port"):
            with self.subTest(value=value):
                self.gateway.load({})
                result = self.run_command(STOP, self.matrix("mux", "enh", MATRIX_GATEWAY_BASE_URL=value))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertEqual(self.gateway.requests, [])
                surfaces = self.record()["surfaces"]
                self.assertEqual(sorted(surfaces), ["metrics", "observability", "vars"])
                for surface in surfaces.values():
                    self.assertEqual(surface, {"read": False, "reason":
                                               "usage: MATRIX_GATEWAY_BASE_URL is not a valid URL"})

    def test_missing_case_directory_is_a_usage_error(self):
        missing = str(self.root / "absent")
        result = self.run_command(STOP, self.matrix("mux", "enh", MATRIX_CASE_DIR=missing))
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(pathlib.Path(missing).exists())
        self.assertEqual(self.gateway.requests, [])


# Code lines of a command: its comment lines removed.
def code_lines(path):
    return [line for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")]


# Environment names a command reads: setting("..."), seconds("...", ...) and
# the override name of each surface.
ENV_READ = re.compile(r'(?:setting|seconds)\("([A-Z0-9_]+)"|\("([A-Z0-9_]+)", "/')
# Every quoted upper-case name; outside MATRIX_* only these state values occur.
QUOTED_NAME = re.compile(r"""["']([A-Z][A-Z0-9]*_[A-Z0-9_]+)["']""")
STATE_VALUES = {"LIVE_READY"}
FILE_OPEN = re.compile(r"(?<![.\w])open\(")
SHELL_VARIABLE = re.compile(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)")
PATH_LITERAL = re.compile(r'"(/[a-z][^"]*)"')
FORBIDDEN = (
    # add-on option writes and supervisor requests
    "supervisor", "hassio", "/addons", "options", "ha addons",
    # adapter requests and adapter configuration
    "matrix_adapter_addr", "matrix_proxy_", "matrix_mux_enh_addr", "/config", "/setup", "/wifi",
    # credentials
    "token", "password", "passwd", "secret", "authorization", "cookie", "api_key", "netrc",
    # other request methods and clients
    '"post"', '"put"', '"patch"', '"delete"', "data=", "curl", "wget", " nc ",
)


class SafetyTest(unittest.TestCase):
    def test_commands_have_no_option_write_adapter_request_or_credential(self):
        for command in COMMANDS:
            code = "\n".join(code_lines(command)).lower()
            for pattern in FORBIDDEN:
                with self.subTest(command=command.name, pattern=pattern):
                    self.assertNotIn(pattern, code)

    def test_commands_read_only_matrix_variables(self):
        for command in COMMANDS:
            code = "\n".join(code_lines(command))
            with self.subTest(command=command.name):
                read = {first or second for first, second in ENV_READ.findall(code)}
                self.assertIn("MATRIX_GATEWAY_BASE_URL", read)
                self.assertEqual({name for name in read if not name.startswith("MATRIX_")}, set())
                quoted = set(QUOTED_NAME.findall(code)) - STATE_VALUES
                self.assertEqual({name for name in quoted if not name.startswith("MATRIX_")}, set())
                self.assertEqual(code.count("os.environ"), 1)
                self.assertIn("os.environ.get(name)", code)
                self.assertNotIn("getenv", code)
                self.assertEqual(set(SHELL_VARIABLE.findall(code)), set())

    def test_commands_send_only_get_requests_to_read_surfaces(self):
        for command in COMMANDS:
            code = "\n".join(code_lines(command))
            with self.subTest(command=command.name):
                self.assertEqual(set(re.findall(r'method="([A-Z]+)"', code)), {"GET"})
                self.assertTrue(set(PATH_LITERAL.findall(code)) <= READ_PATHS)
                self.assertIn("ProxyHandler({})", code)
                self.assertIn("python3 -I -", code)

    def test_only_the_client_command_opens_a_socket_and_sends_state(self):
        for command in COMMANDS:
            code = "\n".join(code_lines(command))
            with self.subTest(command=command.name):
                if command == CLIENT_STARTED:
                    self.assertEqual(code.count("sendall("), 1)
                    self.assertIn("sendall(EBUSD_STATE_REQUEST)", code)
                    self.assertIn('EBUSD_STATE_REQUEST = b"state\\n"', code)
                else:
                    self.assertNotIn("socket", code)

    def test_only_the_stop_command_writes_and_only_into_the_case_directory(self):
        for command in COMMANDS:
            code = "\n".join(code_lines(command))
            with self.subTest(command=command.name):
                if command == STOP:
                    self.assertEqual(len(FILE_OPEN.findall(code)), 1)
                    self.assertIn('with open(partial, "w"', code)
                    self.assertIn("path = os.path.join(case_dir, RECORD_FILE)", code)
                    self.assertIn('partial = path + ".partial"', code)
                else:
                    self.assertEqual(FILE_OPEN.findall(code), [])
                    self.assertNotIn("os.replace", code)
                    self.assertNotIn("makedirs", code)


class ReadmeContractTest(unittest.TestCase):
    def test_readme_says_ebusd_address_is_exported_not_derived(self):
        readme = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cmd", "matrix-runner", "README.md")
        with open(readme, encoding="utf-8") as handle:
            text = " ".join(handle.read().split())
        self.assertIn("does not derive it from `--ebusd-command-addr`", text)
        self.assertIn("export it in the runner's environment", text)


if __name__ == "__main__":
    unittest.main()
