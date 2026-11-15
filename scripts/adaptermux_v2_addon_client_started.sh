#!/bin/sh
# Verify-only client start command of an adaptermux-v2 cell on the add-on
# target, passed to matrix-runner as --start-ebusd.
#
# The supervisor starts the external client from its own add-on options and
# the operator prepares the hardware setup. This command starts nothing and
# changes nothing: it checks that the cell's external client is connected.
#
# - Mux cells (ebusd or VRC Explorer behind ebusd): the gateway reports the
#   proxy listener ready, and its per-session frame delivery counter in the
#   expvar surface grows during the wait, so an external session on the
#   proxy listener is receiving bus symbols.
# - ebusd cells (mux and ebusd-tcp): the ebusd command port in
#   MATRIX_EBUSD_COMMAND_ADDR answers the read-only "state" request with an
#   acquired signal. "state" is the only line this command sends there.
#
# It reads only MATRIX_* variables, sends only HTTP GET requests and that one
# line, sends no credential, writes no file and prints no address.
#
# Exit statuses:
#   0  the external client is connected
#   2  usage error: a MATRIX_* setting is missing or invalid
#   3  refused: the cell has no external client started before its workload
#   10 the gateway did not answer
#   12 the proxy listener is not ready
#   14 no external session on the proxy listener received bus symbols
#   15 the ebusd command port did not answer the state request
#   16 ebusd answered without an acquired signal
set -eu

if ! command -v python3 >/dev/null 2>&1; then
  echo "client-started: python3 is required" >&2
  exit 2
fi

exec python3 -I - <<'PY'
import http.client
import json
import math
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

NAME = "client-started"
EXIT_CONNECTED = 0
EXIT_USAGE = 2
EXIT_REFUSED = 3
EXIT_NO_ANSWER = 10
EXIT_PROXY_NOT_READY = 12
EXIT_NO_SESSION = 14
EXIT_EBUSD_NO_ANSWER = 15
EXIT_EBUSD_NO_SIGNAL = 16

PROXY_READY = "READY"
EBUSD_CLIENTS = ("ebusd", "vrc-explorer")
EBUSD_STATE_REQUEST = b"state\n"
EBUSD_SIGNAL_PREFIX = "signal acquired"
SESSION_FRAMES = "adaptermux_session_frame_latency_us_bucket_total"
# le_100000 and gt_100000 partition every delivered frame.
SESSION_FRAME_BINS = ("le_100000", "gt_100000")
MAX_ANSWER_BYTES = 4096

SURFACES = {
    "health": ("MATRIX_HEALTH_URL", "/portal/api/v1/health"),
    "vars": ("MATRIX_DEBUG_VARS_URL", "/debug/vars"),
}


class UsageError(Exception):
    pass


class RefusalError(Exception):
    pass


class NoAnswer(Exception):
    pass


def setting(name):
    return (os.environ.get(name) or "").strip()


def seconds(name, default, allow_zero):
    raw = setting(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise UsageError(name + " is not a number") from error
    if not math.isfinite(value):
        raise UsageError(name + " must be a finite number")
    if value < 0 or (value == 0 and not allow_zero):
        raise UsageError(name + " must be positive")
    return value


def checked_url(name, raw):
    # A malformed authority (an unclosed IPv6 bracket, a port that is not a
    # number) makes the parser raise ValueError: that is a usage error.
    try:
        parts = urllib.parse.urlsplit(raw)
        hostname = parts.hostname
        parts.port
    except ValueError as error:
        raise UsageError(name + " is not a valid URL") from error
    if parts.scheme not in ("http", "https") or not hostname:
        raise UsageError(name + " must be an http or https URL")
    if "@" in parts.netloc:
        raise UsageError(name + " must not carry credentials")
    if parts.query or parts.fragment:
        raise UsageError(name + " must not carry a query or fragment")
    return raw


def surface_urls(keys):
    base = setting("MATRIX_GATEWAY_BASE_URL").rstrip("/")
    urls = {}
    for key in keys:
        override, path = SURFACES[key]
        if setting(override):
            urls[key] = checked_url(override, setting(override))
        elif base:
            urls[key] = checked_url("MATRIX_GATEWAY_BASE_URL", base) + path
        else:
            raise UsageError("MATRIX_GATEWAY_BASE_URL is required")
    return urls


def ebusd_address():
    raw = setting("MATRIX_EBUSD_COMMAND_ADDR")
    if not raw:
        raise UsageError("MATRIX_EBUSD_COMMAND_ADDR is required")
    host, sep, port = raw.rpartition(":")
    host = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    # ASCII digits only: str.isdigit() also accepts digits that int() rejects.
    if not sep or not host or not port.isascii() or not port.isdigit() or not 0 < int(port) < 65536:
        raise UsageError("MATRIX_EBUSD_COMMAND_ADDR must be host:port")
    return host, int(port)


# No proxy handler: the request goes to the gateway URL and nowhere else.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get_json(url, timeout, surface):
    request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    try:
        with OPENER.open(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        raise NoAnswer(surface + " answered HTTP status " + str(error.code)) from error
    # A truncated body raises http.client.IncompleteRead, an HTTPException.
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as error:
        raise NoAnswer(surface + " did not answer (" + type(error).__name__ + ")") from error
    try:
        value = json.loads(body)
    except ValueError as error:
        raise NoAnswer(surface + " did not answer JSON") from error
    if not isinstance(value, dict):
        raise NoAnswer(surface + " did not answer a JSON object")
    return value


def session_frames(variables):
    bins = variables.get(SESSION_FRAMES)
    if not isinstance(bins, dict):
        return 0
    total = 0
    for key in SESSION_FRAME_BINS:
        value = bins.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            total += value
    return total


def proxy_readiness(health):
    readiness = health.get("readiness")
    return readiness.get("proxy_readiness") if isinstance(readiness, dict) else None


def ebusd_state(address, timeout):
    # One read-only request; the answer ends with an empty line.
    try:
        with socket.create_connection(address, timeout=timeout) as connection:
            connection.settimeout(timeout)
            connection.sendall(EBUSD_STATE_REQUEST)
            data = b""
            while b"\n\n" not in data and len(data) < MAX_ANSWER_BYTES:
                chunk = connection.recv(512)
                if not chunk:
                    break
                data += chunk
    except OSError as error:
        raise NoAnswer("ebusd command port did not answer (" + type(error).__name__ + ")") from error
    lines = [line.strip() for line in data.decode("utf-8", "replace").splitlines()]
    first = next((line for line in lines if line), "")
    if not first:
        raise NoAnswer("ebusd command port sent an empty answer")
    return first


def judge_ebusd(answer):
    if answer.upper().startswith("ERR"):
        return EXIT_EBUSD_NO_ANSWER, "ebusd answered the state request with an error"
    if not answer.lower().startswith(EBUSD_SIGNAL_PREFIX):
        return EXIT_EBUSD_NO_SIGNAL, "ebusd reports no acquired signal"
    return EXIT_CONNECTED, "ebusd reports an acquired signal"


def plan():
    composition = setting("MATRIX_COMPOSITION").lower()
    client = setting("MATRIX_NORTHBOUND_CLIENT").lower()
    if not composition:
        raise UsageError("MATRIX_COMPOSITION is required")
    if composition == "mux":
        if client not in EBUSD_CLIENTS:
            raise RefusalError("mux cell with client %s has no external client started before its workload"
                               % (client or "absent"))
        return True, True
    if composition == "ebusd-tcp":
        return False, True
    raise RefusalError("composition %s has no external client" % composition)


def main():
    case_id = setting("MATRIX_CASE_ID") or "cell"
    try:
        needs_session, needs_ebusd = plan()
        urls = surface_urls(("health", "vars")) if needs_session else {}
        address = ebusd_address() if needs_ebusd else None
        wait = seconds("MATRIX_ADDON_WAIT_SEC", 60.0, True)
        poll = seconds("MATRIX_ADDON_POLL_SEC", 2.0, False)
        timeout = seconds("MATRIX_ADDON_HTTP_TIMEOUT_SEC", 10.0, False)
        if needs_session and wait < poll:
            # The session check compares two readings of the counter.
            raise UsageError("MATRIX_ADDON_WAIT_SEC must be at least MATRIX_ADDON_POLL_SEC")
    except RefusalError as error:
        print("%s: %s: refused: %s" % (NAME, case_id, error), file=sys.stderr)
        return EXIT_REFUSED
    except UsageError as error:
        print("%s: %s: usage: %s" % (NAME, case_id, error), file=sys.stderr)
        return EXIT_USAGE

    deadline = time.monotonic() + wait
    baseline = None
    session_seen = not needs_session
    messages = []
    while True:
        code, message = EXIT_CONNECTED, ""
        if not session_seen:
            try:
                proxy = proxy_readiness(get_json(urls["health"], timeout, "health"))
                if proxy != PROXY_READY:
                    code, message = EXIT_PROXY_NOT_READY, "proxy listener is %s" % (proxy or "absent")
                else:
                    frames = session_frames(get_json(urls["vars"], timeout, "expvar"))
                    if baseline is not None and frames > baseline:
                        session_seen = True
                        messages.append("an external session on the proxy listener receives bus symbols")
                    else:
                        if baseline is None or frames < baseline:
                            baseline = frames
                        code, message = EXIT_NO_SESSION, "no external session on the proxy listener received bus symbols"
            except NoAnswer as error:
                code, message = EXIT_NO_ANSWER, "gateway: " + str(error)
        if session_seen and needs_ebusd:
            try:
                code, message = judge_ebusd(ebusd_state(address, timeout))
            except NoAnswer as error:
                code, message = EXIT_EBUSD_NO_ANSWER, str(error)
        remaining = deadline - time.monotonic()
        if code == EXIT_CONNECTED or remaining <= 0:
            break
        time.sleep(min(poll, remaining))
    if code == EXIT_CONNECTED:
        messages.append(message)
        print("%s: %s: connected: %s" % (NAME, case_id, "; ".join(part for part in messages if part)))
    else:
        print("%s: %s: %s" % (NAME, case_id, message), file=sys.stderr)
    return code


sys.exit(main())
PY
