#!/bin/sh
# Verify-only start command of an adaptermux-v2 cell on the add-on target.
#
# The supervisor starts the gateway from its add-on options and the operator
# prepares the hardware setup. This command starts nothing and changes
# nothing: it checks that the running gateway is in the state the cell needs.
#
# It reads only MATRIX_* variables, sends only HTTP GET requests without any
# credential to the gateway named by MATRIX_GATEWAY_BASE_URL, writes no file
# and prints no URL.
#
# Exit statuses:
#   0  ready: the cell's transport class, the proxy listener ready (mux
#      cells) and the startup phase LIVE_READY
#   2  usage error: a MATRIX_* setting is missing or invalid
#   10 the gateway did not answer
#   11 the gateway reports another transport class than the cell expects
#   12 mux cell: the proxy listener is not ready
#   13 the startup phase is not LIVE_READY
set -eu

if ! command -v python3 >/dev/null 2>&1; then
  echo "gateway-started: python3 is required" >&2
  exit 2
fi

exec python3 -I - <<'PY'
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

NAME = "gateway-started"
EXIT_READY = 0
EXIT_USAGE = 2
EXIT_NO_ANSWER = 10
EXIT_WRONG_TRANSPORT = 11
EXIT_PROXY_NOT_READY = 12
EXIT_STARTUP_NOT_READY = 13

# Transport class the gateway reports for each (composition, southbound)
# pair of an executable cell; the same table as the smoke command.
EXPECTED_TRANSPORT_CLASS = {
    ("mux", "ens"): "adapter-direct-ens",
    ("mux", "enh"): "adapter-direct",
    ("direct", "ens"): "ens",
    ("direct", "enh"): "enh",
    ("direct", "udp"): "udp-plain",
    ("direct", "tcp"): "tcp-plain",
    ("ebusd-tcp", "ebusd-tcp"): "ebusd-tcp",
}
READY_PHASE = "LIVE_READY"
PROXY_READY = "READY"

SURFACES = {
    "health": ("MATRIX_HEALTH_URL", "/portal/api/v1/health"),
    "observability": ("MATRIX_BUS_OBSERVABILITY_URL", "/portal/api/v1/bus/observability"),
}


class UsageError(Exception):
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
    if value < 0 or (value == 0 and not allow_zero):
        raise UsageError(name + " must be positive")
    return value


def checked_url(name, raw):
    parts = urllib.parse.urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.hostname:
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


# No proxy handler: the request goes to the gateway URL and nowhere else.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get_json(url, timeout, surface):
    request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    try:
        with OPENER.open(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        raise NoAnswer(surface + " answered HTTP status " + str(error.code)) from error
    except (urllib.error.URLError, OSError, ValueError) as error:
        raise NoAnswer(surface + " did not answer (" + type(error).__name__ + ")") from error
    try:
        value = json.loads(body)
    except ValueError as error:
        raise NoAnswer(surface + " did not answer JSON") from error
    if not isinstance(value, dict):
        raise NoAnswer(surface + " did not answer a JSON object")
    return value


def judge(composition, expected, health, observability):
    status = observability.get("status")
    status = status if isinstance(status, dict) else {}
    reported = status.get("transport_class")
    if reported != expected:
        return EXIT_WRONG_TRANSPORT, "transport class is %s, the cell expects %s" % (reported or "absent", expected)
    if composition == "mux":
        readiness = health.get("readiness")
        proxy = readiness.get("proxy_readiness") if isinstance(readiness, dict) else None
        if proxy != PROXY_READY:
            return EXIT_PROXY_NOT_READY, "proxy listener is %s, the cell needs %s" % (proxy or "absent", PROXY_READY)
    startup = status.get("startup")
    phase = startup.get("phase") if isinstance(startup, dict) else None
    if phase != READY_PHASE:
        return EXIT_STARTUP_NOT_READY, "startup phase is %s, the cell needs %s" % (phase or "absent", READY_PHASE)
    detail = "transport class " + expected
    if composition == "mux":
        detail += ", proxy listener " + PROXY_READY
    return EXIT_READY, "ready: " + detail + ", startup phase " + READY_PHASE


def main():
    case_id = setting("MATRIX_CASE_ID") or "cell"
    try:
        composition = setting("MATRIX_COMPOSITION").lower()
        southbound = setting("MATRIX_SOUTHBOUND_TRANSPORT").lower()
        if not composition or not southbound:
            raise UsageError("MATRIX_COMPOSITION and MATRIX_SOUTHBOUND_TRANSPORT are required")
        expected = EXPECTED_TRANSPORT_CLASS.get((composition, southbound))
        if expected is None:
            raise UsageError("no executable cell runs composition %s on %s" % (composition, southbound))
        keys = ("health", "observability") if composition == "mux" else ("observability",)
        urls = surface_urls(keys)
        wait = seconds("MATRIX_ADDON_WAIT_SEC", 60.0, True)
        poll = seconds("MATRIX_ADDON_POLL_SEC", 2.0, False)
        timeout = seconds("MATRIX_ADDON_HTTP_TIMEOUT_SEC", 10.0, False)
    except UsageError as error:
        print("%s: %s: usage: %s" % (NAME, case_id, error), file=sys.stderr)
        return EXIT_USAGE

    deadline = time.monotonic() + wait
    while True:
        try:
            health = get_json(urls["health"], timeout, "health") if "health" in urls else {}
            observability = get_json(urls["observability"], timeout, "bus observability")
            code, message = judge(composition, expected, health, observability)
        except NoAnswer as error:
            code, message = EXIT_NO_ANSWER, "gateway: " + str(error)
        remaining = deadline - time.monotonic()
        if code == EXIT_READY or remaining <= 0:
            break
        time.sleep(min(poll, remaining))
    stream = sys.stdout if code == EXIT_READY else sys.stderr
    print("%s: %s: %s" % (NAME, case_id, message), file=stream)
    return code


sys.exit(main())
PY
