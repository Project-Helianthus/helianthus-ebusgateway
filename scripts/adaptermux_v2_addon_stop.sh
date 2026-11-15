#!/bin/sh
# Verify-only stop command of an adaptermux-v2 cell on the add-on target,
# passed to matrix-runner as --stop-gateway and --stop-ebusd.
#
# The supervisor owns the gateway and the external client; this command stops
# nothing. It records the gateway's counters at the end of the cell in
# gateway_counters_end.json in MATRIX_CASE_DIR: the Prometheus counter
# samples, the numeric expvar counters and the bus observability counters.
# A surface that does not answer is recorded as not read.
#
# It reads only MATRIX_* variables, sends only HTTP GET requests without any
# credential, writes only inside MATRIX_CASE_DIR, and records no URL, no
# network address and no time.
#
# Exit statuses:
#   0  the record is written, whatever the gateway answered
#   2  usage error: MATRIX_CASE_DIR is missing or is not a directory
set -eu

if ! command -v python3 >/dev/null 2>&1; then
  echo "addon-stop: python3 is required" >&2
  exit 2
fi

exec python3 -I - <<'PY'
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

NAME = "addon-stop"
EXIT_RECORDED = 0
EXIT_USAGE = 2
RECORD_FILE = "gateway_counters_end.json"
RECORD_SCHEMA = "adaptermux-v2-addon-counters/1"
# The runner runs this command for both stop commands inside one shared
# cleanup budget, so the three reads run together under one total deadline.
TOTAL_DEADLINE_SEC = 10.0

SURFACES = {
    "metrics": ("MATRIX_METRICS_URL", "/metrics"),
    "vars": ("MATRIX_DEBUG_VARS_URL", "/debug/vars"),
    "observability": ("MATRIX_BUS_OBSERVABILITY_URL", "/portal/api/v1/bus/observability"),
}
# expvar entries that are process data, not counters; the command line may
# carry settings, so it is never recorded.
EXPVAR_SKIPPED = ("cmdline", "memstats")
PROM_SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")
PROM_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


class UsageError(Exception):
    pass


class NoAnswer(Exception):
    pass


def setting(name):
    return (os.environ.get(name) or "").strip()


def timeout_setting():
    raw = setting("MATRIX_ADDON_HTTP_TIMEOUT_SEC")
    if not raw:
        return 10.0
    try:
        value = float(raw)
    except ValueError as error:
        raise UsageError("MATRIX_ADDON_HTTP_TIMEOUT_SEC is not a number") from error
    if value <= 0:
        raise UsageError("MATRIX_ADDON_HTTP_TIMEOUT_SEC must be positive")
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


def surface_url(key):
    override, path = SURFACES[key]
    if setting(override):
        return checked_url(override, setting(override))
    base = setting("MATRIX_GATEWAY_BASE_URL").rstrip("/")
    if not base:
        raise UsageError("MATRIX_GATEWAY_BASE_URL is required")
    return checked_url("MATRIX_GATEWAY_BASE_URL", base) + path


# No proxy handler: the request goes to the gateway URL and nowhere else.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get_text(url, timeout, surface):
    request = urllib.request.Request(url, method="GET")
    try:
        with OPENER.open(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        raise NoAnswer(surface + " answered HTTP status " + str(error.code)) from error
    except (urllib.error.URLError, OSError, ValueError) as error:
        raise NoAnswer(surface + " did not answer (" + type(error).__name__ + ")") from error


def get_object(url, timeout, surface):
    try:
        value = json.loads(get_text(url, timeout, surface))
    except ValueError as error:
        raise NoAnswer(surface + " did not answer JSON") from error
    if not isinstance(value, dict):
        raise NoAnswer(surface + " did not answer a JSON object")
    return value


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def prometheus_counters(text):
    samples = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = PROM_SAMPLE.match(line)
        if not match or not match.group(1).endswith("_total"):
            continue
        try:
            value = float(match.group(3))
        except ValueError:
            continue
        labels = dict(PROM_LABEL.findall(match.group(2) or ""))
        samples.append({"name": match.group(1), "labels": labels, "value": value})
    samples.sort(key=lambda sample: (sample["name"], sorted(sample["labels"].items())))
    return samples


def expvar_counters(variables):
    counters = {}
    for key in sorted(variables):
        if key in EXPVAR_SKIPPED:
            continue
        value = variables[key]
        if is_number(value):
            counters[key] = value
        elif isinstance(value, dict) and value and all(is_number(item) for item in value.values()):
            counters[key] = {name: value[name] for name in sorted(value)}
    return counters


def observability_counters(summary):
    counters = summary.get("counters")
    frames = {}
    for entry in summary.get("frames") or []:
        if not isinstance(entry, dict) or not is_number(entry.get("count")):
            continue
        key = "%s/%s" % (entry.get("scope") or "unknown", entry.get("frame_type") or "unknown")
        frames[key] = frames.get(key, 0) + entry["count"]
    return {
        "counters": counters if isinstance(counters, dict) else {},
        "frame_counts": {key: frames[key] for key in sorted(frames)},
    }


READERS = {
    "metrics": lambda url, timeout: prometheus_counters(get_text(url, timeout, "metrics")),
    "vars": lambda url, timeout: expvar_counters(get_object(url, timeout, "expvar")),
    "observability": lambda url, timeout: observability_counters(get_object(url, timeout, "bus observability")),
}


def main():
    case_id = setting("MATRIX_CASE_ID") or "cell"
    case_dir = setting("MATRIX_CASE_DIR")
    if not case_dir or not os.path.isdir(case_dir):
        print("%s: %s: usage: MATRIX_CASE_DIR must name an existing directory" % (NAME, case_id), file=sys.stderr)
        return EXIT_USAGE

    record = {"schema": RECORD_SCHEMA, "case_id": setting("MATRIX_CASE_ID"), "surfaces": {}}
    try:
        timeout = timeout_setting()
        urls = {key: surface_url(key) for key in SURFACES}
    except UsageError as error:
        urls = {}
        for key in SURFACES:
            record["surfaces"][key] = {"read": False, "reason": "usage: " + str(error)}
    deadline = min(timeout, TOTAL_DEADLINE_SEC) if urls else 0.0
    outcomes = {}

    def read(key):
        try:
            outcomes[key] = {"read": True, "counters": READERS[key](urls[key], deadline)}
        except NoAnswer as error:
            outcomes[key] = {"read": False, "reason": str(error)}

    threads = [threading.Thread(target=read, args=(key,), daemon=True) for key in sorted(urls)]
    for thread in threads:
        thread.start()
    end = time.monotonic() + deadline
    for thread in threads:
        thread.join(max(0.0, end - time.monotonic()))
    for key in sorted(urls):
        record["surfaces"][key] = dict(outcomes.get(key) or {"read": False, "reason": key + " did not answer in time"})

    path = os.path.join(case_dir, RECORD_FILE)
    partial = path + ".partial"
    with open(partial, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(partial, path)
    read = sorted(key for key, value in record["surfaces"].items() if value["read"])
    print("%s: %s: stopped nothing; counters recorded from %s" % (NAME, case_id, ", ".join(read) or "no surface"))
    return EXIT_RECORDED


sys.exit(main())
PY
