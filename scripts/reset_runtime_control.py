#!/usr/bin/env python3
"""Offline runtime_control namespace-only reset to desired STOPPED.

This is the explicit repair of the G5 durable control namespace described in
the docs-ebus runtime driver lifecycle page ("Offline namespace-only reset").
It is an offline maintenance action:

* run it only with the gateway process stopped; it takes the exclusive store
  lock on "<state-file>.lock" (the same sidecar the gateway holds) and refuses
  while another process owns it;
* the default mode is a preview that writes nothing; replacement needs both
  --apply and --gateway-stopped;
* the namespace and the STOPPED value are fixed choices; there is no
  whole-file reset mode.

The tool validates the whole base (top-level schema version, meta and a valid
instance GUID) before touching anything and refuses on a corrupt or unreadable
file until a valid GUID-preserving base is restored externally. It reads the
bound H from the validated issued-revision mark and C from the validated
catalog revision, cross-checks the verified D5 recovery envelope against H,
preserves an exclusive, non-overwriting, fsynced quarantine copy of the exact
original bytes, and then atomically replaces only the runtime_control member:
one STOPPED record per configured driver at H+1..H+N in bytewise order, the
mark H+N, catalog revision C+1 and no active work. Every other top-level member
(GUID, meta, eBUS cache, unknown namespaces) and the D5 recovery envelope are
copied byte-for-byte from the original text. It never invents a GUID and never
decodes an unknown schema as empty.

Exit status: 0 success, 2 usage error, 3 FAILED_PRECONDITION refusal,
4 COUNTER_EXHAUSTED refusal. Every refusal replaces nothing.
"""

import argparse
import base64
import binascii
import datetime
import fcntl
import hashlib
import json
import os
import re
import sys

EXIT_OK = 0
EXIT_FAILED_PRECONDITION = 3
EXIT_COUNTER_EXHAUSTED = 4

U64_MAX = (1 << 64) - 1
NAMESPACE = "runtime_control"
DESIRED = "STOPPED"
RECOVERY_FORMAT = "helianthus.runtime-control.d5-recovery/v1"
MAX_DRIVER_RECORDS = 128
QUARANTINE_INFIX = ".runtime-control-quarantine-"

DRIVER_ID_RE = re.compile(r"[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*\Z")
GUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
REVISION_RE = re.compile(r"(?:0|[1-9][0-9]{0,19})\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
JSON_WS = " \t\n\r"
NAMESPACE_MEMBERS = {"schema_version", "issued_revision_mark", "catalog_revision", "drivers", "recovery"}


class Refusal(Exception):
    """A named reset refusal; nothing has been replaced."""

    def __init__(self, code, reason):
        super().__init__(reason)
        self.code = code
        self.reason = reason


def failed(reason):
    return Refusal("FAILED_PRECONDITION", reason)


def _utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def _reject_constant(name):
    raise ValueError("non-standard JSON constant %s" % name)


def _no_duplicates(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError("duplicate member %r" % key)
        out[key] = value
    return out


_DECODER = json.JSONDecoder(object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)


def _skip_ws(text, i):
    while i < len(text) and text[i] in JSON_WS:
        i += 1
    return i


def object_members(text):
    """Return [(key, raw_value_text, value)] of one JSON object, in order.

    The raw value text is the exact slice of the input, so members can be
    copied byte-for-byte. Duplicate members, non-standard constants and
    trailing data are rejected.
    """
    i = _skip_ws(text, 0)
    if i >= len(text) or text[i] != "{":
        raise ValueError("not a JSON object")
    i = _skip_ws(text, i + 1)
    members = []
    seen = set()
    if i < len(text) and text[i] == "}":
        i += 1
    else:
        while True:
            if i >= len(text) or text[i] != '"':
                raise ValueError("expected a member name")
            key, i = _DECODER.raw_decode(text, i)
            if key in seen:
                raise ValueError("duplicate member %r" % key)
            seen.add(key)
            i = _skip_ws(text, i)
            if i >= len(text) or text[i] != ":":
                raise ValueError("expected ':'")
            i = _skip_ws(text, i + 1)
            start = i
            value, i = _DECODER.raw_decode(text, i)
            members.append((key, text[start:i], value))
            i = _skip_ws(text, i)
            if i < len(text) and text[i] == ",":
                i = _skip_ws(text, i + 1)
                continue
            if i < len(text) and text[i] == "}":
                i += 1
                break
            raise ValueError("expected ',' or '}'")
    if _skip_ws(text, i) != len(text):
        raise ValueError("trailing data after the JSON object")
    return members


def parse_revision(value, allow_zero):
    if not isinstance(value, str) or not REVISION_RE.match(value):
        return None
    number = int(value)
    if number > U64_MAX or (number == 0 and not allow_zero):
        return None
    return number


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_driver_ids(driver_ids):
    if not driver_ids:
        raise failed("no configured driver id supplied; refusing to guess ids")
    if len(driver_ids) > MAX_DRIVER_RECORDS:
        raise failed("more than %d configured driver ids" % MAX_DRIVER_RECORDS)
    for driver_id in driver_ids:
        if len(driver_id) > 128 or not DRIVER_ID_RE.match(driver_id):
            raise failed("configured driver id %r is not canonical" % driver_id)
    if len(set(driver_ids)) != len(driver_ids):
        raise failed("configured driver ids contain duplicates")
    return sorted(driver_ids, key=lambda value: value.encode("ascii"))


def verify_recovery(recovery, bound):
    """Verify the typed D5 envelope; return the driver ids it references."""
    if not isinstance(recovery, dict) or set(recovery) != {
        "format", "driver_ids", "max_desired_revision", "payload_sha256", "payload"
    }:
        raise failed("D5 recovery envelope cannot be verified: members are not the closed set")
    if recovery["format"] != RECOVERY_FORMAT:
        raise failed("D5 recovery envelope cannot be verified: unsupported format")
    ids = recovery["driver_ids"]
    if not isinstance(ids, list) or not all(isinstance(v, str) and DRIVER_ID_RE.match(v) for v in ids):
        raise failed("D5 recovery envelope cannot be verified: driver_ids invalid")
    if ids != sorted(set(ids), key=lambda value: value.encode("ascii")):
        raise failed("D5 recovery envelope cannot be verified: driver_ids not sorted and unique")
    max_revision = parse_revision(recovery["max_desired_revision"], True)
    if max_revision is None:
        raise failed("D5 recovery envelope cannot be verified: max_desired_revision invalid")
    digest = recovery["payload_sha256"]
    if not isinstance(digest, str) or not SHA256_RE.match(digest):
        raise failed("D5 recovery envelope cannot be verified: payload_sha256 invalid")
    if not isinstance(recovery["payload"], str):
        raise failed("D5 recovery envelope cannot be verified: payload invalid")
    try:
        payload = base64.b64decode(recovery["payload"].encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError):
        raise failed("D5 recovery envelope cannot be verified: payload is not base64")
    if base64.b64encode(payload).decode("ascii") != recovery["payload"]:
        raise failed("D5 recovery envelope cannot be verified: payload is not canonical base64")
    if hashlib.sha256(payload).hexdigest() != digest:
        raise failed("D5 recovery envelope cannot be verified: payload digest mismatch")
    if max_revision > bound:
        raise failed("D5 recovery records a desired revision above the issued-revision mark")
    return ids


def plan_reset(data, driver_ids):
    """Validate the original bytes and build the replacement.

    Returns (plan, new_bytes). Raises Refusal without side effects.
    """
    try:
        text = data.decode("utf-8")
        members = object_members(text)
    except (UnicodeDecodeError, ValueError) as exc:
        raise failed("state file is corrupt (%s); restore a valid GUID-preserving base first" % exc)
    top = {key: value for key, _, value in members}
    raw_of = {key: raw for key, raw, _ in members}
    if not is_int(top.get("schema_version")) or top["schema_version"] != 1:
        raise failed("state file top-level schema_version is not 1; restore a valid base first")
    meta = top.get("meta")
    if not isinstance(meta, dict):
        raise failed("state file meta is missing or invalid; restore a valid base first")
    guid = meta.get("instance_guid")
    if not isinstance(guid, str) or not GUID_RE.match(guid):
        raise failed("state file has no valid instance GUID; this tool never invents one")
    if not isinstance(meta.get("written_at"), str):
        raise failed("state file meta.written_at is missing; restore a valid base first")
    if NAMESPACE not in raw_of:
        raise failed("runtime_control namespace is missing; the issued-revision mark cannot be validated")
    try:
        namespace_members = object_members(raw_of[NAMESPACE])
    except ValueError:
        raise failed("runtime_control is not a closed JSON object; the mark cannot be validated")
    namespace = {key: value for key, _, value in namespace_members}
    namespace_raw = {key: raw for key, raw, _ in namespace_members}
    version = namespace.get("schema_version")
    if not is_int(version) or version != 1:
        raise failed("runtime_control schema version is unsupported; never decoded as empty")
    unknown = sorted(set(namespace) - NAMESPACE_MEMBERS)
    if unknown:
        raise failed("runtime_control has unknown members %s; never discarded" % ", ".join(unknown))
    bound = parse_revision(namespace.get("issued_revision_mark"), True)
    if bound is None:
        raise failed("issued-revision mark cannot be validated")
    catalog = parse_revision(namespace.get("catalog_revision"), False)
    if catalog is None:
        raise failed("durable catalog revision cannot be validated")
    recovery_ids = []
    if "recovery" in namespace:
        recovery_ids = verify_recovery(namespace["recovery"], bound)
    old_records = namespace.get("drivers")
    sequences = {}
    if isinstance(old_records, dict):
        for driver_id, record in old_records.items():
            if not isinstance(record, dict):
                continue
            revision = parse_revision(record.get("desired_revision"), False)
            if revision is not None and revision > bound:
                raise failed("driver record %r is above the issued-revision mark" % driver_id)
            sequence = parse_revision(record.get("lifecycle_sequence"), True)
            if sequence is not None:
                sequences[driver_id] = sequence
    ordered = validate_driver_ids(driver_ids)
    count = len(ordered)
    if bound + count > U64_MAX:
        raise Refusal("COUNTER_EXHAUSTED", "H + N exceeds the unsigned 64-bit maximum")
    if catalog + 1 > U64_MAX:
        raise Refusal("COUNTER_EXHAUSTED", "C + 1 exceeds the unsigned 64-bit maximum")
    new_records = {}
    for index, driver_id in enumerate(ordered, start=1):
        new_records[driver_id] = {
            "desired_state": DESIRED,
            "desired_revision": str(bound + index),
            "lifecycle_sequence": str(sequences.get(driver_id, 0)),
            "current": None,
            "reserved": None,
        }
    parts = [
        '"schema_version": 1',
        '"issued_revision_mark": %s' % json.dumps(str(bound + count)),
        '"catalog_revision": %s' % json.dumps(str(catalog + 1)),
        '"drivers": %s' % json.dumps(new_records, sort_keys=True, separators=(",", ":")),
    ]
    if "recovery" in namespace_raw:
        parts.append('"recovery": %s' % namespace_raw["recovery"])
    new_namespace = "{\n    " + ",\n    ".join(parts) + "\n  }"
    body = []
    for key, raw, _ in members:
        value = new_namespace if key == NAMESPACE else raw
        body.append("  %s: %s" % (json.dumps(key), value))
    new_text = "{\n" + ",\n".join(body) + "\n}\n"
    removed = []
    if isinstance(old_records, dict):
        removed = sorted((k for k in old_records if k not in new_records), key=lambda v: v.encode("utf-8"))
    plan = {
        "namespace": NAMESPACE,
        "desired_state": DESIRED,
        "bound_mark": str(bound),
        "catalog_revision": str(catalog),
        "new_mark": str(bound + count),
        "new_catalog_revision": str(catalog + 1),
        "drivers": [
            {"driver_id": d, "desired_state": DESIRED, "desired_revision": new_records[d]["desired_revision"]}
            for d in ordered
        ],
        "removed_driver_ids": removed,
        "recovery_preserved": "recovery" in namespace_raw,
        "recovery_driver_ids": recovery_ids,
    }
    return plan, new_text.encode("utf-8")


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_quarantine(state_file, data):
    """Create an exclusive, non-overwriting quarantine copy and fsync it."""
    stamp = _utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = state_file + QUARANTINE_INFIX + stamp
    candidate = base
    for attempt in range(2, 1000):
        try:
            fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            candidate = "%s-%d" % (base, attempt)
            continue
        try:
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        fsync_dir(os.path.dirname(os.path.abspath(state_file)))
        return candidate
    raise OSError("no free quarantine name")


def replace_atomically(state_file, data, mode):
    """Same-directory temp file, file fsync, rename, directory fsync."""
    directory = os.path.dirname(os.path.abspath(state_file))
    temp = state_file + ".reset.tmp"
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temp, state_file)
    fsync_dir(directory)


def acquire_store_lock(state_file):
    try:
        handle = open(state_file + ".lock", "a+")
    except OSError as exc:
        raise failed("store lock cannot be opened: %s" % exc.strerror)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise failed("a gateway process or writer still owns the state file; stop it first")
    return handle


def run(args):
    if args.namespace != NAMESPACE:
        raise failed("only the runtime_control namespace can be reset")
    if args.desired != DESIRED:
        raise failed("only desired STOPPED is accepted")
    if args.apply and not args.gateway_stopped:
        raise failed("--apply requires --gateway-stopped (offline maintenance only)")
    lock = acquire_store_lock(args.state_file)
    try:
        try:
            with open(args.state_file, "rb") as handle:
                original = handle.read()
            mode = os.stat(args.state_file).st_mode & 0o777
        except OSError as exc:
            raise failed("state file is unreadable (%s); restore a valid GUID-preserving base first" % exc.strerror)
        plan, replacement = plan_reset(original, args.driver_id)
        plan["state_file"] = args.state_file
        if not args.apply:
            plan["mode"] = "preview"
            plan["quarantine_copy"] = None
            return plan
        try:
            quarantine = write_quarantine(args.state_file, original)
        except OSError as exc:
            raise failed("quarantine copy could not be preserved (%s); nothing replaced" % exc)
        try:
            replace_atomically(args.state_file, replacement, mode or 0o600)
        except OSError as exc:
            raise failed("replacement failed (%s); original preserved, quarantine copy %s kept" % (exc, quarantine))
        with open(args.state_file, "rb") as handle:
            if handle.read() != replacement:
                raise failed("readback after replacement disagrees; inspect %s" % quarantine)
        plan["mode"] = "apply"
        plan["quarantine_copy"] = quarantine
        return plan
    finally:
        lock.close()


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state-file", required=True, help="runtime-state file of the stopped gateway")
    parser.add_argument("--namespace", required=True, help="must be runtime_control")
    parser.add_argument("--desired", required=True, help="must be STOPPED")
    parser.add_argument(
        "--driver-id",
        action="append",
        default=[],
        help="configured driver id from the installation's configuration (repeat for each)",
    )
    parser.add_argument("--apply", action="store_true", help="replace the namespace (default: preview only)")
    parser.add_argument(
        "--gateway-stopped",
        action="store_true",
        help="operator assertion that the gateway process is stopped (required with --apply)",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        plan = run(args)
    except Refusal as refusal:
        sys.stderr.write("REFUSED %s: %s\n" % (refusal.code, refusal.reason))
        if refusal.code == "COUNTER_EXHAUSTED":
            return EXIT_COUNTER_EXHAUSTED
        return EXIT_FAILED_PRECONDITION
    sys.stdout.write(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
