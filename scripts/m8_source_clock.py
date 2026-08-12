#!/usr/bin/env python3
"""Maintain one private monotonic clock shared by M8 PRE/POST captures."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import socket
import stat
import sys
import time
from typing import Any


CONTRACT = "helianthus.platform.m8-source-campaign-clock.v1"
MAX_STATE_BYTES = 4096
MAX_BOOT_EPOCH_DRIFT_NS = 5_000_000_000
TOKEN_RE = re.compile(r"^[\x20-\x7e]{1,256}$")


def format_ns(unix_ns: int) -> str:
    seconds, nanoseconds = divmod(unix_ns, 1_000_000_000)
    instant = dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)
    rendered = instant.strftime("%Y-%m-%dT%H:%M:%S")
    if nanoseconds:
        rendered += "." + f"{nanoseconds:09d}".rstrip("0")
    return rendered + "Z"


def _validate_state(value: Any, clock_id: str) -> dict[str, Any]:
    required = {
        "boot_epoch_ns",
        "clock_id",
        "contract",
        "monotonic_epoch_id",
        "monotonic_origin_ns",
        "wall_anchor_unix_ns",
        "wall_anchor_utc",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("invalid campaign clock shape")
    if (
        value["contract"] != CONTRACT
        or value["clock_id"] != clock_id
        or not TOKEN_RE.fullmatch(value["clock_id"])
        or not TOKEN_RE.fullmatch(value["monotonic_epoch_id"])
        or any(
            not isinstance(value[key], int) or isinstance(value[key], bool)
            for key in ("boot_epoch_ns", "monotonic_origin_ns", "wall_anchor_unix_ns")
        )
        or value["monotonic_origin_ns"] < 0
        or value["wall_anchor_utc"] != format_ns(value["wall_anchor_unix_ns"])
    ):
        raise ValueError("invalid campaign clock binding")
    return value


def _read_state(path: pathlib.Path, clock_id: str) -> dict[str, Any]:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_size < 1 or info.st_size > MAX_STATE_BYTES:
        raise ValueError("unsafe campaign clock state")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("campaign clock state is not private")
    with path.open("rb") as handle:
        raw = handle.read(MAX_STATE_BYTES + 1)
    if len(raw) > MAX_STATE_BYTES:
        raise ValueError("campaign clock state exceeds limit")
    return _validate_state(json.loads(raw), clock_id)


def _write_new_state(path: pathlib.Path, value: dict[str, Any]) -> None:
    parent = path.parent
    parent_info = parent.lstat()
    if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
        raise ValueError("unsafe campaign clock parent")
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    complete = False
    try:
        if os.write(descriptor, raw) != len(raw):
            raise OSError("short campaign clock write")
        os.fsync(descriptor)
        complete = True
    finally:
        os.close(descriptor)
        if not complete:
            path.unlink(missing_ok=True)
    directory = os.open(parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def start_capture(
    path: pathlib.Path,
    phase: str,
    clock_id: str,
    *,
    wall_ns: int,
    monotonic_ns: int,
    hostname: str,
) -> dict[str, Any]:
    if phase not in {"PRE_RESTART", "POST_RESTART"} or not TOKEN_RE.fullmatch(clock_id):
        raise ValueError("invalid campaign clock request")
    try:
        state = _read_state(path, clock_id)
    except FileNotFoundError:
        if phase != "PRE_RESTART":
            raise ValueError("POST_RESTART requires the PRE campaign clock") from None
        boot_epoch_ns = wall_ns - monotonic_ns
        epoch_source = f"{hostname}\0{boot_epoch_ns}".encode()
        state = {
            "boot_epoch_ns": boot_epoch_ns,
            "clock_id": clock_id,
            "contract": CONTRACT,
            "monotonic_epoch_id": "monotonic-" + hashlib.sha256(epoch_source).hexdigest()[:32],
            "monotonic_origin_ns": monotonic_ns,
            "wall_anchor_unix_ns": wall_ns,
            "wall_anchor_utc": format_ns(wall_ns),
        }
        _write_new_state(path, state)
    else:
        if phase == "PRE_RESTART":
            raise ValueError("PRE_RESTART requires a new campaign clock")

    current_boot_epoch = wall_ns - monotonic_ns
    if abs(current_boot_epoch - state["boot_epoch_ns"]) > MAX_BOOT_EPOCH_DRIFT_NS:
        raise ValueError("campaign clock host epoch changed")
    offset = monotonic_ns - state["monotonic_origin_ns"]
    if offset < 0:
        raise ValueError("campaign monotonic clock moved backwards")
    return {
        "capture_start_offset_ns": offset,
        "captured_at": format_ns(state["wall_anchor_unix_ns"] + offset),
        "clock_id": state["clock_id"],
        "monotonic_epoch_id": state["monotonic_epoch_id"],
        "wall_anchor_utc": state["wall_anchor_utc"],
    }


def end_capture(path: pathlib.Path, clock_id: str, *, wall_ns: int, monotonic_ns: int) -> int:
    state = _read_state(path, clock_id)
    if abs((wall_ns - monotonic_ns) - state["boot_epoch_ns"]) > MAX_BOOT_EPOCH_DRIFT_NS:
        raise ValueError("campaign clock host epoch changed")
    offset = monotonic_ns - state["monotonic_origin_ns"]
    if offset < 0:
        raise ValueError("campaign monotonic clock moved backwards")
    return offset


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "end"))
    parser.add_argument("--state", required=True, type=pathlib.Path)
    parser.add_argument("--clock-id", required=True)
    parser.add_argument("--phase", choices=("PRE_RESTART", "POST_RESTART"))
    args = parser.parse_args()
    if not args.state.is_absolute() or args.state != pathlib.Path(os.path.normpath(args.state)):
        parser.error("--state must be a clean absolute path")
    try:
        if args.action == "start":
            if args.phase is None:
                parser.error("start requires --phase")
            value = start_capture(
                args.state,
                args.phase,
                args.clock_id,
                wall_ns=time.time_ns(),
                monotonic_ns=time.monotonic_ns(),
                hostname=socket.gethostname(),
            )
            print(json.dumps(value, sort_keys=True, separators=(",", ":")))
        else:
            if args.phase is not None:
                parser.error("end does not accept --phase")
            print(end_capture(args.state, args.clock_id, wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns()))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"m8_source_clock: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
