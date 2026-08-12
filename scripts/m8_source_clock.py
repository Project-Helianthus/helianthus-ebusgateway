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
import secrets
import socket
import stat
import sys
import time
from typing import Any


CONTRACT = "helianthus.platform.m8-source-campaign-clock.v1"
MAX_STATE_BYTES = 4096
MAX_BOOT_EPOCH_DRIFT_NS = 5_000_000_000
TOKEN_RE = re.compile(r"^[\x20-\x7e]{1,256}$")


class _StateStore:
    """Descriptor-anchored access to one private campaign clock file."""

    def __init__(self, path: pathlib.Path) -> None:
        if not path.is_absolute() or path != pathlib.Path(os.path.normpath(path)):
            raise ValueError("campaign clock state must be a clean absolute path")
        if path.name in {"", ".", ".."}:
            raise ValueError("invalid campaign clock state name")
        self.path = path
        self.parent = path.parent
        self.name = path.name
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        self.directory = os.open(self.parent, flags)
        info = os.fstat(self.directory)
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077:
            os.close(self.directory)
            raise ValueError("campaign clock parent is not private")
        self.parent_identity = (info.st_dev, info.st_ino)

    def __enter__(self) -> _StateStore:
        return self

    def __exit__(self, *_: object) -> None:
        os.close(self.directory)

    def _assert_parent_binding(self) -> None:
        info = os.stat(self.parent, follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != self.parent_identity:
            raise ValueError("campaign clock parent binding changed")

    @staticmethod
    def _validate_file(info: os.stat_result) -> None:
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_size < 1
            or info.st_size > MAX_STATE_BYTES
            or stat.S_IMODE(info.st_mode) & 0o077
            or info.st_nlink != 1
        ):
            raise ValueError("unsafe campaign clock state")

    @staticmethod
    def _content_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    @staticmethod
    def _published_identity(info: os.stat_result) -> tuple[int, int, int, int]:
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)

    def read(self, clock_id: str) -> dict[str, Any]:
        self._assert_parent_binding()
        before = os.stat(self.name, dir_fd=self.directory, follow_symlinks=False)
        self._validate_file(before)
        descriptor = os.open(
            self.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=self.directory,
        )
        try:
            opened = os.fstat(descriptor)
            self._validate_file(opened)
            if self._content_identity(before) != self._content_identity(opened):
                raise ValueError("campaign clock state binding changed")
            chunks: list[bytes] = []
            remaining = MAX_STATE_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            after = os.fstat(descriptor)
            if self._content_identity(opened) != self._content_identity(after):
                raise ValueError("campaign clock state changed while reading")
        finally:
            os.close(descriptor)
        if len(raw) > MAX_STATE_BYTES:
            raise ValueError("campaign clock state exceeds limit")
        current = os.stat(self.name, dir_fd=self.directory, follow_symlinks=False)
        if self._content_identity(current) != self._content_identity(opened):
            raise ValueError("campaign clock state binding changed")
        self._assert_parent_binding()
        return _validate_state(json.loads(raw), clock_id)

    def create(self, value: dict[str, Any]) -> None:
        self._assert_parent_binding()
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        temporary = f".{self.name}.tmp.{os.getpid()}.{secrets.token_hex(8)}"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=self.directory,
        )
        linked = False
        try:
            written = 0
            while written < len(raw):
                count = os.write(descriptor, raw[written:])
                if count <= 0:
                    raise OSError("short campaign clock write")
                written += count
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            opened = os.fstat(descriptor)
            os.link(
                temporary,
                self.name,
                src_dir_fd=self.directory,
                dst_dir_fd=self.directory,
                follow_symlinks=False,
            )
            linked = True
        finally:
            os.close(descriptor)
            try:
                os.unlink(temporary, dir_fd=self.directory)
            except FileNotFoundError:
                pass
        if not linked:
            return
        os.fsync(self.directory)
        current = os.stat(self.name, dir_fd=self.directory, follow_symlinks=False)
        self._validate_file(current)
        if self._published_identity(current) != self._published_identity(opened):
            raise ValueError("campaign clock state publication changed")
        self._assert_parent_binding()


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
    with _StateStore(path) as store:
        try:
            state = store.read(clock_id)
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
            store.create(state)
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
    with _StateStore(path) as store:
        state = store.read(clock_id)
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
