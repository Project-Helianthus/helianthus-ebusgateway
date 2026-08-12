#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import contextlib
import io
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("m8_source_clock.py")
SPEC = importlib.util.spec_from_file_location("m8_source_clock", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CLOCK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLOCK)


class M8SourceClockTests(unittest.TestCase):
    def run_cli(self, state: pathlib.Path, phase: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "start",
                "--state",
                str(state),
                "--clock-id",
                "campaign-clock",
                "--phase",
                phase,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def run_main(self, state: pathlib.Path, phase: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        argv = [
            str(SCRIPT),
            "start",
            "--state",
            str(state),
            "--clock-id",
            "campaign-clock",
            "--phase",
            phase,
        ]
        with (
            mock.patch.object(sys, "argv", argv),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            result = CLOCK.main()
        return result, stdout.getvalue(), stderr.getvalue()

    def test_pre_and_post_share_one_exact_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            state = pathlib.Path(raw) / "clock.json"
            pre = CLOCK.start_capture(
                state,
                "PRE_RESTART",
                "campaign-clock",
                wall_ns=2_000_000_000_000,
                monotonic_ns=1_000_000_000_000,
                hostname="test-host",
            )
            post = CLOCK.start_capture(
                state,
                "POST_RESTART",
                "campaign-clock",
                wall_ns=2_005_000_000_000,
                monotonic_ns=1_005_000_000_000,
                hostname="test-host",
            )
            self.assertEqual(pre["capture_start_offset_ns"], 0)
            self.assertEqual(post["capture_start_offset_ns"], 5_000_000_000)
            self.assertEqual(pre["wall_anchor_utc"], post["wall_anchor_utc"])
            self.assertEqual(pre["monotonic_epoch_id"], post["monotonic_epoch_id"])
            self.assertEqual(
                post["captured_at"], CLOCK.format_ns(2_000_000_000_000 + 5_000_000_000)
            )
            self.assertEqual(
                CLOCK.end_capture(
                    state,
                    "campaign-clock",
                    wall_ns=2_006_000_000_000,
                    monotonic_ns=1_006_000_000_000,
                ),
                6_000_000_000,
            )

            with self.assertRaises(ValueError):
                CLOCK.start_capture(
                    state,
                    "PRE_RESTART",
                    "campaign-clock",
                    wall_ns=2_007_000_000_000,
                    monotonic_ns=1_007_000_000_000,
                    hostname="test-host",
                )

    def test_post_without_pre_and_changed_epoch_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            state = pathlib.Path(raw) / "clock.json"
            with self.assertRaises(ValueError):
                CLOCK.start_capture(
                    state,
                    "POST_RESTART",
                    "campaign-clock",
                    wall_ns=2_000_000_000_000,
                    monotonic_ns=1_000_000_000_000,
                    hostname="test-host",
                )
            CLOCK.start_capture(
                state,
                "PRE_RESTART",
                "campaign-clock",
                wall_ns=2_000_000_000_000,
                monotonic_ns=1_000_000_000_000,
                hostname="test-host",
            )
            with self.assertRaises(ValueError):
                CLOCK.start_capture(
                    state,
                    "POST_RESTART",
                    "campaign-clock",
                    wall_ns=2_020_000_000_001,
                    monotonic_ns=1_005_000_000_000,
                    hostname="test-host",
                )

    def test_post_rejects_substituted_state_without_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            state = pathlib.Path(raw) / "clock.json"
            CLOCK.start_capture(
                state,
                "PRE_RESTART",
                "campaign-clock",
                wall_ns=2_000_000_000_000,
                monotonic_ns=1_000_000_000_000,
                hostname="test-host",
            )
            original = pathlib.Path(raw) / "original.json"
            state.rename(original)
            state.symlink_to(original)

            result = self.run_cli(state, "POST_RESTART")

            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertIn("m8_source_clock:", result.stderr)

    def test_post_rejects_substituted_parent_without_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            parent = root / "private"
            parent.mkdir(mode=0o700)
            state = parent / "clock.json"
            CLOCK.start_capture(
                state,
                "PRE_RESTART",
                "campaign-clock",
                wall_ns=2_000_000_000_000,
                monotonic_ns=1_000_000_000_000,
                hostname="test-host",
            )
            original_parent = root / "private-original"
            parent.rename(original_parent)
            attacker_parent = root / "private-attacker"
            attacker_parent.mkdir(mode=0o700)
            os.symlink(attacker_parent, parent)

            result = self.run_cli(state, "POST_RESTART")

            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertIn("m8_source_clock:", result.stderr)

    def test_post_rejects_file_swap_between_stat_and_open(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            state = pathlib.Path(raw) / "clock.json"
            CLOCK.start_capture(
                state,
                "PRE_RESTART",
                "campaign-clock",
                wall_ns=2_000_000_000_000,
                monotonic_ns=1_000_000_000_000,
                hostname="test-host",
            )
            original = pathlib.Path(raw) / "original.json"
            real_open = CLOCK.os.open
            swapped = False

            def racing_open(path: object, *args: object, **kwargs: object) -> int:
                nonlocal swapped
                if path == state.name and kwargs.get("dir_fd") is not None and not swapped:
                    state.rename(original)
                    state.symlink_to(original)
                    swapped = True
                return real_open(path, *args, **kwargs)

            with mock.patch.object(CLOCK.os, "open", side_effect=racing_open):
                result, stdout, stderr = self.run_main(state, "POST_RESTART")

            self.assertTrue(swapped)
            self.assertNotEqual(result, 0)
            self.assertEqual(stdout, "")
            self.assertIn("m8_source_clock:", stderr)

    def test_post_rejects_parent_swap_after_directory_open(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            parent = root / "private"
            parent.mkdir(mode=0o700)
            state = parent / "clock.json"
            CLOCK.start_capture(
                state,
                "PRE_RESTART",
                "campaign-clock",
                wall_ns=2_000_000_000_000,
                monotonic_ns=1_000_000_000_000,
                hostname="test-host",
            )
            original_parent = root / "private-original"
            attacker_parent = root / "private-attacker"
            attacker_parent.mkdir(mode=0o700)
            real_open = CLOCK.os.open
            swapped = False

            def racing_open(path: object, *args: object, **kwargs: object) -> int:
                nonlocal swapped
                descriptor = real_open(path, *args, **kwargs)
                if path == parent and not swapped:
                    parent.rename(original_parent)
                    os.symlink(attacker_parent, parent)
                    swapped = True
                return descriptor

            try:
                with mock.patch.object(CLOCK.os, "open", side_effect=racing_open):
                    result, stdout, stderr = self.run_main(state, "POST_RESTART")
            finally:
                if parent.is_symlink():
                    parent.unlink()
                if original_parent.exists():
                    original_parent.rename(parent)

            self.assertTrue(swapped)
            self.assertNotEqual(result, 0)
            self.assertEqual(stdout, "")
            self.assertIn("m8_source_clock:", stderr)


if __name__ == "__main__":
    unittest.main()
