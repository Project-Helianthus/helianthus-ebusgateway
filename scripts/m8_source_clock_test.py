#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("m8_source_clock.py")
SPEC = importlib.util.spec_from_file_location("m8_source_clock", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CLOCK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLOCK)


class M8SourceClockTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
