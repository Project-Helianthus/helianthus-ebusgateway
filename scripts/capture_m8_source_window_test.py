#!/usr/bin/env python3

from __future__ import annotations

import os
import pathlib
import stat
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("capture_m8_source_window.sh")


class CaptureM8SourceWindowTests(unittest.TestCase):
    @staticmethod
    def command(parent: pathlib.Path) -> list[str]:
        return [
            str(SCRIPT),
            "--host",
            "127.0.0.1",
            "--port",
            "1",
            "--phase",
            "PRE_RESTART",
            "--window-id",
            "test-window",
            "--clock-id",
            "test-clock",
            "--clock-state",
            str(parent / "clock.json"),
            "--output",
            str(parent / "pre"),
        ]

    def test_existing_non_private_parent_is_not_modified(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = pathlib.Path(raw) / "shared"
            parent.mkdir(mode=0o755)
            parent.chmod(0o755)

            result = subprocess.run(
                self.command(parent),
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("capture parent must be an existing private 0700 directory", result.stderr)
            self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o755)
            self.assertEqual(list(parent.iterdir()), [])

    def test_missing_parent_is_created_private(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = pathlib.Path(raw) / "private"

            result = subprocess.run(
                self.command(parent),
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(parent.is_dir())
            self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o700)

    def test_operator_socket_flag_reaches_remote_command(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            bindir = root / "bin"
            bindir.mkdir()
            record = root / "ssh-args"
            fake = bindir / "ssh"
            fake.write_text(f'#!/bin/sh\nprintf \'%s\\n\' "$@" >> "{record}"\nexit 1\n')
            fake.chmod(0o755)
            parent = root / "private"
            env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}
            custom = "/tmp/helianthus-eebus-op-1-abc/operator-mcp.sock"
            for extra, want in (([], "/data/eebus-operator/operator-mcp.sock"), (["--operator-socket", custom], custom)):
                p = root / ("p" + str(len(extra)))
                subprocess.run(self.command(p) + extra, check=False, capture_output=True, text=True, env=env)
                self.assertIn(f"--unix-socket {want} ", record.read_text().splitlines()[-1])

    def test_operator_socket_flag_rejects_unsafe_values(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = pathlib.Path(raw) / "private"
            for bad in ("relative.sock", "/a/b'; id; '", "/a/../b.sock"):
                result = subprocess.run(
                    self.command(parent) + ["--operator-socket", bad],
                    check=False, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
