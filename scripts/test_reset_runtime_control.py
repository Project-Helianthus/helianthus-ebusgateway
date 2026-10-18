"""Tests for scripts/reset_runtime_control.py using temporary synthetic files."""

import base64
import contextlib
import datetime
import fcntl
import glob
import hashlib
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import reset_runtime_control as reset

GUID = "00000000-0000-4000-8000-0000000000b2"
PAYLOAD = b"synthetic-d5\x00\x01\xfe"
U64_MAX = str((1 << 64) - 1)


def recovery_text(max_revision="3", payload=PAYLOAD, digest=None):
    digest = digest or hashlib.sha256(payload).hexdigest()
    return (
        '{"format": "%s", "driver_ids": ["ebus.primary"], "max_desired_revision": "%s", '
        '"payload_sha256": "%s",   "payload": "%s"}'
        % (reset.RECOVERY_FORMAT, max_revision, digest, base64.b64encode(payload).decode("ascii"))
    )


def control_text(mark="3", catalog="4", drivers=None, recovery=None, extra=""):
    if drivers is None:
        drivers = (
            '{"ebus.primary": {"desired_state": "RUNNING", "desired_revision": "3", "lifecycle_sequence": "2", '
            '"current": {"kind": "START", "desired_revision": "3", "sequence": "2", "phase": "ACTIVE", "epoch": "", '
            '"generation": "0", "native_close_pending": false, "semantic_cleanup_pending": false, "attempts_used": 1, '
            '"deadline_utc": ""}, "reserved": null}, '
            '"fam.removed": {"desired_state": "STOPPED", "desired_revision": "1", "lifecycle_sequence": "0", '
            '"current": null, "reserved": null}}'
        )
    text = '{"schema_version": 1, "issued_revision_mark": "%s", "catalog_revision": "%s", "drivers": %s' % (
        mark,
        catalog,
        drivers,
    )
    if recovery is not None:
        text += ', "recovery": ' + recovery
    return text + extra + "}"


def state_text(control=None, guid=GUID, top_version="1"):
    parts = [
        '"zeta_plugin" :  {"exact": 1.50, "order": [3, 1, 2]}',
        '"schema_version": ' + top_version,
        '"meta": {"instance_guid": "%s", "written_at": "2001-02-03T04:05:06Z", "addon_version": "0.0.0-rt"}' % guid,
        '"ebus": {"schema_version": 1, "known_bus_members": [{"addr": 8, "companion_addr": null, '
        '"identity": null, "last_seen_at": "2001-02-03T04:05:06Z", "last_source": "passive_observed", '
        '"confidence": "corroborated"}]}',
    ]
    if control is not None:
        parts.append('"runtime_control": ' + control)
    return "{" + ",\n ".join(parts) + "}\n"


class ResetRuntimeControlTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = self._dir.name
        self.state = os.path.join(self.dir, "runtime_state.json")

    def tearDown(self):
        self._dir.cleanup()

    def write_state(self, text):
        data = text.encode("utf-8") if isinstance(text, str) else text
        with open(self.state, "wb") as handle:
            handle.write(data)
        return data

    def read_state(self):
        with open(self.state, "rb") as handle:
            return handle.read()

    def run_main(self, *extra, ids=("ebus.primary", "alpha.new")):
        argv = ["--state-file", self.state, "--namespace", "runtime_control", "--desired", "STOPPED"]
        for driver_id in ids:
            argv += ["--driver-id", driver_id]
        argv += list(extra)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = reset.main(argv)
        return code, out.getvalue(), err.getvalue()

    def quarantine_copies(self):
        return sorted(glob.glob(self.state + reset.QUARANTINE_INFIX + "*"))

    def assert_refused(self, expected_code, *extra, ids=("ebus.primary", "alpha.new"), reason=None):
        before = self.read_state() if os.path.isfile(self.state) else None
        code, out, err = self.run_main(*extra, ids=ids)
        self.assertEqual(code, expected_code, err)
        self.assertEqual(out, "")
        name = "COUNTER_EXHAUSTED" if expected_code == reset.EXIT_COUNTER_EXHAUSTED else "FAILED_PRECONDITION"
        self.assertTrue(err.startswith("REFUSED %s: " % name), err)
        if reason:
            self.assertIn(reason, err)
        if before is not None:
            self.assertEqual(self.read_state(), before)
        self.assertEqual(self.quarantine_copies(), [])
        return err

    # --- preview / apply ---

    def test_preview_is_the_default_and_writes_nothing(self):
        original = self.write_state(state_text(control_text(recovery=recovery_text())))
        code, out, err = self.run_main()
        self.assertEqual(code, 0, err)
        plan = json.loads(out)
        self.assertEqual(plan["mode"], "preview")
        self.assertEqual(plan["bound_mark"], "3")
        self.assertEqual(plan["new_mark"], "5")
        self.assertEqual(plan["new_catalog_revision"], "5")
        self.assertEqual(
            [(d["driver_id"], d["desired_revision"]) for d in plan["drivers"]],
            [("alpha.new", "4"), ("ebus.primary", "5")],
        )
        self.assertEqual(plan["removed_driver_ids"], ["fam.removed"])
        self.assertTrue(plan["recovery_preserved"])
        self.assertEqual(self.read_state(), original)
        self.assertEqual(self.quarantine_copies(), [])
        self.assertFalse(os.path.exists(self.state + ".reset.tmp"))

    def test_apply_replaces_only_the_namespace_and_preserves_everything_else(self):
        recovery = recovery_text()
        original = self.write_state(state_text(control_text(recovery=recovery)))
        code, out, err = self.run_main("--apply", "--gateway-stopped")
        self.assertEqual(code, 0, err)
        plan = json.loads(out)
        copies = self.quarantine_copies()
        self.assertEqual(copies, [plan["quarantine_copy"]])
        with open(copies[0], "rb") as handle:
            self.assertEqual(handle.read(), original)
        new_text = self.read_state().decode("utf-8")
        # Unrelated members are copied byte-for-byte, including formatting.
        self.assertIn('"zeta_plugin": {"exact": 1.50, "order": [3, 1, 2]}', new_text)
        self.assertIn('"instance_guid": "%s"' % GUID, new_text)
        self.assertIn('"addon_version": "0.0.0-rt"', new_text)
        self.assertIn('"last_source": "passive_observed"', new_text)
        # The D5 envelope is byte-identical.
        self.assertIn('"recovery": ' + recovery, new_text)
        doc = json.loads(new_text)
        control = doc["runtime_control"]
        self.assertEqual(control["schema_version"], 1)
        self.assertEqual(control["issued_revision_mark"], "5")
        self.assertEqual(control["catalog_revision"], "5")
        self.assertEqual(
            control["drivers"],
            {
                "alpha.new": {"desired_state": "STOPPED", "desired_revision": "4", "lifecycle_sequence": "0",
                              "current": None, "reserved": None},
                "ebus.primary": {"desired_state": "STOPPED", "desired_revision": "5", "lifecycle_sequence": "2",
                                 "current": None, "reserved": None},
            },
        )
        self.assertEqual(list(doc), ["zeta_plugin", "schema_version", "meta", "ebus", "runtime_control"])
        self.assertFalse(os.path.exists(self.state + ".reset.tmp"))

    def test_malformed_records_within_a_valid_namespace_are_reset(self):
        self.write_state(state_text(control_text(drivers='{"ebus.primary": {"desired_state": 7}, "BAD ID": []}')))
        code, out, err = self.run_main("--apply", "--gateway-stopped", ids=("ebus.primary",))
        self.assertEqual(code, 0, err)
        control = json.loads(self.read_state())["runtime_control"]
        self.assertEqual(control["drivers"]["ebus.primary"]["desired_revision"], "4")
        self.assertEqual(control["issued_revision_mark"], "4")

    def test_apply_requires_the_stopped_gateway_assertion(self):
        self.write_state(state_text(control_text()))
        self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", reason="--gateway-stopped")

    def test_refuses_while_another_writer_holds_the_store_lock(self):
        self.write_state(state_text(control_text()))
        with open(self.state + ".lock", "a+") as holder:
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", "--gateway-stopped", reason="still owns")

    # --- refusals: whole-file faults and unvalidated bounds ---

    def test_whole_file_faults_refuse_until_a_valid_base_is_restored(self):
        cases = {
            "corrupt": b'{"schema_version": 1, "meta": \x00',
            "not_utf8": b"\xff\xfe{}",
            "duplicate_top_level": state_text(control_text()).replace('"schema_version": 1,', '"schema_version": 1, "schema_version": 1,', 1),
            "top_level_version": state_text(control_text(), top_version="2"),
            "top_level_version_bool": state_text(control_text(), top_version="true"),
            "missing_guid": state_text(control_text(), guid=""),
            "uppercase_guid": state_text(control_text(), guid=GUID.upper()),
            "nan_constant": state_text(control_text()).replace("1.50", "NaN"),
            "trailing_value": state_text(control_text()) + "{}",
        }
        for name, text in cases.items():
            with self.subTest(name):
                self.write_state(text)
                self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", "--gateway-stopped")

    def test_missing_or_unreadable_file_refuses(self):
        self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", "--gateway-stopped", reason="unreadable")
        os.mkdir(self.state)
        self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", "--gateway-stopped", reason="unreadable")

    def test_unvalidated_namespace_bounds_refuse(self):
        cases = {
            "namespace_missing": state_text(None),
            "unsupported_version": state_text('{"schema_version": 2, "issued_revision_mark": "3"}'),
            "version_as_string": state_text(control_text().replace('"schema_version": 1', '"schema_version": "1"', 1)),
            "not_an_object": state_text("[]"),
            "unknown_member": state_text(control_text(extra=', "future": {}')),
            "mark_leading_zero": state_text(control_text(mark="03")),
            "mark_missing": state_text('{"schema_version": 1, "catalog_revision": "4", "drivers": {}}'),
            "catalog_zero": state_text(control_text(catalog="0")),
            "catalog_not_string": state_text(control_text().replace('"catalog_revision": "4"', '"catalog_revision": 4')),
            "record_above_mark": state_text(control_text(mark="2")),
            "d5_above_mark": state_text(control_text(recovery=recovery_text(max_revision="9"))),
            "d5_digest_mismatch": state_text(control_text(recovery=recovery_text(digest="0" * 64))),
            "d5_unknown_format": state_text(control_text(recovery=recovery_text().replace(reset.RECOVERY_FORMAT, "other/v9"))),
            "d5_not_base64": state_text(control_text(recovery=recovery_text().replace('"payload": "', '"payload": "!'))),
        }
        for name, text in cases.items():
            with self.subTest(name):
                self.write_state(text)
                self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", "--gateway-stopped")

    def test_configuration_must_validate(self):
        self.write_state(state_text(control_text()))
        for ids in ((), ("Not.Canonical",), ("ebus.primary", "ebus.primary")):
            with self.subTest(ids=ids):
                self.assert_refused(reset.EXIT_FAILED_PRECONDITION, "--apply", "--gateway-stopped", ids=ids)
        for extra in (["--namespace", "ebus"], ["--desired", "RUNNING"]):
            with self.subTest(extra=extra):
                argv = ["--state-file", self.state, "--driver-id", "ebus.primary", "--apply", "--gateway-stopped"]
                argv += ["--namespace", extra[1] if extra[0] == "--namespace" else "runtime_control"]
                argv += ["--desired", extra[1] if extra[0] == "--desired" else "STOPPED"]
                err = io.StringIO()
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
                    self.assertEqual(reset.main(argv), reset.EXIT_FAILED_PRECONDITION)
                self.assertEqual(self.quarantine_copies(), [])

    def test_counter_overflow_refuses_with_counter_exhausted(self):
        self.write_state(state_text(control_text(mark=U64_MAX, drivers="{}")))
        self.assert_refused(reset.EXIT_COUNTER_EXHAUSTED, "--apply", "--gateway-stopped", ids=("ebus.primary",))
        self.write_state(state_text(control_text(catalog=U64_MAX)))
        self.assert_refused(reset.EXIT_COUNTER_EXHAUSTED, "--apply", "--gateway-stopped", ids=("ebus.primary",))

    # --- quarantine and interruption ---

    def test_quarantine_failure_aborts_without_replacement(self):
        original = self.write_state(state_text(control_text()))
        with mock.patch.object(reset, "write_quarantine", side_effect=OSError("disk full")):
            code, _, err = self.run_main("--apply", "--gateway-stopped")
        self.assertEqual(code, reset.EXIT_FAILED_PRECONDITION)
        self.assertIn("quarantine copy could not be preserved", err)
        self.assertEqual(self.read_state(), original)

    def test_existing_quarantine_copies_are_never_replaced(self):
        original = self.write_state(state_text(control_text()))
        fixed = datetime.datetime(2030, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
        existing = self.state + reset.QUARANTINE_INFIX + "20300102T030405Z"
        with open(existing, "wb") as handle:
            handle.write(b"earlier diagnostic copy")
        with mock.patch.object(reset, "_utcnow", return_value=fixed):
            code, out, err = self.run_main("--apply", "--gateway-stopped")
        self.assertEqual(code, 0, err)
        with open(existing, "rb") as handle:
            self.assertEqual(handle.read(), b"earlier diagnostic copy")
        new_copy = json.loads(out)["quarantine_copy"]
        self.assertEqual(new_copy, existing + "-2")
        with open(new_copy, "rb") as handle:
            self.assertEqual(handle.read(), original)

    def test_interrupted_replacement_keeps_the_original_and_a_rerun_completes(self):
        original = self.write_state(state_text(control_text(recovery=recovery_text())))
        with mock.patch.object(reset.os, "replace", side_effect=OSError("interrupted")):
            code, _, err = self.run_main("--apply", "--gateway-stopped")
        self.assertEqual(code, reset.EXIT_FAILED_PRECONDITION)
        self.assertIn("original preserved", err)
        self.assertEqual(self.read_state(), original)
        # The interrupted run left its fsynced temp file and its quarantine copy.
        self.assertTrue(os.path.exists(self.state + ".reset.tmp"))
        first_copies = self.quarantine_copies()
        self.assertEqual(len(first_copies), 1)
        code, out, err = self.run_main("--apply", "--gateway-stopped")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(self.read_state())["runtime_control"]["issued_revision_mark"], "5")
        self.assertFalse(os.path.exists(self.state + ".reset.tmp"))
        copies = self.quarantine_copies()
        self.assertEqual(len(copies), 2)
        for path in copies:
            with open(path, "rb") as handle:
                self.assertEqual(handle.read(), original)

    def test_stale_temp_from_a_crash_is_overwritten(self):
        self.write_state(state_text(control_text()))
        with open(self.state + ".reset.tmp", "wb") as handle:
            handle.write(b"partial garbage from a crashed run")
        code, _, err = self.run_main("--apply", "--gateway-stopped")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(self.read_state())["runtime_control"]["catalog_revision"], "5")
        self.assertFalse(os.path.exists(self.state + ".reset.tmp"))

    def test_repeat_repair_keeps_advancing_revisions(self):
        self.write_state(state_text(control_text()))
        self.assertEqual(self.run_main("--apply", "--gateway-stopped")[0], 0)
        self.assertEqual(self.run_main("--apply", "--gateway-stopped")[0], 0)
        control = json.loads(self.read_state())["runtime_control"]
        self.assertEqual(control["issued_revision_mark"], "7")
        self.assertEqual(control["catalog_revision"], "6")
        self.assertEqual(control["drivers"]["ebus.primary"]["desired_revision"], "7")


if __name__ == "__main__":
    unittest.main()
