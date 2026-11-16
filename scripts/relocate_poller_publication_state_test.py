#!/usr/bin/env python3
"""Offline tests of the poller publication relocation script (synthetic Go)."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest

SCRIPT = pathlib.Path(__file__).with_name("relocate_poller_publication_state.py")
SPEC = importlib.util.spec_from_file_location("relocate_poller_publication_state", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
RELOCATE = importlib.util.module_from_spec(SPEC)
sys.modules["relocate_poller_publication_state"] = RELOCATE
SPEC.loader.exec_module(RELOCATE)

NAMED = """package main

func load() (poller *vaillantSemanticPoller, err error) { return nil, nil }

func use() {
	poller, _ := load()
	_ = poller.provider
}
"""

UNNAMED = NAMED.replace("(poller *vaillantSemanticPoller, err error)", "(*vaillantSemanticPoller, error)")

GENERIC = """package main

func load[T any]() *vaillantSemanticPoller { return nil }

func use() {
	poller := load[int]()
	_ = poller.provider
}
"""

NESTED_GENERIC = GENERIC.replace("load[int]()", "load[map[string]int]()")


class PollerFunctionsTest(unittest.TestCase):
    def functions(self, src: str) -> set[str]:
        return RELOCATE.poller_functions([RELOCATE.mask(src)])

    def test_named_first_result_is_recognised(self) -> None:
        self.assertIn("load", self.functions(NAMED))

    def test_unnamed_first_result_is_recognised(self) -> None:
        self.assertIn("load", self.functions(UNNAMED))

    def test_non_poller_result_is_ignored(self) -> None:
        src = "package main\nfunc load() (n int, err error) { return 0, nil }\n"
        self.assertNotIn("load", self.functions(src))

    def test_named_result_use_is_found(self) -> None:
        msk = RELOCATE.mask(NAMED)
        _, findings = RELOCATE.rewrite(NAMED, msk, self.functions(NAMED))
        self.assertEqual(len(findings), 1)

    def test_generic_call_with_type_arguments_is_found(self) -> None:
        for src in (GENERIC, NESTED_GENERIC):
            with self.subTest(src=src):
                functions = self.functions(src)
                self.assertIn("load", functions)
                rewritten, findings = RELOCATE.rewrite(src, RELOCATE.mask(src), functions)
                self.assertEqual([what for _, what in findings], ["poller.provider"])
                self.assertIn("_ = poller.liveProvider()", rewritten)


if __name__ == "__main__":
    unittest.main()
