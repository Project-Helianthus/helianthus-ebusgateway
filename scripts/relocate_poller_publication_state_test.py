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


ASSIGN = """package main

func set(p *vaillantSemanticPoller, provider *liveProvider) {
	%s
}
"""


class AssignmentTest(unittest.TestCase):
    def rewrite(self, line: str) -> str:
        src = ASSIGN % line
        rewritten, findings = RELOCATE.rewrite(src, RELOCATE.mask(src), set())
        self.assertEqual([what for _, what in findings], [line.split(" ", 1)[0]])
        return rewritten

    def test_trailing_line_comment_stays_after_the_constructor(self) -> None:
        rewritten = self.rewrite("p.provider = provider // explanation")
        self.assertIn(
            "\tp.publication = newVaillantSemanticPublication(provider, p.broadcastHub(), p.cachePersister()) // explanation\n",
            rewritten,
        )

    def test_trailing_block_comment_stays_after_the_constructor(self) -> None:
        rewritten = self.rewrite("p.cache = persister(1) /* why */")
        self.assertIn(
            "\tp.publication = newVaillantSemanticPublication(p.liveProvider(), p.broadcastHub(), persister(1)) /* why */\n",
            rewritten,
        )

    def test_comment_marker_inside_a_string_stays_in_the_value(self) -> None:
        rewritten = self.rewrite('p.hub = hubFor("a // b")')
        self.assertIn(
            '\tp.publication = newVaillantSemanticPublication(p.liveProvider(), hubFor("a // b"), p.cachePersister())\n',
            rewritten,
        )


GROUPED = """package main

func use(first, second *vaillantSemanticPoller) {
	_ = first.provider
	_ = second.hub
}

func block() {
	var (
		head, tail *vaillantSemanticPoller
	)
	var left,
		right *vaillantSemanticPoller
	_ = head.cache
	_ = tail.cache
	_ = left.provider
	_ = right.provider
}

func mixed(n int, poller *vaillantSemanticPoller) {
	_ = poller.hub
}
"""


class GroupedDeclarationTest(unittest.TestCase):
    def test_every_name_of_a_grouped_declaration_is_rewritten(self) -> None:
        rewritten, findings = RELOCATE.rewrite(GROUPED, RELOCATE.mask(GROUPED), set())
        self.assertEqual(
            sorted(what for _, what in findings),
            sorted(
                [
                    "first.provider",
                    "second.hub",
                    "head.cache",
                    "tail.cache",
                    "left.provider",
                    "right.provider",
                    "poller.hub",
                ]
            ),
        )
        self.assertIn("_ = first.liveProvider()", rewritten)
        self.assertIn("_ = second.broadcastHub()", rewritten)
        self.assertIn("_ = head.cachePersister()", rewritten)
        self.assertIn("_ = left.liveProvider()", rewritten)
        self.assertNotIn("first.provider", rewritten)

    def test_a_preceding_parameter_type_is_not_a_poller(self) -> None:
        names = RELOCATE.poller_names(RELOCATE.mask(GROUPED), set())
        self.assertNotIn("int", names)
        self.assertNotIn("n", names)
        self.assertEqual(names, {"first", "second", "head", "tail", "left", "right", "poller"})


if __name__ == "__main__":
    unittest.main()
