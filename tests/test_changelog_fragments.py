"""Synthetic acceptance tests for the changelog fragment check and assembly."""
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "changelog_fragments", ROOT / "scripts/changelog_fragments.py")
FRAGMENTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FRAGMENTS)

GOOD = "## A title\n\nOne paragraph.\n\n| Old | New |\n|---|---|\n| `a` | `b` |\n"
BASE = "# Changelog\n\n## Older entry\n\nOlder text.\n"


def clean_environment():
    """The environment without Git repository overrides and without CHANGELOG_BASE.

    A Git hook exports GIT_DIR and GIT_INDEX_FILE; left in place they would point
    the throwaway repositories of these tests at the real one.
    """
    return {key: value for key, value in os.environ.items()
            if not key.startswith("GIT_") and key != "CHANGELOG_BASE"}


def write_tree(root, files):
    for name, text in files.items():
        path = Path(root) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))


def run_main(root, *argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = FRAGMENTS.main(list(argv), root=Path(root))
    return code, out.getvalue(), err.getvalue()


class FragmentTextTests(unittest.TestCase):
    def test_valid_fragment_has_no_problem(self):
        self.assertEqual(FRAGMENTS.text_problems(GOOD), [])

    def test_each_format_fault_is_reported(self):
        faults = {
            "no title": "A title\n\nText.\n",
            "top-level title": "# A title\n\nText.\n",
            "empty title": "## \n\nText.\n",
            "no blank line": "## A title\nText.\n",
            "no entry text": "## A title\n",
            "blank line 3": "## A title\n\n\nText.\n",
            "second title": "## A title\n\nText.\n\n## Another\n\nMore.\n",
            "top title in text": "## A title\n\nText.\n\n# Changelog\n",
            "trailing whitespace": "## A title\n\nText. \n",
            "trailing tab": "## A title\n\nText.\t\n",
            "title whitespace": "## A title \n\nText.\n",
            "no final newline": "## A title\n\nText.",
            "blank line at end": "## A title\n\nText.\n\n",
            "carriage return": "## A title\r\n\r\nText.\r\n",
            "lone carriage return": "## A title\n\nText.\rMore.\n",
            "conflict start": "## A title\n\n<<<<<<< ours\nText.\n",
            "conflict base": "## A title\n\n||||||| base\nText.\n",
            "conflict middle": "## A title\n\nText.\n=======\nOther.\n",
            "conflict end": "## A title\n\nText.\n>>>>>>> theirs\n",
            "title after a closed fence": "## A title\n\n```\ncode\n```\n## Another\n",
        }
        for label, text in faults.items():
            with self.subTest(label):
                self.assertNotEqual(FRAGMENTS.text_problems(text), [])

    def test_fenced_block_may_hold_hash_lines(self):
        text = "## A title\n\nRun:\n\n```sh\n# a shell comment\n## another\n```\n"
        self.assertEqual(FRAGMENTS.text_problems(text), [])

    def test_lower_headings_are_allowed(self):
        self.assertEqual(FRAGMENTS.text_problems("## A title\n\n### Detail\n\nText.\n"), [])


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_missing_directory_is_a_problem(self):
        names, problems = FRAGMENTS.check_fragments(self.root)
        self.assertEqual(names, [])
        self.assertEqual(problems, ["changelog.d/ is missing"])

    def test_readme_is_ignored_and_order_is_newest_issue_first(self):
        # The issue number is compared as a number (1000 before 120 before 9) and
        # the name breaks ties within one issue (a-first before b-second).
        write_tree(self.root, {
            "changelog.d/README.md": "Not a fragment.\n",
            "changelog.d/9-older.md": GOOD,
            "changelog.d/120-b-second.md": GOOD,
            "changelog.d/120-a-first.md": GOOD,
            "changelog.d/1000-newest.md": GOOD,
        })
        names, problems = FRAGMENTS.check_fragments(self.root)
        self.assertEqual(problems, [])
        self.assertEqual(names, ["1000-newest.md", "120-a-first.md", "120-b-second.md",
                                 "9-older.md"])

    def test_unexpected_entries_are_problems(self):
        write_tree(self.root, {
            "changelog.d/README.md": "Not a fragment.\n",
            "changelog.d/notes.md": GOOD,
            "changelog.d/12-Upper.md": GOOD,
            "changelog.d/12_underscore.md": GOOD,
            "changelog.d/12-entry.txt": GOOD,
            "changelog.d/12-trailing-.md": GOOD,
            "changelog.d/nested/12-entry.md": GOOD,
        })
        names, problems = FRAGMENTS.check_fragments(self.root)
        self.assertEqual(names, [])
        self.assertEqual(len(problems), 6)
        self.assertTrue(all("not a fragment" in problem for problem in problems))

    def test_symbolic_links_are_problems(self):
        write_tree(self.root, {"elsewhere.md": GOOD, "changelog.d/12-entry.md": GOOD})
        try:
            os.symlink(self.root / "elsewhere.md", self.root / "changelog.d/13-link.md")
            os.symlink(self.root / "elsewhere.md", self.root / "changelog.d/README.md")
        except OSError as error:
            self.skipTest(f"symbolic links are not available: {error}")
        names, problems = FRAGMENTS.check_fragments(self.root)
        self.assertEqual(names, ["12-entry.md"])
        self.assertEqual(sorted(problems), [
            "changelog.d/13-link.md: not a fragment; expected changelog.d/<issue-number>-<slug>.md",
            "changelog.d/README.md: not a fragment; expected changelog.d/<issue-number>-<slug>.md",
        ])

    def test_fragment_problems_name_the_file(self):
        write_tree(self.root, {"changelog.d/12-entry.md": "No title.\n"})
        _, problems = FRAGMENTS.check_fragments(self.root)
        self.assertTrue(problems)
        self.assertTrue(all(problem.startswith("changelog.d/12-entry.md: ") for problem in problems))

    def test_invalid_utf8_is_a_problem(self):
        (self.root / "changelog.d").mkdir()
        (self.root / "changelog.d/12-entry.md").write_bytes(b"## A title\n\n\xff\n")
        _, problems = FRAGMENTS.check_fragments(self.root)
        self.assertEqual(problems, ["changelog.d/12-entry.md: not UTF-8"])


class AssembleTests(unittest.TestCase):
    def test_entries_go_before_the_first_section_in_the_given_order(self):
        first = "## First\n\nFirst text.\n"
        second = "## Second\n\nSecond text.\n"
        self.assertEqual(
            FRAGMENTS.assemble(BASE, [first, second]),
            "# Changelog\n\n## First\n\nFirst text.\n\n## Second\n\nSecond text.\n\n"
            "## Older entry\n\nOlder text.\n")

    def test_without_entries_the_changelog_is_unchanged(self):
        self.assertEqual(FRAGMENTS.assemble(BASE, []), BASE)

    def test_changelog_without_sections_gets_the_entries_appended(self):
        self.assertEqual(FRAGMENTS.assemble("# Changelog\n", [GOOD]), "# Changelog\n\n" + GOOD)

    def test_existing_sections_keep_their_bytes(self):
        for tail in (BASE[len("# Changelog\n\n"):], "## Older entry\n\nOlder text.\n\n\n"):
            with self.subTest(repr(tail[-3:])):
                text = FRAGMENTS.assemble("# Changelog\n\n" + tail, [GOOD])
                self.assertEqual(text, "# Changelog\n\n" + GOOD + "\n" + tail)


class NewFragmentRuleTests(unittest.TestCase):
    def test_rule_table(self):
        added = {"changelog.d/12-entry.md": "A"}
        cases = [
            ("adds a fragment", {"a.go": "M", **added}, True),
            ("adds two fragments", {**added, "changelog.d/12-other.md": "A"}, True),
            ("corrects another fragment too", {**added, "changelog.d/7-old.md": "M"}, True),
            ("no fragment", {"a.go": "M"}, False),
            ("only the directory note", {"changelog.d/README.md": "A"}, False),
            ("only corrects a fragment", {"changelog.d/7-old.md": "M"}, False),
            ("edits the changelog", {"CHANGELOG.md": "M", **added}, False),
            ("edits the changelog only", {"CHANGELOG.md": "M"}, False),
            ("removes a fragment", {"changelog.d/7-old.md": "D", **added}, False),
            ("removes a fragment only", {"changelog.d/7-old.md": "D"}, False),
            ("release assembly", {"CHANGELOG.md": "M", "changelog.d/7-old.md": "D"}, True),
            ("assembly that also adds", {"CHANGELOG.md": "M", "changelog.d/7-old.md": "D",
                                         **added}, False),
        ]
        for label, changes, accepted in cases:
            with self.subTest(label):
                self.assertEqual(FRAGMENTS.new_fragment_problems(changes) == [], accepted)


class ReleaseValidationTests(unittest.TestCase):
    def test_release_requires_complete_assembly_for_working_tree_and_head(self):
        changes = {"CHANGELOG.md": "M", "changelog.d/7-old.md": "D",
                   "changelog.d/12-new.md": "D"}
        newer = GOOD.replace("A title", "Newer")
        expected = FRAGMENTS.assemble(BASE, [newer, GOOD])
        for head in (None, "topic"):
            for actual in (BASE + "\nUnrelated edit.\n", FRAGMENTS.assemble(BASE, [GOOD]),
                           expected.replace("One paragraph.", "Lost text.", 1), expected):
                with self.subTest(head=head, actual=actual):
                    with tempfile.TemporaryDirectory() as directory:
                        root = Path(directory)
                        write_tree(root, {"CHANGELOG.md": actual})
                        blobs = {"CHANGELOG.md": BASE, "changelog.d/7-old.md": GOOD,
                                 "changelog.d/12-new.md": newer}

                        def read_git(_root, *args):
                            if args[0] == "merge-base":
                                return 0, "ancestor\n"
                            revision, path = args[1].split(":", 1)
                            return 0, actual if revision == "topic" else blobs[path]

                        with mock.patch.object(FRAGMENTS, "resolves", return_value=True), \
                                mock.patch.object(FRAGMENTS, "change_set", return_value=changes), \
                                mock.patch.object(FRAGMENTS, "git", side_effect=read_git):
                            problems, _ = FRAGMENTS.require_new(root, "main", head)
                        self.assertEqual(problems == [], actual == expected)

    def test_unavailable_release_history_fails_closed(self):
        changes = {"CHANGELOG.md": "M", "changelog.d/7-old.md": "D"}
        with mock.patch.object(FRAGMENTS, "change_set", return_value=changes), \
                mock.patch.object(FRAGMENTS, "git", return_value=(1, "")):
            problems, _ = FRAGMENTS.require_new(ROOT, "main", None)
        self.assertTrue(problems)


class UnavailableGitTests(unittest.TestCase):
    """The new-fragment rule when Git itself cannot run."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        write_tree(self.root, {"CHANGELOG.md": BASE, "changelog.d/12-entry.md": GOOD})
        patcher = mock.patch.object(FRAGMENTS.subprocess, "run",
                                    side_effect=FileNotFoundError("git"))
        patcher.start()
        self.addCleanup(patcher.stop)
        environment = mock.patch.dict(os.environ, clean_environment(), clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def test_default_base_notifies_and_decides_on_the_format(self):
        code, out, err = run_main(self.root, "check", "--require-new")
        self.assertEqual((code, err), (0, ""))
        self.assertIn("was not checked", out)

    def test_explicit_base_fails(self):
        code, _, err = run_main(self.root, "check", "--require-new", "--base", "main")
        self.assertEqual(code, 1)
        self.assertIn("base main is not available", err)


@unittest.skipUnless(shutil.which("git"), "git is not available")
class GitChangeSetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.environment = clean_environment()
        patcher = mock.patch.dict(os.environ, self.environment, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        write_tree(self.root, {"CHANGELOG.md": BASE, "changelog.d/README.md": "Note.\n",
                               "changelog.d/7-old.md": GOOD, "a.txt": "a\n",
                               ".gitignore": "/.tmp/\n"})
        self.git("init", "-q", "-b", "main")
        self.commit("base")
        self.git("switch", "-q", "-c", "topic")

    def git(self, *arguments):
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=Test",
                        "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false",
                        *arguments], check=True, capture_output=True, timeout=60,
                       env=self.environment)

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)

    def test_working_tree_counts_untracked_files_and_skips_ignored_ones(self):
        write_tree(self.root, {"a.txt": "b\n", "changelog.d/12-entry.md": GOOD,
                               ".tmp/scratch.md": "scratch\n"})
        self.assertEqual(FRAGMENTS.change_set(self.root, "main"),
                         {"a.txt": "M", "changelog.d/12-entry.md": "A"})
        self.assertEqual(run_main(self.root, "check", "--require-new", "--base", "main")[0], 0)

    def test_committed_change_is_compared_from_the_merge_base(self):
        write_tree(self.root, {"a.txt": "b\n", "changelog.d/12-entry.md": GOOD})
        self.commit("topic")
        self.git("switch", "-q", "main")
        write_tree(self.root, {"b.txt": "main moved\n", "changelog.d/13-main.md": GOOD})
        self.commit("main moved")
        self.assertEqual(FRAGMENTS.change_set(self.root, "main", "topic"),
                         {"a.txt": "M", "changelog.d/12-entry.md": "A"})
        code, _, err = run_main(self.root, "check", "--require-new", "--base", "main",
                                "--head", "topic")
        self.assertEqual((code, err), (0, ""))
        code, _, err = run_main(self.root, "check", "--require-new", "--base", "topic",
                                "--head", "main")
        self.assertEqual((code, err), (0, ""))

    def test_committed_change_without_fragment_fails(self):
        write_tree(self.root, {"a.txt": "b\n"})
        self.commit("topic")
        code, _, err = run_main(self.root, "check", "--require-new", "--base", "main",
                                "--head", "topic")
        self.assertEqual(code, 1)
        self.assertIn("adds no changelog.d/", err)

    def test_change_without_fragment_fails(self):
        write_tree(self.root, {"a.txt": "b\n"})
        code, _, err = run_main(self.root, "check", "--require-new", "--base", "main")
        self.assertEqual(code, 1)
        self.assertIn("adds no changelog.d/", err)

    def test_changelog_edit_fails_even_with_a_fragment(self):
        write_tree(self.root, {"CHANGELOG.md": BASE + "\nMore.\n",
                               "changelog.d/12-entry.md": GOOD})
        self.assertEqual(run_main(self.root, "check", "--require-new", "--base", "main")[0], 1)

    def test_no_change_against_the_base_passes_with_a_notice(self):
        code, out, _ = run_main(self.root, "check", "--require-new", "--base", "main")
        self.assertEqual(code, 0)
        self.assertIn("does not apply", out)

    def test_explicit_unknown_base_fails_and_default_base_only_notifies(self):
        write_tree(self.root, {"a.txt": "b\n"})
        self.assertEqual(run_main(self.root, "check", "--require-new", "--base", "missing")[0], 1)
        code, out, _ = run_main(self.root, "check", "--require-new")
        self.assertEqual(code, 0)
        self.assertIn("was not checked", out)

    def test_base_from_the_environment_is_explicit(self):
        write_tree(self.root, {"a.txt": "b\n"})
        with mock.patch.dict(os.environ, {"CHANGELOG_BASE": "main"}):
            code, _, err = run_main(self.root, "check", "--require-new")
        self.assertEqual(code, 1)
        self.assertIn("adds no changelog.d/", err)
        with mock.patch.dict(os.environ, {"CHANGELOG_BASE": "missing"}):
            code, _, err = run_main(self.root, "check", "--require-new")
        self.assertEqual(code, 1)
        self.assertIn("base missing is not available", err)

    def test_unknown_head_fails(self):
        code, _, err = run_main(self.root, "check", "--require-new", "--base", "main",
                                "--head", "missing")
        self.assertEqual(code, 1)
        self.assertIn("head missing is not available", err)

    def test_release_assembly_passes_the_rule_and_keeps_the_note(self):
        write_tree(self.root, {"changelog.d/12-entry.md": GOOD.replace("A title", "Newer")})
        self.commit("entry")
        self.git("switch", "-q", "-c", "release")
        code, out, _ = run_main(self.root, "assemble", "--write")
        self.assertEqual(code, 0, out)
        text = (self.root / "CHANGELOG.md").read_text(encoding="utf-8")
        self.assertLess(text.index("## Newer"), text.index("## A title"))
        self.assertLess(text.index("## A title"), text.index("## Older entry"))
        self.assertEqual(sorted(path.name for path in (self.root / "changelog.d").iterdir()),
                         ["README.md"])
        self.assertEqual(run_main(self.root, "check", "--require-new", "--base", "topic")[0], 0)


class CommandTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        write_tree(self.root, {"CHANGELOG.md": BASE, "changelog.d/README.md": "Note.\n",
                               "changelog.d/12-entry.md": GOOD})

    def test_check_reports_the_count(self):
        code, out, _ = run_main(self.root, "check")
        self.assertEqual((code, out), (0, "changelog fragments: 1 valid\n"))

    def test_check_reports_each_problem_on_standard_error(self):
        write_tree(self.root, {"changelog.d/13-bad.md": "No title.\n", "changelog.d/x.md": GOOD})
        code, out, err = run_main(self.root, "check")
        self.assertEqual((code, out), (1, ""))
        lines = err.splitlines()
        self.assertGreaterEqual(len(lines), 2)
        self.assertTrue(all(line.startswith("changelog fragments: ") for line in lines))

    def test_assemble_prints_without_writing(self):
        code, out, _ = run_main(self.root, "assemble")
        self.assertEqual(code, 0)
        self.assertEqual(out, FRAGMENTS.assemble(BASE, [GOOD]))
        self.assertEqual((self.root / "CHANGELOG.md").read_text(encoding="utf-8"), BASE)
        self.assertTrue((self.root / "changelog.d/12-entry.md").exists())

    def test_assemble_without_fragments_prints_the_changelog(self):
        (self.root / "changelog.d/12-entry.md").unlink()
        self.assertEqual(run_main(self.root, "assemble"), (0, BASE, ""))

    def test_assemble_write_moves_the_fragments(self):
        code, out, _ = run_main(self.root, "assemble", "--write")
        self.assertEqual((code, out), (0, "changelog fragments: 1 moved into CHANGELOG.md\n"))
        self.assertEqual((self.root / "CHANGELOG.md").read_text(encoding="utf-8"),
                         FRAGMENTS.assemble(BASE, [GOOD]))
        self.assertEqual(sorted(path.name for path in (self.root / "changelog.d").iterdir()),
                         ["README.md"])

    def test_invalid_fragment_blocks_check_and_assembly(self):
        write_tree(self.root, {"changelog.d/13-bad.md": "No title.\n"})
        self.assertEqual(run_main(self.root, "check")[0], 1)
        self.assertEqual(run_main(self.root, "assemble")[:2], (1, ""))
        self.assertEqual(run_main(self.root, "assemble", "--write")[0], 1)
        self.assertEqual((self.root / "CHANGELOG.md").read_text(encoding="utf-8"), BASE)
        self.assertTrue((self.root / "changelog.d/12-entry.md").exists())

    def test_missing_changelog_blocks_assembly(self):
        (self.root / "CHANGELOG.md").unlink()
        code, _, err = run_main(self.root, "assemble", "--write")
        self.assertEqual(code, 1)
        self.assertIn("CHANGELOG.md is missing", err)
        self.assertTrue((self.root / "changelog.d/12-entry.md").exists())


class CommittedStateTests(unittest.TestCase):
    def test_committed_fragments_are_valid(self):
        self.assertEqual(FRAGMENTS.check_fragments(ROOT)[1], [])

    def test_committed_changelog_assembles_with_its_existing_sections_intact(self):
        names, _ = FRAGMENTS.check_fragments(ROOT)
        changelog = (ROOT / "CHANGELOG.md").read_bytes().decode("utf-8")
        entries = [(ROOT / "changelog.d" / name).read_bytes().decode("utf-8") for name in names]
        text = FRAGMENTS.assemble(changelog, entries)
        first = changelog.index("\n## ") + 1
        self.assertTrue(text.startswith(changelog[:first]))
        self.assertTrue(text.endswith(changelog[first:]))
        for entry in entries:
            self.assertEqual(text.count(entry.rstrip("\n") + "\n"), 1)


if __name__ == "__main__":
    unittest.main()
