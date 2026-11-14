## Changelog entries as one file per pull request

A pull request now adds its changelog entry as a new file `changelog.d/<issue-number>-<slug>.md` (a `## ` title, a blank line, the entry text) and no longer edits `CHANGELOG.md`. Open pull requests therefore never change the same changelog lines, and a pull request no longer has to be rebuilt because another one merged first. `python3 scripts/changelog_fragments.py check` validates the fragments, `check --require-new` also requires a new one and runs in the local gate and in the pull request workflow, and `assemble` prints the complete changelog: the fragments, newest issue number first, followed by the sections already in `CHANGELOG.md`. The release pull request runs `assemble --write`, which moves the fragments into `CHANGELOG.md`.

The existing sections of `CHANGELOG.md` are unchanged. Only repository tooling and contributor instructions change, so the gateway binary is unchanged.
