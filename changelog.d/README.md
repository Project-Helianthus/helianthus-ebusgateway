# Changelog fragments

Every pull request adds its entry as a new file here and leaves `CHANGELOG.md` alone, so
open pull requests never edit the same changelog lines.

- Name: `<issue-number>-<slug>.md`, lower case, digits and hyphens, for example
  `1234-startup-reason.md`. Two pull requests for one issue use different slugs.
- Content: a `## ` title line, one blank line, then the entry text, written like
  the sections of `CHANGELOG.md`. One title per file; lower headings are allowed.
- A pull request adds at least one fragment (one per entry), does not edit
  `CHANGELOG.md` and does not remove a fragment.

```sh
python3 scripts/changelog_fragments.py check                 # validate the fragments
python3 scripts/changelog_fragments.py check --require-new   # also require a new one
python3 scripts/changelog_fragments.py assemble              # print the complete changelog
```

The complete changelog is the fragments, newest issue number first, followed by
the sections already in `CHANGELOG.md`. The release pull request runs
`python3 scripts/changelog_fragments.py assemble --write`, which moves the
fragments into `CHANGELOG.md` and removes them from this directory.

The release check is stricter than the rule for other pull requests. The release
branch must contain the current tip of the base branch, so update it and assemble
again when the base has moved on. Every fragment present at that tip must be
removed, and the resulting changelog must equal the ordered assembly of the
removed fragments. Missing entries, altered entry text and changes to existing
sections fail the check.
