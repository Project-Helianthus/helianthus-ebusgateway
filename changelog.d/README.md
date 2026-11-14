# Changelog fragments

Every pull request adds one new file here and leaves `CHANGELOG.md` alone, so
open pull requests never edit the same changelog lines.

- Name: `<issue-number>-<slug>.md`, lower case, digits and hyphens, for example
  `1234-startup-reason.md`. Two pull requests for one issue use different slugs.
- Content: a `## ` title line, one blank line, then the entry text, written like
  the sections of `CHANGELOG.md`. One title per file; lower headings are allowed.
- A pull request does not edit `CHANGELOG.md` and does not remove a fragment.

```sh
python3 scripts/changelog_fragments.py check                 # validate the fragments
python3 scripts/changelog_fragments.py check --require-new   # also require the new one
python3 scripts/changelog_fragments.py assemble              # print the complete changelog
```

The complete changelog is the fragments, newest issue number first, followed by
the sections already in `CHANGELOG.md`. The release pull request runs
`python3 scripts/changelog_fragments.py assemble --write`, which moves the
fragments into `CHANGELOG.md` and removes them from this directory.
