#!/usr/bin/env python3
"""Offline, token-aware adoption of the catalog spelling in Go sources.

RENAMES drops the work-item prefix from the identifiers of
internal/acceptancecatalog, uses the catalog spelling, and adopts the renamed
helianthus-modbusreg exports that the gateway uses. Identifier tokens and
identifier words inside comments are renamed. In comments and string literals
only the spelling changes. The acceptance report JSON key catalogue_version is
kept unchanged and is exempt. Run without arguments to rewrite FILES; --check
lists any remaining old name or file name in the repository, and any remaining
old spelling in FILES, and fails.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RENAMES = {
    "O22Catalogue": "Catalog",
    "O22Category": "Category",
    "O22Identity": "Identity",
    "O22MaxBytes": "MaxBytes",
    "O22ReadCatalogue": "ReadCatalog",
    "O22ReadReport": "ReadReport",
    "O22ReadSchema": "ReadSchema",
    "O22Record": "Record",
    "O22Report": "Report",
    "O22Review": "Review",
    "O22Row": "Row",
    "O22Signature": "Signature",
    "O22Transition": "Transition",
    "O22ValidateReport": "ValidateReport",
    "CatalogueVersion": "CatalogVersion",
    "o22Contains": "contains",
    "o22Decode": "decode",
    "o22IdentityValid": "identityValid",
    "o22Clone": "clone",
    "o22Equal": "equal",
    "o22File": "repoFile",
    "o22Handoff": "handoffRows",
    "o22Ledger": "repoLedger",
    "o22Load": "loadCatalog",
    "o22Report": "syntheticReport",
    "TestO22CatalogueMutations": "TestCatalogMutations",
    "TestO22Full88ExpectedFailures": "TestFull88ExpectedFailures",
    "TestO22HandoffInventory": "TestHandoffInventory",
    "TestO22HandoffMutationControls": "TestHandoffMutationControls",
    "TestO22MatrixInventory": "TestMatrixInventory",
    "TestO22OutcomesAndCategories": "TestOutcomesAndCategories",
    "TestO22PublishedArtifacts": "TestPublishedArtifacts",
    "TestO22ReadReportOmissions": "TestReadReportOmissions",
    "TestO22ReportIdentityMutations": "TestReportIdentityMutations",
    # helianthus-modbusreg exports renamed upstream.
    "Catalogue": "Catalog",
    "HuaweiSUN2000CatalogueSelected": "HuaweiSUN2000CatalogSelected",
}
FILES = [
    "cmd/gateway/huawei_pv_semreg.go",
    "cmd/gateway/status_provider_test.go",
    "internal/acceptancecatalog/catalog.go",
    "internal/acceptancecatalog/catalog_test.go",
]
TOKEN = re.compile(r"//[^\n]*|/\*[\s\S]*?\*/|\"(?:\\.|[^\"\\])*\"|`[^`]*`|'(?:\\.|[^'\\])*'|\b[A-Za-z_]\w*\b")
WORD = re.compile(r"\b[A-Za-z_]\w*\b")
SPELLING = re.compile(r"(?i:c)atalogue")
KEPT_KEY = "catalogue_version"


def rename(match):
    return RENAMES.get(match[0], match[0])


def spell(text):
    parts = text.split(KEPT_KEY)
    return KEPT_KEY.join(SPELLING.sub(lambda match: match[0][0] + "atalog", part) for part in parts)


def rewrite(source):
    def token(match):
        text = match[0]
        if text.startswith(("//", "/*")):
            return spell(WORD.sub(rename, text))
        if text[0] in "\"'`":
            return spell(text)
        return rename(match)

    return TOKEN.sub(token, source)


def repository_paths():
    for directory, subdirectories, files in os.walk(ROOT):
        subdirectories[:] = sorted(name for name in subdirectories
                                   if not name.startswith(".") and name != "node_modules")
        for name in subdirectories + sorted(files):
            yield Path(directory, name)


def remaining():
    for path in repository_paths():
        name = path.relative_to(ROOT)
        if "catalogue" in path.name.lower():
            yield f"{name}: file name"
        if path.suffix != ".go" or not path.is_file():
            continue
        for number, line in enumerate(path.read_text().splitlines(), 1):
            for match in WORD.finditer(line):
                if match[0] in RENAMES:
                    yield f"{name}:{number}: {match[0]}"
            if str(name) in FILES and SPELLING.search(line.replace(KEPT_KEY, "")):
                yield f"{name}:{number}: spelling"


def main(argv):
    if argv == ["--check"]:
        found = list(remaining())
        print("\n".join(found) if found else "no old acceptance catalog or modbusreg catalog name remains")
        return 1 if found else 0
    if argv:
        raise SystemExit("usage: rename_acceptance_catalog.py [--check]")
    for name in FILES:
        path = ROOT / name
        source = rewrite(path.read_text())
        formatted = subprocess.run(["gofmt"], input=source, text=True, check=True,
                                   stdout=subprocess.PIPE).stdout
        path.write_text(formatted)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
