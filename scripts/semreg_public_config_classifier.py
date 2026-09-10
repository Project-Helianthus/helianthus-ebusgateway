#!/usr/bin/env python3
"""Fail closed unless config.go changes only the Storage public-read slice."""
from __future__ import annotations
import re
import subprocess
import sys
from pathlib import Path

DECLARATION = (
    "// PortalStorageConfig is an independently disabled, read-only BFF for the\n"
    "// versioned SemReg storage projection. It deliberately does not expose any\n"
    "// operation or native fallback fields.\n"
    "type PortalStorageConfig = PortalPVConfig\n"
)
FIELD = re.compile(r"(?m)^\s*PortalStorage\s+PortalStorageConfig\n")
FUNCTION = "func (cfg Config) ValidatePortalStorage() error {"

def show(ref: str) -> str:
    return subprocess.check_output(["git", "show", f"{ref}:config.go"], text=True)

def balanced_block_end(text: str, opening: int) -> int:
    depth = 0
    state = "code"
    index = opening
    while index < len(text):
        char = text[index]
        following = text[index + 1] if index + 1 < len(text) else ""
        if state == "line_comment":
            if char == "\n": state = "code"
        elif state == "block_comment":
            if char == "*" and following == "/":
                state = "code"; index += 1
        elif state == "double":
            if char == "\\": index += 1
            elif char == '"': state = "code"
        elif state == "single":
            if char == "\\": index += 1
            elif char == "'": state = "code"
        elif state == "raw":
            if char == "`": state = "code"
        elif char == "/" and following == "/":
            state = "line_comment"; index += 1
        elif char == "/" and following == "*":
            state = "block_comment"; index += 1
        elif char == '"': state = "double"
        elif char == "'": state = "single"
        elif char == "`": state = "raw"
        elif char == "{": depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                if end < len(text) and text[end] == "\n": end += 1
                return end
        index += 1
    raise ValueError("unbalanced ValidatePortalStorage function")

def remove_function(text: str) -> tuple[str, int]:
    matches = list(re.finditer(r"(?m)^" + re.escape(FUNCTION), text))
    if len(matches) > 1: raise ValueError("duplicate ValidatePortalStorage function")
    if not matches: return text, 0
    start = matches[0].start()
    opening = text.index("{", matches[0].start(), matches[0].end())
    end = balanced_block_end(text, opening)
    return text[:start] + text[end:], 1

def strip_storage(text: str) -> tuple[str, tuple[int, int, int]]:
    declaration_count = text.count(DECLARATION)
    if declaration_count > 1: raise ValueError("duplicate Storage config declaration")
    text = text.replace(DECLARATION, "")
    text, field_count = FIELD.subn("", text)
    if field_count > 1: raise ValueError("duplicate Storage config field")
    text, function_count = remove_function(text)
    normalized = re.sub(r"\n{2,}", "\n", text).strip() + "\n"
    return normalized, (declaration_count, field_count, function_count)

def main() -> int:
    try:
        base_rest, base_counts = strip_storage(show(sys.argv[1]))
        head_rest, head_counts = strip_storage(Path("config.go").read_text())
        if base_rest != head_rest: return 1
        if head_counts != (1, 1, 1): return 1
        if any(count not in (0, 1) for count in base_counts): return 1
        return 0
    except Exception:
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
