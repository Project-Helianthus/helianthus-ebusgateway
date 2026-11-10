#!/usr/bin/env python3
"""Check the actual gateway command's offline dependency closures."""

import json
import os
from pathlib import Path
import subprocess
import sys

MODULE = "github.com/Project-Helianthus/helianthus-ebusgateway"


def matter_dependency(path):
    path = path.lower()
    return (
        path == MODULE.lower() + "/matter"
        or path.startswith(MODULE.lower() + "/matter/")
        or any(part in path for part in ("go-fabric", "matter", "connectedhomeip", "/project-chip/"))
    )


def check(root):
    env = os.environ.copy()
    if not env.get("GOMODCACHE"):
        raise RuntimeError("pre-provisioned GOMODCACHE is required")
    env.update(GOWORK="off", GOPROXY="off", GOSUMDB="off", GOTOOLCHAIN="local")
    closures = {}
    default_packages = set()
    for profile, tags in (("default", []), ("nomatter", ["-tags", "nomatter"])):
        command = ["go", "list", "-mod=readonly", "-buildvcs=false", "-deps", "-test", *tags, "./cmd/gateway"]
        result = subprocess.run(command, cwd=root, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=80)
        if result.returncode:
            raise RuntimeError(f"{profile} dependency listing failed: {result.stderr}")
        packages = result.stdout.splitlines()
        if MODULE + "/cmd/gateway" not in packages:
            raise RuntimeError(f"{profile} command missing from dependency listing")
        if profile == "default":
            default_packages = set(packages)
        closures[profile] = sorted(p for p in packages if matter_dependency(p))
    required = {MODULE + suffix for suffix in ("/matter", "/matter/feedv1", "/matter/libraryhost")}
    if not required <= default_packages:
        raise RuntimeError("default profile lost its Matter dependency closure")
    if closures["nomatter"]:
        raise RuntimeError("nomatter contains Matter dependencies: " + ", ".join(closures["nomatter"]))
    return {"default_matter_packages": len(closures["default"]), "nomatter_matter_packages": 0}


def main():
    try:
        print(json.dumps(check(Path(__file__).resolve().parent.parent), sort_keys=True))
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
