#!/usr/bin/env python3
"""Check that every `just` recipe the GitHub workflows invoke exists.

Each `just [-f FILE] RECIPE [ARGS]` in .github/workflows/*.yml is resolved
against FILE (default: justfile). The `storage` / `bus` dispatchers also need
their `_storage-<backend>` / `_bus-<backend>` recipe. Recipe bodies in
justfile.container must call nested recipes through `{{justfile()}}`: CI runs
that file with `-f`, so a bare `just <recipe>` would resolve against the host
justfile instead. Jobs that check out without submodules still parse the
justfile, so it must not depend on recipes imported from a submodule.
"""

import glob
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

INVOCATION = re.compile(r"(?:^|[\s|;&])just\s+(-f\s+\S+\s+)?([A-Za-z_][\w-]*)(?:\s+([\w-]+))?")


def recipes(justfile: str) -> set[str]:
    dump = subprocess.run(
        ["just", "-f", justfile, "--dump", "--dump-format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return set(json.loads(dump.stdout)["recipes"])


def parses_without_submodules() -> str | None:
    """Parse the justfile in a copy of the tree without submodule content."""
    tracked = subprocess.run(
        ["git", "ls-files", "-s"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    files = [line.split("\t", 1)[1] for line in tracked if not line.startswith("160000")]
    with tempfile.TemporaryDirectory() as tmp:
        for name in files:
            if not (name.endswith("justfile") or name.endswith(".just")):
                continue
            target = pathlib.Path(tmp, name)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(name, target)
        subprocess.run(["git", "init", "-q", tmp], check=True)
        result = subprocess.run(
            ["just", "--summary"], cwd=tmp, capture_output=True, text=True
        )
        return None if result.returncode == 0 else result.stderr.strip()


def main() -> int:
    known: dict[str, set[str]] = {}
    failures = []
    for workflow in sorted(glob.glob(".github/workflows/*.yml")):
        for number, line in enumerate(open(workflow), 1):
            if line.strip().startswith("#"):
                continue
            match = INVOCATION.search(line)
            if not match:
                continue
            justfile = match.group(1).split()[1] if match.group(1) else "justfile"
            recipe, arg = match.group(2), match.group(3)
            available = known.setdefault(justfile, recipes(justfile))
            needed = [recipe]
            if recipe in ("storage", "bus") and arg and arg != "test":
                needed.append(f"_{recipe}-{arg}")
            for name in needed:
                if name not in available:
                    failures.append(f"{workflow}:{number}: {justfile} has no recipe `{name}`")

    for number, line in enumerate(open("justfile.container"), 1):
        if line.startswith((" ", "\t")) and re.search(r"(^|[\s@;&|])just\s+[\w\"_-]", line.strip()):
            if "{{justfile()}}" not in line and not line.strip().startswith("#"):
                failures.append(
                    f"justfile.container:{number}: nested `just` call without -f {{{{justfile()}}}}"
                )

    problem = parses_without_submodules()
    if problem:
        failures.append(f"justfile does not parse without submodules: {problem}")

    for failure in failures:
        print(failure)
    print(f"{len(failures)} problem(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
