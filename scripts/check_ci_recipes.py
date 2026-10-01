#!/usr/bin/env python3
"""Check that every `just` recipe the GitHub workflows invoke exists.

Each `just [-f FILE] RECIPE [ARGS]` in .github/workflows/*.yml is resolved
against FILE (default: justfile). The `storage` / `bus` dispatchers also need
their `_storage-<backend>` / `_bus-<backend>` recipe. Recipe bodies in
justfile.container must call nested recipes through `{{justfile()}}`: CI runs
that file with `-f`, so a bare `just <recipe>` would resolve against the host
justfile instead.
"""

import glob
import json
import re
import subprocess
import sys

INVOCATION = re.compile(r"(?:^|[\s|;&])just\s+(-f\s+\S+\s+)?([A-Za-z_][\w-]*)(?:\s+([\w-]+))?")


def recipes(justfile: str) -> set[str]:
    dump = subprocess.run(
        ["just", "-f", justfile, "--dump", "--dump-format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return set(json.loads(dump.stdout)["recipes"])


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

    for failure in failures:
        print(failure)
    print(f"{len(failures)} problem(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
