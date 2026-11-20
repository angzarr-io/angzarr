"""Print `package<TAB>target` for every test target of the workspace that sets
`harness = false` in its manifest (a cucumber runner, say). cargo-nextest lists
a test binary by running it with `--list`, which such a binary does not answer,
so the gates skip these by binary id and run them with `cargo test`.

    python3 -I gates/harnessless.py <workspace Cargo.toml>
"""
import json
import subprocess
import sys
import tomllib


def main() -> int:
    manifest = sys.argv[1]
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps", "--offline", "--manifest-path", manifest],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    for package in sorted(metadata["packages"], key=lambda p: p["name"]):
        with open(package["manifest_path"], "rb") as handle:
            tests = tomllib.load(handle).get("test", [])
        for target in tests:
            if target.get("harness") is False:
                print(f'{package["name"]}\t{target["name"]}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
