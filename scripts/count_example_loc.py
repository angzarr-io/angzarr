#!/usr/bin/env python3
"""Count lines of code in example implementations.

Counts non-blank, non-comment lines for client logic files.
Used to generate accurate LOC statistics for documentation.
"""

import argparse
import json
import sys
from pathlib import Path


def count_loc_python(file_path: Path) -> int:
    """Count lines of code in a Python file, excluding blanks and comments."""
    loc = 0
    in_docstring = False
    docstring_char = None

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()

            # Handle docstrings
            if in_docstring:
                if docstring_char in stripped:
                    in_docstring = False
                continue

            # Skip blank lines
            if not stripped:
                continue

            # Skip single-line comments
            if stripped.startswith("#"):
                continue

            # Check for docstring start
            if stripped.startswith('"""') or stripped.startswith("'''"):
                docstring_char = stripped[:3]
                # Check if docstring ends on same line
                if stripped.count(docstring_char) >= 2:
                    continue
                in_docstring = True
                continue

            loc += 1

    return loc


def count_loc_go(file_path: Path) -> int:
    """Count lines of code in a Go file, excluding blanks and comments."""
    loc = 0
    in_block_comment = False

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()

            # Handle block comments
            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue

            # Skip blank lines
            if not stripped:
                continue

            # Skip single-line comments
            if stripped.startswith("//"):
                continue

            # Check for block comment start
            if stripped.startswith("/*"):
                if "*/" not in stripped:
                    in_block_comment = True
                continue

            loc += 1

    return loc


def count_loc_rust(file_path: Path) -> int:
    """Count lines of code in a Rust file, excluding blanks and comments."""
    loc = 0
    in_block_comment = False

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()

            # Handle block comments
            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue

            # Skip blank lines
            if not stripped:
                continue

            # Skip single-line comments (// and ///)
            if stripped.startswith("//"):
                continue

            # Check for block comment start
            if stripped.startswith("/*"):
                if "*/" not in stripped:
                    in_block_comment = True
                continue

            loc += 1

    return loc


def count_loc(file_path: Path) -> int:
    """Count lines of code based on file extension."""
    suffix = file_path.suffix.lower()
    if suffix == ".py":
        return count_loc_python(file_path)
    elif suffix == ".go":
        return count_loc_go(file_path)
    elif suffix == ".rs":
        return count_loc_rust(file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def find_example_files(repo_root: Path) -> dict:
    """Find all client logic files in examples."""
    examples = {}

    examples_dir = repo_root / "examples"
    if not examples_dir.exists():
        return examples

    # Define what constitutes "client logic" files per domain
    domains = ["customer", "product", "cart", "order", "inventory", "fulfillment"]

    for lang in ["go", "python", "rust"]:
        lang_dir = examples_dir / lang
        if not lang_dir.exists():
            continue

        for domain in domains:
            domain_dir = lang_dir / domain
            if not domain_dir.exists():
                continue

            key = f"{lang}/{domain}"
            examples[key] = {"files": [], "total_loc": 0}

            # Find logic files based on language
            if lang == "go":
                logic_dir = domain_dir / "logic"
                if logic_dir.exists():
                    for f in logic_dir.glob("*.go"):
                        if "_test.go" not in f.name:
                            loc = count_loc(f)
                            examples[key]["files"].append(
                                {"path": str(f.relative_to(repo_root)), "loc": loc}
                            )
                            examples[key]["total_loc"] += loc

            elif lang == "python":
                # Look for *_logic.py, state.py files
                for pattern in ["*_logic.py", "state.py"]:
                    for f in domain_dir.glob(pattern):
                        if "test" not in f.name:
                            loc = count_loc(f)
                            examples[key]["files"].append(
                                {"path": str(f.relative_to(repo_root)), "loc": loc}
                            )
                            examples[key]["total_loc"] += loc

                # Include handlers subdirectory
                handlers_dir = domain_dir / "handlers"
                if handlers_dir.exists():
                    for f in handlers_dir.glob("*.py"):
                        if f.name != "__init__.py" and "test" not in f.name:
                            loc = count_loc(f)
                            examples[key]["files"].append(
                                {"path": str(f.relative_to(repo_root)), "loc": loc}
                            )
                            examples[key]["total_loc"] += loc

            elif lang == "rust":
                src_dir = domain_dir / "src"
                if src_dir.exists():
                    for f in src_dir.glob("*.rs"):
                        loc = count_loc(f)
                        examples[key]["files"].append(
                            {"path": str(f.relative_to(repo_root)), "loc": loc}
                        )
                        examples[key]["total_loc"] += loc

    return examples


def main():
    parser = argparse.ArgumentParser(description="Count LOC in example implementations")
    parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).parent.parent,
        help="Repository root directory",
    )
    args = parser.parse_args()

    examples = find_example_files(args.repo_root)

    if args.format == "json":
        print(json.dumps(examples, indent=2))

    elif args.format == "markdown":
        print("| Example | Files | LOC |")
        print("|---------|-------|-----|")
        for key in sorted(examples.keys()):
            data = examples[key]
            file_count = len(data["files"])
            print(f"| {key} | {file_count} | {data['total_loc']} |")

    else:  # text
        for key in sorted(examples.keys()):
            data = examples[key]
            print(f"\n{key}: {data['total_loc']} LOC")
            for f in data["files"]:
                print(f"  {f['path']}: {f['loc']}")


if __name__ == "__main__":
    main()
