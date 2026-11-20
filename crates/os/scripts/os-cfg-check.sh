#!/usr/bin/env bash
# What differs between operating systems lives in crates/os alone. Fail on
# a cfg (or cfg_attr) naming an OS, `cfg!(`, `std::os::` or `split_paths`
# anywhere else under crates/, tests included, and on a crate outside os with
# target-specific dependencies. The exceptions are the allowlist,
# scripts/os-cfg-allow.txt (one per line: `<file> | <text on the line> |
# <why>`), so each carries its reason and a new one is seen in review; an
# entry that no longer matches anything fails too, so the list stays true.
set -euo pipefail
top=$(git rev-parse --show-toplevel)
cd "$top"
allow_file=scripts/os-cfg-allow.txt
pattern='cfg(_attr)?\((all\(|any\(|not\()*[^)]*\b(unix|windows|target_os|target_family)\b|cfg!\(|std::os::|split_paths'

files=() fragments=()
if [ -f "$allow_file" ]; then
  while IFS= read -r entry; do
    [[ -z ${entry//[[:space:]]/} || $entry == \#* ]] && continue
    IFS='|' read -r file fragment _ <<< "$entry"
    file=$(echo "$file" | xargs)
    fragment=${fragment#"${fragment%%[![:space:]]*}"}
    fragment=${fragment%"${fragment##*[![:space:]]}"}
    if [ -z "$file" ] || [ -z "$fragment" ]; then
      echo "FAIL lint-os: $allow_file: an entry needs a file and the text on its line: $entry" >&2
      exit 1
    fi
    files+=("$file") fragments+=("$fragment")
  done < "$allow_file"
fi
used=()
for _ in "${files[@]}"; do used+=(0); done

found=0
while IFS= read -r hit; do
  file=${hit%%:*}
  [[ $file == crates/os/* ]] && continue
  rest=${hit#*:}
  line=${rest#*:}
  allowed=0
  for i in "${!files[@]}"; do
    if [[ $file == "${files[$i]}" && $line == *"${fragments[$i]}"* ]]; then
      allowed=1 used[i]=1
    fi
  done
  [ "$allowed" = 1 ] && continue
  echo "$hit"
  found=$((found + 1))
done < <(grep -rnE --include='*.rs' "$pattern" crates || true)

while IFS= read -r hit; do
  [[ $hit == crates/os/* ]] && continue
  echo "$hit"
  found=$((found + 1))
done < <(grep -nE "^\[target\." crates/*/Cargo.toml || true)

stale=0
for i in "${!files[@]}"; do
  if [ "${used[$i]}" = 0 ]; then
    echo "$allow_file: matches nothing now: ${files[$i]} | ${fragments[$i]}" >&2
    stale=$((stale + 1))
  fi
done

if [ "$found" -gt 0 ]; then
  echo "FAIL lint-os: $found OS-specific lines outside crates/os: move them onto the os crate (its Flavor or Os), or list an allowed one in $allow_file with why" >&2
  exit 1
fi
if [ "$stale" -gt 0 ]; then
  echo "FAIL lint-os: $stale entries of $allow_file match nothing: remove them" >&2
  exit 1
fi
echo "ok   lint-os (workspace)"
