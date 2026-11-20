#!/usr/bin/env bash
# The per-crate licence table: licences.tsv is the one source. `apply` writes
# every crate's Cargo `license`, its LICENSE-* files, deny.toml's exceptions
# and the README's table from it; `check` (the lint gate runs it) fails where
# any of them disagrees with the table or where a workspace crate is missing
# from it. It also reports a crate of group B or C that depends (not only for
# tests) on a plain-AGPL crate of group A: the runtime exception cannot reach
# what such a crate links. That is a WARN, or a failure with --strict.
#   scripts/licences.sh apply | check [--strict]
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-check}; strict=${2:-}
declare -A SPDX=(
  [A]='AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission'
  [B]='AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception'
  [C]='Apache-2.0 OR BSD-3-Clause'
)
declare -A FILES=(
  [A]='LICENSE-AGPL LICENSE-GRPC-PERMISSION'
  [B]='LICENSE-AGPL LICENSE-EXCEPTION'
  [C]='LICENSE-APACHE LICENSE-BSD'
)
ALL_FILES='LICENSE-AGPL LICENSE-EXCEPTION LICENSE-GRPC-PERMISSION LICENSE-APACHE LICENSE-BSD'
fail=0
bad() { echo "FAIL licences: $*" >&2; fail=1; }
dir_of() { if [ "$1" = archlint ]; then echo tools/archlint; else echo "crates/$1"; fi; }
rows() { grep -v '^#' licences.tsv | grep -v '^$'; }
members() { cargo metadata -q --no-deps --format-version 1; }
M=$(members)

if [ "$mode" = apply ]; then
  mkdir -p target
  while IFS=$'\t' read -r crate group _ _; do
    d=$(dir_of "$crate"); s=${SPDX[$group]}
    S=$s perl -0pi -e 's/^license(?:\.workspace)?\s*=\s*(?:true|"[^"]*")\s*$/license = "$ENV{S}"/m' "$d/Cargo.toml"
    grep -q '^license = ' "$d/Cargo.toml" || bad "$crate has no license line to set"
    for f in $ALL_FILES; do rm -f "$d/$f"; done
    for f in ${FILES[$group]}; do cp "$f" "$d/$f"; done
  done < <(rows)
  # no inherited licence: a new crate states its own
  perl -0pi -e 's/^(\[workspace\.package\]\n)license = "[^"]*"\n/$1/m' Cargo.toml
  # deny.toml: one exception per AGPL crate, between markers
  {
    echo '# BEGIN licences.sh (generated from licences.tsv)'
    while IFS=$'\t' read -r crate group _ _; do
      [ "$group" = C ] && continue
      printf '[[licenses.exceptions]]\ncrate = "%s"\nallow = ["%s"]\n\n' "$crate" "${SPDX[$group]}"
    done < <(rows)
    echo '# END licences.sh'
  } > target/.deny-exceptions
  perl -0pi -e 'BEGIN{local $/; open F,"<target/.deny-exceptions"; $x=<F>; close F} s/\n# BEGIN licences\.sh.*?# END licences\.sh\n//s; $_ .= "\n$x"' deny.toml
  # README table between markers
  {
    echo '<!-- BEGIN licences.sh (generated from licences.tsv) -->'
    echo '| Crate | Group | Licence (SPDX) | What it is |'
    echo '|-------|-------|----------------|------------|'
    while IFS=$'\t' read -r crate group what _; do
      echo "| \`$crate\` | $group | \`${SPDX[$group]}\` | $what |"
    done < <(rows)
    echo '<!-- END licences.sh -->'
  } > target/.readme-table
  perl -0pi -e 'BEGIN{local $/; open F,"<target/.readme-table"; $x=<F>; close F; chomp $x} s/<!-- BEGIN licences\.sh.*?<!-- END licences\.sh -->/$x/s' README.md
  M=$(members)
fi

# ---- check ------------------------------------------------------------------
diff <(jq -r '.packages[].name' <<<"$M" | sort) <(rows | cut -f1 | sort) >/dev/null \
  || bad "workspace members and licences.tsv differ: $(diff <(jq -r '.packages[].name' <<<"$M" | sort) <(rows | cut -f1 | sort) | tr '\n' ' ')"
declare -A GROUP
while IFS=$'\t' read -r crate group _ _; do
  GROUP[$crate]=$group
  d=$(dir_of "$crate"); want=${SPDX[$group]}
  got=$(jq -r --arg n "$crate" '.packages[] | select(.name == $n) | .license // "none"' <<<"$M")
  [ "$got" = "$want" ] || bad "$crate: Cargo license is '$got', group $group says '$want'"
  for f in $ALL_FILES; do
    if [[ " ${FILES[$group]} " == *" $f "* ]]; then
      { [ -f "$d/$f" ] && cmp -s "$f" "$d/$f"; } || bad "$crate: $d/$f missing or differs from the root's"
    else
      [ ! -e "$d/$f" ] || bad "$crate: $d/$f must not be there (group $group)"
    fi
  done
  grep -qF "| \`$crate\` | $group | \`$want\` |" README.md || bad "$crate: README table row missing or differs"
  [ "$group" = C ] || grep -qF "crate = \"$crate\"" deny.toml || bad "$crate: no deny.toml licence exception"
done < <(rows)
while IFS=$'\t' read -r from to; do
  [ -n "${GROUP[$from]:-}" ] && [ "${GROUP[$to]:-}" = A ] && [ "${GROUP[$from]}" != A ] || continue
  if [ "$strict" = --strict ]; then bad "$from (group ${GROUP[$from]}) depends on $to (group A, plain AGPL): the runtime exception cannot reach it"
  else echo "WARN licences: $from (group ${GROUP[$from]}) depends on $to (group A, plain AGPL): the runtime exception cannot reach it" >&2; fi
done < <(jq -r '.packages[] | .name as $n | .dependencies[] | select(.kind != "dev") | [$n, .name] | @tsv' <<<"$M" | sort -u)
while IFS=$'\t' read -r from to; do
  [ "${GROUP[$from]:-}" = C ] && [ "${GROUP[$to]:-}" = B ] || continue
  echo "NOTE licences: $from (C) depends on $to (B): covered by the runtime exception" >&2
done < <(jq -r '.packages[] | .name as $n | .dependencies[] | select(.kind != "dev") | [$n, .name] | @tsv' <<<"$M" | sort -u)
[ "$fail" = 0 ] && echo "ok   licences ($(rows | wc -l) crates: Cargo, LICENSE files, deny.toml, README)"
exit "$fail"
