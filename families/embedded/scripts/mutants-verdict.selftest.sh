#!/usr/bin/env bash
# Self-test of the mutation gates' verdict, `_mutants-verdict` in
# gates/gates.just (ctxloom rulings 99/105: inline jq over cargo-mutants' own
# outcomes.json, its `outcomes[]` and its `total_mutants`; no log is read),
# over fixture cargo-mutants outputs, and of `_mutants-no-features`, the
# mutation recipes' refusal of a non-empty `feature_rules`.
#
# Checks: a pass; a tier below its threshold; mutants found and fewer tested;
# a file under no threshold; no mutants at all; a run whose every mutant was
# unviable judged nothing and FAILS, while one tier all unviable beside a tier
# that judged passes (owner rulings 120, 124); cargo-mutants' red baseline (exit 4) and
# a run that ended before a verdict fail; and a feature rule is refused.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
top=$(cd "$here/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail() { echo "FAIL mutants-verdict.selftest: $*" >&2; exit 1; }
thresholds="crates/core/domain=90 crates/core/application=90"
gate() { just --justfile "$top/justfile" "$@"; }

# fixture <name> <file:summary> ...: a cargo-mutants output (outcomes.json)
# whose mutants are in <file>, judged <summary>, `total_mutants` their count.
fixture() {
    local dir=$tmp/$1; shift
    mkdir -p "$dir/mutants.out"
    local n=0 outcomes='[{"scenario":"Baseline","summary":"Success"}]'
    for spec in "$@"; do
        n=$((n + 1))
        IFS=: read -r file summary <<< "$spec"
        outcomes=$(jq --arg f "$file" --arg n "$file:$n: replace f with ()" --arg s "$summary" \
            '. + [{scenario: {Mutant: {name: $n, file: $f}}, summary: $s}]' <<< "$outcomes")
    done
    jq -n --argjson o "$outcomes" --argjson n "$n" '{outcomes: $o, total_mutants: $n}' \
        > "$dir/mutants.out/outcomes.json"
    echo "$dir"
}
judge() { gate _mutants-verdict mutants-selftest "$1" "${2:-0}" "$thresholds" 2>&1; }
D=crates/core/domain/x/src/lib.rs
A=crates/core/application/y/src/lib.rs

dir=$(fixture pass "$D:CaughtMutant" "$D:Timeout" "$A:CaughtMutant" "$A:Unviable")
said=$(judge "$dir") || fail "a pass failed: $said"
grep -q "pass  crates/core/application: 1/1 killed (100%), threshold 90%; 1 unviable, not counted" <<< "$said" \
    || fail "a pass's report: $said"
grep -q "pass  crates/core/domain: 2/2 killed (100%), threshold 90%; 1 timed out, counted killed" <<< "$said" \
    || fail "a pass's report: $said"

dir=$(fixture low "$D:CaughtMutant" "$D:MissedMutant")
if said=$(judge "$dir"); then fail "a tier below its threshold passed: $said"; fi
grep -q "below the kill-rate threshold: crates/core/domain: 1/2" "$dir/verdict.txt" || fail "low verdict: $(cat "$dir/verdict.txt")"

dir=$(fixture short "$D:CaughtMutant")
jq '.total_mutants = 2' "$dir/mutants.out/outcomes.json" > "$tmp/o" && mv "$tmp/o" "$dir/mutants.out/outcomes.json"
if said=$(judge "$dir"); then fail "a run that tested fewer than it found passed: $said"; fi
grep -q "2 mutants found but 1 tested" "$dir/verdict.txt" || fail "short verdict: $(cat "$dir/verdict.txt")"

dir=$(fixture untiered "crates/elsewhere/src/lib.rs:CaughtMutant")
if said=$(judge "$dir"); then fail "an untiered file passed: $said"; fi
grep -q "no threshold in mutation_thresholds" "$dir/verdict.txt" || fail "untiered verdict"

mkdir -p "$tmp/none"
said=$(judge "$tmp/none") || fail "no mutants failed: $said"
grep -q "no mutants" <<< "$said" || fail "no mutants said: $said"
dir=$(fixture empty)
said=$(judge "$dir") || fail "zero mutants found failed: $said"
grep -q "no mutants" <<< "$said" || fail "zero mutants said: $said"

# Owner rulings 120, 124 (amending 99): "judged none" is the WHOLE RUN's
# rule. One tier whose every mutant was unviable, beside a tier that judged,
# passes, and its report says it judged none; a run that judged none at all
# (every mutant unviable, in one tier or several) has shown nothing, and fails.
dir=$(fixture onehollow "$D:CaughtMutant" "$A:Unviable" "$A:Unviable")
said=$(judge "$dir") || fail "one tier all unviable beside a judged tier failed: $said"
grep -q "pass  crates/core/application: none judged; 2 unviable, not counted" <<< "$said" \
    || fail "the all-unviable tier's report: $said"
dir=$(fixture hollow "$A:Unviable" "$A:Unviable")
if said=$(judge "$dir"); then fail "a run that judged nothing (all unviable) passed: $said"; fi
grep -q "the run judged nothing: 2 mutants found and none judged (every one unviable)" "$dir/verdict.txt" \
    || fail "hollow verdict: $(cat "$dir/verdict.txt" 2>/dev/null)"
dir=$(fixture hollows "$D:Unviable" "$A:Unviable")
if said=$(judge "$dir"); then fail "a run all unviable across tiers passed: $said"; fi
grep -q "the run judged nothing: 2 mutants found" "$dir/verdict.txt" \
    || fail "hollows verdict: $(cat "$dir/verdict.txt" 2>/dev/null)"

# cargo-mutants' own exit codes: 4 is its baseline (the tests fail
# unmutated), kept as the gate's; any code but 0, 2, 3 and 4 is a run that
# ended before a verdict.
dir=$(fixture red "$D:CaughtMutant")
rc=0; said=$(judge "$dir" 4) || rc=$?
[ "$rc" -eq 4 ] || fail "a red baseline exited $rc, not 4: $said"
grep -q "fail unmutated" "$dir/verdict.txt" || fail "red verdict: $(cat "$dir/verdict.txt" 2>/dev/null)"
dir=$(fixture broke "$D:CaughtMutant")
if said=$(judge "$dir" 1); then fail "a run cargo mutants ended before a verdict passed: $said"; fi
grep -q "failed before a verdict (exit 1)" "$dir/verdict.txt" || fail "broke verdict"

# No feature rule reaches the mutation run; a crate turns its own features on
# through a dev-dependency on itself.
gate _mutants-no-features mutants-selftest >/dev/null 2>&1 || fail "an empty feature_rules was refused"
if said=$(gate --set feature_rules "angzarr-embedded=some-feature" _mutants-no-features mutants-selftest 2>&1); then
    fail "a feature rule was not refused: $said"
fi
grep -q "feature_rules" <<< "$said" || fail "the refusal names feature_rules: $said"
echo "ok   mutants-verdict.selftest"
