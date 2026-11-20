# release

Release workflow for versionator. User MUST specify version.

User input: $ARGUMENTS
- Fixed: X.Y.Z
- Bump: "patch" | "minor" | "major"

STOP and ask if no version specified. Never guess.

Steps:
1. Parse user input for version or bump instruction
2. `git add -A`
3. Analyze changes; create conventional commit message
4. Bump instruction:
   - Commit with prefix (fix:/feat:/feat!:)
   - `./versionator bump` to increment and amend
5. Fixed version:
   - Write to VERSION file
   - Commit with version in message
6. `./versionator release push` — creates tag/branch and pushes atomically.

CRITICAL — ALWAYS `versionator release push`. Never:
- `versionator release` alone (creates local only; doesn't publish)
- Raw `git push origin <tag>` or `git push origin <branch>`
- Split create/push (causes drift, orphaned tags, conflicts)

`release push` = source of truth for public version. Downstream automation (GitHub Releases, CI/CD, deploys) requires tag+branch on origin together.

If tag/branch created locally, run `versionator release push --force` to reconcile + `git push origin main`.

RULE: Never auto-select version. User specifies explicitly.