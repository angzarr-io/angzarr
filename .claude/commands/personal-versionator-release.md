# release

Release workflow for versionator. User MUST specify version.

Input: $ARGUMENTS
- Fixed: "0.1.0", "1.0.0"
- Bump: "patch", "minor", "major"

No version → STOP and ask. Never guess.

Steps:
1. Parse input
2. `git add -A`
3. Create conventional commit
4. Bump instruction: commit w/ fix:/feat:/feat!:, run `./versionator bump`, amend
5. Fixed version: write VERSION, commit
6. `./versionator release push` → atomic tag+branch+push to origin

ALWAYS use `release push`. Never `release` alone, never raw
`git push origin <tag>`. `release` without `push` only creates
local refs — downstream automation (GitHub Releases, release
completers, deploy pipelines) only fires on the tag-push event.
If you already ran bare `release`, recover with
`release push --force` then `git push origin main`.