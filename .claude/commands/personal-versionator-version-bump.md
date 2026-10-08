# version-bump

Analyze changes, recommend semver bump:

Current: $1
Changes: $2

- Breaking API → major
- New features (backward-compat) → minor
- Bug fixes → patch
- Pre-1.0: different rules

Recommend version + reasoning.

Recommends only — does not execute. To cut, use `release`
(which mandates `versionator release push`).