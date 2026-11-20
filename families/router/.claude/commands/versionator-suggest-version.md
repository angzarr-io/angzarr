# suggest-version

Analyze repo and suggest next semantic version.

Steps:
1. Read current version from VERSION file
2. Get changes: `git log --oneline $(git describe --tags --abbrev=0 2>/dev/null || echo "")..HEAD`
3. Analyze commit messages and code changes

Output:
```
Current version: X.Y.Z
Commits since last release: N

Changes:
- [key changes list]

Suggested next version: X.Y.Z
Reasoning: [semver justification]
```

Semver:
- MAJOR (X): Breaking API changes
- MINOR (Y): New backward-compatible features
- PATCH (Z): Bug fixes, minor improvements

Pre-1.0: Breaking changes → MINOR (0.X.0), features → PATCH (0.0.X)

**BE CONSERVATIVE** — uncertain cases use lower bump.

**HAND OFF to `release` prompt** for actual release execution (mandates `versionator release push`, never bare `versionator release` or raw `git push`).