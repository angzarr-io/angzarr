# suggest-version

Analyze repo, suggest next semver.

Steps:
1. Read VERSION file
2. Get changes: `git log --oneline $(git describe --tags --abbrev=0 2>/dev/null || echo "")..HEAD`
3. Analyze commits/changes

Output:
```
Current version: X.Y.Z
Commits since last release: N
Changes: [list]
Suggested next version: X.Y.Z
Reasoning: [semver basis]
```

Semver:
- MAJOR: Breaking/incompatible changes
- MINOR: New backward-compatible features
- PATCH: Bug fixes, docs, minor improvements

Pre-1.0:
- Breaking → MINOR (0.X.0)
- Features → PATCH (0.0.X)

When uncertain, suggest lower bump.

Suggest only — does not execute. To cut the release, hand off
to `release`, which mandates `versionator release push`.