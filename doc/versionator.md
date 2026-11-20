# Versionator Integration

Angzarr uses [versionator](https://github.com/benjaminabbitt/versionator) for semantic version management.

## Files

- `VERSION` - Single source of truth for the version number
- `.versionator.yaml` - Configuration for versionator behavior

## Basic Commands

### View Current Version

```bash
versionator version
# Output: 0.1.0
```

### Increment Version

```bash
# Increment patch: 0.1.0 -> 0.1.1
versionator patch increment

# Increment minor: 0.1.1 -> 0.2.0
versionator minor increment

# Increment major: 0.2.0 -> 1.0.0
versionator major increment
```

### Decrement Version

```bash
versionator patch decrement
versionator minor decrement
versionator major decrement
```

### Create Git Tag

```bash
# Create tag v0.1.0 for current version
versionator tag

# With custom message
versionator tag -m "Release 0.1.0 - Initial public release"

# Force overwrite existing tag
versionator tag --force
```

## Emit Version Files

Generate version files in various formats:

```bash
# Rust
versionator emit rust --output src/version.rs

# Python
versionator emit python --output mypackage/_version.py

# Go
versionator emit go --output version.go

# JSON (for CI/CD)
versionator emit json --output version.json

# With pre-release suffix
versionator emit rust --prerelease="alpha-{{CommitsSinceTag}}"

# With build metadata
versionator emit json --metadata="{{BuildDateTimeCompact}}.{{ShortHash}}"
```

## Template Variables

Use Mustache syntax in templates:

| Variable | Example | Description |
|----------|---------|-------------|
| `{{Major}}` | 1 | Major version |
| `{{Minor}}` | 2 | Minor version |
| `{{Patch}}` | 3 | Patch version |
| `{{MajorMinorPatch}}` | 1.2.3 | Full version |
| `{{ShortHash}}` | abc1234 | Git commit (7 chars) |
| `{{BranchName}}` | feature/foo | Current branch |
| `{{CommitsSinceTag}}` | 42 | Commits since last tag |
| `{{BuildDateTimeCompact}}` | 20240115103045 | Build timestamp |

## Configuration

`.versionator.yaml`:

```yaml
# Version prefix for git tags
prefix: "v"

# Pre-release configuration
prerelease:
  enabled: false
  template: "alpha-{{CommitsSinceTag}}"

# Build metadata
metadata:
  enabled: false
  template: "{{BuildDateTimeCompact}}.{{MediumHash}}"
```

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Get version
  id: version
  run: echo "version=$(versionator version)" >> $GITHUB_OUTPUT

- name: Tag release
  if: github.ref == 'refs/heads/main'
  run: |
    versionator tag
    git push --tags
```

## Workflow

1. Make changes and commit
2. When ready to release: `versionator patch increment` (or minor/major)
3. Commit VERSION file: `git add VERSION && git commit -m "Bump version to $(versionator version)"`
4. Create tag: `versionator tag`
5. Push: `git push && git push --tags`

## Multi-Component Versioning

Angzarr uses separate VERSION files for different components:

| Component | VERSION File | Injection Method |
|-----------|--------------|------------------|
| Core (Rust) | `VERSION` | Cargo.toml |
| Rust Client | `client/rust/VERSION` | build.rs → `cargo:rustc-env` |
| Go Client | `client/go/VERSION` | ldflags `-X` |
| Python Client | `client/python/VERSION` | hatch dynamic version |
| Java Client | `client/java/VERSION` | Gradle Kotlin DSL |
| C# Client | `client/csharp/VERSION` | MSBuild property function |
| C++ Client | `client/cpp/VERSION` | CMake file(READ) |

### Version Access by Language

**Rust:**
```rust
use angzarr_client::VERSION;
println!("Client version: {}", VERSION);
```

**Go:**
```go
import angzarr "github.com/benjaminabbitt/angzarr/client/go"
fmt.Println("Client version:", angzarr.Version)
```

**Python:**
```python
import angzarr_client
print(f"Client version: {angzarr_client.__version__}")
```

**Java:**
```java
// Via Gradle properties at build time
String version = BuildConfig.VERSION;
```

**C#:**
```csharp
var assembly = Assembly.GetAssembly(typeof(AngzarrClient));
var version = assembly.GetName().Version;
```

**C++:**
```cpp
#include <angzarr/angzarr.hpp>
std::cout << "Client version: " << angzarr::version() << std::endl;
```

### Releasing Client Libraries

Each client can be versioned independently:

```bash
# Bump Rust client
cd client/rust
versionator patch increment
git add VERSION
git commit -m "Bump rust client to $(cat VERSION)"

# Bump Python client
cd client/python
versionator minor increment
git add VERSION
git commit -m "Bump python client to $(cat VERSION)"
```

## Notes

- The VERSION file contains only the semver string (e.g., `0.1.0`)
- Pre-release and metadata are computed at emit time, not stored in VERSION
- Tags follow the format `v{major}.{minor}.{patch}` by default
- Client libraries are versioned independently from core
