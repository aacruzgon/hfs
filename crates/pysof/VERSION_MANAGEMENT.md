# Version Management Guide for pysof

## Version Sources

pysof version appears in two places and must be kept synchronized:

### 1. Rust Workspace Version
**File**: `Cargo.toml` (repository root)
```toml
[workspace.package]
version = "0.1.26"
```

### 2. Python Package Version
**File**: `crates/pysof/pyproject.toml`
```toml
[project]
version = "0.1.26"
```

## Why Two Versions?

- **Cargo.toml**: Used by Rust build system, inherited by all workspace crates
- **pyproject.toml**: Used by maturin/PyPI for Python package metadata

## Version Update Process

### When Releasing a New Version

1. **Update workspace version**:
   ```toml
   # File: Cargo.toml
   [workspace.package]
   version = "0.1.27"  # Bump version
   ```

2. **Update Python package version**:
   ```toml
   # File: crates/pysof/pyproject.toml
   [project]
   version = "0.1.27"  # Must match workspace version
   ```

3. **Commit both changes together**:
   ```bash
   git add Cargo.toml crates/pysof/pyproject.toml
   git commit -m "chore: bump version to 0.1.27"
   ```

4. **Create version tag**:
   ```bash
   git tag -a v0.1.27 -m "Release v0.1.27"
   git push origin main
   git push origin v0.1.27
   ```

## Verification

Before tagging, verify versions match:

```bash
# Check workspace version
grep 'version = ' Cargo.toml | head -1

# Check Python package version  
grep 'version = ' crates/pysof/pyproject.toml | head -1

# Should output the same version number
```

## Future Automation

Consider adding a CI check to prevent version drift:

```yaml
# .github/workflows/ci.yml
- name: Verify version sync
  run: |
    RUST_VERSION=$(grep -m1 'version = ' Cargo.toml | cut -d'"' -f2)
    PYTHON_VERSION=$(grep -m1 'version = ' crates/pysof/pyproject.toml | cut -d'"' -f2)
    if [ "$RUST_VERSION" != "$PYTHON_VERSION" ]; then
      echo "❌ Version mismatch: Rust=$RUST_VERSION, Python=$PYTHON_VERSION"
      exit 1
    fi
    echo "✅ Versions match: $RUST_VERSION"
```

## Current Version

**Latest**: 0.1.26 (synchronized as of 2025-01-11)

---

**Note**: Always update both versions together in the same commit to maintain synchronization.
