# Release Process for HFS

This document describes the release process for the Helios FHIR Server (HFS) project.

## Overview

HFS is a multi-crate Rust workspace where all crates share the same version number. This ensures compatibility and simplifies dependency management.

## Prerequisites

1. Install `cargo-release`:
   ```bash
   cargo install cargo-release
   ```

2. Ensure you have publishing rights on crates.io for all HFS crates

3. Set up your crates.io token:
   ```bash
   cargo login
   ```

## Release Steps

### 1. Prepare for Release

- Consider downloading the latest FHIR Specification (R6) and test files, [generating](readme.md#code-generation), then running a [comprehensive test](readme.md#running-tests), and checking in the latest into GitHub. 
```bash
cargo build -p helios-fhir-gen --features R4,R4B,R5,R6
./target/debug/helios-fhir-gen --all
cargo fmt --all 
cargo test --workspace --all-features
```
- Check in these changes.
- Ensure that your build in GitHub Actions has succeeded fully.  These are found in [ci.yml](.github/workflows/ci.yml).

### 1.1. Update pysof Python Package Version

**Important**: The `pysof` Python package version must be manually synchronized with the workspace version.

Since `cargo-release` only updates Rust `Cargo.toml` files, you must manually update the Python package version:

```bash
# Edit crates/pysof/pyproject.toml
# Update the version field to match the new workspace version
# For example, if releasing 0.1.26:
version = "0.1.26"
```

**Why this is necessary:**
- `pysof/Cargo.toml` uses `version.workspace = true` (handled by cargo-release)
- `pyproject.toml` must be manually updated since cargo-release doesn't modify Python files
- Version mismatch will cause confusion about which Rust version corresponds to the Python package

**Verification:**
```bash
# After updating, verify versions match:
grep "^version" Cargo.toml                    # Should show workspace version
grep "^version" crates/pysof/pyproject.toml  # Should match workspace version
```

### 2. Create a Release

To create a new release, use cargo-release with the appropriate version bump:

```bash
# For a patch release (0.1.3 -> 0.1.4)
cargo release patch --dry-run

# For a minor release (0.1.3 -> 0.2.0)
cargo release minor --dry-run

# For a major release (0.1.3 -> 1.0.0)
cargo release major --dry-run
```

Review the dry-run output, then execute without `--dry-run`:

```bash
cargo release patch --execute
```

This will:
- Update version numbers in all Cargo.toml files
- Update internal dependency versions
- Create a git commit with the version bump
- Create a git tag
- Publish to crates.io
- Push these changes to GitHub

After the tag is pushed, GitHub Actions will automatically:
- Build release artifacts
- Create a [GitHub Release](https://github.com/HeliosSoftware/hfs/releases) with the artifacts
- Build pysof wheels for multiple platforms (Linux, Windows, macOS)
- Publish pysof to PyPI (if `PYPI_API_TOKEN` secret is configured)

### 3. Verify PyPI Release

After the release is complete, verify the PyPI package:

1. **Check PyPI page**: Visit https://pypi.org/project/pysof/
   - Verify version number is correct
   - Confirm all project links are visible (Homepage, Repository, Documentation, Bug Tracker, Source)
   - Check that README renders correctly with badges and formatting
   - Verify supported platforms list is accurate

2. **Test installation**:
   ```bash
   # Create a fresh environment
   python -m venv test-env
   source test-env/bin/activate  # On Windows: test-env\Scripts\activate
   
   # Install from PyPI
   pip install pysof
   
   # Verify version
   python -c "import pysof; print(pysof.__version__)"
   
   # Quick functionality test
   python -c "import pysof; print(pysof.get_supported_fhir_versions())"
   ```

3. **Verify wheel availability**:
   - Check that wheels are available for all supported platforms
   - Download a wheel and inspect metadata: `unzip -p pysof-*.whl */METADATA`

For a detailed pre-release checklist, see [PYPI_CHECKLIST.md](PYPI_CHECKLIST.md).

## PyPI-Only Releases

If you need to release only the Python package without a full workspace release:

```bash
cd crates/pysof

# Update version in pyproject.toml manually

# Build wheels
uv run maturin build --release -o dist

# Upload to TestPyPI first (recommended)
twine upload --repository testpypi dist/*

# After verification, upload to PyPI
twine upload dist/*
```

## Automation Considerations

**Future improvement**: Consider automating the pyproject.toml version update:

- **Option 1**: Pre-commit hook that syncs versions before commit
- **Option 2**: GitHub Actions workflow that validates version consistency
- **Option 3**: Custom cargo-release hook (via `release.toml`)
- **Option 4**: Build script that reads Cargo.toml and updates pyproject.toml

Until automated, manual verification is required for each release.

