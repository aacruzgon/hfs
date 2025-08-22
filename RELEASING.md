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

- Ensure that your build in GitHub Actions has succeeded fully.


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
- Push these changes to main

### 3. Push Changes

After reviewing the changes:

```bash
git push origin main
git push origin --tags
```

### 4. Publish to crates.io

You have two options for publishing:

#### Option A: Let cargo-release handle publishing (Recommended)

Update the release.toml to enable publishing:
```bash
# Edit release.toml and set publish = true
# Then run:
cargo release patch --execute
```

This will handle dependency order and rate limits automatically.

#### Option B: Manual publishing

If you prefer manual control or if cargo-release fails, publish crates in dependency order:

```bash
# First batch - foundational crates
cd crates/fhirpath-support && cargo publish
cd ../fhir-macro && cargo publish

# Wait 2-3 minutes for crates.io to index

# Second batch
cd ../fhir && cargo publish

# Wait 2-3 minutes

# Third batch
cd ../fhir-gen && cargo publish
cd ../fhirpath && cargo publish
cd ../hfs && cargo publish

# Wait 2-3 minutes

# Final crate
cd ../sof && cargo publish
```

### 5. Create GitHub Release

After the tag is pushed, GitHub Actions will automatically:
- Build release artifacts
- Create a GitHub release with the artifacts

## Important Notes

1. **Version Synchronization**: All crates in the workspace share the same version number defined in the root `Cargo.toml`

2. **Rate Limits**: crates.io limits publishing to 5 new crates within a short time period. The publishing script includes delays to avoid this limit.

3. **Dependency Order**: Crates must be published in dependency order:
   - helios-fhirpath-support (no internal deps)
   - helios-fhir-macro (depends on fhirpath-support)
   - helios-fhir (depends on fhir-macro and fhirpath-support)
   - helios-fhir-gen, helios-fhirpath, helios-hfs (depend on fhir)
   - helios-sof (depends on fhir, fhirpath, and fhirpath-support)

4. **Package Size**: The `exclude` fields in Cargo.toml files ensure that large test data and resource files are not included in the published packages.

## Troubleshooting

### Rate Limit Errors

If you encounter rate limit errors, wait the specified time or contact help@crates.io to request a limit increase.

### Version Mismatch Errors

Ensure all internal dependencies use the same version number. The workspace configuration should handle this automatically.

### Large Package Errors

Check that the `exclude` patterns in Cargo.toml files are working correctly:
- `crates/fhir/Cargo.toml`: excludes `tests/data/**`
- `crates/fhir-gen/Cargo.toml`: excludes `resources/**`
