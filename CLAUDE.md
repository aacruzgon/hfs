# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

**Note:** Build times can exceed 10 minutes, especially for full workspace builds with all features or when building the FHIR generator due to large generated files.

### Building
```bash
# Build default (R4 only)
cargo build

# Build with all FHIR versions
cargo build --features R4,R4B,R5,R6

# Build specific crate
cargo build -p helios-sof
cargo build -p helios-fhirpath
```

### Testing
```bash
# Run all tests (default R4)
cargo test

# Test with all FHIR versions
cargo test --features R4,R4B,R5,R6

# Test specific crate
cargo test -p helios-sof
cargo test -p helios-fhirpath

# Run single test
cargo test test_name_pattern

# Run tests in specific file
cargo test --test test_file_name

# Show test output
cargo test -- --nocapture
```

### Linting and Formatting
```bash
# Format code
cargo fmt --all

# Lint code (with CI-compatible flags)
cargo clippy --all-targets --all-features -- -D warnings \
  -A clippy::items_after_test_module \
  -A clippy::large_enum_variant \
  -A clippy::question_mark \
  -A clippy::collapsible_match \
  -A clippy::collapsible_if \
  -A clippy::field_reassign_with_default \
  -A clippy::doc-overindented-list-items \
  -A clippy::doc-lazy-continuation

# Check types without building
cargo check
```

### Before Completing Code Changes
Before declaring a plan complete after significant code changes, always run:
1. `cargo fmt --all` - Format all code
2. `cargo clippy` with the CI flags shown above - Fix any linting issues
3. `cargo test` for affected crates - Ensure tests pass

### Documentation
```bash
# Generate and view docs
cargo doc --no-deps --open
```

### FHIR Code Generation
```bash
# Generate FHIR models for all versions
cargo build -p helios-fhir-gen --features R6
./target/debug/fhir_gen --all

# Note: R6 specification files are auto-downloaded from HL7 build server
# Note: Building fhir-gen can take 5-10 minutes due to large generated files
```

### SQL-on-FHIR Executables
```bash
# Run CLI tool
cargo run --bin sof-cli -- --view view.json --bundle data.json --format csv

# Run HTTP server (default port 8080)
cargo run --bin sof-server

# With environment variables
SOF_SERVER_PORT=3000 SOF_LOG_LEVEL=debug cargo run --bin sof-server
```

## Architecture Overview

### Workspace Structure
The project is a Rust workspace with 7 main crates:

1. **`helios-fhir`** - Core FHIR data models (auto-generated)
   - Supports R4, R4B, R5, R6 via feature flags
   - Version-specific modules: `r4.rs`, `r4b.rs`, etc.
   - Code generated from official FHIR JSON schemas

2. **`helios-fhir-gen`** - Code generator for FHIR models
   - Generates Rust structs from FHIR JSON schemas
   - Run with `./target/debug/fhir_gen --all` after building
   - R6 specs auto-downloaded from HL7 build server

3. **`helios-fhirpath`** - FHIRPath expression language implementation
   - Parser based on ANTLR grammar using chumsky
   - Comprehensive function support (see README for feature matrix)
   - Version-aware type checking with auto-detection
   - Namespace resolution for FHIR and System types

4. **`helios-sof`** - SQL-on-FHIR implementation (actively developed)
   - Two binaries: `sof-cli` and `sof-server`
   - ViewDefinition processing for tabular data transformation
   - HTTP API with Axum framework
   - `$viewdefinition-run` operation with extensive parameters

5. **`helios-fhir-macro`** - Procedural macros for FHIR functionality

6. **`helios-fhirpath-support`** - Support utilities for FHIRPath

7. **`helios-hfs`** - Main FHIR server binary
   - Combines `helios-rest` with storage backends
   - Binary name: `hfs`
   - Supports SQLite, PostgreSQL, MongoDB backends
   - See `crates/hfs/README.md` for configuration

### Key Design Patterns

#### Version-Agnostic Abstraction
The codebase uses enum wrappers and traits to handle multiple FHIR versions:

```rust
// Example from sof crate
pub enum SofViewDefinition {
    R4(fhir::r4::ViewDefinition),
    R4B(fhir::r4b::ViewDefinition),
    R5(fhir::r5::ViewDefinition),
    R6(fhir::r6::ViewDefinition),
}
```

#### Trait-Based Processing
Core functionality is defined through traits, allowing version-independent logic:
- `ViewDefinitionTrait`
- `BundleTrait`
- `ResourceTrait`

### Active Development Areas
Currently focused on the `helios-sof` crate:
- Implementation of `$viewdefinition-run` operation
- Server API enhancements
- Parameter validation
- Test coverage improvements

## Environment Setup

### LLD Linker Configuration
Add to `~/.cargo/config.toml`:
```toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

### Memory-Constrained Builds
```bash
export CARGO_BUILD_JOBS=4
```

## SOF Server Configuration

### Environment Variables
- `SOF_SERVER_PORT` - Server port (default: 8080)
- `SOF_SERVER_HOST` - Host to bind (default: 127.0.0.1)
- `SOF_LOG_LEVEL` - Log level: error, warn, info, debug, trace (default: info)
- `SOF_MAX_BODY_SIZE` - Max request body size in bytes (default: 10485760)
- `SOF_REQUEST_TIMEOUT` - Request timeout in seconds (default: 30)
- `SOF_ENABLE_CORS` - Enable CORS (default: true)
- `SOF_CORS_ORIGINS` - Allowed CORS origins (default: *)
- `SOF_CORS_METHODS` - Allowed HTTP methods (default: GET,POST,PUT,DELETE,OPTIONS)
- `SOF_CORS_HEADERS` - Allowed headers (default: Content-Type,Authorization,X-Requested-With)

### API Endpoints
- `GET /metadata` - Returns CapabilityStatement
- `GET /health` - Health check endpoint
- `POST /ViewDefinition/$viewdefinition-run` - Execute ViewDefinition transformation
  - Parameters (in request body or query):
    - `_format` - Output format (csv, ndjson, json, parquet)
    - `header` - CSV header control (true/false)
    - `viewResource` - ViewDefinition resource
    - `resource` - FHIR resources to transform
    - `patient` - Filter by patient reference
    - `_limit` - Limit results (1-10000)
    - `_since` - Filter by modification time
  - Parameter precedence: Request body > Query params > Accept header

### Parquet Export
The SOF implementation now supports Apache Parquet format export with the following features:
- Automatic schema inference from data
- Support for all FHIR primitive types following Pathling conventions:
  - boolean → BOOLEAN
  - string/code/uri → UTF8
  - integer → INT32
  - decimal → FLOAT64
  - dateTime/date → UTF8
- Arrays/collections mapped to Arrow List types
- All fields are OPTIONAL to handle FHIR's nullable nature
- Snappy compression by default
- Complex objects serialized as JSON strings

Usage:
```bash
# CLI
cargo run --bin sof-cli -- --view view.json --bundle data.json --format parquet

# Server
curl -X POST http://localhost:8080/ViewDefinition/\$viewdefinition-run \
  -H "Content-Type: application/json" \
  -d '{"_format": "parquet", "viewResource": {...}, "resource": [...]}'
```

## Testing Patterns

### FHIRPath Tests
- Test cases in `crates/fhirpath/tests/`
- Official FHIR test cases from `fhir-test-cases` repository

### SQL-on-FHIR Tests
- Unit tests in `src/` files
- Integration tests in `tests/` directory
- Parameter validation tests being added

### Test Data
- FHIR examples in `crates/fhir/tests/data/`
- ViewDefinition examples in test files

## Common Development Tasks

### Adding a New FHIRPath Function
1. Add function implementation in appropriate module under `crates/fhirpath/src/`
2. Update parser if needed in `parser.rs`
3. Add test cases covering the function
4. Update feature matrix in README.md

### Working with ViewDefinitions
1. ViewDefinition JSON goes through version-specific parsing
2. Wrapped in `SofViewDefinition` enum for version-agnostic processing
3. Use `run_view_definition()` for transformation

### Debugging Tips
- Use `cargo test -- --nocapture` to see println! output
- Enable trace logging: `RUST_LOG=trace cargo run`
- FHIRPath expressions can be tested independently

## Important Notes

- Default FHIR version is R4 when no features specified
- The project follows standard Rust conventions
- Git status shows active work on `sof` crate files
- Server returns appropriate HTTP status codes and FHIR OperationOutcomes for errors