# helios-rest

FHIR RESTful API implementation for the Helios FHIR Server.

## Overview

This crate provides a complete implementation of the [FHIR RESTful API](https://hl7.org/fhir/http.html) specification, including:

- **Full CRUD Support**: Create, Read, Update, Delete for all FHIR resource types
- **Versioning**: Version history with vread and history interactions
- **Conditional Operations**: Conditional create, update, delete, and patch
- **Search**: Type-level and system-level search with modifiers
- **Batch/Transaction**: Bundle processing with atomic transaction support
- **Content Negotiation**: JSON format support with proper MIME types
- **Multi-Tenant**: Built-in tenant isolation for multi-tenant deployments

## Quick Start

```rust
use helios_rest::{create_app_with_config, ServerConfig};
use helios_persistence::backends::sqlite::SqliteBackend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a storage backend
    let backend = SqliteBackend::new("fhir.db")?;
    backend.init_schema()?;

    // Configure the server
    let config = ServerConfig::default();

    // Create the Axum application
    let app = create_app_with_config(backend, config.clone());

    // Start the server
    let listener = tokio::net::TcpListener::bind(config.socket_addr()).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
```

## Running the Server

```bash
# Run with default settings (SQLite, port 8080)
cargo run --bin rest-server

# Configure via environment variables
REST_SERVER_PORT=3000 REST_LOG_LEVEL=debug cargo run --bin rest-server

# Or via command line arguments
cargo run --bin rest-server -- --port 3000 --log-level debug
```

## API Endpoints

| Interaction | Method | URL Pattern |
|------------|--------|-------------|
| read | GET | `/[type]/[id]` |
| vread | GET | `/[type]/[id]/_history/[vid]` |
| update | PUT | `/[type]/[id]` |
| patch | PATCH | `/[type]/[id]` |
| delete | DELETE | `/[type]/[id]` |
| create | POST | `/[type]` |
| search | GET/POST | `/[type]?params` or `/[type]/_search` |
| capabilities | GET | `/metadata` |
| history (instance) | GET | `/[type]/[id]/_history` |
| history (type) | GET | `/[type]/_history` |
| history (system) | GET | `/_history` |
| batch/transaction | POST | `/` |

## Configuration

The server is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REST_SERVER_PORT` | 8080 | Server port |
| `REST_SERVER_HOST` | 127.0.0.1 | Host to bind |
| `REST_LOG_LEVEL` | info | Log level |
| `REST_MAX_BODY_SIZE` | 10485760 | Max request body (bytes) |
| `REST_REQUEST_TIMEOUT` | 30 | Request timeout (seconds) |
| `REST_ENABLE_CORS` | true | Enable CORS |
| `REST_DEFAULT_TENANT` | default | Default tenant ID |
| `REST_DATABASE_URL` | - | Database connection string |

## Features

Enable different FHIR versions and backends via Cargo features:

```toml
[dependencies]
helios-rest = { version = "0.1", features = ["R4", "sqlite"] }
```

### FHIR Versions
- `R4` (default) - FHIR R4 (4.0.1)
- `R4B` - FHIR R4B (4.3.0)
- `R5` - FHIR R5 (5.0.0)
- `R6` - FHIR R6 (6.0.0-ballot)

### Backends
- `sqlite` (default) - SQLite (great for development)
- `postgres` - PostgreSQL (recommended for production)
- `mongodb` - MongoDB

## HTTP Headers

The server supports standard FHIR HTTP headers:

| Header | Purpose |
|--------|---------|
| `Accept` | Content negotiation |
| `Content-Type` | Request body format |
| `ETag` / `If-Match` | Optimistic locking |
| `If-None-Match` | Conditional read |
| `If-None-Exist` | Conditional create |
| `If-Modified-Since` | Conditional read by date |
| `Prefer` | Response preference |
| `X-Tenant-ID` | Multi-tenant identification |

## Error Handling

All errors are returned as FHIR OperationOutcome resources:

```json
{
  "resourceType": "OperationOutcome",
  "issue": [{
    "severity": "error",
    "code": "not-found",
    "details": {
      "text": "Resource Patient/123 not found"
    }
  }]
}
```

## Testing

Tests use a JSON-driven specification format:

```bash
# Run all tests
cargo test -p helios-rest

# Run with specific backend
cargo test -p helios-rest --features sqlite
```

### Test Specifications

Tests are defined in JSON files under `tests/specs/`:

```json
{
  "name": "Patient Read Tests",
  "tests": [
    {
      "name": "read_existing_patient",
      "request": {
        "method": "GET",
        "path": "/Patient/123"
      },
      "expect": {
        "status": 200,
        "body": {
          "resourceType": "Patient",
          "id": "123"
        }
      }
    }
  ]
}
```

## Architecture

```
src/
├── lib.rs          # Crate entry point
├── config.rs       # Server configuration
├── error.rs        # Error types → OperationOutcome
├── state.rs        # Application state
├── handlers/       # HTTP request handlers
├── middleware/     # Axum middleware
├── extractors/     # Axum extractors
├── responses/      # Response formatting
└── routing/        # Route configuration
```

## License

MIT
