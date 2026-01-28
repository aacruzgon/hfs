# Helios FHIR Server (HFS)

A high-performance FHIR server built in Rust.

## Features

- Full FHIR RESTful API support 
- Multiple FHIR version support
- Pluggable storage backends (SQLite, PostgreSQL, MongoDB)
- Content negotiation (JSON)
- Conditional operations with ETag support
- Multi-tenant support via X-Tenant-ID header
- CORS support

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/HeliosSoftware/hfs.git
cd hfs

# Build with default features (R4 + SQLite)
cargo build --release -p helios-hfs

# Build with all FHIR versions
cargo build --release -p helios-hfs --features R4,R4B,R5,R6,sqlite
```

## Usage

### Running the Server

```bash
# Run with default settings (R4, SQLite, port 8080)
./target/release/hfs

# Specify a different port
./target/release/hfs --port 3000

# Use an in-memory database
./target/release/hfs --database-url :memory:

# Enable debug logging
./target/release/hfs --log-level debug
```

### Command Line Options

```
Usage: hfs [OPTIONS]

Options:
      --port <PORT>              Server port [env: REST_SERVER_PORT=] [default: 8080]
      --host <HOST>              Host to bind [env: REST_SERVER_HOST=] [default: 127.0.0.1]
      --log-level <LOG_LEVEL>    Log level (error, warn, info, debug, trace)
                                 [env: REST_LOG_LEVEL=] [default: info]
      --database-url <URL>       Database connection URL [env: DATABASE_URL=]
      --max-body-size <BYTES>    Maximum request body size [env: REST_MAX_BODY_SIZE=]
                                 [default: 10485760]
      --request-timeout <SECS>   Request timeout in seconds [env: REST_REQUEST_TIMEOUT=]
                                 [default: 30]
      --enable-cors              Enable CORS [env: REST_ENABLE_CORS=] [default: true]
      --cors-origins <ORIGINS>   Allowed CORS origins [env: REST_CORS_ORIGINS=] [default: *]
  -h, --help                     Print help
  -V, --version                  Print version
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REST_SERVER_PORT` | 8080 | Server port |
| `REST_SERVER_HOST` | 127.0.0.1 | Host to bind |
| `REST_LOG_LEVEL` | info | Log level (error, warn, info, debug, trace) |
| `DATABASE_URL` | fhir.db | Database connection string |
| `REST_MAX_BODY_SIZE` | 10485760 | Max request body size (bytes) |
| `REST_REQUEST_TIMEOUT` | 30 | Request timeout (seconds) |
| `REST_ENABLE_CORS` | true | Enable CORS |
| `REST_CORS_ORIGINS` | * | Allowed CORS origins |
| `REST_CORS_METHODS` | GET,POST,PUT,DELETE,OPTIONS | Allowed HTTP methods |
| `REST_CORS_HEADERS` | Content-Type,Authorization,X-Requested-With | Allowed headers |
| `REST_DEFAULT_TENANT` | default | Default tenant ID |

## FHIR Version Support

Build with specific FHIR versions using feature flags:

```bash
# R4 only (default)
cargo build -p helios-hfs --features R4,sqlite

# R5 only
cargo build -p helios-hfs --no-default-features --features R5,sqlite

# Multiple versions
cargo build -p helios-hfs --features R4,R4B,R5,R6,sqlite
```

## Database Backends

### SQLite (Default)

```bash
cargo build -p helios-hfs --features sqlite

# Run with file-based database
./target/release/hfs --database-url ./data/fhir.db

# Run with in-memory database
./target/release/hfs --database-url :memory:
```

### PostgreSQL

```bash
cargo build -p helios-hfs --no-default-features --features R4,postgres

./target/release/hfs --database-url "postgresql://user:pass@localhost/fhir"
```

### MongoDB

```bash
cargo build -p helios-hfs --no-default-features --features R4,mongodb

./target/release/hfs --database-url "mongodb://localhost:27017/fhir"
```

## API Endpoints

| Interaction | Method | URL |
|------------|--------|-----|
| capabilities | GET | `/metadata` |
| read | GET | `/[type]/[id]` |
| vread | GET | `/[type]/[id]/_history/[vid]` |
| update | PUT | `/[type]/[id]` |
| patch | PATCH | `/[type]/[id]` |
| delete | DELETE | `/[type]/[id]` |
| create | POST | `/[type]` |
| search | GET/POST | `/[type]?params` or `/[type]/_search` |
| history (instance) | GET | `/[type]/[id]/_history` |
| history (type) | GET | `/[type]/_history` |
| history (system) | GET | `/_history` |
| batch/transaction | POST | `/` |
| health | GET | `/health` |

## Examples

### Create a Patient

```bash
curl -X POST http://localhost:8080/Patient \
  -H "Content-Type: application/fhir+json" \
  -d '{
    "resourceType": "Patient",
    "name": [{"family": "Smith", "given": ["John"]}],
    "birthDate": "1970-01-01"
  }'
```

### Read a Patient

```bash
curl http://localhost:8080/Patient/123
```

### Search for Patients

```bash
curl "http://localhost:8080/Patient?family=Smith"
```

### Get CapabilityStatement

```bash
curl http://localhost:8080/metadata
```

## Multi-Tenant Support

Use the `X-Tenant-ID` header to isolate data between tenants:

```bash
curl -H "X-Tenant-ID: clinic-a" http://localhost:8080/Patient
curl -H "X-Tenant-ID: clinic-b" http://localhost:8080/Patient
```

