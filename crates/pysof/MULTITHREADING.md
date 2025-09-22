# Multithreading Demo for PySOF

This demonstrates the newly added multithreading support in both the CLI and Python bindings.

## CLI Usage with Threading

```bash
# Basic usage with threading
sof-cli -v view_definition.json -b patient_bundle.json --threads 4

# Combined with other options
sof-cli -v view_definition.json -b patient_bundle.json \
    --threads 8 --limit 1000 --format json \
    --since 2024-01-01T00:00:00Z

# Use maximum available threads (let system decide)
sof-cli -v view_definition.json -b patient_bundle.json
```

## Python Usage with Threading

```python
import pysof

# Sample data
view_definition = {
    "resourceType": "ViewDefinition",
    "id": "patient-demographics",
    "name": "PatientDemographics",
    "status": "active", 
    "resource": "Patient",
    "select": [{
        "column": [
            {"name": "id", "path": "id"},
            {"name": "family_name", "path": "name.family"},
            {"name": "given_name", "path": "name.given.first()"},
            {"name": "gender", "path": "gender"},
            {"name": "birth_date", "path": "birthDate"}
        ]
    }]
}

bundle = {
    "resourceType": "Bundle",
    "type": "collection",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-1",
                "name": [{"family": "Doe", "given": ["John"]}],
                "gender": "male",
                "birthDate": "1990-01-01"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-2", 
                "name": [{"family": "Smith", "given": ["Jane"]}],
                "gender": "female",
                "birthDate": "1985-05-15"
            }
        }
        # ... more patients
    ]
}

# Single-threaded (default)
result = pysof.run_view_definition(view_definition, bundle, "json")

# Multi-threaded with options
result = pysof.run_view_definition_with_options(
    view_definition,
    bundle,
    "json",
    limit=1000,           # Limit results
    page=1,               # Pagination 
    since="2024-01-01T00:00:00Z",  # Filter by date
    num_threads=8,        # Use 8 threads for parallel processing
    fhir_version="R4"     # FHIR version
)

print(f"Processed {len(result)} bytes of output")
```

## Threading Benefits

- **Performance**: Parallel processing of large FHIR Bundles
- **GIL Release**: Python GIL is released during Rust execution
- **Resource Control**: Specify thread count or let system optimize
- **Backward Compatible**: Existing code continues to work

## Thread Count Guidelines

- **1-2 threads**: Small datasets, I/O bound operations
- **4-8 threads**: Medium datasets, balanced CPU/I/O
- **8+ threads**: Large datasets, CPU-intensive transformations
- **None/Default**: Let the system choose optimal thread count

## Notes

- The basic `run_view_definition()` function uses default threading
- Use `run_view_definition_with_options()` for explicit thread control
- CLI `--threads` option controls parallelism in command-line usage
- All threading happens in Rust code; Python GIL is properly released