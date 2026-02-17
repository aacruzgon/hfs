# helios-serde

Version-agnostic serialization for FHIR resources (R4, R4B, R5, R6).

## Features

**JSON** support is always available — no extra feature flags required.

**XML** support requires the `xml` feature flag:

```toml
[dependencies]
helios-serde = { version = "...", features = ["xml"] }
```

### Performance note

Enabling the `xml` feature introduces a ~2% overhead on JSON deserialization. For optimal performance, do not enable it if you don't need XML support.
