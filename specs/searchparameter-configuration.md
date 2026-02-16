# How FHIR Servers Enable SearchParameter Configuration

## Question

How do FHIR Servers enable the configuration of SearchParameters? Are they enabled as a result of POSTing them to a FHIR server alone, or are they enabled and disabled as a result of an operation? This analysis looks at the Aidbox implementation and the HAPI FHIR implementation.

---

## Summary: SearchParameter Enablement Approaches

### Aidbox

Aidbox takes a **"POST and immediately active"** approach:

1. **Activation mechanism**: When you POST a SearchParameter resource to `/fhir/SearchParameter`, it becomes immediately available for searching. There's no separate operation required to enable it.

2. **No explicit reindexing required**: Aidbox uses a document-store approach with PostgreSQL JSONB. Aidbox uses PostgreSQL database for storage, with most resource data contained in a `resource` column with jsonb type. Search queries are translated directly to JSONB containment operators (`@>`) at query time, so there's no pre-computed index that needs rebuilding when you add a SearchParameter.

3. **Optional index optimization**: Database indexes are essential for performance, particularly to speed up search requests. Aidbox provides RPC methods like `aidbox.index/suggest-index` to recommend PostgreSQL GIN indexes for performance, but these are optional optimizations rather than required enablement steps.

4. **Status field**: The `status` field (draft/active/retired) appears to be informational rather than controlling whether the parameter is actually usable.

#### Example: Creating a SearchParameter in Aidbox

```json
POST /fhir/SearchParameter
Content-Type: application/json

{
  "resourceType": "SearchParameter",
  "id": "patient-occupation",
  "url": "http://example.org/fhir/SearchParameter/patient-occupation",
  "version": "1.0.0",
  "name": "occupation",
  "status": "active",
  "description": "Search patients by occupation",
  "code": "occupation",
  "base": ["Patient"],
  "type": "string",
  "expression": "Patient.extension.where(url='http://example.org/fhir/StructureDefinition/occupation').value.as(string)"
}
```

Once posted, this SearchParameter is immediately usable:

```
GET /fhir/Patient?occupation=Engineer
```

---

### HAPI FHIR

HAPI FHIR takes a **"POST activates, with optional automatic reindexing"** approach:

1. **Activation mechanism**: Search parameters are defined using the SearchParameter resource. You create a SearchParameter resource with a code (the URL parameter name), a type (the search parameter type), and an expression (the FHIRPath expression to be indexed). In HAPI FHIR's JPA server, custom search parameters are indexed just like any other search parameter.

2. **Automatic reindexing (optional setting)**: HAPI FHIR can parse the expression, add new or updated search parameters to an internal registry of indexed paths, and mark existing resources as requiring reindexing. This means newly added search parameters will cover resources added after the search parameter was created, and will also cover older resources after the server has had a chance to reindex them.

3. **Configurable behavior**: The "Mark Resources for Reindexing After SearchParameter Change" property controls this behavior. When disabled (the default), a manual reindex may be required to force data created prior to SearchParameter changes to be indexed.

4. **Manual $reindex operation**: When automatic reindexing is disabled, you need to call the `$reindex` operation explicitly:

```json
POST /$reindex
Content-Type: application/fhir+json

{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "url",
      "valueString": "Patient?"
    }
  ]
}
```

5. **Status-based disabling**: The `status` field actually controls whether the parameter is active. Setting status to `retired` disables the SearchParameter:

```json
{
  "resourceType": "SearchParameter",
  "id": "Resource-content",
  "url": "http://hl7.org/fhir/SearchParameter/Resource-content",
  "name": "_content",
  "status": "retired",
  "code": "_content",
  "base": ["Resource"],
  "type": "string"
}
```

6. **Extensions for fine-grained control**: HAPI supports extensions on SearchParameter resources to control indexing behavior, such as `searchparameter-token-suppress-text-index` to disable text indexing for specific parameters.

#### Example: Creating a SearchParameter in HAPI FHIR

```json
POST /SearchParameter
Content-Type: application/fhir+json

{
  "resourceType": "SearchParameter",
  "title": "Eye Colour",
  "base": ["Patient"],
  "status": "active",
  "code": "eyecolour",
  "type": "token",
  "expression": "Patient.extension('http://acme.org/eyecolour')",
  "xpathUsage": "normal"
}
```

---

## Key Differences

| Aspect | Aidbox | HAPI FHIR |
|--------|--------|-----------|
| **Activation** | Immediate on POST | Immediate on POST, but indexing is async |
| **Reindexing** | Not typically needed (query-time evaluation) | Required for existing resources |
| **Disabling** | Delete the resource | Set `status` to `retired` |
| **Index management** | Optional PostgreSQL GIN indexes | Required pre-computed search indexes |
| **Operation required** | No | Optional `$reindex` operation |

---

## Architectural Implications

The fundamental architectural difference is:

- **Aidbox** evaluates FHIRPath expressions at **query time** against JSONB data
- **HAPI FHIR** maintains **pre-computed search indexes** that must be updated when SearchParameters change

This architectural difference has practical implications:

### Aidbox Advantages
- More flexible for dynamic search parameter changes
- No waiting for reindexing to complete
- Simpler operational model

### Aidbox Disadvantages
- Potentially slower for complex queries without proper PostgreSQL indexes
- Query performance depends on index optimization

### HAPI FHIR Advantages
- Optimized pre-indexed searches
- Predictable query performance once indexed
- Fine-grained control over indexing behavior

### HAPI FHIR Disadvantages
- Requires explicit management of reindexing
- Delay between SearchParameter creation and full availability for existing data
- More complex operational requirements

---

## Recommendations

### For Aidbox
- Create SearchParameters as needed; they work immediately
- Consider adding PostgreSQL GIN indexes for frequently-used search parameters on large datasets
- Use `aidbox.index/suggest-index` RPC to get index recommendations

### For HAPI FHIR
- Enable "Mark Resources for Reindexing After SearchParameter Change" in development environments
- Plan for reindexing time when adding SearchParameters to production systems with existing data
- Use the `$reindex-dryrun` operation to test SearchParameter changes before full reindex
- Consider the `status` field carefully—use `retired` to disable rather than delete
