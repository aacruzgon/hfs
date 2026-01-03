use serde::Deserialize;

/// Helper that accepts either a single value or an array when deserializing.
///
/// FHIR allows most repeatable elements to appear either once or multiple times
/// depending on the instance’s actual cardinality. While JSON carries enough
/// structure (`[]` vs scalar) so serde can infer that automatically, the XML
/// stream does not embed the schema-driven cardinality constraints. During
/// XML deserialization we therefore wrap every field with a `min > 0` upper
/// bound in `SingleOrVec` so we can accept both the single-element case and
/// the repeated-element case without schema knowledge at parse time.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum SingleOrVec<T> {
    Vec(Vec<T>),
    Single(T),
}

impl<T> SingleOrVec<T> {
    pub fn into_vec(self) -> Vec<T> {
        match self {
            SingleOrVec::Single(value) => vec![value],
            SingleOrVec::Vec(values) => values,
        }
    }
}

impl<T> Default for SingleOrVec<T> {
    fn default() -> Self {
        SingleOrVec::Vec(Vec::new())
    }
}

/// Accepts either JSON primitive values or XML element structures with metadata.
///
/// **JSON Format**: Primitive values come through as scalars, metadata merged from `_field` by macro.
///   - `"birthDate": "1970-03-30"` → `Primitive("1970-03-30")` (String directly)
///   - Metadata in `_field` is handled separately by the generated macro code
///
/// **XML Format**: All primitives are elements with inline metadata, no `_field` exists.
///   - `<birthDate value="1970-03-30"/>` → `Element(Element { value: Some(...), id: None, ... })`
///   - `<birthDate id="x" value="...">` → `Element(Element { value, id, ... })`
///   - `<birthDate id="x" value="..."><extension>...</extension></birthDate>` → `Element` with full metadata
///
/// The untagged enum lets serde choose the variant based on the incoming data structure:
/// - JSON scalars match `Primitive` variant (deserialized directly into final primitive type)
/// - XML element structures (objects with value/id/extension) match `Element` variant
/// This eliminates the need for XML-to-JSON conversion.
///
/// # Type Parameters
/// - `P`: Primitive type (the final deserialized type, e.g. `String`, `i32`, `bool`)
/// - `E`: Element type (struct containing value and metadata fields)
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum PrimitiveOrElement<P, E> {
    // Try Element first (more specific - requires object structure)
    Element(E),
    // Fall back to Primitive (catch-all for JSON scalars)
    Primitive(P),
}

/// Helper struct for serializing id and extension metadata for FHIR primitives.
///
/// In FHIR JSON, primitive values can have associated metadata stored in a parallel
/// `_fieldName` object containing an `id` and/or `extension` array.
///
/// This helper is used during serialization to output only the id/extension metadata
/// while the primitive value itself is serialized separately.
///
/// # Type Parameters
/// - `'a`: Lifetime of the borrowed data
/// - `E`: Extension type (varies by FHIR version: R4, R4B, R5, R6)
///
/// # Example
/// ```json
/// {
///   "status": "active",
///   "_status": {
///     "id": "status-1",
///     "extension": [...]
///   }
/// }
/// ```
#[derive(serde::Serialize)]
pub struct IdAndExtensionHelper<'a, E> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension: &'a Option<Vec<E>>,
}

/// Helper struct for deserializing id and extension metadata for FHIR primitives.
///
/// This is the owned version of `IdAndExtensionHelper`, used during deserialization
/// to capture id and extension data from the `_fieldName` JSON object.
///
/// # Type Parameters
/// - `E`: Extension type (varies by FHIR version: R4, R4B, R5, R6)
#[derive(Clone, serde::Deserialize, Default)]
pub struct IdAndExtensionOwned<E> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension: Option<Vec<E>>,
}
