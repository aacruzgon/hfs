//! XML serialization and deserialization for FHIR resources.
//!
//! This module provides streaming XML support through custom `serde::Serializer` and
//! `serde::Deserializer` implementations that convert between FHIR resources and XML
//! without materializing a full JSON intermediate representation.
//!
//! ## Architecture
//!
//! The XML implementation uses a streaming approach:
//!
//! - **Serialization**: Custom `Serializer` receives serialize calls as the FhirSerde macro
//!   traverses the resource, buffers minimally to detect FHIR patterns (field/_field pairs),
//!   and writes quick-xml events directly to output.
//!
//! - **Deserialization**: Custom `Deserializer` reads quick-xml events from input, uses
//!   one-event lookahead to detect arrays, and reconstructs FHIR JSON patterns (field/_field)
//!   on-the-fly for the FhirSerde macro.
//!
//! ## FHIR JSON ↔ XML Mapping
//!
//! The implementation handles FHIR's unique serialization patterns:
//!
//! ### Primitives with Extensions
//!
//! **JSON Pattern**:
//! ```json
//! {
//!   "birthDate": "1974-12-25",
//!   "_birthDate": {
//!     "id": "bd1",
//!     "extension": [...]
//!   }
//! }
//! ```
//!
//! **XML Pattern**:
//! ```xml
//! <birthDate id="bd1" value="1974-12-25">
//!   <extension url="...">...</extension>
//! </birthDate>
//! ```
//!
//! ### Simple Primitives
//!
//! **JSON Pattern**:
//! ```json
//! { "active": true }
//! ```
//!
//! **XML Pattern**:
//! ```xml
//! <active value="true"/>
//! ```
//!
//! ### Arrays
//!
//! **JSON Pattern**:
//! ```json
//! { "given": ["John", "Doe"] }
//! ```
//!
//! **XML Pattern**:
//! ```xml
//! <given value="John"/>
//! <given value="Doe"/>
//! ```
//!
//! ### Arrays with Extensions
//!
//! **JSON Pattern**:
//! ```json
//! {
//!   "given": ["Alice", null],
//!   "_given": [null, {"id": "g1"}]
//! }
//! ```
//!
//! **XML Pattern**:
//! ```xml
//! <given value="Alice"/>
//! <given id="g1"/>
//! ```
//!
//! ### Complex Objects
//!
//! **JSON Pattern**:
//! ```json
//! {
//!   "code": {
//!     "coding": [...]
//!   }
//! }
//! ```
//!
//! **XML Pattern**:
//! ```xml
//! <code>
//!   <coding>...</coding>
//! </code>
//! ```
//!
//! ## Special Attributes
//!
//! Three XML attributes have special meaning in FHIR:
//!
//! - **`value`**: The primitive value of an element
//! - **`id`**: Element identifier (from `_field` in JSON)
//! - **`url`**: Used in extensions and references
//!
//! All other data is represented as child elements.
//!
//! ## Namespace Handling
//!
//! - FHIR namespace (`http://hl7.org/fhir`) is added to the root resource element
//! - XHTML namespace (`http://www.w3.org/1999/xhtml`) is used for `<div>` elements
//!   containing narrative text
//!
//! ## Examples
//!
//! ```ignore
//! use helios_serde::xml::{to_xml_string, from_xml_str};
//! use helios_fhir::r4::Patient;
//!
//! // Serialize to XML
//! let patient = Patient {
//!     id: Some("example".to_string()),
//!     active: Some(true.into()),
//!     ..Default::default()
//! };
//! let xml = to_xml_string(&patient)?;
//!
//! // Deserialize from XML
//! let patient: Patient = from_xml_str(&xml)?;
//! ```

pub mod de;
pub mod ser;
mod utils;

// Re-export serialization functions
pub use ser::{to_xml_string, to_xml_vec, to_xml_writer};

// Re-export deserialization functions
pub use de::{from_xml_reader, from_xml_slice, from_xml_str};
