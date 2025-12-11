//! Utility functions and constants for XML serialization/deserialization.
//!
//! This module provides helper functions for identifying FHIR patterns,
//! handling special attributes, and managing namespaces.

/// FHIR namespace URI.
///
/// This namespace is added to the root resource element in FHIR XML documents.
pub const FHIR_NAMESPACE: &str = "http://hl7.org/fhir";

/// Checks if a field name represents an extension field in FHIR JSON.
///
/// Extension fields start with an underscore (e.g., `_birthDate`).
pub fn is_extension_field(key: &str) -> bool {
    key.starts_with('_')
}

/// Strips the underscore prefix from an extension field name.
///
/// Returns the field name without the leading underscore. If the field
/// doesn't start with underscore, returns the original name.
pub fn strip_underscore(key: &str) -> &str {
    key.strip_prefix('_').unwrap_or(key)
}

/// Checks if an element name represents a FHIR resource.
///
/// FHIR resources are identified by having an uppercase first letter.
pub fn is_resource_name(name: &str) -> bool {
    name.chars()
        .next()
        .map(|c| c.is_uppercase())
        .unwrap_or(false)
}

/// Checks if an element name is the special XHTML div element.
///
/// The `div` element requires special handling because it can contain
/// arbitrary XHTML content and uses a different namespace.
pub fn is_div_element(name: &str) -> bool {
    name == "div"
}

/// Converts a Rust boolean to its string representation for XML.
pub fn bool_to_string(b: bool) -> &'static str {
    if b { "true" } else { "false" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_extension_field() {
        assert!(is_extension_field("_birthDate"));
        assert!(is_extension_field("_given"));
        assert!(!is_extension_field("birthDate"));
        assert!(!is_extension_field("given"));
    }

    #[test]
    fn test_strip_underscore() {
        assert_eq!(strip_underscore("_birthDate"), "birthDate");
        assert_eq!(strip_underscore("_given"), "given");
        assert_eq!(strip_underscore("birthDate"), "birthDate");
    }

    #[test]
    fn test_is_resource_name() {
        assert!(is_resource_name("Patient"));
        assert!(is_resource_name("Observation"));
        assert!(is_resource_name("Bundle"));
        assert!(!is_resource_name("active"));
        assert!(!is_resource_name("birthDate"));
        assert!(!is_resource_name(""));
    }

    #[test]
    fn test_is_div_element() {
        assert!(is_div_element("div"));
        assert!(!is_div_element("Div"));
        assert!(!is_div_element("text"));
        assert!(!is_div_element("DIV"));
    }

    #[test]
    fn test_bool_to_string() {
        assert_eq!(bool_to_string(true), "true");
        assert_eq!(bool_to_string(false), "false");
    }
}
