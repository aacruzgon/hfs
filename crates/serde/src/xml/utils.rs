//! Utility functions and constants for XML serialization/deserialization.
//!
//! This module provides helper functions for identifying FHIR patterns,
//! handling special attributes, and managing namespaces.

/// FHIR namespace URI.
///
/// This namespace is added to the root resource element in FHIR XML documents.
pub const FHIR_NAMESPACE: &str = "http://hl7.org/fhir";

/// XHTML namespace URI.
///
/// This namespace is used for `<div>` elements containing narrative XHTML content.
pub const XHTML_NAMESPACE: &str = "http://www.w3.org/1999/xhtml";

/// XML version and encoding declaration.
pub const XML_DECLARATION: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

/// Checks if a field name represents an extension field in FHIR JSON.
///
/// Extension fields start with an underscore (e.g., `_birthDate`).
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::is_extension_field;
/// assert!(is_extension_field("_birthDate"));
/// assert!(is_extension_field("_given"));
/// assert!(!is_extension_field("birthDate"));
/// assert!(!is_extension_field("given"));
/// ```
pub fn is_extension_field(key: &str) -> bool {
    key.starts_with('_')
}

/// Strips the underscore prefix from an extension field name.
///
/// Returns the field name without the leading underscore. If the field
/// doesn't start with underscore, returns the original name.
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::strip_underscore;
/// assert_eq!(strip_underscore("_birthDate"), "birthDate");
/// assert_eq!(strip_underscore("_given"), "given");
/// assert_eq!(strip_underscore("birthDate"), "birthDate");
/// ```
pub fn strip_underscore(key: &str) -> &str {
    key.strip_prefix('_').unwrap_or(key)
}

/// Checks if an attribute name should be serialized as an XML attribute.
///
/// In FHIR XML, only three attribute names have special meaning:
/// - `id`: Element identifier
/// - `url`: Used in extensions and references
/// - `value`: The primitive value
///
/// All other data is represented as child elements.
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::should_be_attribute;
/// assert!(should_be_attribute("id"));
/// assert!(should_be_attribute("url"));
/// assert!(should_be_attribute("value"));
/// assert!(!should_be_attribute("extension"));
/// assert!(!should_be_attribute("coding"));
/// ```
pub fn should_be_attribute(key: &str) -> bool {
    matches!(key, "id" | "url" | "value")
}

/// Checks if a JSON value represents a primitive type in FHIR.
///
/// FHIR primitives are:
/// - Strings
/// - Numbers (integers and decimals)
/// - Booleans
/// - null
///
/// Arrays and objects are not primitives.
///
/// # Examples
///
/// ```
/// # use serde_json::json;
/// # use helios_hfs_serde::xml::utils::is_primitive_value;
/// assert!(is_primitive_value(&json!("hello")));
/// assert!(is_primitive_value(&json!(42)));
/// assert!(is_primitive_value(&json!(3.14)));
/// assert!(is_primitive_value(&json!(true)));
/// assert!(is_primitive_value(&json!(null)));
/// assert!(!is_primitive_value(&json!({"key": "value"})));
/// assert!(!is_primitive_value(&json!([1, 2, 3])));
/// ```
pub fn is_primitive_value(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => true,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => false,
    }
}

/// Checks if an element name represents a FHIR resource.
///
/// FHIR resources are identified by having an uppercase first letter.
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::is_resource_name;
/// assert!(is_resource_name("Patient"));
/// assert!(is_resource_name("Observation"));
/// assert!(!is_resource_name("active"));
/// assert!(!is_resource_name("birthDate"));
/// ```
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
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::is_div_element;
/// assert!(is_div_element("div"));
/// assert!(!is_div_element("text"));
/// assert!(!is_div_element("Div"));
/// ```
pub fn is_div_element(name: &str) -> bool {
    name == "div"
}

/// Escapes special XML characters in text content.
///
/// Escapes the five XML special characters:
/// - `<` → `&lt;`
/// - `>` → `&gt;`
/// - `&` → `&amp;`
/// - `'` → `&apos;`
/// - `"` → `&quot;`
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::escape_xml_text;
/// assert_eq!(escape_xml_text("Hello & <World>"), "Hello &amp; &lt;World&gt;");
/// assert_eq!(escape_xml_text(r#"Say "Hi""#), r#"Say &quot;Hi&quot;"#);
/// ```
pub fn escape_xml_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\'', "&apos;")
        .replace('"', "&quot;")
}

/// Unescapes XML entities in text content.
///
/// Converts XML entities back to their original characters:
/// - `&lt;` → `<`
/// - `&gt;` → `>`
/// - `&amp;` → `&`
/// - `&apos;` → `'`
/// - `&quot;` → `"`
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::unescape_xml_text;
/// assert_eq!(unescape_xml_text("Hello &amp; &lt;World&gt;"), "Hello & <World>");
/// assert_eq!(unescape_xml_text(r#"Say &quot;Hi&quot;"#), r#"Say "Hi""#);
/// ```
pub fn unescape_xml_text(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&apos;", "'")
        .replace("&quot;", "\"")
        .replace("&amp;", "&") // Must be last
}

/// Converts a Rust boolean to its string representation for XML.
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::bool_to_string;
/// assert_eq!(bool_to_string(true), "true");
/// assert_eq!(bool_to_string(false), "false");
/// ```
pub fn bool_to_string(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// Parses a boolean from its XML string representation.
///
/// Accepts "true" and "false" (case-insensitive), as well as "1" and "0".
///
/// # Examples
///
/// ```
/// # use helios_hfs_serde::xml::utils::parse_bool;
/// assert_eq!(parse_bool("true"), Some(true));
/// assert_eq!(parse_bool("false"), Some(false));
/// assert_eq!(parse_bool("1"), Some(true));
/// assert_eq!(parse_bool("0"), Some(false));
/// assert_eq!(parse_bool("TRUE"), Some(true));
/// assert_eq!(parse_bool("invalid"), None);
/// ```
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn test_should_be_attribute() {
        assert!(should_be_attribute("id"));
        assert!(should_be_attribute("url"));
        assert!(should_be_attribute("value"));
        assert!(!should_be_attribute("extension"));
        assert!(!should_be_attribute("coding"));
        assert!(!should_be_attribute("system"));
    }

    #[test]
    fn test_is_primitive_value() {
        assert!(is_primitive_value(&json!("hello")));
        assert!(is_primitive_value(&json!(42)));
        assert!(is_primitive_value(&json!(3.14)));
        assert!(is_primitive_value(&json!(true)));
        assert!(is_primitive_value(&json!(null)));
        assert!(!is_primitive_value(&json!({"key": "value"})));
        assert!(!is_primitive_value(&json!([1, 2, 3])));
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
    fn test_escape_xml_text() {
        assert_eq!(escape_xml_text("Hello & <World>"), "Hello &amp; &lt;World&gt;");
        assert_eq!(escape_xml_text(r#"Say "Hi""#), r#"Say &quot;Hi&quot;"#);
        assert_eq!(escape_xml_text("It's ok"), "It&apos;s ok");
    }

    #[test]
    fn test_unescape_xml_text() {
        assert_eq!(unescape_xml_text("Hello &amp; &lt;World&gt;"), "Hello & <World>");
        assert_eq!(unescape_xml_text(r#"Say &quot;Hi&quot;"#), r#"Say "Hi""#);
        assert_eq!(unescape_xml_text("It&apos;s ok"), "It's ok");
    }

    #[test]
    fn test_bool_to_string() {
        assert_eq!(bool_to_string(true), "true");
        assert_eq!(bool_to_string(false), "false");
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("FALSE"), Some(false));
        assert_eq!(parse_bool("invalid"), None);
        assert_eq!(parse_bool(""), None);
    }
}
