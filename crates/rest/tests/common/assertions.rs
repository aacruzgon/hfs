//! HTTP response assertions.
//!
//! Provides assertion utilities for testing HTTP responses.

use axum_test::TestResponse;
use serde_json::Value;

/// Asserts that the response has the expected status code.
pub fn assert_status(response: &TestResponse, expected: u16) {
    let actual = response.status_code().as_u16();
    assert_eq!(
        actual, expected,
        "Expected status {}, got {}",
        expected, actual
    );
}

/// Asserts that the response is a success (2xx).
pub fn assert_success(response: &TestResponse) {
    let status = response.status_code().as_u16();
    assert!(
        (200..300).contains(&status),
        "Expected success status, got {}",
        status
    );
}

/// Asserts that the response is a client error (4xx).
pub fn assert_client_error(response: &TestResponse) {
    let status = response.status_code().as_u16();
    assert!(
        (400..500).contains(&status),
        "Expected client error status, got {}",
        status
    );
}

/// Asserts that the response is a server error (5xx).
pub fn assert_server_error(response: &TestResponse) {
    let status = response.status_code().as_u16();
    assert!(
        (500..600).contains(&status),
        "Expected server error status, got {}",
        status
    );
}

/// Asserts that the response has an ETag header.
pub fn assert_has_etag(response: &TestResponse) {
    assert!(
        response.headers().contains_key("etag"),
        "Expected ETag header"
    );
}

/// Asserts that the response has a Location header.
pub fn assert_has_location(response: &TestResponse) {
    assert!(
        response.headers().contains_key("location"),
        "Expected Location header"
    );
}

/// Asserts that the response body is a FHIR resource of the expected type.
pub fn assert_resource_type(body: &Value, expected: &str) {
    let actual = body
        .get("resourceType")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        actual, expected,
        "Expected resourceType {}, got {}",
        expected, actual
    );
}

/// Asserts that the response body is an OperationOutcome.
pub fn assert_operation_outcome(body: &Value) {
    assert_resource_type(body, "OperationOutcome");
}

/// Asserts that the OperationOutcome has an issue with the expected severity.
pub fn assert_issue_severity(body: &Value, expected: &str) {
    let issues = body.get("issue").and_then(|v| v.as_array());
    assert!(
        issues.is_some(),
        "Expected issues array in OperationOutcome"
    );

    let has_severity = issues
        .unwrap()
        .iter()
        .any(|issue| issue.get("severity").and_then(|v| v.as_str()) == Some(expected));

    assert!(has_severity, "Expected issue with severity {}", expected);
}

/// Asserts that the OperationOutcome has an issue with the expected code.
pub fn assert_issue_code(body: &Value, expected: &str) {
    let issues = body.get("issue").and_then(|v| v.as_array());
    assert!(
        issues.is_some(),
        "Expected issues array in OperationOutcome"
    );

    let has_code = issues
        .unwrap()
        .iter()
        .any(|issue| issue.get("code").and_then(|v| v.as_str()) == Some(expected));

    assert!(has_code, "Expected issue with code {}", expected);
}

/// Asserts that the response body is a Bundle.
pub fn assert_bundle(body: &Value) {
    assert_resource_type(body, "Bundle");
}

/// Asserts that the Bundle has the expected type.
pub fn assert_bundle_type(body: &Value, expected: &str) {
    assert_bundle(body);
    let actual = body.get("type").and_then(|v| v.as_str()).unwrap_or("");
    assert_eq!(
        actual, expected,
        "Expected Bundle type {}, got {}",
        expected, actual
    );
}

/// Asserts that the Bundle has the expected number of entries.
pub fn assert_bundle_entry_count(body: &Value, expected: usize) {
    assert_bundle(body);
    let entries = body
        .get("entry")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        entries, expected,
        "Expected {} bundle entries, got {}",
        expected, entries
    );
}

/// Asserts a JSON path value in the body.
pub fn assert_json_path(body: &Value, path: &str, expected: &Value) {
    let actual = json_path_get(body, path);
    assert_eq!(
        actual,
        Some(expected),
        "JSON path {} expected {:?}, got {:?}",
        path,
        expected,
        actual
    );
}

/// Gets a value from a JSON object using a simple path notation.
///
/// Supports:
/// - `field` - Direct field access
/// - `field.nested` - Nested field access
/// - `field[0]` - Array index access
fn json_path_get<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;

    for part in path.split('.') {
        // Check for array index
        if let Some(bracket_pos) = part.find('[') {
            let field_name = &part[..bracket_pos];
            let index_str = &part[bracket_pos + 1..part.len() - 1];

            // Get the field
            current = current.get(field_name)?;

            // Get the array index
            let index: usize = index_str.parse().ok()?;
            current = current.get(index)?;
        } else {
            current = current.get(part)?;
        }
    }

    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_path_simple() {
        let value = json!({"name": "John"});
        assert_eq!(json_path_get(&value, "name"), Some(&json!("John")));
    }

    #[test]
    fn test_json_path_nested() {
        let value = json!({"person": {"name": "John"}});
        assert_eq!(json_path_get(&value, "person.name"), Some(&json!("John")));
    }

    #[test]
    fn test_json_path_array() {
        let value = json!({"names": ["John", "Jane"]});
        assert_eq!(json_path_get(&value, "names[0]"), Some(&json!("John")));
        assert_eq!(json_path_get(&value, "names[1]"), Some(&json!("Jane")));
    }

    #[test]
    fn test_json_path_nested_array() {
        let value = json!({"data": {"items": [{"id": 1}, {"id": 2}]}});
        assert_eq!(json_path_get(&value, "data.items[0].id"), Some(&json!(1)));
    }
}
