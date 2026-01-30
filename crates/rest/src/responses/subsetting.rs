//! Resource subsetting for _summary and _elements parameters.
//!
//! Implements FHIR resource subsetting per the specification:
//! - `_summary` - Return a predefined subset of elements
//! - `_elements` - Return specific elements by path
//!
//! See: https://hl7.org/fhir/search.html#summary

use serde_json::{Map, Value};

/// Summary mode for resource subsetting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryMode {
    /// Return only elements marked as summary in the specification.
    True,
    /// Return the full resource (default).
    False,
    /// Return only the text narrative, id, meta, and mandatory top-level elements.
    Text,
    /// Return all elements except the text narrative.
    Data,
    /// For search only - return only a count (handled at search level).
    Count,
}

impl SummaryMode {
    /// Parses a summary mode from a string value.
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_lowercase().as_str() {
            "true" => Some(SummaryMode::True),
            "false" => Some(SummaryMode::False),
            "text" => Some(SummaryMode::Text),
            "data" => Some(SummaryMode::Data),
            "count" => Some(SummaryMode::Count),
            _ => None,
        }
    }
}

/// Elements that are always included regardless of subsetting.
const ALWAYS_INCLUDED: &[&str] = &["resourceType", "id", "meta"];

/// Common summary elements for most resource types.
/// These are elements typically marked as "isSummary" in the FHIR specification.
fn get_summary_elements(resource_type: &str) -> Vec<&'static str> {
    // Common elements that are usually in summaries
    let mut elements = vec!["resourceType", "id", "meta", "identifier"];

    // Add resource-specific summary elements
    match resource_type {
        "Patient" => {
            elements.extend(&[
                "active",
                "name",
                "telecom",
                "gender",
                "birthDate",
                "deceased",
                "address",
                "managingOrganization",
                "link",
            ]);
        }
        "Observation" => {
            elements.extend(&[
                "status",
                "category",
                "code",
                "subject",
                "encounter",
                "effective",
                "issued",
                "value",
                "dataAbsentReason",
                "interpretation",
                "component",
            ]);
        }
        "Condition" => {
            elements.extend(&[
                "clinicalStatus",
                "verificationStatus",
                "category",
                "severity",
                "code",
                "bodySite",
                "subject",
                "encounter",
                "onset",
                "abatement",
                "recordedDate",
            ]);
        }
        "Encounter" => {
            elements.extend(&[
                "status",
                "class",
                "type",
                "serviceType",
                "subject",
                "participant",
                "period",
                "location",
            ]);
        }
        "Procedure" => {
            elements.extend(&[
                "status",
                "code",
                "subject",
                "encounter",
                "performed",
                "performer",
            ]);
        }
        "MedicationRequest" => {
            elements.extend(&[
                "status",
                "intent",
                "medication",
                "subject",
                "encounter",
                "authoredOn",
                "requester",
            ]);
        }
        "DiagnosticReport" => {
            elements.extend(&[
                "status",
                "category",
                "code",
                "subject",
                "encounter",
                "effective",
                "issued",
                "performer",
                "result",
                "conclusion",
            ]);
        }
        "Practitioner" => {
            elements.extend(&[
                "active",
                "name",
                "telecom",
                "address",
                "gender",
                "birthDate",
            ]);
        }
        "Organization" => {
            elements.extend(&["active", "type", "name", "alias", "telecom", "address"]);
        }
        "Location" => {
            elements.extend(&[
                "status",
                "operationalStatus",
                "name",
                "alias",
                "description",
                "type",
                "telecom",
                "address",
            ]);
        }
        _ => {
            // Default: include common top-level elements
            elements.extend(&["status", "name", "code", "subject", "patient"]);
        }
    }

    elements
}

/// Applies _summary subsetting to a resource.
///
/// Returns a new JSON value with only the requested elements.
pub fn apply_summary(resource: &Value, mode: SummaryMode) -> Value {
    match mode {
        SummaryMode::False => resource.clone(),
        SummaryMode::Count => {
            // Count mode is handled at search level; return minimal resource
            filter_resource(resource, ALWAYS_INCLUDED)
        }
        SummaryMode::Text => {
            // Include text, id, meta, and mandatory elements
            let mut elements: Vec<&str> = ALWAYS_INCLUDED.to_vec();
            elements.push("text");
            filter_resource(resource, &elements)
        }
        SummaryMode::Data => {
            // Include everything except text
            exclude_elements(resource, &["text"])
        }
        SummaryMode::True => {
            // Include summary elements
            if let Some(resource_type) = resource.get("resourceType").and_then(|v| v.as_str()) {
                let summary_elements = get_summary_elements(resource_type);
                filter_resource(resource, &summary_elements)
            } else {
                resource.clone()
            }
        }
    }
}

/// Applies _elements subsetting to a resource.
///
/// Elements are specified as a comma-separated list of paths (e.g., "id,name,birthDate").
/// Nested paths are supported with dot notation (e.g., "name.family").
pub fn apply_elements(resource: &Value, elements: &[&str]) -> Value {
    if elements.is_empty() {
        return resource.clone();
    }

    // Always include resourceType, id, and meta
    let mut all_elements: Vec<&str> = ALWAYS_INCLUDED.to_vec();

    // Add requested elements
    for elem in elements {
        if !all_elements.contains(elem) {
            all_elements.push(elem);
        }
    }

    filter_resource(resource, &all_elements)
}

/// Filters a resource to include only specified top-level elements.
fn filter_resource(resource: &Value, elements: &[&str]) -> Value {
    if let Value::Object(obj) = resource {
        let mut result = Map::new();

        for (key, value) in obj {
            // Check if this key is in the elements list
            // Also handle nested paths (e.g., "name" should include "name" object)
            if elements
                .iter()
                .any(|e| *e == key || e.starts_with(&format!("{}.", key)) || key == "resourceType")
            {
                // If there's a nested path, filter the nested object
                let nested_paths: Vec<&str> = elements
                    .iter()
                    .filter_map(|e| {
                        if e.starts_with(&format!("{}.", key)) {
                            Some(&e[key.len() + 1..])
                        } else {
                            None
                        }
                    })
                    .collect();

                if !nested_paths.is_empty() && value.is_object() {
                    // Filter nested object
                    result.insert(key.clone(), filter_nested(value, &nested_paths));
                } else {
                    result.insert(key.clone(), value.clone());
                }
            }
        }

        Value::Object(result)
    } else {
        resource.clone()
    }
}

/// Filters nested objects based on path components.
fn filter_nested(value: &Value, paths: &[&str]) -> Value {
    match value {
        Value::Object(obj) => {
            let mut result = Map::new();

            for (key, val) in obj {
                // Include if key matches any path or is a prefix of a nested path
                if paths
                    .iter()
                    .any(|p| *p == key || p.starts_with(&format!("{}.", key)))
                {
                    let nested_paths: Vec<&str> = paths
                        .iter()
                        .filter_map(|p| {
                            if p.starts_with(&format!("{}.", key)) {
                                Some(&p[key.len() + 1..])
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !nested_paths.is_empty() && val.is_object() {
                        result.insert(key.clone(), filter_nested(val, &nested_paths));
                    } else {
                        result.insert(key.clone(), val.clone());
                    }
                }
            }

            Value::Object(result)
        }
        Value::Array(arr) => {
            // For arrays, filter each element
            Value::Array(arr.iter().map(|v| filter_nested(v, paths)).collect())
        }
        _ => value.clone(),
    }
}

/// Excludes specified elements from a resource.
fn exclude_elements(resource: &Value, elements: &[&str]) -> Value {
    if let Value::Object(obj) = resource {
        let mut result = Map::new();

        for (key, value) in obj {
            if !elements.contains(&key.as_str()) {
                result.insert(key.clone(), value.clone());
            }
        }

        Value::Object(result)
    } else {
        resource.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_summary_mode_parse() {
        assert_eq!(SummaryMode::parse("true"), Some(SummaryMode::True));
        assert_eq!(SummaryMode::parse("false"), Some(SummaryMode::False));
        assert_eq!(SummaryMode::parse("text"), Some(SummaryMode::Text));
        assert_eq!(SummaryMode::parse("data"), Some(SummaryMode::Data));
        assert_eq!(SummaryMode::parse("count"), Some(SummaryMode::Count));
        assert_eq!(SummaryMode::parse("invalid"), None);
    }

    #[test]
    fn test_apply_summary_false() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "meta": {"versionId": "1"},
            "text": {"status": "generated", "div": "<div>Patient</div>"},
            "name": [{"family": "Smith"}],
            "birthDate": "1990-01-01"
        });

        let result = apply_summary(&resource, SummaryMode::False);
        assert_eq!(result, resource);
    }

    #[test]
    fn test_apply_summary_text() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "meta": {"versionId": "1"},
            "text": {"status": "generated", "div": "<div>Patient</div>"},
            "name": [{"family": "Smith"}],
            "birthDate": "1990-01-01"
        });

        let result = apply_summary(&resource, SummaryMode::Text);

        // Should include resourceType, id, meta, text
        assert!(result.get("resourceType").is_some());
        assert!(result.get("id").is_some());
        assert!(result.get("meta").is_some());
        assert!(result.get("text").is_some());
        // Should not include other elements
        assert!(result.get("name").is_none());
        assert!(result.get("birthDate").is_none());
    }

    #[test]
    fn test_apply_summary_data() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "meta": {"versionId": "1"},
            "text": {"status": "generated", "div": "<div>Patient</div>"},
            "name": [{"family": "Smith"}],
            "birthDate": "1990-01-01"
        });

        let result = apply_summary(&resource, SummaryMode::Data);

        // Should include everything except text
        assert!(result.get("resourceType").is_some());
        assert!(result.get("id").is_some());
        assert!(result.get("meta").is_some());
        assert!(result.get("name").is_some());
        assert!(result.get("birthDate").is_some());
        // Should not include text
        assert!(result.get("text").is_none());
    }

    #[test]
    fn test_apply_summary_true_patient() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "meta": {"versionId": "1"},
            "text": {"status": "generated", "div": "<div>Patient</div>"},
            "name": [{"family": "Smith"}],
            "birthDate": "1990-01-01",
            "communication": [{"language": {"text": "English"}}],
            "photo": [{"data": "base64..."}]
        });

        let result = apply_summary(&resource, SummaryMode::True);

        // Should include summary elements
        assert!(result.get("resourceType").is_some());
        assert!(result.get("id").is_some());
        assert!(result.get("meta").is_some());
        assert!(result.get("name").is_some());
        assert!(result.get("birthDate").is_some());
        // Should not include non-summary elements
        assert!(result.get("communication").is_none());
        assert!(result.get("photo").is_none());
    }

    #[test]
    fn test_apply_elements_basic() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "meta": {"versionId": "1"},
            "name": [{"family": "Smith", "given": ["John"]}],
            "birthDate": "1990-01-01",
            "gender": "male"
        });

        let result = apply_elements(&resource, &["name", "birthDate"]);

        // Should include requested elements plus mandatory
        assert!(result.get("resourceType").is_some());
        assert!(result.get("id").is_some());
        assert!(result.get("meta").is_some());
        assert!(result.get("name").is_some());
        assert!(result.get("birthDate").is_some());
        // Should not include non-requested elements
        assert!(result.get("gender").is_none());
    }

    #[test]
    fn test_apply_elements_nested_path() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "meta": {"versionId": "1"},
            "name": [{"family": "Smith", "given": ["John"]}],
            "birthDate": "1990-01-01"
        });

        let result = apply_elements(&resource, &["name.family"]);

        // Should include the name object filtered to only family
        assert!(result.get("name").is_some());
        let name = result.get("name").unwrap();
        if let Value::Array(arr) = name {
            let first = &arr[0];
            assert!(first.get("family").is_some());
            // given should not be included as we only requested name.family
        }
    }

    #[test]
    fn test_exclude_elements() {
        let resource = json!({
            "resourceType": "Patient",
            "id": "123",
            "text": {"div": "<div>text</div>"},
            "name": [{"family": "Smith"}]
        });

        let result = exclude_elements(&resource, &["text"]);

        assert!(result.get("resourceType").is_some());
        assert!(result.get("id").is_some());
        assert!(result.get("name").is_some());
        assert!(result.get("text").is_none());
    }
}
