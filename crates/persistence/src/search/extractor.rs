//! SearchParameter Value Extractor.
//!
//! Uses FHIRPath expressions to extract searchable values from FHIR resources.

use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::SearchParamType;

use super::converters::{IndexValue, ValueConverter};
use super::errors::ExtractionError;
use super::registry::{SearchParameterDefinition, SearchParameterRegistry};

/// A value extracted from a resource for indexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedValue {
    /// The parameter name (e.g., "name", "identifier").
    pub param_name: String,

    /// The parameter URL.
    pub param_url: String,

    /// The parameter type.
    pub param_type: SearchParamType,

    /// The extracted and converted value.
    pub value: IndexValue,

    /// Composite group ID (for composite parameters).
    /// Values with the same group ID are part of the same composite match.
    pub composite_group: Option<u32>,
}

impl ExtractedValue {
    /// Creates a new extracted value.
    pub fn new(
        param_name: impl Into<String>,
        param_url: impl Into<String>,
        param_type: SearchParamType,
        value: IndexValue,
    ) -> Self {
        Self {
            param_name: param_name.into(),
            param_url: param_url.into(),
            param_type,
            value,
            composite_group: None,
        }
    }

    /// Sets the composite group ID.
    pub fn with_composite_group(mut self, group: u32) -> Self {
        self.composite_group = Some(group);
        self
    }
}

/// Extracts searchable values from FHIR resources using FHIRPath.
pub struct SearchParameterExtractor {
    registry: Arc<RwLock<SearchParameterRegistry>>,
}

impl SearchParameterExtractor {
    /// Creates a new extractor with the given registry.
    pub fn new(registry: Arc<RwLock<SearchParameterRegistry>>) -> Self {
        Self { registry }
    }

    /// Extracts all searchable values from a resource.
    ///
    /// Returns values for all active search parameters that apply to this resource type.
    pub fn extract(
        &self,
        resource: &Value,
        resource_type: &str,
    ) -> Result<Vec<ExtractedValue>, ExtractionError> {
        // Validate resource
        let obj = resource.as_object().ok_or_else(|| ExtractionError::InvalidResource {
            message: "Resource must be a JSON object".to_string(),
        })?;

        // Verify resource type
        if let Some(rt) = obj.get("resourceType").and_then(|v| v.as_str()) {
            if rt != resource_type {
                return Err(ExtractionError::InvalidResource {
                    message: format!(
                        "Resource type mismatch: expected {}, got {}",
                        resource_type, rt
                    ),
                });
            }
        }

        let mut results = Vec::new();

        // Get active parameters for this resource type
        let params = {
            let registry = self.registry.read();
            registry.get_active_params(resource_type)
        };

        for param in &params {
            match self.extract_for_param(resource, param) {
                Ok(values) => results.extend(values),
                Err(e) => {
                    // Log the error but continue with other parameters
                    tracing::warn!(
                        "Failed to extract values for parameter '{}': {}",
                        param.code,
                        e
                    );
                }
            }
        }

        // Also extract common Resource-level parameters
        let common_params = {
            let registry = self.registry.read();
            registry.get_active_params("Resource")
        };

        for param in &common_params {
            if !params.iter().any(|p| p.code == param.code) {
                match self.extract_for_param(resource, param) {
                    Ok(values) => results.extend(values),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to extract values for common parameter '{}': {}",
                            param.code,
                            e
                        );
                    }
                }
            }
        }

        Ok(results)
    }

    /// Extracts values for a specific parameter from a resource.
    pub fn extract_for_param(
        &self,
        resource: &Value,
        param: &SearchParameterDefinition,
    ) -> Result<Vec<ExtractedValue>, ExtractionError> {
        if param.expression.is_empty() {
            return Ok(Vec::new());
        }

        // For now, use a simple JSON path-based extraction
        // In a full implementation, this would use the FHIRPath evaluator
        let values = self.evaluate_expression(resource, &param.expression)?;

        let mut results = Vec::new();
        for value in values {
            let converted = ValueConverter::convert(&value, param.param_type, &param.code)?;
            for idx_value in converted {
                results.push(ExtractedValue::new(
                    &param.code,
                    &param.url,
                    param.param_type,
                    idx_value,
                ));
            }
        }

        Ok(results)
    }

    /// Evaluates a FHIRPath expression against a resource.
    ///
    /// This is a simplified implementation that handles common patterns.
    /// A full implementation would use the helios-fhirpath crate.
    fn evaluate_expression(
        &self,
        resource: &Value,
        expression: &str,
    ) -> Result<Vec<Value>, ExtractionError> {
        // Parse the expression into path segments
        let segments = self.parse_path(expression);
        if segments.is_empty() {
            return Ok(Vec::new());
        }

        // Navigate to the values
        self.navigate_path(resource, &segments)
    }

    /// Parses a FHIRPath expression into path segments.
    fn parse_path(&self, expression: &str) -> Vec<PathSegment> {
        let mut segments = Vec::new();
        let mut current = expression.to_string();

        // Handle Resource.path prefix
        if let Some(dot_pos) = current.find('.') {
            let prefix = &current[..dot_pos];
            // Skip the resource type prefix (e.g., "Patient.")
            if prefix.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                current = current[dot_pos + 1..].to_string();
            }
        }

        // Split on dots, but handle function calls
        let mut path = current.as_str();
        while !path.is_empty() {
            // Check for function call
            if let Some(paren_start) = path.find('(') {
                let name = &path[..paren_start];
                if let Some(paren_end) = path.find(')') {
                    let func_name = name.split('.').last().unwrap_or(name);
                    let arg = &path[paren_start + 1..paren_end];

                    // Handle common functions
                    match func_name {
                        "where" | "ofType" | "resolve" => {
                            // Skip function, continue with remaining path
                            if paren_end + 1 < path.len() && path.as_bytes()[paren_end + 1] == b'.' {
                                path = &path[paren_end + 2..];
                            } else {
                                path = &path[paren_end + 1..];
                            }

                            // For ofType, filter by type
                            if func_name == "ofType" {
                                segments.push(PathSegment::TypeFilter(arg.to_string()));
                            }
                            continue;
                        }
                        _ => {}
                    }
                }
            }

            // Regular path segment
            let dot_pos = path.find('.').unwrap_or(path.len());
            let paren_pos = path.find('(').unwrap_or(path.len());
            let segment_end = dot_pos.min(paren_pos);

            if segment_end > 0 {
                let segment = &path[..segment_end];
                if !segment.is_empty() {
                    segments.push(PathSegment::Field(segment.to_string()));
                }
            }

            if segment_end < path.len() && path.as_bytes()[segment_end] == b'.' {
                path = &path[segment_end + 1..];
            } else {
                path = &path[segment_end..];
            }

            if path.starts_with('(') {
                // Skip to after closing paren
                if let Some(end) = path.find(')') {
                    path = &path[end + 1..];
                    if path.starts_with('.') {
                        path = &path[1..];
                    }
                } else {
                    break;
                }
            }
        }

        segments
    }

    /// Navigates a resource according to path segments.
    fn navigate_path(
        &self,
        value: &Value,
        segments: &[PathSegment],
    ) -> Result<Vec<Value>, ExtractionError> {
        if segments.is_empty() {
            return Ok(vec![value.clone()]);
        }

        let segment = &segments[0];
        let remaining = &segments[1..];

        match segment {
            PathSegment::Field(name) => {
                match value {
                    Value::Object(obj) => {
                        if let Some(child) = obj.get(name) {
                            self.navigate_path(child, remaining)
                        } else {
                            // Check for polymorphic field (e.g., "effective" -> "effectiveDateTime")
                            let candidates: Vec<_> = obj
                                .keys()
                                .filter(|k| k.starts_with(name))
                                .collect();

                            if candidates.len() == 1 {
                                self.navigate_path(&obj[candidates[0]], remaining)
                            } else {
                                Ok(Vec::new())
                            }
                        }
                    }
                    Value::Array(arr) => {
                        let mut results = Vec::new();
                        for item in arr {
                            results.extend(self.navigate_path(item, segments)?);
                        }
                        Ok(results)
                    }
                    _ => Ok(Vec::new()),
                }
            }
            PathSegment::TypeFilter(type_name) => {
                // Filter by FHIR type
                match value {
                    Value::Object(obj) => {
                        if let Some(Value::String(rt)) = obj.get("resourceType") {
                            if rt == type_name {
                                return self.navigate_path(value, remaining);
                            }
                        }
                        // Check if it's a complex type (e.g., Quantity, Period)
                        // For now, pass through and let downstream handle it
                        self.navigate_path(value, remaining)
                    }
                    Value::Array(arr) => {
                        let mut results = Vec::new();
                        for item in arr {
                            results.extend(self.navigate_path(item, segments)?);
                        }
                        Ok(results)
                    }
                    _ => self.navigate_path(value, remaining),
                }
            }
        }
    }
}

/// A segment of a parsed FHIRPath expression.
#[derive(Debug, Clone)]
enum PathSegment {
    /// A field name to navigate to.
    Field(String),
    /// A type filter (from ofType() function).
    TypeFilter(String),
}

impl std::fmt::Debug for SearchParameterExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchParameterExtractor").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::loader::{FhirVersion, SearchParameterLoader};
    use serde_json::json;

    fn create_test_extractor() -> SearchParameterExtractor {
        let loader = SearchParameterLoader::new(FhirVersion::R4);
        let mut registry = SearchParameterRegistry::new();
        let _ = tokio::runtime::Runtime::new().unwrap().block_on(async {
            registry.load_all(&loader).await
        });

        SearchParameterExtractor::new(Arc::new(RwLock::new(registry)))
    }

    #[test]
    fn test_extract_patient_name() {
        let extractor = create_test_extractor();

        let patient = json!({
            "resourceType": "Patient",
            "id": "123",
            "name": [
                {
                    "family": "Smith",
                    "given": ["John", "James"]
                }
            ]
        });

        let values = extractor.extract(&patient, "Patient").unwrap();

        // Should have extracted name values
        let name_values: Vec<_> = values.iter().filter(|v| v.param_name == "name").collect();
        assert!(!name_values.is_empty());

        // Should have extracted family
        let family_values: Vec<_> = values.iter().filter(|v| v.param_name == "family").collect();
        assert!(!family_values.is_empty());
    }

    #[test]
    fn test_extract_patient_identifier() {
        let extractor = create_test_extractor();

        let patient = json!({
            "resourceType": "Patient",
            "id": "123",
            "identifier": [
                {
                    "system": "http://hospital.org/mrn",
                    "value": "12345"
                }
            ]
        });

        let values = extractor.extract(&patient, "Patient").unwrap();

        let id_values: Vec<_> = values.iter().filter(|v| v.param_name == "identifier").collect();
        assert!(!id_values.is_empty());

        if let IndexValue::Token { system, code } = &id_values[0].value {
            assert_eq!(system.as_ref().unwrap(), "http://hospital.org/mrn");
            assert_eq!(code, "12345");
        }
    }

    #[test]
    fn test_parse_simple_path() {
        let extractor = create_test_extractor();
        let segments = extractor.parse_path("Patient.name");

        assert_eq!(segments.len(), 1);
        if let PathSegment::Field(name) = &segments[0] {
            assert_eq!(name, "name");
        }
    }

    #[test]
    fn test_parse_nested_path() {
        let extractor = create_test_extractor();
        let segments = extractor.parse_path("Patient.name.family");

        assert_eq!(segments.len(), 2);
        if let PathSegment::Field(name) = &segments[0] {
            assert_eq!(name, "name");
        }
        if let PathSegment::Field(name) = &segments[1] {
            assert_eq!(name, "family");
        }
    }

    #[test]
    fn test_parse_path_with_function() {
        let extractor = create_test_extractor();
        let segments = extractor.parse_path("Observation.value.ofType(Quantity)");

        // Should have "value" and a type filter
        assert!(segments.len() >= 1);
    }

    #[test]
    fn test_extract_observation_values() {
        let extractor = create_test_extractor();

        let observation = json!({
            "resourceType": "Observation",
            "id": "obs1",
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "8867-4"
                    }
                ]
            },
            "subject": {
                "reference": "Patient/123"
            },
            "valueQuantity": {
                "value": 120.5,
                "unit": "mmHg"
            }
        });

        let values = extractor.extract(&observation, "Observation").unwrap();

        // Should have code
        let code_values: Vec<_> = values.iter().filter(|v| v.param_name == "code").collect();
        assert!(!code_values.is_empty());

        // Should have subject
        let subject_values: Vec<_> = values.iter().filter(|v| v.param_name == "subject").collect();
        assert!(!subject_values.is_empty());
    }

    #[test]
    fn test_invalid_resource() {
        let extractor = create_test_extractor();

        let not_object = json!("string");
        let result = extractor.extract(&not_object, "Patient");
        assert!(result.is_err());
    }

    #[test]
    fn test_resource_type_mismatch() {
        let extractor = create_test_extractor();

        let patient = json!({
            "resourceType": "Patient",
            "id": "123"
        });

        let result = extractor.extract(&patient, "Observation");
        assert!(result.is_err());
    }
}
