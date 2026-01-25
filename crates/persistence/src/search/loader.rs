//! SearchParameter Loader.
//!
//! Loads SearchParameter definitions from multiple sources:
//! - Embedded standard parameters (compiled into the binary)
//! - Stored SearchParameter resources (from database)
//! - Runtime configuration files

use std::path::Path;

use serde_json::Value;

use crate::types::SearchParamType;

use super::errors::LoaderError;
use super::registry::{
    CompositeComponentDef, SearchParameterDefinition, SearchParameterSource, SearchParameterStatus,
};

/// FHIR version for loading appropriate search parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FhirVersion {
    /// FHIR R4 (4.0.1)
    R4,
    /// FHIR R4B (4.3.0)
    R4B,
    /// FHIR R5 (5.0.0)
    R5,
    /// FHIR R6 (6.0.0-ballot1)
    R6,
}

impl Default for FhirVersion {
    fn default() -> Self {
        FhirVersion::R4
    }
}

impl FhirVersion {
    /// Returns the version string.
    pub fn as_str(&self) -> &'static str {
        match self {
            FhirVersion::R4 => "R4",
            FhirVersion::R4B => "R4B",
            FhirVersion::R5 => "R5",
            FhirVersion::R6 => "R6",
        }
    }
}

/// Loader for SearchParameter definitions.
pub struct SearchParameterLoader {
    fhir_version: FhirVersion,
}

impl SearchParameterLoader {
    /// Creates a new loader for the specified FHIR version.
    pub fn new(fhir_version: FhirVersion) -> Self {
        Self { fhir_version }
    }

    /// Returns the FHIR version.
    pub fn version(&self) -> FhirVersion {
        self.fhir_version
    }

    /// Loads embedded standard parameters for the FHIR version.
    ///
    /// This returns the core search parameters from the FHIR specification.
    pub fn load_embedded(&self) -> Result<Vec<SearchParameterDefinition>, LoaderError> {
        // For now, return a set of commonly used search parameters
        // In production, this would load from embedded JSON files
        Ok(self.get_core_search_parameters())
    }

    /// Loads SearchParameter resources from a JSON bundle or array.
    pub fn load_from_json(&self, json: &Value) -> Result<Vec<SearchParameterDefinition>, LoaderError> {
        let mut params = Vec::new();

        // Handle Bundle
        if let Some(entries) = json.get("entry").and_then(|e| e.as_array()) {
            for entry in entries {
                if let Some(resource) = entry.get("resource") {
                    if resource.get("resourceType").and_then(|t| t.as_str()) == Some("SearchParameter")
                    {
                        params.push(self.parse_resource(resource)?);
                    }
                }
            }
        }
        // Handle array of SearchParameter resources
        else if let Some(array) = json.as_array() {
            for item in array {
                if item.get("resourceType").and_then(|t| t.as_str()) == Some("SearchParameter") {
                    params.push(self.parse_resource(item)?);
                }
            }
        }
        // Handle single SearchParameter
        else if json.get("resourceType").and_then(|t| t.as_str()) == Some("SearchParameter") {
            params.push(self.parse_resource(json)?);
        }

        Ok(params)
    }

    /// Loads parameters from a configuration file.
    pub fn load_config(&self, config_path: &Path) -> Result<Vec<SearchParameterDefinition>, LoaderError> {
        let content = std::fs::read_to_string(config_path).map_err(|e| {
            LoaderError::ConfigLoadFailed {
                path: config_path.display().to_string(),
                message: e.to_string(),
            }
        })?;

        let json: Value = serde_json::from_str(&content).map_err(|e| {
            LoaderError::ConfigLoadFailed {
                path: config_path.display().to_string(),
                message: format!("Invalid JSON: {}", e),
            }
        })?;

        let mut params = self.load_from_json(&json)?;

        // Mark all as config source
        for param in &mut params {
            param.source = SearchParameterSource::Config;
        }

        Ok(params)
    }

    /// Parses a SearchParameter FHIR resource into a definition.
    pub fn parse_resource(&self, resource: &Value) -> Result<SearchParameterDefinition, LoaderError> {
        let url = resource
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| LoaderError::MissingField {
                field: "url".to_string(),
                url: None,
            })?
            .to_string();

        let code = resource
            .get("code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| LoaderError::MissingField {
                field: "code".to_string(),
                url: Some(url.clone()),
            })?
            .to_string();

        let type_str = resource
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| LoaderError::MissingField {
                field: "type".to_string(),
                url: Some(url.clone()),
            })?;

        let param_type = type_str.parse::<SearchParamType>().map_err(|_| {
            LoaderError::InvalidResource {
                message: format!("Unknown search parameter type: {}", type_str),
                url: Some(url.clone()),
            }
        })?;

        let expression = resource
            .get("expression")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // For non-composite types, expression is required
        if expression.is_empty() && param_type != SearchParamType::Composite {
            // Some special parameters don't have expressions
            if !code.starts_with('_') {
                return Err(LoaderError::MissingField {
                    field: "expression".to_string(),
                    url: Some(url),
                });
            }
        }

        let base: Vec<String> = resource
            .get("base")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let target: Option<Vec<String>> = resource.get("target").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

        let status = resource
            .get("status")
            .and_then(|v| v.as_str())
            .and_then(SearchParameterStatus::from_fhir_status)
            .unwrap_or(SearchParameterStatus::Active);

        let component = self.parse_components(resource)?;

        let modifier: Option<Vec<String>> = resource.get("modifier").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

        let comparator: Option<Vec<String>> = resource.get("comparator").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

        Ok(SearchParameterDefinition {
            url,
            code,
            name: resource.get("name").and_then(|v| v.as_str()).map(String::from),
            description: resource.get("description").and_then(|v| v.as_str()).map(String::from),
            param_type,
            expression,
            base,
            target,
            component,
            status,
            source: SearchParameterSource::Stored,
            modifier,
            multiple_or: resource.get("multipleOr").and_then(|v| v.as_bool()),
            multiple_and: resource.get("multipleAnd").and_then(|v| v.as_bool()),
            comparator,
            xpath: resource.get("xpath").and_then(|v| v.as_str()).map(String::from),
        })
    }

    /// Parses composite components from a SearchParameter resource.
    fn parse_components(&self, resource: &Value) -> Result<Option<Vec<CompositeComponentDef>>, LoaderError> {
        let components = match resource.get("component").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return Ok(None),
        };

        let mut result = Vec::new();
        for comp in components {
            let definition = comp
                .get("definition")
                .and_then(|v| v.as_str())
                .ok_or_else(|| LoaderError::InvalidResource {
                    message: "Composite component missing definition".to_string(),
                    url: resource.get("url").and_then(|v| v.as_str()).map(String::from),
                })?
                .to_string();

            let expression = comp
                .get("expression")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            result.push(CompositeComponentDef {
                definition,
                expression,
            });
        }

        Ok(if result.is_empty() { None } else { Some(result) })
    }

    /// Returns core search parameters for the FHIR version.
    fn get_core_search_parameters(&self) -> Vec<SearchParameterDefinition> {
        let mut params = Vec::new();

        // Common parameters for all resource types
        // Note: We use simplified expressions without "Resource." prefix since our FHIRPath
        // evaluator doesn't support Resource type filtering. The FHIR spec uses "Resource.id",
        // but we simplify to just "id" which works correctly when evaluated in the resource context.
        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Resource-id",
                "_id",
                SearchParamType::Token,
                "id",
            )
            .with_base(vec!["Resource"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Resource-lastUpdated",
                "_lastUpdated",
                SearchParamType::Date,
                "meta.lastUpdated",
            )
            .with_base(vec!["Resource"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Resource-tag",
                "_tag",
                SearchParamType::Token,
                "meta.tag",
            )
            .with_base(vec!["Resource"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Resource-profile",
                "_profile",
                SearchParamType::Uri,
                "meta.profile",
            )
            .with_base(vec!["Resource"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Resource-security",
                "_security",
                SearchParamType::Token,
                "meta.security",
            )
            .with_base(vec!["Resource"])
            .with_source(SearchParameterSource::Embedded),
        );

        // Patient search parameters
        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-name",
                "name",
                SearchParamType::String,
                "Patient.name",
            )
            .with_base(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-family",
                "family",
                SearchParamType::String,
                "Patient.name.family",
            )
            .with_base(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-given",
                "given",
                SearchParamType::String,
                "Patient.name.given",
            )
            .with_base(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-identifier",
                "identifier",
                SearchParamType::Token,
                "Patient.identifier",
            )
            .with_base(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-birthdate",
                "birthdate",
                SearchParamType::Date,
                "Patient.birthDate",
            )
            .with_base(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-gender",
                "gender",
                SearchParamType::Token,
                "Patient.gender",
            )
            .with_base(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Patient-organization",
                "organization",
                SearchParamType::Reference,
                "Patient.managingOrganization",
            )
            .with_base(vec!["Patient"])
            .with_targets(vec!["Organization"])
            .with_source(SearchParameterSource::Embedded),
        );

        // Observation search parameters
        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Observation-code",
                "code",
                SearchParamType::Token,
                "Observation.code",
            )
            .with_base(vec!["Observation"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Observation-subject",
                "subject",
                SearchParamType::Reference,
                "Observation.subject",
            )
            .with_base(vec!["Observation"])
            .with_targets(vec!["Patient", "Group", "Device", "Location"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Observation-patient",
                "patient",
                SearchParamType::Reference,
                "Observation.subject.where(resolve() is Patient)",
            )
            .with_base(vec!["Observation"])
            .with_targets(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Observation-date",
                "date",
                SearchParamType::Date,
                "Observation.effective",
            )
            .with_base(vec!["Observation"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Observation-value-quantity",
                "value-quantity",
                SearchParamType::Quantity,
                "Observation.value.ofType(Quantity)",
            )
            .with_base(vec!["Observation"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Observation-status",
                "status",
                SearchParamType::Token,
                "Observation.status",
            )
            .with_base(vec!["Observation"])
            .with_source(SearchParameterSource::Embedded),
        );

        // Encounter search parameters
        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Encounter-patient",
                "patient",
                SearchParamType::Reference,
                "Encounter.subject.where(resolve() is Patient)",
            )
            .with_base(vec!["Encounter"])
            .with_targets(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Encounter-status",
                "status",
                SearchParamType::Token,
                "Encounter.status",
            )
            .with_base(vec!["Encounter"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Encounter-date",
                "date",
                SearchParamType::Date,
                "Encounter.period",
            )
            .with_base(vec!["Encounter"])
            .with_source(SearchParameterSource::Embedded),
        );

        // Condition search parameters
        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Condition-patient",
                "patient",
                SearchParamType::Reference,
                "Condition.subject.where(resolve() is Patient)",
            )
            .with_base(vec!["Condition"])
            .with_targets(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/Condition-code",
                "code",
                SearchParamType::Token,
                "Condition.code",
            )
            .with_base(vec!["Condition"])
            .with_source(SearchParameterSource::Embedded),
        );

        // MedicationRequest search parameters
        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/MedicationRequest-patient",
                "patient",
                SearchParamType::Reference,
                "MedicationRequest.subject.where(resolve() is Patient)",
            )
            .with_base(vec!["MedicationRequest"])
            .with_targets(vec!["Patient"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/MedicationRequest-medication",
                "medication",
                SearchParamType::Reference,
                "MedicationRequest.medication.reference",
            )
            .with_base(vec!["MedicationRequest"])
            .with_targets(vec!["Medication"])
            .with_source(SearchParameterSource::Embedded),
        );

        params.push(
            SearchParameterDefinition::new(
                "http://hl7.org/fhir/SearchParameter/MedicationRequest-status",
                "status",
                SearchParamType::Token,
                "MedicationRequest.status",
            )
            .with_base(vec!["MedicationRequest"])
            .with_source(SearchParameterSource::Embedded),
        );

        params
    }
}

impl Default for SearchParameterLoader {
    fn default() -> Self {
        Self::new(FhirVersion::R4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fhir_version() {
        assert_eq!(FhirVersion::R4.as_str(), "R4");
        assert_eq!(FhirVersion::default(), FhirVersion::R4);
    }

    #[test]
    fn test_load_embedded() {
        let loader = SearchParameterLoader::new(FhirVersion::R4);
        let params = loader.load_embedded().unwrap();

        assert!(!params.is_empty());

        // Check for common parameters
        let has_patient_name = params.iter().any(|p| p.code == "name" && p.base.contains(&"Patient".to_string()));
        assert!(has_patient_name);

        let has_id = params.iter().any(|p| p.code == "_id");
        assert!(has_id);
    }

    #[test]
    fn test_parse_resource() {
        let loader = SearchParameterLoader::new(FhirVersion::R4);

        let json = serde_json::json!({
            "resourceType": "SearchParameter",
            "url": "http://example.org/sp/test",
            "code": "test",
            "type": "string",
            "expression": "Patient.test",
            "base": ["Patient"],
            "status": "active"
        });

        let param = loader.parse_resource(&json).unwrap();

        assert_eq!(param.url, "http://example.org/sp/test");
        assert_eq!(param.code, "test");
        assert_eq!(param.param_type, SearchParamType::String);
        assert_eq!(param.expression, "Patient.test");
        assert!(param.base.contains(&"Patient".to_string()));
        assert_eq!(param.status, SearchParameterStatus::Active);
    }

    #[test]
    fn test_parse_resource_missing_field() {
        let loader = SearchParameterLoader::new(FhirVersion::R4);

        let json = serde_json::json!({
            "resourceType": "SearchParameter",
            "code": "test",
            "type": "string"
        });

        let result = loader.parse_resource(&json);
        assert!(matches!(result, Err(LoaderError::MissingField { field, .. }) if field == "url"));
    }

    #[test]
    fn test_load_from_json_bundle() {
        let loader = SearchParameterLoader::new(FhirVersion::R4);

        let json = serde_json::json!({
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "SearchParameter",
                        "url": "http://example.org/sp/test1",
                        "code": "test1",
                        "type": "string",
                        "expression": "Patient.test1",
                        "base": ["Patient"]
                    }
                },
                {
                    "resource": {
                        "resourceType": "SearchParameter",
                        "url": "http://example.org/sp/test2",
                        "code": "test2",
                        "type": "token",
                        "expression": "Patient.test2",
                        "base": ["Patient"]
                    }
                }
            ]
        });

        let params = loader.load_from_json(&json).unwrap();
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_parse_composite_components() {
        let loader = SearchParameterLoader::new(FhirVersion::R4);

        let json = serde_json::json!({
            "resourceType": "SearchParameter",
            "url": "http://example.org/sp/composite",
            "code": "composite-test",
            "type": "composite",
            "expression": "",
            "base": ["Observation"],
            "component": [
                {
                    "definition": "http://hl7.org/fhir/SearchParameter/Observation-code",
                    "expression": "code"
                },
                {
                    "definition": "http://hl7.org/fhir/SearchParameter/Observation-value-quantity",
                    "expression": "value"
                }
            ]
        });

        let param = loader.parse_resource(&json).unwrap();
        assert!(param.is_composite());
        assert_eq!(param.component.as_ref().unwrap().len(), 2);
    }
}
