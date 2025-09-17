use crate::{SofBundle, SofError};
use async_trait::async_trait;
use helios_fhir::Element;
use reqwest;
use serde_json;
use tokio::fs;
use url::Url;

/// Trait for loading FHIR data from various sources
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Load FHIR data from the source and return as a Bundle
    async fn load(&self, source: &str) -> Result<SofBundle, SofError>;
}

/// Implementation for loading data from various sources based on URL scheme
pub struct UniversalDataSource {
    client: reqwest::Client,
}

impl Default for UniversalDataSource {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl UniversalDataSource {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }
}

#[async_trait]
impl DataSource for UniversalDataSource {
    async fn load(&self, source: &str) -> Result<SofBundle, SofError> {
        // Parse the source as a URL to determine the protocol
        let url = Url::parse(source).map_err(|e| {
            SofError::InvalidSource(format!("Invalid source URL '{}': {}", source, e))
        })?;

        match url.scheme() {
            "file" => load_from_file(&url).await,
            "http" | "https" => load_from_http(&self.client, &url).await,
            scheme => Err(SofError::UnsupportedSourceProtocol(format!(
                "Unsupported source protocol: {}",
                scheme
            ))),
        }
    }
}

/// Load FHIR data from a local file
async fn load_from_file(url: &Url) -> Result<SofBundle, SofError> {
    // Convert file URL to path
    let path = url
        .to_file_path()
        .map_err(|_| SofError::InvalidSource(format!("Invalid file URL: {}", url)))?;

    // Check if file exists
    if !path.exists() {
        return Err(SofError::SourceNotFound(format!(
            "File not found: {}",
            path.display()
        )));
    }

    // Read file contents
    let contents = fs::read_to_string(&path)
        .await
        .map_err(|e| SofError::SourceReadError(format!("Failed to read file: {}", e)))?;

    // Parse and convert to bundle
    parse_fhir_content(&contents, &path.to_string_lossy())
}

/// Load FHIR data from HTTP/HTTPS URL
async fn load_from_http(client: &reqwest::Client, url: &Url) -> Result<SofBundle, SofError> {
    // Fetch content from URL
    let response = client
        .get(url.as_str())
        .header("Accept", "application/fhir+json, application/json")
        .send()
        .await
        .map_err(|e| {
            SofError::SourceFetchError(format!("Failed to fetch from URL '{}': {}", url, e))
        })?;

    // Check response status
    if !response.status().is_success() {
        return Err(SofError::SourceFetchError(format!(
            "HTTP error {} when fetching '{}'",
            response.status(),
            url
        )));
    }

    // Get content
    let contents = response
        .text()
        .await
        .map_err(|e| SofError::SourceReadError(format!("Failed to read response body: {}", e)))?;

    // Parse and convert to bundle
    parse_fhir_content(&contents, url.as_str())
}

/// Parse FHIR content and convert to SofBundle
fn parse_fhir_content(contents: &str, source_name: &str) -> Result<SofBundle, SofError> {
    // First, try to determine what type of content we have
    let value: serde_json::Value = serde_json::from_str(contents).map_err(|e| {
        SofError::InvalidSourceContent(format!(
            "Failed to parse JSON from '{}': {}",
            source_name, e
        ))
    })?;

    // Check if it's already a Bundle
    if let Some(resource_type) = value.get("resourceType").and_then(|v| v.as_str()) {
        if resource_type == "Bundle" {
            // Try parsing as each FHIR version
            #[cfg(feature = "R4")]
            if let Ok(bundle) = serde_json::from_value::<helios_fhir::r4::Bundle>(value.clone()) {
                return Ok(SofBundle::R4(bundle));
            }
            #[cfg(feature = "R4B")]
            if let Ok(bundle) = serde_json::from_value::<helios_fhir::r4b::Bundle>(value.clone()) {
                return Ok(SofBundle::R4B(bundle));
            }
            #[cfg(feature = "R5")]
            if let Ok(bundle) = serde_json::from_value::<helios_fhir::r5::Bundle>(value.clone()) {
                return Ok(SofBundle::R5(bundle));
            }
            #[cfg(feature = "R6")]
            if let Ok(bundle) = serde_json::from_value::<helios_fhir::r6::Bundle>(value.clone()) {
                return Ok(SofBundle::R6(bundle));
            }
            return Err(SofError::InvalidSourceContent(format!(
                "Bundle from '{}' could not be parsed as any supported FHIR version",
                source_name
            )));
        }

        // It's a single resource - wrap it in a Bundle
        return wrap_resource_in_bundle(value, source_name);
    }

    // Check if it's an array of resources
    if value.is_array() {
        return wrap_resources_in_bundle(value, source_name);
    }

    Err(SofError::InvalidSourceContent(format!(
        "Content from '{}' is not a valid FHIR resource or Bundle",
        source_name
    )))
}

/// Wrap a single resource in a Bundle
fn wrap_resource_in_bundle(
    resource: serde_json::Value,
    source_name: &str,
) -> Result<SofBundle, SofError> {
    // Try each FHIR version
    // R4
    #[cfg(feature = "R4")]
    if let Ok(res) = serde_json::from_value::<helios_fhir::r4::Resource>(resource.clone()) {
        let mut bundle = helios_fhir::r4::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        bundle.entry = Some(vec![helios_fhir::r4::BundleEntry {
            resource: Some(res),
            ..Default::default()
        }]);
        return Ok(SofBundle::R4(bundle));
    }

    // R4B
    #[cfg(feature = "R4B")]
    if let Ok(res) = serde_json::from_value::<helios_fhir::r4b::Resource>(resource.clone()) {
        let mut bundle = helios_fhir::r4b::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        bundle.entry = Some(vec![helios_fhir::r4b::BundleEntry {
            resource: Some(res),
            ..Default::default()
        }]);
        return Ok(SofBundle::R4B(bundle));
    }

    // R5
    #[cfg(feature = "R5")]
    if let Ok(res) = serde_json::from_value::<helios_fhir::r5::Resource>(resource.clone()) {
        let mut bundle = helios_fhir::r5::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        bundle.entry = Some(vec![helios_fhir::r5::BundleEntry {
            resource: Some(Box::new(res)),
            ..Default::default()
        }]);
        return Ok(SofBundle::R5(bundle));
    }

    // R6
    #[cfg(feature = "R6")]
    if let Ok(res) = serde_json::from_value::<helios_fhir::r6::Resource>(resource.clone()) {
        let mut bundle = helios_fhir::r6::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        bundle.entry = Some(vec![helios_fhir::r6::BundleEntry {
            resource: Some(Box::new(res)),
            ..Default::default()
        }]);
        return Ok(SofBundle::R6(bundle));
    }

    Err(SofError::InvalidSourceContent(format!(
        "Resource from '{}' could not be parsed as any supported FHIR version",
        source_name
    )))
}

/// Wrap an array of resources in a Bundle
fn wrap_resources_in_bundle(
    resources: serde_json::Value,
    source_name: &str,
) -> Result<SofBundle, SofError> {
    let arr = resources
        .as_array()
        .ok_or_else(|| SofError::InvalidSourceContent("Expected array of resources".to_string()))?;

    if arr.is_empty() {
        return Err(SofError::InvalidSourceContent(format!(
            "Empty array of resources from '{}'",
            source_name
        )));
    }

    // Try to parse the first resource to determine version
    let first = &arr[0];

    // Try R4
    #[cfg(feature = "R4")]
    if serde_json::from_value::<helios_fhir::r4::Resource>(first.clone()).is_ok() {
        let mut bundle = helios_fhir::r4::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        let mut entries = Vec::new();

        for resource in arr {
            let res = serde_json::from_value::<helios_fhir::r4::Resource>(resource.clone())
                .map_err(|e| {
                    SofError::InvalidSourceContent(format!(
                        "Failed to parse R4 resource from '{}': {}",
                        source_name, e
                    ))
                })?;
            entries.push(helios_fhir::r4::BundleEntry {
                resource: Some(res),
                ..Default::default()
            });
        }

        bundle.entry = Some(entries);
        return Ok(SofBundle::R4(bundle));
    }

    // Try R4B
    #[cfg(feature = "R4B")]
    if serde_json::from_value::<helios_fhir::r4b::Resource>(first.clone()).is_ok() {
        let mut bundle = helios_fhir::r4b::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        let mut entries = Vec::new();

        for resource in arr {
            let res = serde_json::from_value::<helios_fhir::r4b::Resource>(resource.clone())
                .map_err(|e| {
                    SofError::InvalidSourceContent(format!(
                        "Failed to parse R4B resource from '{}': {}",
                        source_name, e
                    ))
                })?;
            entries.push(helios_fhir::r4b::BundleEntry {
                resource: Some(res),
                ..Default::default()
            });
        }

        bundle.entry = Some(entries);
        return Ok(SofBundle::R4B(bundle));
    }

    // Try R5
    #[cfg(feature = "R5")]
    if serde_json::from_value::<helios_fhir::r5::Resource>(first.clone()).is_ok() {
        let mut bundle = helios_fhir::r5::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        let mut entries = Vec::new();

        for resource in arr {
            let res = serde_json::from_value::<helios_fhir::r5::Resource>(resource.clone())
                .map_err(|e| {
                    SofError::InvalidSourceContent(format!(
                        "Failed to parse R5 resource from '{}': {}",
                        source_name, e
                    ))
                })?;
            entries.push(helios_fhir::r5::BundleEntry {
                resource: Some(Box::new(res)),
                ..Default::default()
            });
        }

        bundle.entry = Some(entries);
        return Ok(SofBundle::R5(bundle));
    }

    // Try R6
    #[cfg(feature = "R6")]
    if serde_json::from_value::<helios_fhir::r6::Resource>(first.clone()).is_ok() {
        let mut bundle = helios_fhir::r6::Bundle::default();
        bundle.r#type = Element {
            id: None,
            extension: None,
            value: Some("collection".to_string()),
        };
        let mut entries = Vec::new();

        for resource in arr {
            let res = serde_json::from_value::<helios_fhir::r6::Resource>(resource.clone())
                .map_err(|e| {
                    SofError::InvalidSourceContent(format!(
                        "Failed to parse R6 resource from '{}': {}",
                        source_name, e
                    ))
                })?;
            entries.push(helios_fhir::r6::BundleEntry {
                resource: Some(Box::new(res)),
                ..Default::default()
            });
        }

        bundle.entry = Some(entries);
        return Ok(SofBundle::R6(bundle));
    }

    Err(SofError::InvalidSourceContent(format!(
        "Resources from '{}' could not be parsed as any supported FHIR version",
        source_name
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_fhir_bundle() {
        let bundle_json = r#"{
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [{
                "resource": {
                    "resourceType": "Patient",
                    "id": "123"
                }
            }]
        }"#;

        let result = parse_fhir_content(bundle_json, "test").unwrap();
        #[cfg(feature = "R4")]
        assert!(matches!(result, SofBundle::R4(_)));
        #[cfg(not(feature = "R4"))]
        assert!(matches!(result, _));
    }

    #[tokio::test]
    async fn test_parse_single_resource() {
        let patient_json = r#"{
            "resourceType": "Patient",
            "id": "123"
        }"#;

        let result = parse_fhir_content(patient_json, "test").unwrap();
        #[cfg(feature = "R4")]
        match result {
            SofBundle::R4(bundle) => {
                assert_eq!(bundle.entry.as_ref().unwrap().len(), 1);
            }
            #[cfg(feature = "R4B")]
            SofBundle::R4B(_) => panic!("Expected R4 bundle"),
            #[cfg(feature = "R5")]
            SofBundle::R5(_) => panic!("Expected R4 bundle"),
            #[cfg(feature = "R6")]
            SofBundle::R6(_) => panic!("Expected R4 bundle"),
        }
    }

    #[tokio::test]
    async fn test_parse_resource_array() {
        let resources_json = r#"[
            {
                "resourceType": "Patient",
                "id": "123"
            },
            {
                "resourceType": "Patient",
                "id": "456"
            }
        ]"#;

        let result = parse_fhir_content(resources_json, "test").unwrap();
        #[cfg(feature = "R4")]
        match result {
            SofBundle::R4(bundle) => {
                assert_eq!(bundle.entry.as_ref().unwrap().len(), 2);
            }
            #[cfg(feature = "R4B")]
            SofBundle::R4B(_) => panic!("Expected R4 bundle"),
            #[cfg(feature = "R5")]
            SofBundle::R5(_) => panic!("Expected R4 bundle"),
            #[cfg(feature = "R6")]
            SofBundle::R6(_) => panic!("Expected R4 bundle"),
        }
    }

    #[tokio::test]
    async fn test_invalid_content() {
        let invalid_json = r#"{"not": "fhir"}"#;
        let result = parse_fhir_content(invalid_json, "test");
        assert!(result.is_err());
    }
}
