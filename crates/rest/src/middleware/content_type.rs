//! Content negotiation middleware.
//!
//! Handles content type negotiation for FHIR requests and responses.

use axum::http::{HeaderMap, StatusCode, header};

/// Supported FHIR content types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FhirContentType {
    /// JSON format (application/fhir+json)
    Json,
    /// XML format (application/fhir+xml)
    Xml,
    /// NDJSON format (application/fhir+ndjson) - for bulk operations
    NdJson,
}

impl FhirContentType {
    /// Returns the MIME type string for this content type.
    pub fn mime_type(&self) -> &'static str {
        match self {
            FhirContentType::Json => "application/fhir+json",
            FhirContentType::Xml => "application/fhir+xml",
            FhirContentType::NdJson => "application/fhir+ndjson",
        }
    }

    /// Parses a content type string into a FhirContentType.
    pub fn parse(content_type: &str) -> Option<Self> {
        let ct = content_type.to_lowercase();

        if ct.contains("fhir+json") || ct.contains("application/json") {
            Some(FhirContentType::Json)
        } else if ct.contains("fhir+xml") || ct.contains("application/xml") {
            Some(FhirContentType::Xml)
        } else if ct.contains("fhir+ndjson") || ct.contains("application/ndjson") {
            Some(FhirContentType::NdJson)
        } else {
            None
        }
    }
}

/// Determines the response content type from the Accept header.
///
/// Returns JSON if no Accept header is present or if the client accepts
/// multiple types including JSON.
pub fn negotiate_content_type(headers: &HeaderMap) -> FhirContentType {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/fhir+json");

    // Parse Accept header (simplified - doesn't handle quality values)
    for media_type in accept.split(',') {
        let media_type = media_type.trim();

        if let Some(ct) = FhirContentType::parse(media_type) {
            return ct;
        }

        // Handle wildcards
        if media_type == "*/*" || media_type == "application/*" {
            return FhirContentType::Json; // Default to JSON
        }
    }

    // Default to JSON if nothing matched
    FhirContentType::Json
}

/// Validates the Content-Type header for incoming requests.
///
/// Returns an error status if the content type is not supported.
pub fn validate_request_content_type(headers: &HeaderMap) -> Result<FhirContentType, StatusCode> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());

    match content_type {
        Some(ct) => FhirContentType::parse(ct).ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE),
        None => {
            // If no Content-Type, assume JSON
            Ok(FhirContentType::Json)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_content_type() {
        assert_eq!(
            FhirContentType::parse("application/fhir+json"),
            Some(FhirContentType::Json)
        );
        assert_eq!(
            FhirContentType::parse("application/fhir+xml"),
            Some(FhirContentType::Xml)
        );
        assert_eq!(
            FhirContentType::parse("application/json"),
            Some(FhirContentType::Json)
        );
        assert_eq!(FhirContentType::parse("text/plain"), None);
    }

    #[test]
    fn test_mime_type() {
        assert_eq!(FhirContentType::Json.mime_type(), "application/fhir+json");
        assert_eq!(FhirContentType::Xml.mime_type(), "application/fhir+xml");
    }
}
