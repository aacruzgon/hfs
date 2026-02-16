//! Prefer header handling.
//!
//! Handles the HTTP Prefer header for controlling response behavior.
//! See: https://hl7.org/fhir/http.html#ops

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, request::Parts},
};

/// Extracted Prefer header values from a request.
#[derive(Debug, Default)]
pub struct PreferHeader {
    /// Return preference (minimal, representation, OperationOutcome).
    return_preference: Option<String>,

    /// Handling preference (strict, lenient).
    handling: Option<String>,

    /// Respond-async preference.
    respond_async: bool,
}

impl PreferHeader {
    /// Creates a new PreferHeader from a HeaderMap.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let prefer = headers
            .get("prefer")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let mut result = Self::default();

        // Parse Prefer header directives
        for directive in prefer.split(',') {
            let directive = directive.trim();

            if let Some(value) = directive.strip_prefix("return=") {
                result.return_preference = Some(value.to_string());
            } else if let Some(value) = directive.strip_prefix("handling=") {
                result.handling = Some(value.to_string());
            } else if directive == "respond-async" {
                result.respond_async = true;
            }
        }

        result
    }

    /// Returns the return preference.
    ///
    /// Possible values:
    /// - `minimal` - Return only the response headers
    /// - `representation` - Return the created/updated resource
    /// - `OperationOutcome` - Return an OperationOutcome
    pub fn return_preference(&self) -> Option<&str> {
        self.return_preference.as_deref()
    }

    /// Returns the handling preference.
    ///
    /// Possible values:
    /// - `strict` - Fail on any error
    /// - `lenient` - Ignore unknown elements
    pub fn handling(&self) -> Option<&str> {
        self.handling.as_deref()
    }

    /// Returns whether async response is preferred.
    pub fn prefer_async(&self) -> bool {
        self.respond_async
    }

    /// Checks if minimal return is requested.
    pub fn is_minimal(&self) -> bool {
        self.return_preference.as_deref() == Some("minimal")
    }

    /// Checks if OperationOutcome return is requested.
    pub fn is_operation_outcome(&self) -> bool {
        self.return_preference.as_deref() == Some("OperationOutcome")
    }

    /// Checks if strict handling is requested.
    pub fn is_strict(&self) -> bool {
        self.handling.as_deref() == Some("strict")
    }
}

/// Axum extractor for Prefer header.
impl<S> FromRequestParts<S> for PreferHeader
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(PreferHeader::from_headers(&parts.headers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_return_minimal() {
        let mut headers = HeaderMap::new();
        headers.insert("prefer", HeaderValue::from_static("return=minimal"));

        let prefer = PreferHeader::from_headers(&headers);
        assert_eq!(prefer.return_preference(), Some("minimal"));
        assert!(prefer.is_minimal());
    }

    #[test]
    fn test_return_representation() {
        let mut headers = HeaderMap::new();
        headers.insert("prefer", HeaderValue::from_static("return=representation"));

        let prefer = PreferHeader::from_headers(&headers);
        assert_eq!(prefer.return_preference(), Some("representation"));
        assert!(!prefer.is_minimal());
    }

    #[test]
    fn test_handling_strict() {
        let mut headers = HeaderMap::new();
        headers.insert("prefer", HeaderValue::from_static("handling=strict"));

        let prefer = PreferHeader::from_headers(&headers);
        assert_eq!(prefer.handling(), Some("strict"));
        assert!(prefer.is_strict());
    }

    #[test]
    fn test_respond_async() {
        let mut headers = HeaderMap::new();
        headers.insert("prefer", HeaderValue::from_static("respond-async"));

        let prefer = PreferHeader::from_headers(&headers);
        assert!(prefer.prefer_async());
    }

    #[test]
    fn test_multiple_directives() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "prefer",
            HeaderValue::from_static("return=minimal, handling=lenient"),
        );

        let prefer = PreferHeader::from_headers(&headers);
        assert_eq!(prefer.return_preference(), Some("minimal"));
        assert_eq!(prefer.handling(), Some("lenient"));
    }

    #[test]
    fn test_empty() {
        let headers = HeaderMap::new();
        let prefer = PreferHeader::from_headers(&headers);

        assert!(prefer.return_preference().is_none());
        assert!(prefer.handling().is_none());
        assert!(!prefer.prefer_async());
    }
}
