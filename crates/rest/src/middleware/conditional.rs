//! Conditional request header handling.
//!
//! Handles HTTP conditional headers for FHIR requests:
//! - If-Match: Optimistic locking for updates
//! - If-None-Match: Conditional read
//! - If-Modified-Since: Conditional read by date
//! - If-None-Exist: Conditional create

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, header, request::Parts},
};
use chrono::{DateTime, Utc};

/// Extracted conditional headers from a request.
#[derive(Debug, Default)]
pub struct ConditionalHeaders {
    /// If-Match header value (for optimistic locking).
    if_match: Option<String>,

    /// If-None-Match header value (for conditional read).
    if_none_match: Option<String>,

    /// If-Modified-Since header value.
    if_modified_since: Option<DateTime<Utc>>,

    /// If-None-Exist header value (for conditional create).
    if_none_exist: Option<String>,
}

impl ConditionalHeaders {
    /// Creates a new ConditionalHeaders from a HeaderMap.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let if_match = headers
            .get(header::IF_MATCH)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let if_none_match = headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let if_modified_since = headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        // If-None-Exist is a custom FHIR header
        let if_none_exist = headers
            .get("if-none-exist")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        Self {
            if_match,
            if_none_match,
            if_modified_since,
            if_none_exist,
        }
    }

    /// Returns the If-Match header value.
    ///
    /// Used for optimistic locking - the update should only proceed if
    /// the current resource version matches this ETag.
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }

    /// Returns the If-None-Match header value.
    ///
    /// Used for conditional read - return 304 Not Modified if the
    /// current resource version matches this ETag.
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }

    /// Returns the If-Modified-Since header value.
    ///
    /// Used for conditional read - return 304 Not Modified if the
    /// resource has not been modified since this date.
    pub fn if_modified_since(&self) -> Option<DateTime<Utc>> {
        self.if_modified_since
    }

    /// Returns the If-None-Exist header value.
    ///
    /// Used for conditional create - only create the resource if
    /// no resource matches these search parameters.
    pub fn if_none_exist(&self) -> Option<&str> {
        self.if_none_exist.as_deref()
    }

    /// Checks if any conditional headers are present.
    pub fn has_conditions(&self) -> bool {
        self.if_match.is_some()
            || self.if_none_match.is_some()
            || self.if_modified_since.is_some()
            || self.if_none_exist.is_some()
    }
}

/// Axum extractor for conditional headers.
impl<S> FromRequestParts<S> for ConditionalHeaders
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(ConditionalHeaders::from_headers(&parts.headers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    #[test]
    fn test_from_headers_if_match() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, HeaderValue::from_static("W/\"1\""));

        let conditional = ConditionalHeaders::from_headers(&headers);
        assert_eq!(conditional.if_match(), Some("W/\"1\""));
    }

    #[test]
    fn test_from_headers_if_none_match() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("W/\"2\""));

        let conditional = ConditionalHeaders::from_headers(&headers);
        assert_eq!(conditional.if_none_match(), Some("W/\"2\""));
    }

    #[test]
    fn test_from_headers_if_none_exist() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("if-none-exist"),
            HeaderValue::from_static("identifier=12345"),
        );

        let conditional = ConditionalHeaders::from_headers(&headers);
        assert_eq!(conditional.if_none_exist(), Some("identifier=12345"));
    }

    #[test]
    fn test_has_conditions() {
        let empty = ConditionalHeaders::default();
        assert!(!empty.has_conditions());

        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, HeaderValue::from_static("W/\"1\""));
        let with_conditions = ConditionalHeaders::from_headers(&headers);
        assert!(with_conditions.has_conditions());
    }
}
