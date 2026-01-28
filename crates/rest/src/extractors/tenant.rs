//! Tenant context extractor.
//!
//! Extracts tenant information from request headers and creates
//! a TenantContext for use in handlers.

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, request::Parts},
};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};

use crate::middleware::tenant::X_TENANT_ID;

/// Axum extractor for tenant context.
///
/// Extracts the tenant ID from the X-Tenant-ID header and creates
/// a TenantContext with appropriate permissions.
///
/// # Example
///
/// ```rust,ignore
/// use helios_rest::extractors::TenantExtractor;
///
/// async fn handler(tenant: TenantExtractor) {
///     println!("Tenant ID: {}", tenant.tenant_id());
/// }
/// ```
#[derive(Debug, Clone)]
pub struct TenantExtractor {
    context: TenantContext,
}

impl TenantExtractor {
    /// Creates a new TenantExtractor with the given tenant ID.
    pub fn new(tenant_id: &str) -> Self {
        Self {
            context: TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access()),
        }
    }

    /// Creates a TenantExtractor with the default tenant.
    pub fn default_tenant() -> Self {
        Self::new("default")
    }

    /// Returns a reference to the tenant context.
    pub fn context(&self) -> &TenantContext {
        &self.context
    }

    /// Returns the tenant ID as a string.
    pub fn tenant_id(&self) -> &str {
        self.context.tenant_id().as_str()
    }

    /// Consumes the extractor and returns the tenant context.
    pub fn into_context(self) -> TenantContext {
        self.context
    }
}

impl std::fmt::Display for TenantExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.tenant_id())
    }
}

/// Extracts tenant ID from headers.
fn extract_tenant_id_from_headers(headers: &HeaderMap, default: &str) -> String {
    headers
        .get(&X_TENANT_ID)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| default.to_string())
}

impl<S> FromRequestParts<S> for TenantExtractor
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Try to extract from X-Tenant-ID header, default to "default"
        let tenant_id = extract_tenant_id_from_headers(&parts.headers, "default");

        // Validate tenant ID (basic validation)
        if tenant_id.is_empty() {
            return Err((StatusCode::BAD_REQUEST, "Invalid tenant ID"));
        }

        // In a production system, we might validate the tenant exists
        // and load its permissions from a database

        Ok(TenantExtractor::new(&tenant_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_new() {
        let extractor = TenantExtractor::new("test-tenant");
        assert_eq!(extractor.tenant_id(), "test-tenant");
    }

    #[test]
    fn test_default_tenant() {
        let extractor = TenantExtractor::default_tenant();
        assert_eq!(extractor.tenant_id(), "default");
    }

    #[test]
    fn test_extract_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(&X_TENANT_ID, HeaderValue::from_static("my-tenant"));

        let tenant_id = extract_tenant_id_from_headers(&headers, "default");
        assert_eq!(tenant_id, "my-tenant");
    }

    #[test]
    fn test_extract_missing_uses_default() {
        let headers = HeaderMap::new();
        let tenant_id = extract_tenant_id_from_headers(&headers, "default");
        assert_eq!(tenant_id, "default");
    }
}
