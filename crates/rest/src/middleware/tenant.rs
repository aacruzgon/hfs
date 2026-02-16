//! Tenant identification middleware.
//!
//! Extracts tenant information from the X-Tenant-ID header or uses the
//! default tenant from configuration.

use axum::{extract::Request, http::header::HeaderName, middleware::Next, response::Response};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use tracing::debug;

/// Header name for tenant identification.
pub static X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");

/// Extracts tenant ID from a request.
///
/// This function can be used by handlers that need the tenant ID
/// before the full middleware chain runs.
pub fn extract_tenant_id(request: &Request, default_tenant: &str) -> String {
    request
        .headers()
        .get(&X_TENANT_ID)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| default_tenant.to_string())
}

/// Creates a tenant context from a tenant ID.
///
/// By default, tenants have full access permissions. In a production
/// system, permissions would be loaded from a database or auth system.
pub fn create_tenant_context(tenant_id: &str) -> TenantContext {
    TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access())
}

/// Middleware function for tenant extraction.
///
/// This can be used with `axum::middleware::from_fn`.
pub async fn tenant_middleware(request: Request, next: Next) -> Response {
    // Extract tenant ID (or use default "default")
    let tenant_id = extract_tenant_id(&request, "default");
    debug!(tenant_id = %tenant_id, "Extracted tenant ID");

    // Continue processing
    next.run(request).await
}
