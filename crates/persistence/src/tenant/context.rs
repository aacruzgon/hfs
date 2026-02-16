//! Tenant context for storage operations.
//!
//! This module defines [`TenantContext`], which provides validated tenant
//! information required for ALL storage operations. This design ensures
//! tenant isolation at the type level - operations cannot be performed
//! without a valid tenant context.

use std::sync::Arc;

use super::id::TenantId;
use super::permissions::{Operation, TenantPermissions};
use crate::error::{TenantError, ValidationError};

/// A validated tenant context required for all storage operations.
///
/// `TenantContext` encapsulates the tenant identity and permissions, providing
/// a type-level guarantee that all storage operations are tenant-aware.
///
/// # Design Philosophy
///
/// The persistence layer requires a `TenantContext` for every operation.
/// There is no "escape hatch" or way to bypass tenant isolation. This ensures:
///
/// 1. **Compile-time safety**: Forgetting to pass tenant context is a compile error
/// 2. **Audit trail**: Every operation has an associated tenant
/// 3. **Isolation**: Cross-tenant access is explicitly controlled
///
/// # Creation
///
/// Contexts are created through validated constructors that ensure the tenant
/// is valid and has appropriate permissions:
///
/// ```
/// use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
///
/// let context = TenantContext::new(
///     TenantId::new("acme"),
///     TenantPermissions::full_access(),
/// );
/// ```
///
/// # System Tenant
///
/// For accessing shared resources, use the system tenant context:
///
/// ```
/// use helios_persistence::tenant::TenantContext;
///
/// let system = TenantContext::system();
/// assert!(system.is_system());
/// ```
#[derive(Debug, Clone)]
pub struct TenantContext {
    /// The tenant identifier.
    tenant_id: TenantId,
    /// The permissions for this context.
    permissions: Arc<TenantPermissions>,
    /// Optional correlation ID for request tracing.
    correlation_id: Option<String>,
    /// Optional user ID for audit purposes.
    user_id: Option<String>,
}

impl TenantContext {
    /// Creates a new tenant context with the given ID and permissions.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `permissions` - The permissions for this context
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
    ///
    /// let ctx = TenantContext::new(
    ///     TenantId::new("my-tenant"),
    ///     TenantPermissions::full_access(),
    /// );
    /// ```
    pub fn new(tenant_id: TenantId, permissions: TenantPermissions) -> Self {
        Self {
            tenant_id,
            permissions: Arc::new(permissions),
            correlation_id: None,
            user_id: None,
        }
    }

    /// Creates a system tenant context for accessing shared resources.
    ///
    /// The system tenant has full access permissions and is used for
    /// terminology resources, configuration, and other shared data.
    pub fn system() -> Self {
        Self::new(TenantId::system(), TenantPermissions::full_access())
    }

    /// Creates a context with the specified correlation ID for tracing.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Creates a context with the specified user ID for auditing.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Returns the tenant ID.
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the permissions for this context.
    pub fn permissions(&self) -> &TenantPermissions {
        &self.permissions
    }

    /// Returns the correlation ID, if set.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// Returns the user ID, if set.
    pub fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    /// Returns `true` if this is the system tenant context.
    pub fn is_system(&self) -> bool {
        self.tenant_id.is_system()
    }

    /// Checks if the given operation is permitted on the given resource type.
    ///
    /// Returns `Ok(())` if permitted, or an error describing why access was denied.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions, Operation};
    ///
    /// let ctx = TenantContext::new(
    ///     TenantId::new("my-tenant"),
    ///     TenantPermissions::read_only(),
    /// );
    ///
    /// assert!(ctx.check_permission(Operation::Read, "Patient").is_ok());
    /// assert!(ctx.check_permission(Operation::Create, "Patient").is_err());
    /// ```
    pub fn check_permission(
        &self,
        operation: Operation,
        resource_type: &str,
    ) -> Result<(), TenantError> {
        if self.permissions.can_perform(operation, resource_type) {
            Ok(())
        } else {
            Err(TenantError::OperationNotPermitted {
                tenant_id: self.tenant_id.clone(),
                operation: operation.to_string(),
            })
        }
    }

    /// Checks if this context can access resources belonging to the given tenant.
    ///
    /// Access is allowed if:
    /// 1. The resource tenant matches this context's tenant
    /// 2. The resource is in the system tenant and system access is allowed
    /// 3. The resource is in a child tenant and child access is allowed
    ///
    /// # Arguments
    ///
    /// * `resource_tenant` - The tenant that owns the resource
    ///
    /// # Returns
    ///
    /// `Ok(())` if access is allowed, or a `TenantError` describing the denial.
    pub fn check_access(&self, resource_tenant: &TenantId) -> Result<(), TenantError> {
        // Same tenant always allowed
        if &self.tenant_id == resource_tenant {
            return Ok(());
        }

        // System tenant resources accessible if permitted
        if resource_tenant.is_system() && self.permissions.can_access_system_tenant() {
            return Ok(());
        }

        // Child tenant resources accessible if permitted
        if self.permissions.can_access_child_tenants()
            && resource_tenant.is_descendant_of(&self.tenant_id)
        {
            return Ok(());
        }

        Err(TenantError::AccessDenied {
            tenant_id: self.tenant_id.clone(),
            resource_type: "unknown".to_string(),
            resource_id: "unknown".to_string(),
        })
    }

    /// Validates that a reference target is accessible from this tenant context.
    ///
    /// This is used during resource creation/update to ensure references don't
    /// cross tenant boundaries inappropriately.
    ///
    /// # Arguments
    ///
    /// * `reference` - The reference string (e.g., "Patient/123")
    /// * `target_tenant` - The tenant that owns the referenced resource
    ///
    /// # Returns
    ///
    /// `Ok(())` if the reference is valid, or an error if cross-tenant reference
    /// is not allowed.
    pub fn validate_reference(
        &self,
        reference: &str,
        target_tenant: &TenantId,
    ) -> Result<(), TenantError> {
        // References within same tenant always allowed
        if &self.tenant_id == target_tenant {
            return Ok(());
        }

        // References to system tenant allowed if we can access it
        if target_tenant.is_system() && self.permissions.can_access_system_tenant() {
            return Ok(());
        }

        // Cross-tenant references not allowed
        Err(TenantError::CrossTenantReference {
            source_tenant: self.tenant_id.clone(),
            target_tenant: target_tenant.clone(),
            reference: reference.to_string(),
        })
    }
}

/// Builder for creating tenant contexts with validation.
///
/// This builder ensures that tenant contexts are properly validated before use.
/// It's particularly useful when constructing contexts from external input
/// (e.g., HTTP headers, JWT claims).
pub struct TenantContextBuilder {
    tenant_id: Option<TenantId>,
    permissions: Option<TenantPermissions>,
    correlation_id: Option<String>,
    user_id: Option<String>,
}

impl TenantContextBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self {
            tenant_id: None,
            permissions: None,
            correlation_id: None,
            user_id: None,
        }
    }

    /// Sets the tenant ID.
    pub fn tenant_id(mut self, tenant_id: TenantId) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    /// Sets the tenant ID from a string.
    pub fn tenant_id_str(mut self, tenant_id: &str) -> Self {
        self.tenant_id = Some(TenantId::new(tenant_id));
        self
    }

    /// Sets the permissions.
    pub fn permissions(mut self, permissions: TenantPermissions) -> Self {
        self.permissions = Some(permissions);
        self
    }

    /// Sets the correlation ID.
    pub fn correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Sets the user ID.
    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Builds the tenant context, returning an error if required fields are missing.
    pub fn build(self) -> Result<TenantContext, ValidationError> {
        let tenant_id = self
            .tenant_id
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "tenant_id".to_string(),
            })?;

        let permissions = self
            .permissions
            .unwrap_or_else(TenantPermissions::full_access);

        let mut ctx = TenantContext::new(tenant_id, permissions);
        ctx.correlation_id = self.correlation_id;
        ctx.user_id = self.user_id;

        Ok(ctx)
    }
}

impl Default for TenantContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_context_creation() {
        let ctx = TenantContext::new(TenantId::new("my-tenant"), TenantPermissions::full_access());
        assert_eq!(ctx.tenant_id().as_str(), "my-tenant");
        assert!(!ctx.is_system());
    }

    #[test]
    fn test_system_context() {
        let ctx = TenantContext::system();
        assert!(ctx.is_system());
    }

    #[test]
    fn test_with_correlation_id() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access())
            .with_correlation_id("req-123");
        assert_eq!(ctx.correlation_id(), Some("req-123"));
    }

    #[test]
    fn test_with_user_id() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access())
            .with_user_id("user-456");
        assert_eq!(ctx.user_id(), Some("user-456"));
    }

    #[test]
    fn test_check_permission_allowed() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        assert!(ctx.check_permission(Operation::Create, "Patient").is_ok());
    }

    #[test]
    fn test_check_permission_denied() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::read_only());
        let result = ctx.check_permission(Operation::Create, "Patient");
        assert!(result.is_err());
    }

    #[test]
    fn test_check_access_same_tenant() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        assert!(ctx.check_access(&TenantId::new("t1")).is_ok());
    }

    #[test]
    fn test_check_access_different_tenant() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        assert!(ctx.check_access(&TenantId::new("t2")).is_err());
    }

    #[test]
    fn test_check_access_system_tenant() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        assert!(ctx.check_access(&TenantId::system()).is_ok());
    }

    #[test]
    fn test_check_access_child_tenant() {
        let perms = TenantPermissions::builder()
            .can_access_child_tenants(true)
            .build();
        let ctx = TenantContext::new(TenantId::new("parent"), perms);
        assert!(ctx.check_access(&TenantId::new("parent/child")).is_ok());
    }

    #[test]
    fn test_validate_reference_same_tenant() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        assert!(
            ctx.validate_reference("Patient/123", &TenantId::new("t1"))
                .is_ok()
        );
    }

    #[test]
    fn test_validate_reference_cross_tenant() {
        let ctx = TenantContext::new(TenantId::new("t1"), TenantPermissions::full_access());
        let result = ctx.validate_reference("Patient/123", &TenantId::new("t2"));
        assert!(result.is_err());
    }

    #[test]
    fn test_builder() {
        let ctx = TenantContextBuilder::new()
            .tenant_id_str("my-tenant")
            .permissions(TenantPermissions::read_only())
            .correlation_id("corr-123")
            .user_id("user-456")
            .build()
            .unwrap();

        assert_eq!(ctx.tenant_id().as_str(), "my-tenant");
        assert_eq!(ctx.correlation_id(), Some("corr-123"));
        assert_eq!(ctx.user_id(), Some("user-456"));
    }

    #[test]
    fn test_builder_missing_tenant_id() {
        let result = TenantContextBuilder::new()
            .permissions(TenantPermissions::full_access())
            .build();
        assert!(result.is_err());
    }
}
