//! Tenant permission types.
//!
//! This module defines the permission model for tenant operations, controlling
//! what actions a tenant context is allowed to perform.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Operations that can be performed on resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    /// Create new resources.
    Create,
    /// Read existing resources.
    Read,
    /// Update existing resources.
    Update,
    /// Delete resources (soft or hard).
    Delete,
    /// Read resource history.
    History,
    /// Search for resources.
    Search,
    /// Execute transactions/batches.
    Transaction,
    /// Perform bulk operations (export, import).
    Bulk,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Create => write!(f, "create"),
            Operation::Read => write!(f, "read"),
            Operation::Update => write!(f, "update"),
            Operation::Delete => write!(f, "delete"),
            Operation::History => write!(f, "history"),
            Operation::Search => write!(f, "search"),
            Operation::Transaction => write!(f, "transaction"),
            Operation::Bulk => write!(f, "bulk"),
        }
    }
}

/// Permissions granted to a tenant context.
///
/// `TenantPermissions` controls what operations a tenant can perform and
/// on which resource types. Permissions can be:
///
/// - **Full access**: All operations on all resource types
/// - **Operation-limited**: Only specific operations allowed
/// - **Resource-limited**: Only specific resource types allowed
/// - **Compartment-limited**: Only resources within a specific compartment
///
/// # Examples
///
/// ```
/// use helios_persistence::tenant::{TenantPermissions, Operation};
///
/// // Full access
/// let full = TenantPermissions::full_access();
/// assert!(full.can_perform(Operation::Create, "Patient"));
///
/// // Read-only access
/// let read_only = TenantPermissions::read_only();
/// assert!(read_only.can_perform(Operation::Read, "Patient"));
/// assert!(!read_only.can_perform(Operation::Create, "Patient"));
///
/// // Custom permissions
/// let custom = TenantPermissions::builder()
///     .allow_operations(vec![Operation::Read, Operation::Search])
///     .allow_resource_types(vec!["Patient", "Observation"])
///     .build();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantPermissions {
    /// Allowed operations. If None, all operations are allowed.
    allowed_operations: Option<HashSet<Operation>>,

    /// Allowed resource types. If None, all resource types are allowed.
    allowed_resource_types: Option<HashSet<String>>,

    /// Compartment restrictions. If Some, only resources in the specified
    /// compartment are accessible.
    compartment: Option<CompartmentRestriction>,

    /// Whether this tenant can access system tenant resources.
    can_access_system_tenant: bool,

    /// Whether this tenant can access child tenant resources.
    can_access_child_tenants: bool,
}

/// Restricts access to resources within a specific compartment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompartmentRestriction {
    /// The compartment type (e.g., "Patient", "Practitioner").
    pub compartment_type: String,
    /// The compartment owner resource ID.
    pub compartment_id: String,
}

impl TenantPermissions {
    /// Creates permissions with full access to all operations and resource types.
    pub fn full_access() -> Self {
        Self {
            allowed_operations: None,
            allowed_resource_types: None,
            compartment: None,
            can_access_system_tenant: true,
            can_access_child_tenants: false,
        }
    }

    /// Creates read-only permissions (read, history, search only).
    pub fn read_only() -> Self {
        let mut ops = HashSet::new();
        ops.insert(Operation::Read);
        ops.insert(Operation::History);
        ops.insert(Operation::Search);

        Self {
            allowed_operations: Some(ops),
            allowed_resource_types: None,
            compartment: None,
            can_access_system_tenant: true,
            can_access_child_tenants: false,
        }
    }

    /// Creates a builder for custom permissions.
    pub fn builder() -> TenantPermissionsBuilder {
        TenantPermissionsBuilder::new()
    }

    /// Returns `true` if the given operation is permitted on the given resource type.
    pub fn can_perform(&self, operation: Operation, resource_type: &str) -> bool {
        // Check operation permission
        if let Some(ref allowed_ops) = self.allowed_operations {
            if !allowed_ops.contains(&operation) {
                return false;
            }
        }

        // Check resource type permission
        if let Some(ref allowed_types) = self.allowed_resource_types {
            if !allowed_types.contains(resource_type) {
                return false;
            }
        }

        true
    }

    /// Returns `true` if access to system tenant resources is allowed.
    pub fn can_access_system_tenant(&self) -> bool {
        self.can_access_system_tenant
    }

    /// Returns `true` if access to child tenant resources is allowed.
    pub fn can_access_child_tenants(&self) -> bool {
        self.can_access_child_tenants
    }

    /// Returns the compartment restriction, if any.
    pub fn compartment(&self) -> Option<&CompartmentRestriction> {
        self.compartment.as_ref()
    }

    /// Returns the set of allowed operations, or None if all are allowed.
    pub fn allowed_operations(&self) -> Option<&HashSet<Operation>> {
        self.allowed_operations.as_ref()
    }

    /// Returns the set of allowed resource types, or None if all are allowed.
    pub fn allowed_resource_types(&self) -> Option<&HashSet<String>> {
        self.allowed_resource_types.as_ref()
    }
}

impl Default for TenantPermissions {
    fn default() -> Self {
        Self::full_access()
    }
}

/// Builder for creating custom tenant permissions.
#[derive(Default)]
pub struct TenantPermissionsBuilder {
    allowed_operations: Option<HashSet<Operation>>,
    allowed_resource_types: Option<HashSet<String>>,
    compartment: Option<CompartmentRestriction>,
    can_access_system_tenant: bool,
    can_access_child_tenants: bool,
}

impl TenantPermissionsBuilder {
    /// Creates a new builder with no permissions.
    pub fn new() -> Self {
        Self {
            allowed_operations: None,
            allowed_resource_types: None,
            compartment: None,
            can_access_system_tenant: true,
            can_access_child_tenants: false,
        }
    }

    /// Sets the allowed operations.
    pub fn allow_operations(mut self, operations: Vec<Operation>) -> Self {
        self.allowed_operations = Some(operations.into_iter().collect());
        self
    }

    /// Sets the allowed resource types.
    pub fn allow_resource_types(mut self, types: Vec<&str>) -> Self {
        self.allowed_resource_types = Some(types.into_iter().map(String::from).collect());
        self
    }

    /// Restricts access to a specific compartment.
    pub fn restrict_to_compartment(mut self, compartment_type: &str, compartment_id: &str) -> Self {
        self.compartment = Some(CompartmentRestriction {
            compartment_type: compartment_type.to_string(),
            compartment_id: compartment_id.to_string(),
        });
        self
    }

    /// Sets whether system tenant resources can be accessed.
    pub fn can_access_system_tenant(mut self, can_access: bool) -> Self {
        self.can_access_system_tenant = can_access;
        self
    }

    /// Sets whether child tenant resources can be accessed.
    pub fn can_access_child_tenants(mut self, can_access: bool) -> Self {
        self.can_access_child_tenants = can_access;
        self
    }

    /// Builds the permissions.
    pub fn build(self) -> TenantPermissions {
        TenantPermissions {
            allowed_operations: self.allowed_operations,
            allowed_resource_types: self.allowed_resource_types,
            compartment: self.compartment,
            can_access_system_tenant: self.can_access_system_tenant,
            can_access_child_tenants: self.can_access_child_tenants,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_access() {
        let perms = TenantPermissions::full_access();
        assert!(perms.can_perform(Operation::Create, "Patient"));
        assert!(perms.can_perform(Operation::Read, "Observation"));
        assert!(perms.can_perform(Operation::Delete, "Encounter"));
        assert!(perms.can_access_system_tenant());
    }

    #[test]
    fn test_read_only() {
        let perms = TenantPermissions::read_only();
        assert!(perms.can_perform(Operation::Read, "Patient"));
        assert!(perms.can_perform(Operation::Search, "Observation"));
        assert!(perms.can_perform(Operation::History, "Encounter"));
        assert!(!perms.can_perform(Operation::Create, "Patient"));
        assert!(!perms.can_perform(Operation::Update, "Patient"));
        assert!(!perms.can_perform(Operation::Delete, "Patient"));
    }

    #[test]
    fn test_custom_permissions() {
        let perms = TenantPermissions::builder()
            .allow_operations(vec![Operation::Read, Operation::Search])
            .allow_resource_types(vec!["Patient", "Observation"])
            .build();

        // Allowed
        assert!(perms.can_perform(Operation::Read, "Patient"));
        assert!(perms.can_perform(Operation::Search, "Observation"));

        // Not allowed - wrong operation
        assert!(!perms.can_perform(Operation::Create, "Patient"));

        // Not allowed - wrong resource type
        assert!(!perms.can_perform(Operation::Read, "Encounter"));
    }

    #[test]
    fn test_compartment_restriction() {
        let perms = TenantPermissions::builder()
            .restrict_to_compartment("Patient", "123")
            .build();

        let compartment = perms.compartment().unwrap();
        assert_eq!(compartment.compartment_type, "Patient");
        assert_eq!(compartment.compartment_id, "123");
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(Operation::Create.to_string(), "create");
        assert_eq!(Operation::Read.to_string(), "read");
        assert_eq!(Operation::Update.to_string(), "update");
        assert_eq!(Operation::Delete.to_string(), "delete");
    }

    #[test]
    fn test_child_tenant_access() {
        let perms = TenantPermissions::builder()
            .can_access_child_tenants(true)
            .build();
        assert!(perms.can_access_child_tenants());

        let perms2 = TenantPermissions::full_access();
        assert!(!perms2.can_access_child_tenants());
    }
}
