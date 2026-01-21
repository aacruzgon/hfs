//! Tenant identifier type.
//!
//! This module defines the [`TenantId`] type, an opaque identifier for tenants
//! with support for hierarchical namespaces.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The system tenant identifier, used for shared/global resources.
///
/// Resources stored under the system tenant are accessible to all tenants
/// (subject to permission checks). This is used for shared resources like
/// CodeSystems, ValueSets, and other terminology resources.
pub const SYSTEM_TENANT: &str = "__system__";

/// An opaque tenant identifier with hierarchical namespace support.
///
/// `TenantId` supports hierarchical organization using a `/` separator,
/// enabling nested tenant structures like `org/department/team`.
///
/// # Hierarchy
///
/// Tenant IDs can form a hierarchy:
/// - `acme` - Top-level tenant
/// - `acme/research` - Child tenant under acme
/// - `acme/research/oncology` - Further nested child
///
/// A parent tenant may have visibility into child tenant data depending
/// on the configured tenancy strategy and permissions.
///
/// # Examples
///
/// ```
/// use helios_persistence::tenant::TenantId;
///
/// let tenant = TenantId::new("acme/research");
/// assert_eq!(tenant.as_str(), "acme/research");
/// assert!(tenant.is_descendant_of(&TenantId::new("acme")));
/// ```
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    /// Creates a new tenant ID from the given string.
    ///
    /// # Arguments
    ///
    /// * `id` - The tenant identifier string. Can include `/` for hierarchy.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let tenant = TenantId::new("my-tenant");
    /// let nested = TenantId::new("parent/child");
    /// ```
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the system tenant ID.
    ///
    /// The system tenant is used for shared resources that should be
    /// accessible across all tenants.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let system = TenantId::system();
    /// assert!(system.is_system());
    /// ```
    pub fn system() -> Self {
        Self(SYSTEM_TENANT.to_string())
    }

    /// Returns the tenant ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns `true` if this is the system tenant.
    pub fn is_system(&self) -> bool {
        self.0 == SYSTEM_TENANT
    }

    /// Returns `true` if this tenant is a descendant of the given ancestor.
    ///
    /// A tenant is a descendant if its ID starts with the ancestor's ID
    /// followed by a `/` separator.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let parent = TenantId::new("acme");
    /// let child = TenantId::new("acme/research");
    /// let grandchild = TenantId::new("acme/research/oncology");
    ///
    /// assert!(child.is_descendant_of(&parent));
    /// assert!(grandchild.is_descendant_of(&parent));
    /// assert!(grandchild.is_descendant_of(&child));
    /// assert!(!parent.is_descendant_of(&child));
    /// ```
    pub fn is_descendant_of(&self, ancestor: &TenantId) -> bool {
        if self.0 == ancestor.0 {
            return false; // A tenant is not a descendant of itself
        }
        self.0.starts_with(&ancestor.0) && self.0[ancestor.0.len()..].starts_with('/')
    }

    /// Returns `true` if this tenant is an ancestor of the given descendant.
    ///
    /// This is the inverse of [`is_descendant_of`](Self::is_descendant_of).
    pub fn is_ancestor_of(&self, descendant: &TenantId) -> bool {
        descendant.is_descendant_of(self)
    }

    /// Returns the parent tenant ID, if this is a nested tenant.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let child = TenantId::new("acme/research");
    /// assert_eq!(child.parent(), Some(TenantId::new("acme")));
    ///
    /// let root = TenantId::new("acme");
    /// assert_eq!(root.parent(), None);
    /// ```
    pub fn parent(&self) -> Option<TenantId> {
        self.0.rfind('/').map(|idx| TenantId::new(&self.0[..idx]))
    }

    /// Returns the depth of this tenant in the hierarchy.
    ///
    /// A root tenant has depth 0, its direct children have depth 1, etc.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// assert_eq!(TenantId::new("acme").depth(), 0);
    /// assert_eq!(TenantId::new("acme/research").depth(), 1);
    /// assert_eq!(TenantId::new("acme/research/oncology").depth(), 2);
    /// ```
    pub fn depth(&self) -> usize {
        self.0.matches('/').count()
    }

    /// Returns an iterator over all ancestor tenant IDs, from immediate parent to root.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let tenant = TenantId::new("acme/research/oncology");
    /// let ancestors: Vec<_> = tenant.ancestors().collect();
    /// assert_eq!(ancestors.len(), 2);
    /// assert_eq!(ancestors[0].as_str(), "acme/research");
    /// assert_eq!(ancestors[1].as_str(), "acme");
    /// ```
    pub fn ancestors(&self) -> impl Iterator<Item = TenantId> + '_ {
        TenantAncestorIterator { current: self.clone() }
    }

    /// Returns the root tenant ID (the first segment of the hierarchy).
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let tenant = TenantId::new("acme/research/oncology");
    /// assert_eq!(tenant.root().as_str(), "acme");
    /// ```
    pub fn root(&self) -> TenantId {
        match self.0.find('/') {
            Some(idx) => TenantId::new(&self.0[..idx]),
            None => self.clone(),
        }
    }

    /// Creates a child tenant ID by appending a segment.
    ///
    /// # Examples
    ///
    /// ```
    /// use helios_persistence::tenant::TenantId;
    ///
    /// let parent = TenantId::new("acme");
    /// let child = parent.child("research");
    /// assert_eq!(child.as_str(), "acme/research");
    /// ```
    pub fn child(&self, segment: &str) -> TenantId {
        TenantId::new(format!("{}/{}", self.0, segment))
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantId({})", self.0)
    }
}

impl FromStr for TenantId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TenantId::new(s))
    }
}

impl From<&str> for TenantId {
    fn from(s: &str) -> Self {
        TenantId::new(s)
    }
}

impl From<String> for TenantId {
    fn from(s: String) -> Self {
        TenantId::new(s)
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Iterator over ancestor tenant IDs.
struct TenantAncestorIterator {
    current: TenantId,
}

impl Iterator for TenantAncestorIterator {
    type Item = TenantId;

    fn next(&mut self) -> Option<Self::Item> {
        let parent = self.current.parent()?;
        self.current = parent.clone();
        Some(parent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id_creation() {
        let tenant = TenantId::new("my-tenant");
        assert_eq!(tenant.as_str(), "my-tenant");
    }

    #[test]
    fn test_system_tenant() {
        let system = TenantId::system();
        assert!(system.is_system());
        assert_eq!(system.as_str(), SYSTEM_TENANT);
    }

    #[test]
    fn test_hierarchy_descendant() {
        let parent = TenantId::new("acme");
        let child = TenantId::new("acme/research");
        let grandchild = TenantId::new("acme/research/oncology");
        let unrelated = TenantId::new("other");

        assert!(child.is_descendant_of(&parent));
        assert!(grandchild.is_descendant_of(&parent));
        assert!(grandchild.is_descendant_of(&child));
        assert!(!parent.is_descendant_of(&child));
        assert!(!child.is_descendant_of(&unrelated));
        assert!(!parent.is_descendant_of(&parent)); // Not descendant of self
    }

    #[test]
    fn test_hierarchy_ancestor() {
        let parent = TenantId::new("acme");
        let child = TenantId::new("acme/research");

        assert!(parent.is_ancestor_of(&child));
        assert!(!child.is_ancestor_of(&parent));
    }

    #[test]
    fn test_parent() {
        let root = TenantId::new("acme");
        let child = TenantId::new("acme/research");
        let grandchild = TenantId::new("acme/research/oncology");

        assert_eq!(root.parent(), None);
        assert_eq!(child.parent(), Some(TenantId::new("acme")));
        assert_eq!(grandchild.parent(), Some(TenantId::new("acme/research")));
    }

    #[test]
    fn test_depth() {
        assert_eq!(TenantId::new("acme").depth(), 0);
        assert_eq!(TenantId::new("acme/research").depth(), 1);
        assert_eq!(TenantId::new("acme/research/oncology").depth(), 2);
    }

    #[test]
    fn test_ancestors() {
        let tenant = TenantId::new("acme/research/oncology");
        let ancestors: Vec<_> = tenant.ancestors().collect();

        assert_eq!(ancestors.len(), 2);
        assert_eq!(ancestors[0].as_str(), "acme/research");
        assert_eq!(ancestors[1].as_str(), "acme");
    }

    #[test]
    fn test_root() {
        assert_eq!(TenantId::new("acme").root().as_str(), "acme");
        assert_eq!(TenantId::new("acme/research").root().as_str(), "acme");
        assert_eq!(TenantId::new("acme/research/oncology").root().as_str(), "acme");
    }

    #[test]
    fn test_child() {
        let parent = TenantId::new("acme");
        let child = parent.child("research");
        assert_eq!(child.as_str(), "acme/research");
    }

    #[test]
    fn test_serde_roundtrip() {
        let tenant = TenantId::new("acme/research");
        let json = serde_json::to_string(&tenant).unwrap();
        assert_eq!(json, "\"acme/research\"");

        let parsed: TenantId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, tenant);
    }

    #[test]
    fn test_from_string() {
        let tenant: TenantId = "my-tenant".into();
        assert_eq!(tenant.as_str(), "my-tenant");

        let tenant2: TenantId = String::from("my-tenant").into();
        assert_eq!(tenant2.as_str(), "my-tenant");
    }
}
