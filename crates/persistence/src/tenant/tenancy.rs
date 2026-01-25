//! Tenancy model definitions.
//!
//! This module defines the tenancy model types that determine how resources
//! are isolated between tenants.

use serde::{Deserialize, Serialize};

/// The tenancy model for resource isolation.
///
/// This enum defines how resources are associated with tenants and whether
/// they can be shared across tenant boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TenancyModel {
    /// Resources are strictly scoped to a single tenant.
    ///
    /// Each resource belongs to exactly one tenant and cannot be accessed
    /// by other tenants. This is the default for clinical data.
    #[default]
    TenantScoped,

    /// Resources are shared across all tenants.
    ///
    /// These resources are stored in the system tenant and accessible
    /// to all tenants (subject to permissions). This is used for
    /// terminology resources (CodeSystem, ValueSet), conformance resources
    /// (StructureDefinition, CapabilityStatement), and other shared data.
    Shared,

    /// Tenancy is determined by resource content.
    ///
    /// Some resources may be either tenant-scoped or shared depending on
    /// their configuration or content. For example, an Organization might
    /// be shared if it represents a well-known entity, or tenant-scoped
    /// if it's a local organization.
    Configurable,
}

/// Trait for determining the tenancy model of a resource type.
///
/// Implement this trait to specify how different resource types should
/// be handled with respect to tenant isolation.
///
/// # Default Implementation
///
/// The default implementation returns `TenancyModel::TenantScoped` for
/// clinical resources and `TenancyModel::Shared` for terminology and
/// conformance resources.
///
/// # Examples
///
/// ```
/// use helios_persistence::tenant::{ResourceTenancy, TenancyModel};
///
/// struct DefaultResourceTenancy;
///
/// impl ResourceTenancy for DefaultResourceTenancy {
///     fn tenancy_model(&self, resource_type: &str) -> TenancyModel {
///         match resource_type {
///             // Terminology resources are shared
///             "CodeSystem" | "ValueSet" | "ConceptMap" | "NamingSystem" => {
///                 TenancyModel::Shared
///             }
///             // Everything else is tenant-scoped
///             _ => TenancyModel::TenantScoped,
///         }
///     }
/// }
/// ```
pub trait ResourceTenancy: Send + Sync {
    /// Returns the tenancy model for the given resource type.
    fn tenancy_model(&self, resource_type: &str) -> TenancyModel;

    /// Returns `true` if the resource type is shared across tenants.
    fn is_shared(&self, resource_type: &str) -> bool {
        self.tenancy_model(resource_type) == TenancyModel::Shared
    }

    /// Returns `true` if the resource type is tenant-scoped.
    fn is_tenant_scoped(&self, resource_type: &str) -> bool {
        self.tenancy_model(resource_type) == TenancyModel::TenantScoped
    }
}

/// Default resource tenancy implementation based on FHIR resource categories.
///
/// This implementation categorizes resources as:
///
/// - **Shared**: Terminology (CodeSystem, ValueSet, ConceptMap, NamingSystem),
///   Conformance (StructureDefinition, CapabilityStatement, SearchParameter,
///   OperationDefinition, CompartmentDefinition, ImplementationGuide)
///
/// - **Configurable**: Organization, Location (may be shared or tenant-scoped)
///
/// - **Tenant-Scoped**: All clinical and administrative resources
#[derive(Debug, Clone, Default)]
pub struct DefaultResourceTenancy;

impl ResourceTenancy for DefaultResourceTenancy {
    fn tenancy_model(&self, resource_type: &str) -> TenancyModel {
        match resource_type {
            // Terminology resources - typically shared
            "CodeSystem" | "ValueSet" | "ConceptMap" | "NamingSystem" => TenancyModel::Shared,

            // Conformance resources - typically shared
            "StructureDefinition"
            | "CapabilityStatement"
            | "SearchParameter"
            | "OperationDefinition"
            | "CompartmentDefinition"
            | "ImplementationGuide"
            | "MessageDefinition"
            | "StructureMap"
            | "GraphDefinition"
            | "ExampleScenario" => TenancyModel::Shared,

            // Knowledge resources - often shared
            "Library" | "Measure" | "PlanDefinition" | "ActivityDefinition" | "Questionnaire" => {
                TenancyModel::Shared
            }

            // May be shared or tenant-scoped depending on use case
            "Organization" | "Location" | "HealthcareService" | "Endpoint" => {
                TenancyModel::Configurable
            }

            // All other resources are tenant-scoped
            _ => TenancyModel::TenantScoped,
        }
    }
}

/// Custom resource tenancy that allows overriding defaults.
///
/// This implementation allows you to specify custom tenancy for specific
/// resource types while falling back to another implementation for others.
#[derive(Debug, Clone)]
pub struct CustomResourceTenancy<F: ResourceTenancy> {
    overrides: std::collections::HashMap<String, TenancyModel>,
    fallback: F,
}

impl<F: ResourceTenancy> CustomResourceTenancy<F> {
    /// Creates a new custom tenancy with the given fallback.
    pub fn new(fallback: F) -> Self {
        Self {
            overrides: std::collections::HashMap::new(),
            fallback,
        }
    }

    /// Sets the tenancy model for a specific resource type.
    pub fn with_override(mut self, resource_type: &str, model: TenancyModel) -> Self {
        self.overrides.insert(resource_type.to_string(), model);
        self
    }
}

impl<F: ResourceTenancy> ResourceTenancy for CustomResourceTenancy<F> {
    fn tenancy_model(&self, resource_type: &str) -> TenancyModel {
        self.overrides
            .get(resource_type)
            .copied()
            .unwrap_or_else(|| self.fallback.tenancy_model(resource_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_tenancy_clinical() {
        let tenancy = DefaultResourceTenancy;
        assert_eq!(tenancy.tenancy_model("Patient"), TenancyModel::TenantScoped);
        assert_eq!(
            tenancy.tenancy_model("Observation"),
            TenancyModel::TenantScoped
        );
        assert_eq!(
            tenancy.tenancy_model("Encounter"),
            TenancyModel::TenantScoped
        );
    }

    #[test]
    fn test_default_tenancy_terminology() {
        let tenancy = DefaultResourceTenancy;
        assert_eq!(tenancy.tenancy_model("CodeSystem"), TenancyModel::Shared);
        assert_eq!(tenancy.tenancy_model("ValueSet"), TenancyModel::Shared);
        assert_eq!(tenancy.tenancy_model("ConceptMap"), TenancyModel::Shared);
    }

    #[test]
    fn test_default_tenancy_conformance() {
        let tenancy = DefaultResourceTenancy;
        assert_eq!(
            tenancy.tenancy_model("StructureDefinition"),
            TenancyModel::Shared
        );
        assert_eq!(
            tenancy.tenancy_model("CapabilityStatement"),
            TenancyModel::Shared
        );
    }

    #[test]
    fn test_default_tenancy_configurable() {
        let tenancy = DefaultResourceTenancy;
        assert_eq!(
            tenancy.tenancy_model("Organization"),
            TenancyModel::Configurable
        );
        assert_eq!(
            tenancy.tenancy_model("Location"),
            TenancyModel::Configurable
        );
    }

    #[test]
    fn test_is_shared() {
        let tenancy = DefaultResourceTenancy;
        assert!(tenancy.is_shared("CodeSystem"));
        assert!(!tenancy.is_shared("Patient"));
    }

    #[test]
    fn test_is_tenant_scoped() {
        let tenancy = DefaultResourceTenancy;
        assert!(tenancy.is_tenant_scoped("Patient"));
        assert!(!tenancy.is_tenant_scoped("CodeSystem"));
    }

    #[test]
    fn test_custom_tenancy() {
        let tenancy = CustomResourceTenancy::new(DefaultResourceTenancy)
            .with_override("Organization", TenancyModel::TenantScoped);

        // Override takes effect
        assert_eq!(
            tenancy.tenancy_model("Organization"),
            TenancyModel::TenantScoped
        );

        // Fallback still works
        assert_eq!(tenancy.tenancy_model("Patient"), TenancyModel::TenantScoped);
        assert_eq!(tenancy.tenancy_model("CodeSystem"), TenancyModel::Shared);
    }
}
