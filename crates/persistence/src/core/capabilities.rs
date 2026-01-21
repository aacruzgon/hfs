//! Storage capabilities and capability statement generation.
//!
//! This module defines traits for runtime capability discovery and
//! generation of FHIR CapabilityStatement fragments.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::SearchParamType;

/// Supported FHIR interactions for a resource type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Interaction {
    /// read - Read the current state of the resource.
    Read,
    /// vread - Read a specific version.
    Vread,
    /// update - Update an existing resource.
    Update,
    /// patch - Partial update of a resource.
    Patch,
    /// delete - Delete a resource.
    Delete,
    /// history-instance - Retrieve history for a resource instance.
    HistoryInstance,
    /// history-type - Retrieve history for a resource type.
    HistoryType,
    /// create - Create a new resource.
    Create,
    /// search-type - Search for resources of a type.
    SearchType,
}

impl std::fmt::Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Interaction::Read => write!(f, "read"),
            Interaction::Vread => write!(f, "vread"),
            Interaction::Update => write!(f, "update"),
            Interaction::Patch => write!(f, "patch"),
            Interaction::Delete => write!(f, "delete"),
            Interaction::HistoryInstance => write!(f, "history-instance"),
            Interaction::HistoryType => write!(f, "history-type"),
            Interaction::Create => write!(f, "create"),
            Interaction::SearchType => write!(f, "search-type"),
        }
    }
}

/// Supported system-level interactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SystemInteraction {
    /// transaction - Process a transaction bundle.
    Transaction,
    /// batch - Process a batch bundle.
    Batch,
    /// history-system - Retrieve history for all resources.
    HistorySystem,
    /// search-system - Search across all resource types.
    SearchSystem,
}

impl std::fmt::Display for SystemInteraction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemInteraction::Transaction => write!(f, "transaction"),
            SystemInteraction::Batch => write!(f, "batch"),
            SystemInteraction::HistorySystem => write!(f, "history-system"),
            SystemInteraction::SearchSystem => write!(f, "search-system"),
        }
    }
}

/// Information about a supported search parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchParamCapability {
    /// The parameter name.
    pub name: String,
    /// The parameter type.
    pub param_type: SearchParamType,
    /// Supported modifiers for this parameter.
    pub modifiers: Vec<String>,
    /// Whether chaining is supported for reference parameters.
    pub supports_chaining: bool,
    /// Documentation for this parameter.
    pub documentation: Option<String>,
}

impl SearchParamCapability {
    /// Creates a new search parameter capability.
    pub fn new(name: impl Into<String>, param_type: SearchParamType) -> Self {
        Self {
            name: name.into(),
            param_type,
            modifiers: Vec::new(),
            supports_chaining: false,
            documentation: None,
        }
    }

    /// Adds supported modifiers.
    pub fn with_modifiers(mut self, modifiers: Vec<&str>) -> Self {
        self.modifiers = modifiers.into_iter().map(String::from).collect();
        self
    }

    /// Enables chaining support.
    pub fn with_chaining(mut self) -> Self {
        self.supports_chaining = true;
        self
    }

    /// Adds documentation.
    pub fn with_documentation(mut self, doc: impl Into<String>) -> Self {
        self.documentation = Some(doc.into());
        self
    }
}

/// Capabilities for a specific resource type.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceCapabilities {
    /// The resource type.
    pub resource_type: String,
    /// Supported interactions.
    pub interactions: HashSet<Interaction>,
    /// Supported search parameters.
    pub search_params: Vec<SearchParamCapability>,
    /// Whether _include is supported.
    pub supports_include: bool,
    /// Whether _revinclude is supported.
    pub supports_revinclude: bool,
    /// Supported _include targets.
    pub include_targets: Vec<String>,
    /// Supported _revinclude targets.
    pub revinclude_targets: Vec<String>,
    /// Whether conditional create is supported.
    pub conditional_create: bool,
    /// Whether conditional update is supported.
    pub conditional_update: bool,
    /// Whether conditional delete is supported.
    pub conditional_delete: bool,
    /// Additional documentation.
    pub documentation: Option<String>,
}

impl ResourceCapabilities {
    /// Creates capabilities for a resource type.
    pub fn new(resource_type: impl Into<String>) -> Self {
        Self {
            resource_type: resource_type.into(),
            ..Default::default()
        }
    }

    /// Adds supported interactions.
    pub fn with_interactions(mut self, interactions: impl IntoIterator<Item = Interaction>) -> Self {
        self.interactions.extend(interactions);
        self
    }

    /// Adds all CRUD interactions.
    pub fn with_crud(mut self) -> Self {
        self.interactions.insert(Interaction::Read);
        self.interactions.insert(Interaction::Create);
        self.interactions.insert(Interaction::Update);
        self.interactions.insert(Interaction::Delete);
        self
    }

    /// Adds version support.
    pub fn with_versioning(mut self) -> Self {
        self.interactions.insert(Interaction::Vread);
        self.interactions.insert(Interaction::HistoryInstance);
        self
    }

    /// Adds search support.
    pub fn with_search(mut self, params: Vec<SearchParamCapability>) -> Self {
        self.interactions.insert(Interaction::SearchType);
        self.search_params = params;
        self
    }

    /// Enables _include support.
    pub fn with_include(mut self, targets: Vec<&str>) -> Self {
        self.supports_include = true;
        self.include_targets = targets.into_iter().map(String::from).collect();
        self
    }

    /// Enables _revinclude support.
    pub fn with_revinclude(mut self, targets: Vec<&str>) -> Self {
        self.supports_revinclude = true;
        self.revinclude_targets = targets.into_iter().map(String::from).collect();
        self
    }

    /// Enables conditional operations.
    pub fn with_conditional_ops(mut self) -> Self {
        self.conditional_create = true;
        self.conditional_update = true;
        self.conditional_delete = true;
        self
    }
}

/// Overall storage capabilities.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageCapabilities {
    /// Capabilities by resource type.
    pub resources: HashMap<String, ResourceCapabilities>,
    /// Supported system-level interactions.
    pub system_interactions: HashSet<SystemInteraction>,
    /// Whether system-level history is supported.
    pub supports_system_history: bool,
    /// Whether system-level search is supported.
    pub supports_system_search: bool,
    /// Supported sort parameters.
    pub supported_sorts: Vec<String>,
    /// Whether total counts are supported.
    pub supports_total: bool,
    /// Maximum page size.
    pub max_page_size: Option<u32>,
    /// Default page size.
    pub default_page_size: u32,
    /// Backend name.
    pub backend_name: String,
    /// Backend version.
    pub backend_version: Option<String>,
}

impl StorageCapabilities {
    /// Creates new storage capabilities.
    pub fn new(backend_name: impl Into<String>) -> Self {
        Self {
            backend_name: backend_name.into(),
            default_page_size: 20,
            ..Default::default()
        }
    }

    /// Adds resource capabilities.
    pub fn with_resource(mut self, caps: ResourceCapabilities) -> Self {
        self.resources.insert(caps.resource_type.clone(), caps);
        self
    }

    /// Adds system interactions.
    pub fn with_system_interactions(
        mut self,
        interactions: impl IntoIterator<Item = SystemInteraction>,
    ) -> Self {
        self.system_interactions.extend(interactions);
        self
    }

    /// Enables system history.
    pub fn with_system_history(mut self) -> Self {
        self.supports_system_history = true;
        self.system_interactions.insert(SystemInteraction::HistorySystem);
        self
    }

    /// Enables system search.
    pub fn with_system_search(mut self) -> Self {
        self.supports_system_search = true;
        self.system_interactions.insert(SystemInteraction::SearchSystem);
        self
    }

    /// Enables transaction support.
    pub fn with_transactions(mut self) -> Self {
        self.system_interactions.insert(SystemInteraction::Transaction);
        self.system_interactions.insert(SystemInteraction::Batch);
        self
    }

    /// Sets pagination limits.
    pub fn with_pagination(mut self, default: u32, max: Option<u32>) -> Self {
        self.default_page_size = default;
        self.max_page_size = max;
        self
    }

    /// Adds supported sort parameters.
    pub fn with_sorts(mut self, sorts: Vec<&str>) -> Self {
        self.supported_sorts = sorts.into_iter().map(String::from).collect();
        self
    }

    /// Enables total count support.
    pub fn with_total_support(mut self) -> Self {
        self.supports_total = true;
        self
    }

    /// Generates a FHIR CapabilityStatement rest resource for this storage.
    pub fn to_capability_rest(&self) -> Value {
        let mut resources = Vec::new();

        for caps in self.resources.values() {
            let mut resource = serde_json::json!({
                "type": caps.resource_type,
                "interaction": caps.interactions.iter().map(|i| {
                    serde_json::json!({"code": i.to_string()})
                }).collect::<Vec<_>>(),
            });

            if !caps.search_params.is_empty() {
                resource["searchParam"] = serde_json::json!(
                    caps.search_params.iter().map(|sp| {
                        serde_json::json!({
                            "name": sp.name,
                            "type": sp.param_type.to_string(),
                        })
                    }).collect::<Vec<_>>()
                );
            }

            if caps.conditional_create {
                resource["conditionalCreate"] = serde_json::json!(true);
            }
            if caps.conditional_update {
                resource["conditionalUpdate"] = serde_json::json!(true);
            }
            if caps.conditional_delete {
                resource["conditionalDelete"] = serde_json::json!("single");
            }

            resources.push(resource);
        }

        let mut rest = serde_json::json!({
            "mode": "server",
            "resource": resources,
        });

        if !self.system_interactions.is_empty() {
            rest["interaction"] = serde_json::json!(
                self.system_interactions.iter().map(|i| {
                    serde_json::json!({"code": i.to_string()})
                }).collect::<Vec<_>>()
            );
        }

        rest
    }
}

/// Trait for storage backends to declare their capabilities.
pub trait CapabilityProvider {
    /// Returns the capabilities of this storage backend.
    fn capabilities(&self) -> StorageCapabilities;

    /// Checks if a specific resource type interaction is supported.
    fn supports_interaction(&self, resource_type: &str, interaction: Interaction) -> bool {
        self.capabilities()
            .resources
            .get(resource_type)
            .map(|r| r.interactions.contains(&interaction))
            .unwrap_or(false)
    }

    /// Checks if a system interaction is supported.
    fn supports_system_interaction(&self, interaction: SystemInteraction) -> bool {
        self.capabilities()
            .system_interactions
            .contains(&interaction)
    }

    /// Gets the capabilities for a specific resource type.
    fn resource_capabilities(&self, resource_type: &str) -> Option<ResourceCapabilities> {
        self.capabilities().resources.get(resource_type).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interaction_display() {
        assert_eq!(Interaction::Read.to_string(), "read");
        assert_eq!(Interaction::HistoryInstance.to_string(), "history-instance");
    }

    #[test]
    fn test_system_interaction_display() {
        assert_eq!(SystemInteraction::Transaction.to_string(), "transaction");
        assert_eq!(SystemInteraction::HistorySystem.to_string(), "history-system");
    }

    #[test]
    fn test_search_param_capability() {
        let cap = SearchParamCapability::new("name", SearchParamType::String)
            .with_modifiers(vec!["exact", "contains"])
            .with_documentation("Search by patient name");

        assert_eq!(cap.name, "name");
        assert_eq!(cap.modifiers.len(), 2);
        assert!(cap.documentation.is_some());
    }

    #[test]
    fn test_resource_capabilities() {
        let caps = ResourceCapabilities::new("Patient")
            .with_crud()
            .with_versioning()
            .with_conditional_ops();

        assert!(caps.interactions.contains(&Interaction::Read));
        assert!(caps.interactions.contains(&Interaction::Create));
        assert!(caps.interactions.contains(&Interaction::Vread));
        assert!(caps.conditional_create);
    }

    #[test]
    fn test_storage_capabilities() {
        let patient_caps = ResourceCapabilities::new("Patient")
            .with_crud()
            .with_search(vec![
                SearchParamCapability::new("name", SearchParamType::String),
                SearchParamCapability::new("identifier", SearchParamType::Token),
            ]);

        let caps = StorageCapabilities::new("sqlite")
            .with_resource(patient_caps)
            .with_transactions()
            .with_pagination(20, Some(100));

        assert!(caps.resources.contains_key("Patient"));
        assert!(caps.system_interactions.contains(&SystemInteraction::Transaction));
        assert_eq!(caps.default_page_size, 20);
        assert_eq!(caps.max_page_size, Some(100));
    }

    #[test]
    fn test_to_capability_rest() {
        let caps = StorageCapabilities::new("test")
            .with_resource(ResourceCapabilities::new("Patient").with_crud())
            .with_transactions();

        let rest = caps.to_capability_rest();
        assert_eq!(rest["mode"], "server");
        assert!(rest["resource"].is_array());
        assert!(rest["interaction"].is_array());
    }
}
