//! Core storage traits and abstractions.
//!
//! This module provides the foundational traits for the persistence layer:
//!
//! - [`Backend`] - Database driver abstraction
//! - [`ResourceStorage`] - Core CRUD operations
//! - [`VersionedStorage`] - Version-aware operations
//! - History providers - Instance, type, and system-level history
//! - Search providers - Various levels of search capability
//! - [`Transaction`] - ACID transaction support
//! - [`CapabilityProvider`] - Runtime capability discovery
//!
//! # Trait Hierarchy
//!
//! The traits form a progressive hierarchy where more advanced traits
//! extend simpler ones:
//!
//! ```text
//! ResourceStorage
//!     └── VersionedStorage
//!             └── InstanceHistoryProvider
//!                     └── TypeHistoryProvider
//!                             └── SystemHistoryProvider
//!
//! ResourceStorage
//!     └── SearchProvider
//!             ├── MultiTypeSearchProvider
//!             ├── IncludeProvider
//!             ├── RevincludeProvider
//!             ├── ChainedSearchProvider
//!             ├── TerminologySearchProvider
//!             └── TextSearchProvider
//!
//! ResourceStorage
//!     └── TransactionProvider
//!             └── BundleProvider
//! ```
//!
//! # Backend Capabilities
//!
//! Not all backends support all features. Use [`CapabilityProvider`] to
//! discover what a backend supports at runtime:
//!
//! ```ignore
//! use helios_persistence::core::{CapabilityProvider, Interaction};
//!
//! fn check_capabilities<S: CapabilityProvider>(storage: &S) {
//!     if storage.supports_interaction("Patient", Interaction::HistoryType) {
//!         // Use type-level history
//!     }
//!
//!     let caps = storage.capabilities();
//!     println!("Backend: {}", caps.backend_name);
//!     println!("Supports transactions: {}",
//!         caps.system_interactions.contains(&SystemInteraction::Transaction));
//! }
//! ```
//!
//! # Example: Implementing a Storage Backend
//!
//! ```ignore
//! use async_trait::async_trait;
//! use helios_persistence::core::{ResourceStorage, Backend, BackendKind};
//! use helios_persistence::tenant::TenantContext;
//! use helios_persistence::types::StoredResource;
//! use helios_persistence::error::StorageResult;
//!
//! struct MyBackend {
//!     // ... backend-specific fields
//! }
//!
//! #[async_trait]
//! impl ResourceStorage for MyBackend {
//!     fn backend_name(&self) -> &'static str {
//!         "my-backend"
//!     }
//!
//!     async fn create(
//!         &self,
//!         tenant: &TenantContext,
//!         resource_type: &str,
//!         resource: serde_json::Value,
//!     ) -> StorageResult<StoredResource> {
//!         // Implementation...
//!         todo!()
//!     }
//!
//!     // ... implement other required methods
//! }
//! ```

pub mod backend;
pub mod capabilities;
pub mod history;
pub mod search;
pub mod storage;
pub mod transaction;
pub mod versioned;

// Re-export main types
pub use backend::{Backend, BackendCapability, BackendConfig, BackendKind, BackendPoolStats};
pub use capabilities::{
    CapabilityProvider, Interaction, ResourceCapabilities, SearchParamCapability,
    StorageCapabilities, SystemInteraction,
};
pub use history::{
    DifferentialHistoryProvider, HistoryEntry, HistoryMethod, HistoryPage, HistoryParams,
    InstanceHistoryProvider, SystemHistoryProvider, TypeHistoryProvider,
};
pub use search::{
    ChainedSearchProvider, FullSearchProvider, IncludeProvider, MultiTypeSearchProvider,
    RevincludeProvider, SearchProvider, SearchResult, TerminologySearchProvider,
    TextSearchProvider,
};
pub use storage::{
    ConditionalCreateResult, ConditionalDeleteResult, ConditionalStorage, ConditionalUpdateResult,
    PurgableStorage, ResourceStorage,
};
pub use transaction::{
    BundleEntry, BundleEntryResult, BundleMethod, BundleProvider, BundleResult, BundleType,
    IsolationLevel, LockingStrategy, Transaction, TransactionOptions, TransactionProvider,
};
pub use versioned::{
    check_version_match, normalize_etag, VersionConflictInfo, VersionedStorage,
};
