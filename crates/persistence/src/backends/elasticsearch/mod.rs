//! Elasticsearch backend implementation.
//!
//! This module provides an Elasticsearch implementation optimized for use as a
//! **search-optimized secondary backend** in the composite storage layer. While it
//! implements the full `ResourceStorage` trait (required for sync support), its
//! primary value is in `SearchProvider` and `TextSearchProvider`, where ES excels.
//!
//! # Role in Composite Architecture
//!
//! Elasticsearch serves the `BackendRole::Search` role:
//! - **Full-text search**: `_text`, `_content`, `:text`, `:text-advanced`
//! - **Basic search**: All standard FHIR search parameter types
//! - **Relevance scoring**: Results ranked by relevance
//! - **Cursor pagination**: Efficient deep pagination via `search_after`
//!
//! # Index Structure
//!
//! Each tenant+resource type combination gets its own index:
//! `{prefix}_{tenant_id}_{resource_type_lowercase}` (e.g., `hfs_acme_patient`)
//!
//! Documents use nested objects for search parameters to ensure correct
//! multi-value matching (e.g., system+code must co-occur in the same token).
//!
//! # Example
//!
//! ```ignore
//! use helios_persistence::backends::elasticsearch::{ElasticsearchBackend, ElasticsearchConfig};
//!
//! let config = ElasticsearchConfig {
//!     nodes: vec!["http://localhost:9200".to_string()],
//!     ..Default::default()
//! };
//! let backend = ElasticsearchBackend::new(config)?;
//! backend.initialize().await?;
//! ```

mod backend;
mod schema;
pub mod search;
mod search_impl;
mod storage;

pub use backend::{ElasticsearchAuth, ElasticsearchBackend, ElasticsearchConfig};
