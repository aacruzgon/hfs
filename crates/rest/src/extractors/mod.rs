//! Axum extractors for FHIR-specific data.
//!
//! This module provides custom Axum extractors for common FHIR patterns:
//!
//! - [`TenantExtractor`] - Extract tenant context from request
//! - [`FhirResource`] - Extract and validate FHIR resources
//! - [`SearchParams`] - Extract and parse search parameters
//! - [`Pagination`] - Extract pagination parameters

mod fhir_resource;
mod pagination;
mod search_params;
mod tenant;

pub use fhir_resource::FhirResource;
pub use pagination::Pagination;
pub use search_params::SearchParams;
pub use tenant::TenantExtractor;
