//! Response formatting for the FHIR REST API.
//!
//! This module provides utilities for building FHIR-compliant responses:
//!
//! - [`operation_outcome`] - OperationOutcome generation
//! - [`bundle`] - Bundle response building
//! - [`headers`] - Response header generation (ETag, Location, etc.)

pub mod bundle;
pub mod headers;
pub mod operation_outcome;

pub use bundle::BundleBuilder;
pub use headers::ResourceHeaders;
pub use operation_outcome::OperationOutcomeBuilder;
