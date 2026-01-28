//! HTTP middleware for the FHIR REST API.
//!
//! This module contains Axum middleware components:
//!
//! - [`tenant`] - Tenant identification and extraction
//! - [`content_type`] - Content negotiation
//! - [`conditional`] - Conditional request headers (If-Match, etc.)
//! - [`prefer`] - Prefer header handling

pub mod conditional;
pub mod content_type;
pub mod prefer;
pub mod tenant;

pub use conditional::ConditionalHeaders;
pub use prefer::PreferHeader;
