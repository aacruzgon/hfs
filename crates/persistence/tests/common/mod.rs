//! Test infrastructure for the persistence layer.
//!
//! This module provides reusable test utilities, traits, and macros for testing
//! storage backends across the full FHIR specification.

pub mod assertions;
pub mod capabilities;
pub mod fixtures;
pub mod harness;

// Re-export commonly used items
pub use assertions::*;
pub use capabilities::*;
pub use fixtures::*;
pub use harness::*;
