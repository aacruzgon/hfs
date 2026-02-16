//! Parameter type handlers for Elasticsearch query building.
//!
//! Each module translates a FHIR search parameter type into ES Query DSL.

pub mod composite;
pub mod date;
pub mod number;
pub mod quantity;
pub mod reference;
pub mod string;
pub mod token;
pub mod uri;
