//! Configuration Advisor for Composite Storage.
//!
//! This module provides an HTTP API for analyzing and optimizing composite
//! storage configurations. It helps users understand trade-offs between
//! different backend configurations and provides suggestions for optimal setup.
//!
//! # Features
//!
//! - **Configuration Analysis**: Validate and analyze backend configurations
//! - **Capability Coverage**: Check which FHIR operations are supported
//! - **Query Simulation**: Simulate query routing and cost estimation
//! - **Optimization Suggestions**: Get recommendations based on workload patterns
//!
//! # Example
//!
//! ```ignore
//! use helios_persistence::advisor::{AdvisorServer, AdvisorConfig};
//!
//! let config = AdvisorConfig {
//!     host: "127.0.0.1".to_string(),
//!     port: 8081,
//! };
//!
//! let server = AdvisorServer::new(config);
//! server.run().await?;
//! ```

pub mod analysis;
pub mod handlers;
pub mod server;
pub mod suggestions;

pub use analysis::{
    AnalysisResult, CapabilityCoverage, ConfigurationAnalyzer, GapAnalysis, RedundancyReport,
};
pub use handlers::{
    AnalyzeRequest, AnalyzeResponse, BackendInfo, SimulateRequest, SimulateResponse,
    SuggestRequest, SuggestResponse, ValidateRequest, ValidateResponse,
};
pub use server::{AdvisorConfig, AdvisorServer};
pub use suggestions::{
    OptimizationSuggestion, SuggestionEngine, SuggestionPriority, WorkloadPattern,
};
