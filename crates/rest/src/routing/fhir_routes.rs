//! FHIR route configuration.
//!
//! Defines all routes for the FHIR RESTful API.

use axum::{
    Router,
    routing::{delete, get, patch, post, put},
};
use helios_persistence::core::{ConditionalStorage, ResourceStorage};

use crate::handlers;
use crate::state::AppState;

/// Creates all FHIR REST API routes.
///
/// # Routes
///
/// ## System-level
/// - `GET /metadata` - CapabilityStatement
/// - `GET /health` - Health check
/// - `GET /_history` - System history
/// - `POST /` - Batch/Transaction
///
/// ## Type-level
/// - `GET /{type}` - Search
/// - `POST /{type}` - Create
/// - `POST /{type}/_search` - Search (POST)
/// - `GET /{type}/_history` - Type history
///
/// ## Instance-level
/// - `GET /{type}/{id}` - Read
/// - `PUT /{type}/{id}` - Update
/// - `PATCH /{type}/{id}` - Patch
/// - `DELETE /{type}/{id}` - Delete
/// - `GET /{type}/{id}/_history` - Instance history
/// - `GET /{type}/{id}/_history/{vid}` - Version read
pub fn create_routes<S>(state: AppState<S>) -> Router
where
    S: ResourceStorage + ConditionalStorage + Send + Sync + 'static,
{
    Router::new()
        // System-level routes
        .route("/metadata", get(handlers::capabilities_handler::<S>))
        .route("/health", get(handlers::health_handler::<S>))
        .route("/_liveness", get(handlers::health::liveness_handler))
        .route("/_readiness", get(handlers::health::readiness_handler::<S>))
        .route("/_history", get(handlers::history_system_handler::<S>))
        .route("/", post(handlers::batch_handler::<S>))
        // Type-level routes
        .route("/{resource_type}", get(handlers::search_get_handler::<S>))
        .route("/{resource_type}", post(handlers::create_handler::<S>))
        .route(
            "/{resource_type}/_search",
            post(handlers::search_post_handler::<S>),
        )
        .route(
            "/{resource_type}/_history",
            get(handlers::history_type_handler::<S>),
        )
        // Instance-level routes
        .route("/{resource_type}/{id}", get(handlers::read_handler::<S>))
        .route("/{resource_type}/{id}", put(handlers::update_handler::<S>))
        .route("/{resource_type}/{id}", patch(handlers::patch_handler::<S>))
        .route(
            "/{resource_type}/{id}",
            delete(handlers::delete_handler::<S>),
        )
        .route(
            "/{resource_type}/{id}/_history",
            get(handlers::history_instance_handler::<S>),
        )
        .route(
            "/{resource_type}/{id}/_history/{version_id}",
            get(handlers::vread_handler::<S>),
        )
        // State
        .with_state(state)
}

/// Creates a minimal set of routes for testing.
///
/// This is useful for integration tests that only need a subset
/// of functionality.
pub fn create_minimal_routes<S>(state: AppState<S>) -> Router
where
    S: ResourceStorage + Send + Sync + 'static,
{
    Router::new()
        .route("/health", get(handlers::health_handler::<S>))
        .route("/metadata", get(handlers::capabilities_handler::<S>))
        .route("/{resource_type}/{id}", get(handlers::read_handler::<S>))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    // Route tests will be in integration tests
}
