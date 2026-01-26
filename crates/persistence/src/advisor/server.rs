//! HTTP server for the configuration advisor.
//!
//! This module provides an Axum-based HTTP server for the configuration
//! advisor API. The server exposes endpoints for analyzing, validating,
//! and optimizing composite storage configurations.
//!
//! # Endpoints
//!
//! | Endpoint | Method | Description |
//! |----------|--------|-------------|
//! | `/health` | GET | Health check |
//! | `/backends` | GET | List available backend types |
//! | `/backends/:kind` | GET | Get capabilities for a backend type |
//! | `/analyze` | POST | Analyze a configuration |
//! | `/validate` | POST | Validate a configuration |
//! | `/suggest` | POST | Get optimization suggestions |
//! | `/simulate` | POST | Simulate query routing |
//!
//! # Example
//!
//! ```ignore
//! use helios_persistence::advisor::{AdvisorConfig, AdvisorServer};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = AdvisorConfig::default();
//!     let server = AdvisorServer::new(config);
//!     server.run().await.unwrap();
//! }
//! ```

use std::net::SocketAddr;

use super::handlers::{
    AnalyzeRequest, SimulateRequest, SuggestRequest, ValidateRequest, handle_analyze,
    handle_backend_capabilities, handle_backends, handle_simulate, handle_suggest, handle_validate,
};

/// Configuration for the advisor server.
#[derive(Debug, Clone)]
pub struct AdvisorConfig {
    /// Host to bind to.
    pub host: String,

    /// Port to bind to.
    pub port: u16,

    /// Enable CORS.
    pub enable_cors: bool,

    /// Request timeout in seconds.
    pub request_timeout_secs: u64,
}

impl Default for AdvisorConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8081,
            enable_cors: true,
            request_timeout_secs: 30,
        }
    }
}

impl AdvisorConfig {
    /// Creates a new configuration with the given host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            ..Default::default()
        }
    }

    /// Creates a configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("ADVISOR_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("ADVISOR_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8081),
            enable_cors: std::env::var("ADVISOR_ENABLE_CORS")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(true),
            request_timeout_secs: std::env::var("ADVISOR_TIMEOUT")
                .ok()
                .and_then(|t| t.parse().ok())
                .unwrap_or(30),
        }
    }

    /// Returns the socket address to bind to.
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("Invalid host:port configuration")
    }
}

/// The configuration advisor HTTP server.
///
/// This server provides REST endpoints for analyzing and optimizing
/// composite storage configurations.
pub struct AdvisorServer {
    /// Server configuration.
    config: AdvisorConfig,
}

impl AdvisorServer {
    /// Creates a new advisor server.
    pub fn new(config: AdvisorConfig) -> Self {
        Self { config }
    }

    /// Creates a server with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(AdvisorConfig::default())
    }

    /// Creates a server with configuration from environment.
    pub fn from_env() -> Self {
        Self::new(AdvisorConfig::from_env())
    }

    /// Returns the server configuration.
    pub fn config(&self) -> &AdvisorConfig {
        &self.config
    }

    /// Runs the server (blocking).
    ///
    /// This method starts the HTTP server and blocks until it is shut down.
    /// Use `run_with_shutdown` for graceful shutdown support.
    #[cfg(feature = "advisor")]
    pub async fn run(&self) -> Result<(), std::io::Error> {
        use tower_http::cors::{Any, CorsLayer};
        use tracing::info;

        let app = self.create_router();

        // Add CORS if enabled
        let app = if self.config.enable_cors {
            let cors = CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any);
            app.layer(cors)
        } else {
            app
        };

        let addr = self.config.socket_addr();
        info!("Starting advisor server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await
    }

    /// Runs the server with graceful shutdown support.
    #[cfg(feature = "advisor")]
    pub async fn run_with_shutdown(
        &self,
        shutdown_signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), std::io::Error> {
        use tower_http::cors::{Any, CorsLayer};
        use tracing::info;

        let app = self.create_router();

        // Add CORS if enabled
        let app = if self.config.enable_cors {
            let cors = CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any);
            app.layer(cors)
        } else {
            app
        };

        let addr = self.config.socket_addr();
        info!("Starting advisor server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .await
    }

    /// Creates the Axum router with all endpoints.
    #[cfg(feature = "advisor")]
    fn create_router(&self) -> axum::Router {
        use axum::{
            routing::{get, post},
            Router,
        };

        Router::new()
            .route("/health", get(health_handler))
            .route("/backends", get(backends_handler))
            .route("/backends/{kind}", get(backend_capabilities_handler))
            .route("/analyze", post(analyze_handler))
            .route("/validate", post(validate_handler))
            .route("/suggest", post(suggest_handler))
            .route("/simulate", post(simulate_handler))
    }

    /// Creates the router without the advisor feature (for library use).
    #[cfg(not(feature = "advisor"))]
    fn create_router(&self) {
        // No-op without the advisor feature
    }
}

// ============================================================================
// Axum Handler Functions
// ============================================================================

#[cfg(feature = "advisor")]
async fn health_handler() -> impl axum::response::IntoResponse {
    use axum::Json;
    use serde_json::json;

    Json(json!({
        "status": "healthy",
        "service": "config-advisor",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

#[cfg(feature = "advisor")]
async fn backends_handler() -> impl axum::response::IntoResponse {
    use axum::Json;
    Json(handle_backends())
}

#[cfg(feature = "advisor")]
async fn backend_capabilities_handler(
    axum::extract::Path(kind): axum::extract::Path<String>,
) -> impl axum::response::IntoResponse {
    use axum::{Json, http::StatusCode};

    match handle_backend_capabilities(&kind) {
        Ok(info) => (StatusCode::OK, Json(serde_json::to_value(info).unwrap())).into_response(),
        Err(msg) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
    }
}

#[cfg(feature = "advisor")]
async fn analyze_handler(
    axum::extract::Json(request): axum::extract::Json<AnalyzeRequest>,
) -> impl axum::response::IntoResponse {
    use axum::{Json, http::StatusCode};

    match handle_analyze(request) {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        )
            .into_response(),
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
    }
}

#[cfg(feature = "advisor")]
async fn validate_handler(
    axum::extract::Json(request): axum::extract::Json<ValidateRequest>,
) -> impl axum::response::IntoResponse {
    use axum::{Json, http::StatusCode};

    match handle_validate(request) {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        )
            .into_response(),
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
    }
}

#[cfg(feature = "advisor")]
async fn suggest_handler(
    axum::extract::Json(request): axum::extract::Json<SuggestRequest>,
) -> impl axum::response::IntoResponse {
    use axum::{Json, http::StatusCode};

    match handle_suggest(request) {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        )
            .into_response(),
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
    }
}

#[cfg(feature = "advisor")]
async fn simulate_handler(
    axum::extract::Json(request): axum::extract::Json<SimulateRequest>,
) -> impl axum::response::IntoResponse {
    use axum::{Json, http::StatusCode};

    match handle_simulate(request) {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap()),
        )
            .into_response(),
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
    }
}

#[cfg(feature = "advisor")]
use axum::response::IntoResponse;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_advisor_config_default() {
        let config = AdvisorConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8081);
        assert!(config.enable_cors);
    }

    #[test]
    fn test_advisor_config_new() {
        let config = AdvisorConfig::new("0.0.0.0", 9000);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9000);
    }

    #[test]
    fn test_socket_addr() {
        let config = AdvisorConfig::new("127.0.0.1", 8081);
        let addr = config.socket_addr();
        assert_eq!(addr.port(), 8081);
    }

    #[test]
    fn test_server_creation() {
        let server = AdvisorServer::with_defaults();
        assert_eq!(server.config().port, 8081);
    }
}
