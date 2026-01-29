//! Server configuration for the FHIR REST API.
//!
//! This module provides configuration types for the REST server, supporting
//! both programmatic configuration and environment variable overrides.
//!
//! # Environment Variables
//!
//! | Variable | Default | Description |
//! |----------|---------|-------------|
//! | `REST_SERVER_PORT` | 8080 | Server port |
//! | `REST_SERVER_HOST` | 127.0.0.1 | Host to bind |
//! | `REST_LOG_LEVEL` | info | Log level |
//! | `REST_MAX_BODY_SIZE` | 10485760 | Max request body (bytes) |
//! | `REST_REQUEST_TIMEOUT` | 30 | Request timeout (seconds) |
//! | `REST_ENABLE_CORS` | true | Enable CORS |
//! | `REST_CORS_ORIGINS` | * | Allowed origins |
//! | `REST_CORS_METHODS` | GET,POST,PUT,PATCH,DELETE,OPTIONS | Allowed methods |
//! | `REST_CORS_HEADERS` | Content-Type,Authorization,Accept,If-Match,If-None-Match,Prefer | Allowed headers |
//! | `REST_DEFAULT_TENANT` | default | Default tenant ID |
//! | `REST_BASE_URL` | http://localhost:8080 | Server base URL |
//! | `REST_DEFAULT_FHIR_VERSION` | R4 | Default FHIR version (R4, R4B, R5, R6) |
//! | `REST_TENANT_ROUTING_MODE` | header_only | Tenant routing mode (header_only, url_path, both) |
//! | `REST_TENANT_STRICT_VALIDATION` | false | Error if URL and header tenant disagree |
//! | `REST_JWT_TENANT_CLAIM` | tenant_id | JWT claim name for tenant (future use) |
//!
//! # Example
//!
//! ```rust
//! use helios_rest::ServerConfig;
//!
//! // Create from environment
//! let config = ServerConfig::from_env();
//!
//! // Or create programmatically
//! let config = ServerConfig {
//!     port: 3000,
//!     host: "0.0.0.0".to_string(),
//!     enable_cors: true,
//!     ..Default::default()
//! };
//! ```

use std::fmt;
use std::str::FromStr;

use clap::Parser;
use helios_fhir::FhirVersion;

/// Tenant routing mode for multi-tenant deployments.
///
/// Determines how the server identifies tenants from incoming requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TenantRoutingMode {
    /// Tenant identified only from X-Tenant-ID header (default, backward compatible).
    #[default]
    HeaderOnly,
    /// Tenant identified from URL path prefix: `/{tenant}/Patient/123`.
    UrlPath,
    /// Both URL and header supported; URL takes precedence over header.
    Both,
}

impl TenantRoutingMode {
    /// Returns true if URL-based tenant routing is enabled.
    pub fn supports_url_path(&self) -> bool {
        matches!(self, TenantRoutingMode::UrlPath | TenantRoutingMode::Both)
    }

    /// Returns true if header-based tenant routing is enabled.
    pub fn supports_header(&self) -> bool {
        matches!(
            self,
            TenantRoutingMode::HeaderOnly | TenantRoutingMode::Both
        )
    }
}

impl fmt::Display for TenantRoutingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantRoutingMode::HeaderOnly => write!(f, "header_only"),
            TenantRoutingMode::UrlPath => write!(f, "url_path"),
            TenantRoutingMode::Both => write!(f, "both"),
        }
    }
}

impl FromStr for TenantRoutingMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "header_only" | "headeronly" | "header" => Ok(TenantRoutingMode::HeaderOnly),
            "url_path" | "urlpath" | "url" | "path" => Ok(TenantRoutingMode::UrlPath),
            "both" | "combined" => Ok(TenantRoutingMode::Both),
            _ => Err(format!(
                "Invalid tenant routing mode '{}'. Valid values: header_only, url_path, both",
                s
            )),
        }
    }
}

/// Configuration for multi-tenant behavior.
#[derive(Debug, Clone)]
pub struct MultitenancyConfig {
    /// How tenants are identified from requests.
    pub routing_mode: TenantRoutingMode,
    /// If true, error when URL path and header specify different tenants.
    pub strict_validation: bool,
    /// JWT claim name containing tenant ID (for future JWT-based tenant resolution).
    pub jwt_tenant_claim: String,
}

impl Default for MultitenancyConfig {
    fn default() -> Self {
        Self {
            routing_mode: TenantRoutingMode::HeaderOnly,
            strict_validation: false,
            jwt_tenant_claim: "tenant_id".to_string(),
        }
    }
}

impl MultitenancyConfig {
    /// Creates a new MultitenancyConfig from environment variables.
    pub fn from_env() -> Self {
        let routing_mode = std::env::var("REST_TENANT_ROUTING_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();

        let strict_validation = std::env::var("REST_TENANT_STRICT_VALIDATION")
            .map(|s| s.to_lowercase() == "true" || s == "1")
            .unwrap_or(false);

        let jwt_tenant_claim =
            std::env::var("REST_JWT_TENANT_CLAIM").unwrap_or_else(|_| "tenant_id".to_string());

        Self {
            routing_mode,
            strict_validation,
            jwt_tenant_claim,
        }
    }
}

/// Server configuration for the FHIR REST API.
///
/// This struct can be constructed from environment variables using [`ServerConfig::from_env`],
/// from command line arguments using [`ServerConfig::parse`], or programmatically.
#[derive(Debug, Clone, Parser)]
#[command(name = "rest-server")]
#[command(about = "FHIR RESTful API Server")]
pub struct ServerConfig {
    /// Port to listen on.
    #[arg(short, long, env = "REST_SERVER_PORT", default_value = "8080")]
    pub port: u16,

    /// Host address to bind to.
    #[arg(long, env = "REST_SERVER_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Log level (error, warn, info, debug, trace).
    #[arg(long, env = "REST_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Maximum request body size in bytes.
    #[arg(long, env = "REST_MAX_BODY_SIZE", default_value = "10485760")]
    pub max_body_size: usize,

    /// Request timeout in seconds.
    #[arg(long, env = "REST_REQUEST_TIMEOUT", default_value = "30")]
    pub request_timeout: u64,

    /// Enable CORS.
    #[arg(long, env = "REST_ENABLE_CORS", default_value = "true")]
    pub enable_cors: bool,

    /// Allowed CORS origins (comma-separated, or * for all).
    #[arg(long, env = "REST_CORS_ORIGINS", default_value = "*")]
    pub cors_origins: String,

    /// Allowed CORS methods (comma-separated, or * for all).
    #[arg(
        long,
        env = "REST_CORS_METHODS",
        default_value = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
    )]
    pub cors_methods: String,

    /// Allowed CORS headers (comma-separated, or * for all).
    #[arg(
        long,
        env = "REST_CORS_HEADERS",
        default_value = "Content-Type,Authorization,Accept,If-Match,If-None-Match,If-None-Exist,If-Modified-Since,Prefer,X-Tenant-ID"
    )]
    pub cors_headers: String,

    /// Default tenant ID for requests without X-Tenant-ID header.
    #[arg(long, env = "REST_DEFAULT_TENANT", default_value = "default")]
    pub default_tenant: String,

    /// Base URL for the server (used in Location headers and Bundle links).
    #[arg(long, env = "REST_BASE_URL", default_value = "http://localhost:8080")]
    pub base_url: String,

    /// Database connection string.
    #[arg(long, env = "REST_DATABASE_URL")]
    pub database_url: Option<String>,

    /// Enable request ID tracking.
    #[arg(long, env = "REST_ENABLE_REQUEST_ID", default_value = "true")]
    pub enable_request_id: bool,

    /// Return deleted resources with 410 Gone instead of 404 Not Found.
    #[arg(long, env = "REST_RETURN_GONE", default_value = "true")]
    pub return_gone: bool,

    /// Enable versioning (ETag support).
    #[arg(long, env = "REST_ENABLE_VERSIONING", default_value = "true")]
    pub enable_versioning: bool,

    /// Require If-Match header for updates.
    #[arg(long, env = "REST_REQUIRE_IF_MATCH", default_value = "false")]
    pub require_if_match: bool,

    /// Default FHIR version for operations that need it before request parsing
    /// (e.g., tenant resolution, resource type detection).
    #[arg(
        long,
        env = "REST_DEFAULT_FHIR_VERSION",
        value_enum,
        default_value = "R4"
    )]
    pub default_fhir_version: FhirVersion,

    /// Default page size for search results.
    #[arg(long, env = "REST_DEFAULT_PAGE_SIZE", default_value = "20")]
    pub default_page_size: usize,

    /// Maximum page size for search results.
    #[arg(long, env = "REST_MAX_PAGE_SIZE", default_value = "1000")]
    pub max_page_size: usize,

    /// Multitenancy configuration (loaded from environment variables).
    #[arg(skip)]
    pub multitenancy: MultitenancyConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            host: "127.0.0.1".to_string(),
            log_level: "info".to_string(),
            max_body_size: 10 * 1024 * 1024, // 10MB
            request_timeout: 30,
            enable_cors: true,
            cors_origins: "*".to_string(),
            cors_methods: "GET,POST,PUT,PATCH,DELETE,OPTIONS".to_string(),
            cors_headers: "Content-Type,Authorization,Accept,If-Match,If-None-Match,If-None-Exist,If-Modified-Since,Prefer,X-Tenant-ID".to_string(),
            default_tenant: "default".to_string(),
            base_url: "http://localhost:8080".to_string(),
            database_url: None,
            enable_request_id: true,
            return_gone: true,
            enable_versioning: true,
            require_if_match: false,
            default_fhir_version: FhirVersion::default(),
            default_page_size: 20,
            max_page_size: 1000,
            multitenancy: MultitenancyConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Creates a new ServerConfig from environment variables.
    ///
    /// This is a convenience method that parses environment variables without
    /// requiring command line arguments.
    pub fn from_env() -> Self {
        // Try to parse from environment, falling back to defaults
        let mut config = Self::try_parse().unwrap_or_default();
        // Load multitenancy config from environment
        config.multitenancy = MultitenancyConfig::from_env();
        config
    }

    /// Returns the socket address to bind to.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Returns the full base URL for the server.
    pub fn full_base_url(&self) -> &str {
        &self.base_url
    }

    /// Validates the configuration and returns errors if any.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.port == 0 {
            errors.push("Port cannot be 0".to_string());
        }

        if self.max_body_size == 0 {
            errors.push("Max body size cannot be 0".to_string());
        }

        if self.request_timeout == 0 {
            errors.push("Request timeout cannot be 0".to_string());
        }

        if self.default_page_size == 0 {
            errors.push("Default page size cannot be 0".to_string());
        }

        if self.default_page_size > self.max_page_size {
            errors.push("Default page size cannot exceed max page size".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Creates a configuration suitable for testing.
    ///
    /// This uses ephemeral port 0 and disables features that might interfere
    /// with tests.
    pub fn for_testing() -> Self {
        Self {
            port: 0, // Let OS assign port
            host: "127.0.0.1".to_string(),
            log_level: "debug".to_string(),
            max_body_size: 10 * 1024 * 1024,
            request_timeout: 5, // Shorter timeout for tests
            enable_cors: false,
            cors_origins: "*".to_string(),
            cors_methods: "*".to_string(),
            cors_headers: "*".to_string(),
            default_tenant: "test-tenant".to_string(),
            base_url: "http://localhost:0".to_string(),
            database_url: None,
            enable_request_id: false,
            return_gone: true,
            enable_versioning: true,
            require_if_match: false,
            default_fhir_version: FhirVersion::default(),
            default_page_size: 10,
            max_page_size: 100,
            multitenancy: MultitenancyConfig::default(),
        }
    }

    /// Returns the multitenancy configuration.
    pub fn multitenancy(&self) -> &MultitenancyConfig {
        &self.multitenancy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.port, 8080);
        assert_eq!(config.host, "127.0.0.1");
        assert!(config.enable_cors);
    }

    #[test]
    fn test_socket_addr() {
        let config = ServerConfig {
            port: 3000,
            host: "0.0.0.0".to_string(),
            ..Default::default()
        };
        assert_eq!(config.socket_addr(), "0.0.0.0:3000");
    }

    #[test]
    fn test_validate_valid() {
        let config = ServerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_port() {
        let config = ServerConfig {
            port: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().iter().any(|e| e.contains("Port")));
    }

    #[test]
    fn test_validate_invalid_page_sizes() {
        let config = ServerConfig {
            default_page_size: 100,
            max_page_size: 50,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_for_testing() {
        let config = ServerConfig::for_testing();
        assert_eq!(config.port, 0);
        assert!(!config.enable_cors);
        assert_eq!(config.default_tenant, "test-tenant");
    }

    #[test]
    fn test_tenant_routing_mode_parse() {
        assert_eq!(
            "header_only".parse::<TenantRoutingMode>().unwrap(),
            TenantRoutingMode::HeaderOnly
        );
        assert_eq!(
            "url_path".parse::<TenantRoutingMode>().unwrap(),
            TenantRoutingMode::UrlPath
        );
        assert_eq!(
            "both".parse::<TenantRoutingMode>().unwrap(),
            TenantRoutingMode::Both
        );
        assert_eq!(
            "HEADER".parse::<TenantRoutingMode>().unwrap(),
            TenantRoutingMode::HeaderOnly
        );
        assert!("invalid".parse::<TenantRoutingMode>().is_err());
    }

    #[test]
    fn test_tenant_routing_mode_display() {
        assert_eq!(TenantRoutingMode::HeaderOnly.to_string(), "header_only");
        assert_eq!(TenantRoutingMode::UrlPath.to_string(), "url_path");
        assert_eq!(TenantRoutingMode::Both.to_string(), "both");
    }

    #[test]
    fn test_tenant_routing_mode_supports() {
        assert!(TenantRoutingMode::HeaderOnly.supports_header());
        assert!(!TenantRoutingMode::HeaderOnly.supports_url_path());

        assert!(!TenantRoutingMode::UrlPath.supports_header());
        assert!(TenantRoutingMode::UrlPath.supports_url_path());

        assert!(TenantRoutingMode::Both.supports_header());
        assert!(TenantRoutingMode::Both.supports_url_path());
    }

    #[test]
    fn test_multitenancy_config_default() {
        let config = MultitenancyConfig::default();
        assert_eq!(config.routing_mode, TenantRoutingMode::HeaderOnly);
        assert!(!config.strict_validation);
        assert_eq!(config.jwt_tenant_claim, "tenant_id");
    }
}
