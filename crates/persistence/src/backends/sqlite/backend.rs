//! SQLite backend implementation.

use std::fmt::Debug;
use std::path::Path;

use async_trait::async_trait;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use serde::{Deserialize, Serialize};

use crate::core::{Backend, BackendCapability, BackendKind};
use crate::error::{BackendError, StorageResult};

use super::schema;

/// SQLite backend for FHIR resource storage.
#[derive(Debug)]
pub struct SqliteBackend {
    pool: Pool<SqliteConnectionManager>,
    config: SqliteBackendConfig,
    is_memory: bool,
}

/// Configuration for the SQLite backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteBackendConfig {
    /// Maximum number of connections in the pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Minimum number of idle connections.
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,

    /// Connection timeout in milliseconds.
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,

    /// SQLite busy timeout in milliseconds.
    #[serde(default = "default_busy_timeout_ms")]
    pub busy_timeout_ms: u32,

    /// Enable WAL mode for better concurrency.
    #[serde(default = "default_true")]
    pub enable_wal: bool,

    /// Enable foreign key constraints.
    #[serde(default = "default_true")]
    pub enable_foreign_keys: bool,
}

fn default_max_connections() -> u32 {
    10
}

fn default_min_connections() -> u32 {
    1
}

fn default_connection_timeout_ms() -> u64 {
    30000
}

fn default_busy_timeout_ms() -> u32 {
    5000
}

fn default_true() -> bool {
    true
}

impl Default for SqliteBackendConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            connection_timeout_ms: default_connection_timeout_ms(),
            busy_timeout_ms: default_busy_timeout_ms(),
            enable_wal: true,
            enable_foreign_keys: true,
        }
    }
}

impl SqliteBackend {
    /// Creates a new in-memory SQLite backend.
    pub fn in_memory() -> StorageResult<Self> {
        Self::with_config(":memory:", SqliteBackendConfig::default())
    }

    /// Opens or creates a file-based SQLite database.
    pub fn open<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        Self::with_config(path, SqliteBackendConfig::default())
    }

    /// Creates a backend with custom configuration.
    pub fn with_config<P: AsRef<Path>>(path: P, config: SqliteBackendConfig) -> StorageResult<Self> {
        let path_str = path.as_ref().to_string_lossy();
        let is_memory = path_str == ":memory:";

        let manager = SqliteConnectionManager::file(path.as_ref());

        let pool = Pool::builder()
            .max_size(config.max_connections)
            .min_idle(Some(config.min_connections))
            .connection_timeout(std::time::Duration::from_millis(config.connection_timeout_ms))
            .build(manager)
            .map_err(|e| crate::error::StorageError::Backend(BackendError::ConnectionFailed {
                backend_name: "sqlite".to_string(),
                message: e.to_string(),
            }))?;

        let backend = Self {
            pool,
            config,
            is_memory,
        };

        // Configure the connection
        backend.configure_connection()?;

        Ok(backend)
    }

    /// Initialize the database schema.
    pub fn init_schema(&self) -> StorageResult<()> {
        let conn = self.get_connection()?;
        schema::initialize_schema(&conn)
    }

    /// Get a connection from the pool.
    pub(crate) fn get_connection(
        &self,
    ) -> StorageResult<PooledConnection<SqliteConnectionManager>> {
        self.pool
            .get()
            .map_err(|e| crate::error::StorageError::Backend(BackendError::ConnectionFailed {
                backend_name: "sqlite".to_string(),
                message: e.to_string(),
            }))
    }

    /// Configure connection settings.
    fn configure_connection(&self) -> StorageResult<()> {
        let conn = self.get_connection()?;

        conn.busy_timeout(std::time::Duration::from_millis(
            self.config.busy_timeout_ms as u64,
        ))
        .map_err(|e| crate::error::StorageError::Backend(BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to set busy timeout: {}", e),
            source: None,
        }))?;

        if self.config.enable_foreign_keys {
            conn.execute("PRAGMA foreign_keys = ON", [])
                .map_err(|e| crate::error::StorageError::Backend(BackendError::Internal {
                    backend_name: "sqlite".to_string(),
                    message: format!("Failed to enable foreign keys: {}", e),
                    source: None,
                }))?;
        }

        if self.config.enable_wal && !self.is_memory {
            conn.execute("PRAGMA journal_mode = WAL", [])
                .map_err(|e| crate::error::StorageError::Backend(BackendError::Internal {
                    backend_name: "sqlite".to_string(),
                    message: format!("Failed to enable WAL mode: {}", e),
                    source: None,
                }))?;
        }

        Ok(())
    }

    /// Returns whether this is an in-memory database.
    pub fn is_memory(&self) -> bool {
        self.is_memory
    }

    /// Returns the backend configuration.
    pub fn config(&self) -> &SqliteBackendConfig {
        &self.config
    }
}

/// Connection wrapper for SQLite.
pub struct SqliteConnection(pub(crate) PooledConnection<SqliteConnectionManager>);

impl Debug for SqliteConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteConnection").finish()
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    type Connection = SqliteConnection;

    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }

    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn supports(&self, capability: BackendCapability) -> bool {
        matches!(
            capability,
            BackendCapability::Crud
                | BackendCapability::Versioning
                | BackendCapability::InstanceHistory
                | BackendCapability::TypeHistory
                | BackendCapability::SystemHistory
                | BackendCapability::BasicSearch
                | BackendCapability::DateSearch
                | BackendCapability::ReferenceSearch
                | BackendCapability::Sorting
                | BackendCapability::OffsetPagination
                | BackendCapability::Transactions
                | BackendCapability::OptimisticLocking
                | BackendCapability::Include
                | BackendCapability::Revinclude
                | BackendCapability::SharedSchema
        )
    }

    fn capabilities(&self) -> Vec<BackendCapability> {
        vec![
            BackendCapability::Crud,
            BackendCapability::Versioning,
            BackendCapability::InstanceHistory,
            BackendCapability::TypeHistory,
            BackendCapability::SystemHistory,
            BackendCapability::BasicSearch,
            BackendCapability::DateSearch,
            BackendCapability::ReferenceSearch,
            BackendCapability::Sorting,
            BackendCapability::OffsetPagination,
            BackendCapability::Transactions,
            BackendCapability::OptimisticLocking,
            BackendCapability::Include,
            BackendCapability::Revinclude,
            BackendCapability::SharedSchema,
        ]
    }

    async fn acquire(&self) -> Result<Self::Connection, BackendError> {
        let conn = self.pool.get().map_err(|e| BackendError::ConnectionFailed {
            backend_name: "sqlite".to_string(),
            message: e.to_string(),
        })?;
        Ok(SqliteConnection(conn))
    }

    async fn release(&self, _conn: Self::Connection) {
        // Connection is automatically returned to pool when dropped
    }

    async fn health_check(&self) -> Result<(), BackendError> {
        let conn = self.get_connection().map_err(|_| BackendError::Unavailable {
            backend_name: "sqlite".to_string(),
            message: "Failed to get connection".to_string(),
        })?;
        conn.query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|e| BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Health check failed: {}", e),
                source: None,
            })?;
        Ok(())
    }

    async fn initialize(&self) -> Result<(), BackendError> {
        self.init_schema().map_err(|e| BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to initialize schema: {}", e),
            source: None,
        })
    }

    async fn migrate(&self) -> Result<(), BackendError> {
        // Schema migrations are handled by initialize_schema
        self.init_schema().map_err(|e| BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to run migrations: {}", e),
            source: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_backend() {
        let backend = SqliteBackend::in_memory().unwrap();
        assert!(backend.is_memory());
        assert_eq!(backend.name(), "sqlite");
        assert_eq!(backend.kind(), BackendKind::Sqlite);
    }

    #[test]
    fn test_backend_initialization() {
        let backend = SqliteBackend::in_memory().unwrap();
        backend.init_schema().unwrap();
        backend.init_schema().unwrap(); // Should be idempotent
    }

    #[test]
    fn test_backend_capabilities() {
        let backend = SqliteBackend::in_memory().unwrap();

        assert!(backend.supports(BackendCapability::Crud));
        assert!(backend.supports(BackendCapability::BasicSearch));
        assert!(backend.supports(BackendCapability::Transactions));
        assert!(!backend.supports(BackendCapability::FullTextSearch));
    }

    #[tokio::test]
    async fn test_health_check() {
        let backend = SqliteBackend::in_memory().unwrap();
        backend.init_schema().unwrap();
        assert!(backend.health_check().await.is_ok());
    }

    #[tokio::test]
    async fn test_acquire_release() {
        let backend = SqliteBackend::in_memory().unwrap();
        let conn = backend.acquire().await.unwrap();
        backend.release(conn).await;
    }
}
