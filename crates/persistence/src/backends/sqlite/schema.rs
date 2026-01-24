//! SQLite schema definitions and migrations.

use rusqlite::Connection;

use crate::error::StorageResult;

/// Current schema version.
pub const SCHEMA_VERSION: i32 = 3;

/// Initialize the database schema.
pub fn initialize_schema(conn: &Connection) -> StorageResult<()> {
    // Check current version
    let current_version = get_schema_version(conn)?;

    if current_version == 0 {
        // Fresh database - create schema
        create_schema_v1(conn)?;
        set_schema_version(conn, SCHEMA_VERSION)?;
    } else if current_version < SCHEMA_VERSION {
        // Run migrations
        migrate_schema(conn, current_version)?;
    }

    Ok(())
}

/// Get the current schema version.
fn get_schema_version(conn: &Connection) -> StorageResult<i32> {
    // Create version table if it doesn't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        )",
        [],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to create schema_version table: {}", e),
            source: None,
        })
    })?;

    let version: Option<i32> = conn
        .query_row("SELECT version FROM schema_version LIMIT 1", [], |row| {
            row.get(0)
        })
        .ok();

    Ok(version.unwrap_or(0))
}

/// Set the schema version.
fn set_schema_version(conn: &Connection, version: i32) -> StorageResult<()> {
    conn.execute("DELETE FROM schema_version", [])
        .map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to clear schema_version: {}", e),
                source: None,
            })
        })?;

    conn.execute(
        "INSERT INTO schema_version (version) VALUES (?1)",
        [version],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to set schema_version: {}", e),
            source: None,
        })
    })?;

    Ok(())
}

/// Create the initial schema (version 1).
fn create_schema_v1(conn: &Connection) -> StorageResult<()> {
    // Main resources table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS resources (
            tenant_id TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            id TEXT NOT NULL,
            version_id TEXT NOT NULL,
            data BLOB NOT NULL,
            last_updated TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            deleted_at TEXT,
            PRIMARY KEY (tenant_id, resource_type, id)
        )",
        [],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to create resources table: {}", e),
            source: None,
        })
    })?;

    // Resource history table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS resource_history (
            tenant_id TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            id TEXT NOT NULL,
            version_id TEXT NOT NULL,
            data BLOB NOT NULL,
            last_updated TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tenant_id, resource_type, id, version_id)
        )",
        [],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to create resource_history table: {}", e),
            source: None,
        })
    })?;

    // Search index table for extracted values
    conn.execute(
        "CREATE TABLE IF NOT EXISTS search_index (
            tenant_id TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            param_name TEXT NOT NULL,
            param_url TEXT,
            value_string TEXT,
            value_token_system TEXT,
            value_token_code TEXT,
            value_date TEXT,
            value_date_precision TEXT,
            value_number REAL,
            value_quantity_value REAL,
            value_quantity_unit TEXT,
            value_quantity_system TEXT,
            value_reference TEXT,
            value_uri TEXT,
            composite_group INTEGER,
            FOREIGN KEY (tenant_id, resource_type, resource_id)
                REFERENCES resources(tenant_id, resource_type, id) ON DELETE CASCADE
        )",
        [],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to create search_index table: {}", e),
            source: None,
        })
    })?;

    // Create indexes for efficient queries
    create_indexes(conn)?;

    // Create FTS5 table for full-text search (if available)
    create_fts_table(conn)?;

    Ok(())
}

/// Create indexes for efficient queries.
fn create_indexes(conn: &Connection) -> StorageResult<()> {
    let indexes = [
        // Resources table indexes
        "CREATE INDEX IF NOT EXISTS idx_resources_type ON resources(tenant_id, resource_type)",
        "CREATE INDEX IF NOT EXISTS idx_resources_updated ON resources(tenant_id, last_updated)",
        // History table indexes
        "CREATE INDEX IF NOT EXISTS idx_history_resource ON resource_history(tenant_id, resource_type, id)",
        "CREATE INDEX IF NOT EXISTS idx_history_updated ON resource_history(tenant_id, last_updated)",
        // Search index indexes
        "CREATE INDEX IF NOT EXISTS idx_search_string ON search_index(tenant_id, resource_type, param_name, value_string)",
        "CREATE INDEX IF NOT EXISTS idx_search_token ON search_index(tenant_id, resource_type, param_name, value_token_system, value_token_code)",
        "CREATE INDEX IF NOT EXISTS idx_search_date ON search_index(tenant_id, resource_type, param_name, value_date)",
        "CREATE INDEX IF NOT EXISTS idx_search_number ON search_index(tenant_id, resource_type, param_name, value_number)",
        "CREATE INDEX IF NOT EXISTS idx_search_quantity ON search_index(tenant_id, resource_type, param_name, value_quantity_value, value_quantity_unit)",
        "CREATE INDEX IF NOT EXISTS idx_search_reference ON search_index(tenant_id, resource_type, param_name, value_reference)",
        "CREATE INDEX IF NOT EXISTS idx_search_uri ON search_index(tenant_id, resource_type, param_name, value_uri)",
        // Index for composite parameter matching
        "CREATE INDEX IF NOT EXISTS idx_search_composite ON search_index(tenant_id, resource_type, resource_id, param_name, composite_group)",
        // Index for resource-based lookups
        "CREATE INDEX IF NOT EXISTS idx_search_resource ON search_index(tenant_id, resource_type, resource_id)",
    ];

    for index_sql in &indexes {
        conn.execute(index_sql, []).map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to create index: {}", e),
                source: None,
            })
        })?;
    }

    Ok(())
}

/// Create FTS5 virtual table for full-text search.
///
/// This is optional - if FTS5 is not available, the function succeeds silently.
fn create_fts_table(conn: &Connection) -> StorageResult<()> {
    // Check if FTS5 is available
    let fts5_available: i32 = conn
        .query_row(
            "SELECT sqlite_compileoption_used('ENABLE_FTS5')",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if fts5_available == 0 {
        // FTS5 not available - skip silently
        return Ok(());
    }

    // Create the FTS5 virtual table for full-text search
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS resource_fts USING fts5(
            resource_id UNINDEXED,
            resource_type UNINDEXED,
            tenant_id UNINDEXED,
            narrative_text,
            full_content,
            tokenize='porter unicode61 remove_diacritics 1'
        )",
        [],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to create resource_fts table: {}", e),
            source: None,
        })
    })?;

    Ok(())
}

/// Run schema migrations from current version to latest.
fn migrate_schema(conn: &Connection, from_version: i32) -> StorageResult<()> {
    let mut version = from_version;

    while version < SCHEMA_VERSION {
        match version {
            1 => migrate_v1_to_v2(conn)?,
            2 => migrate_v2_to_v3(conn)?,
            _ => {
                return Err(crate::error::StorageError::Backend(
                    crate::error::BackendError::Internal {
                        backend_name: "sqlite".to_string(),
                        message: format!("Unknown schema version: {}", version),
                        source: None,
                    },
                ));
            }
        }
        version += 1;
        set_schema_version(conn, version)?;
    }

    Ok(())
}

/// Migrate from schema version 1 to version 2.
///
/// This migration adds new columns to the search_index table:
/// - param_url: Canonical URL for the search parameter
/// - value_date_precision: Precision tracking for date values
/// - value_quantity_system: System URI for quantity units
/// - composite_group: Group ID for composite parameter components
fn migrate_v1_to_v2(conn: &Connection) -> StorageResult<()> {
    let migrations = [
        // Add new columns to search_index table
        "ALTER TABLE search_index ADD COLUMN param_url TEXT",
        "ALTER TABLE search_index ADD COLUMN value_date_precision TEXT",
        "ALTER TABLE search_index ADD COLUMN value_quantity_system TEXT",
        "ALTER TABLE search_index ADD COLUMN composite_group INTEGER",
    ];

    for sql in &migrations {
        // Ignore errors for column already exists (idempotent migration)
        let _ = conn.execute(sql, []);
    }

    // Create new indexes
    let indexes = [
        "CREATE INDEX IF NOT EXISTS idx_search_quantity ON search_index(tenant_id, resource_type, param_name, value_quantity_value, value_quantity_unit)",
        "CREATE INDEX IF NOT EXISTS idx_search_composite ON search_index(tenant_id, resource_type, resource_id, param_name, composite_group)",
        "CREATE INDEX IF NOT EXISTS idx_search_resource ON search_index(tenant_id, resource_type, resource_id)",
    ];

    for index_sql in &indexes {
        conn.execute(index_sql, []).map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to create index in migration: {}", e),
                source: None,
            })
        })?;
    }

    Ok(())
}

/// Migrate from schema version 2 to version 3.
///
/// This migration adds FTS5 full-text search support for _text and _content searches:
/// - resource_fts: FTS5 virtual table for full-text search
/// - Stores narrative text (for _text) and full content (for _content)
fn migrate_v2_to_v3(conn: &Connection) -> StorageResult<()> {
    // Check if FTS5 is available
    let fts5_available: i32 = conn
        .query_row(
            "SELECT sqlite_compileoption_used('ENABLE_FTS5')",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if fts5_available == 0 {
        // FTS5 not available - log warning but don't fail
        // _text and _content searches will be unsupported
        tracing::warn!("FTS5 not available - full-text search features will be disabled");
        return Ok(());
    }

    // Create the FTS5 virtual table for full-text search
    // Uses external content mode for smaller index size
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS resource_fts USING fts5(
            resource_id UNINDEXED,
            resource_type UNINDEXED,
            tenant_id UNINDEXED,
            narrative_text,
            full_content,
            tokenize='porter unicode61 remove_diacritics 1'
        )",
        [],
    )
    .map_err(|e| {
        crate::error::StorageError::Backend(crate::error::BackendError::Internal {
            backend_name: "sqlite".to_string(),
            message: format!("Failed to create resource_fts table: {}", e),
            source: None,
        })
    })?;

    Ok(())
}

/// Drop all tables (for testing).
#[cfg(test)]
pub fn drop_all_tables(conn: &Connection) -> StorageResult<()> {
    // Drop FTS5 table first (if exists)
    let _ = conn.execute("DROP TABLE IF EXISTS resource_fts", []);
    conn.execute("DROP TABLE IF EXISTS search_index", [])
        .map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to drop search_index: {}", e),
                source: None,
            })
        })?;
    conn.execute("DROP TABLE IF EXISTS resource_history", [])
        .map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to drop resource_history: {}", e),
                source: None,
            })
        })?;
    conn.execute("DROP TABLE IF EXISTS resources", [])
        .map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to drop resources: {}", e),
                source: None,
            })
        })?;
    conn.execute("DROP TABLE IF EXISTS schema_version", [])
        .map_err(|e| {
            crate::error::StorageError::Backend(crate::error::BackendError::Internal {
                backend_name: "sqlite".to_string(),
                message: format!("Failed to drop schema_version: {}", e),
                source: None,
            })
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_initialization() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_schema(&conn).unwrap();

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"resources".to_string()));
        assert!(tables.contains(&"resource_history".to_string()));
        assert!(tables.contains(&"search_index".to_string()));
        assert!(tables.contains(&"schema_version".to_string()));
    }

    #[test]
    fn test_schema_version() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_schema(&conn).unwrap();

        let version = get_schema_version(&conn).unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn test_schema_idempotent() {
        let conn = Connection::open_in_memory().unwrap();

        // Initialize twice - should not fail
        initialize_schema(&conn).unwrap();
        initialize_schema(&conn).unwrap();

        let version = get_schema_version(&conn).unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }
}
