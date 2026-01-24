//! FTS5 Full-Text Search integration.
//!
//! Provides optional FTS5-based searching for string and text content.

use super::query_builder::{SqlFragment, SqlParam};

/// FTS5 search helper for full-text search operations.
pub struct Fts5Search;

impl Fts5Search {
    /// The name of the FTS5 virtual table.
    pub const FTS_TABLE_NAME: &'static str = "search_index_fts";

    /// Generates the SQL to create the FTS5 virtual table.
    pub fn create_table_sql() -> &'static str {
        r#"
        CREATE VIRTUAL TABLE IF NOT EXISTS search_index_fts USING fts5(
            text_content,
            content='search_index',
            content_rowid='rowid',
            tokenize='porter unicode61'
        )
        "#
    }

    /// Generates triggers to keep FTS5 table in sync with search_index.
    pub fn create_triggers_sql() -> &'static str {
        r#"
        -- Trigger for INSERT
        CREATE TRIGGER IF NOT EXISTS search_index_fts_insert AFTER INSERT ON search_index
        WHEN new.value_string IS NOT NULL
        BEGIN
            INSERT INTO search_index_fts(rowid, text_content) VALUES (new.rowid, new.value_string);
        END;

        -- Trigger for DELETE
        CREATE TRIGGER IF NOT EXISTS search_index_fts_delete AFTER DELETE ON search_index
        WHEN old.value_string IS NOT NULL
        BEGIN
            INSERT INTO search_index_fts(search_index_fts, rowid, text_content) VALUES ('delete', old.rowid, old.value_string);
        END;

        -- Trigger for UPDATE
        CREATE TRIGGER IF NOT EXISTS search_index_fts_update AFTER UPDATE ON search_index
        WHEN old.value_string IS NOT NULL OR new.value_string IS NOT NULL
        BEGIN
            INSERT INTO search_index_fts(search_index_fts, rowid, text_content) VALUES ('delete', old.rowid, old.value_string);
            INSERT INTO search_index_fts(rowid, text_content) VALUES (new.rowid, new.value_string);
        END;
        "#
    }

    /// Builds a full-text search query using FTS5 MATCH syntax.
    ///
    /// The search_term is escaped for safe use in FTS5 queries.
    pub fn build_fts_query(search_term: &str, param_num: usize) -> SqlFragment {
        SqlFragment::with_params(
            format!(
                "rowid IN (SELECT rowid FROM {} WHERE {} MATCH ?{})",
                Self::FTS_TABLE_NAME,
                Self::FTS_TABLE_NAME,
                param_num
            ),
            vec![SqlParam::string(Self::escape_fts_query(search_term))],
        )
    }

    /// Builds an FTS5 query with phrase matching.
    pub fn build_phrase_query(phrase: &str, param_num: usize) -> SqlFragment {
        let escaped = Self::escape_fts_query(phrase);
        SqlFragment::with_params(
            format!(
                "rowid IN (SELECT rowid FROM {} WHERE {} MATCH ?{})",
                Self::FTS_TABLE_NAME,
                Self::FTS_TABLE_NAME,
                param_num
            ),
            vec![SqlParam::string(format!("\"{}\"", escaped))],
        )
    }

    /// Builds an FTS5 prefix search query.
    pub fn build_prefix_query(prefix: &str, param_num: usize) -> SqlFragment {
        let escaped = Self::escape_fts_query(prefix);
        SqlFragment::with_params(
            format!(
                "rowid IN (SELECT rowid FROM {} WHERE {} MATCH ?{})",
                Self::FTS_TABLE_NAME,
                Self::FTS_TABLE_NAME,
                param_num
            ),
            vec![SqlParam::string(format!("{}*", escaped))],
        )
    }

    /// Escapes special characters for FTS5 queries.
    fn escape_fts_query(term: &str) -> String {
        // FTS5 special characters that need escaping in queries
        let mut result = String::with_capacity(term.len());
        for c in term.chars() {
            match c {
                '"' | '*' | ':' | '^' | '(' | ')' | '+' | '-' | '~' => {
                    // Skip these special characters or escape them
                    result.push(' ');
                }
                _ => result.push(c),
            }
        }
        result.trim().to_string()
    }

    /// Checks if FTS5 is available in the database.
    ///
    /// This should be called during backend initialization.
    pub fn check_fts5_available_sql() -> &'static str {
        "SELECT sqlite_compileoption_used('ENABLE_FTS5')"
    }

    /// Rebuilds the FTS5 index from the search_index table.
    ///
    /// Call this after bulk imports or if the FTS index gets out of sync.
    pub fn rebuild_index_sql() -> String {
        format!(
            "INSERT INTO {}({}) VALUES ('rebuild')",
            Self::FTS_TABLE_NAME,
            Self::FTS_TABLE_NAME
        )
    }

    /// Optimizes the FTS5 index for better query performance.
    pub fn optimize_index_sql() -> String {
        format!(
            "INSERT INTO {}({}) VALUES ('optimize')",
            Self::FTS_TABLE_NAME,
            Self::FTS_TABLE_NAME
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_fts_query() {
        assert_eq!(Fts5Search::escape_fts_query("simple"), "simple");
        assert_eq!(Fts5Search::escape_fts_query("has\"quotes"), "has quotes");
        assert_eq!(Fts5Search::escape_fts_query("star*"), "star");
        assert_eq!(
            Fts5Search::escape_fts_query("complex:query+term"),
            "complex query term"
        );
    }

    #[test]
    fn test_build_fts_query() {
        let frag = Fts5Search::build_fts_query("smith", 1);

        assert!(frag.sql.contains("search_index_fts"));
        assert!(frag.sql.contains("MATCH"));
        assert_eq!(frag.params.len(), 1);
    }

    #[test]
    fn test_build_phrase_query() {
        let frag = Fts5Search::build_phrase_query("john smith", 1);

        assert!(frag.sql.contains("MATCH"));
        // The param should be quoted for phrase search
    }

    #[test]
    fn test_build_prefix_query() {
        let frag = Fts5Search::build_prefix_query("smi", 1);

        assert!(frag.sql.contains("MATCH"));
    }
}
