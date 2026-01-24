//! FTS5 Full-Text Search integration.
//!
//! Provides optional FTS5-based searching for string and text content.
//! Supports FHIR _text and _content search parameters.

use serde_json::Value;

use super::query_builder::{SqlFragment, SqlParam};

/// Content extracted from a resource for full-text search.
#[derive(Debug, Clone, Default)]
pub struct SearchableContent {
    /// Narrative text from the resource's text.div element (HTML stripped).
    /// Used for _text searches.
    pub narrative: String,
    /// Full text content extracted from all string fields in the resource.
    /// Used for _content searches.
    pub full_content: String,
}

impl SearchableContent {
    /// Creates a new empty SearchableContent.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if both narrative and full_content are empty.
    pub fn is_empty(&self) -> bool {
        self.narrative.is_empty() && self.full_content.is_empty()
    }
}

/// Extracts searchable text content from a FHIR resource.
///
/// Extracts:
/// - Narrative text from text.div (with HTML stripped)
/// - Full content from all string values in the resource
pub fn extract_searchable_content(resource: &Value) -> SearchableContent {
    SearchableContent {
        // _text: Extract and strip HTML from narrative
        narrative: extract_narrative(resource),
        // _content: Extract all string values recursively
        full_content: extract_all_strings(resource),
    }
}

/// Extracts narrative text from a resource's text.div element.
///
/// Strips HTML tags and returns plain text.
fn extract_narrative(resource: &Value) -> String {
    resource
        .get("text")
        .and_then(|t| t.get("div"))
        .and_then(|d| d.as_str())
        .map(strip_html_tags)
        .unwrap_or_default()
}

/// Strips HTML tags from a string.
///
/// Simple HTML stripping - removes everything between < and >.
fn strip_html_tags(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;

    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => result.push(c),
            _ => {}
        }
    }

    // Normalize whitespace
    result
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Extracts all string values from a JSON value recursively.
///
/// Concatenates all string values found in the resource, separated by spaces.
fn extract_all_strings(value: &Value) -> String {
    let mut strings = Vec::new();
    extract_strings_recursive(value, &mut strings);
    strings.join(" ")
}

/// Recursively extracts string values from a JSON value.
fn extract_strings_recursive(value: &Value, strings: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            // Skip empty strings and URLs (typically not useful for text search)
            if !s.is_empty() && !s.starts_with("http://") && !s.starts_with("https://") {
                strings.push(s.clone());
            }
        }
        Value::Array(arr) => {
            for item in arr {
                extract_strings_recursive(item, strings);
            }
        }
        Value::Object(obj) => {
            for (key, val) in obj {
                // Skip technical fields that aren't useful for text search
                if !matches!(
                    key.as_str(),
                    "resourceType" | "id" | "meta" | "extension" | "url" | "reference"
                ) {
                    extract_strings_recursive(val, strings);
                }
            }
        }
        _ => {}
    }
}

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
    pub fn escape_fts_query(term: &str) -> String {
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
    use serde_json::json;

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

    #[test]
    fn test_strip_html_tags() {
        assert_eq!(strip_html_tags("<p>Hello</p>"), "Hello");
        assert_eq!(
            strip_html_tags("<div><p>Hello <b>world</b></p></div>"),
            "Hello world"
        );
        assert_eq!(strip_html_tags("No tags here"), "No tags here");
        assert_eq!(strip_html_tags("<br/>"), "");
        assert_eq!(
            strip_html_tags("<div xmlns=\"http://www.w3.org/1999/xhtml\">Test</div>"),
            "Test"
        );
    }

    #[test]
    fn test_extract_narrative() {
        let patient = json!({
            "resourceType": "Patient",
            "text": {
                "status": "generated",
                "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>John Smith, born 1970-01-15</p></div>"
            }
        });

        let narrative = extract_narrative(&patient);
        assert!(narrative.contains("John Smith"));
        assert!(narrative.contains("born"));
        assert!(!narrative.contains("<"));
    }

    #[test]
    fn test_extract_narrative_no_text() {
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith"}]
        });

        let narrative = extract_narrative(&patient);
        assert!(narrative.is_empty());
    }

    #[test]
    fn test_extract_all_strings() {
        let patient = json!({
            "resourceType": "Patient",
            "id": "123",
            "name": [{
                "family": "Smith",
                "given": ["John", "James"]
            }],
            "address": [{
                "city": "Boston",
                "state": "MA"
            }]
        });

        let content = extract_all_strings(&patient);
        assert!(content.contains("Smith"));
        assert!(content.contains("John"));
        assert!(content.contains("James"));
        assert!(content.contains("Boston"));
        // Should skip resourceType and id
        assert!(!content.contains("Patient"));
    }

    #[test]
    fn test_extract_searchable_content() {
        let patient = json!({
            "resourceType": "Patient",
            "text": {
                "div": "<div>John Smith from Boston</div>"
            },
            "name": [{"family": "Smith", "given": ["John"]}],
            "address": [{"city": "Boston"}]
        });

        let content = extract_searchable_content(&patient);
        assert!(!content.is_empty());
        assert!(content.narrative.contains("John Smith"));
        assert!(content.full_content.contains("Smith"));
        assert!(content.full_content.contains("Boston"));
    }

    #[test]
    fn test_searchable_content_is_empty() {
        let content = SearchableContent::new();
        assert!(content.is_empty());

        let content = SearchableContent {
            narrative: "test".to_string(),
            full_content: String::new(),
        };
        assert!(!content.is_empty());
    }
}
