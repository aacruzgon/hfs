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

/// Strips HTML tags from a string and decodes HTML entities.
///
/// Handles:
/// - HTML tags (removes everything between < and >)
/// - CDATA sections (extracts content)
/// - HTML entities (&lt;, &gt;, &amp;, &nbsp;, &quot;, &apos;, &#123;, &#x1F;, etc.)
fn strip_html_tags(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut chars = html.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '<' => {
                // Check for CDATA section: <![CDATA[...]]>
                if chars.peek() == Some(&'!') {
                    let lookahead: String = chars.clone().take(8).collect();
                    if lookahead.starts_with("![CDATA[") {
                        // Skip the "![CDATA[" prefix
                        for _ in 0..8 {
                            chars.next();
                        }
                        // Extract until ]]>
                        let mut cdata_content = String::new();
                        while let Some(ch) = chars.next() {
                            if ch == ']' {
                                let next_two: String = chars.clone().take(2).collect();
                                if next_two == "]>" {
                                    chars.next(); // skip ]
                                    chars.next(); // skip >
                                    break;
                                }
                            }
                            cdata_content.push(ch);
                        }
                        result.push_str(&cdata_content);
                        result.push(' ');
                        continue;
                    }
                }
                in_tag = true;
            }
            '>' if in_tag => {
                in_tag = false;
            }
            '&' if !in_tag => {
                // Collect entity up to ';' (max 10 chars for safety)
                let mut entity = String::new();
                let mut found_semicolon = false;
                for _ in 0..10 {
                    if let Some(&ch) = chars.peek() {
                        if ch == ';' {
                            chars.next(); // consume the semicolon
                            found_semicolon = true;
                            break;
                        } else if ch.is_alphanumeric() || ch == '#' {
                            entity.push(ch);
                            chars.next();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if found_semicolon {
                    if let Some(decoded) = decode_html_entity(&entity) {
                        result.push(decoded);
                    } else {
                        // Unknown entity, keep as-is
                        result.push('&');
                        result.push_str(&entity);
                        result.push(';');
                    }
                } else {
                    // Not a valid entity, keep the ampersand and collected chars
                    result.push('&');
                    result.push_str(&entity);
                }
            }
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

/// Decodes an HTML entity to its character equivalent.
///
/// Supports named entities (lt, gt, amp, nbsp, quot, apos) and
/// numeric entities (&#123; decimal, &#x1F; hexadecimal).
fn decode_html_entity(entity: &str) -> Option<char> {
    match entity {
        "lt" => Some('<'),
        "gt" => Some('>'),
        "amp" => Some('&'),
        "nbsp" => Some(' '),
        "quot" => Some('"'),
        "apos" => Some('\''),
        s if s.starts_with('#') => {
            let num = s.strip_prefix('#')?;
            let code = if let Some(hex) = num.strip_prefix('x').or_else(|| num.strip_prefix('X')) {
                u32::from_str_radix(hex, 16).ok()?
            } else {
                num.parse().ok()?
            };
            char::from_u32(code)
        }
        _ => None,
    }
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
    fn test_strip_html_entities() {
        // Named entities
        assert_eq!(strip_html_tags("&lt;tag&gt;"), "<tag>");
        assert_eq!(strip_html_tags("Tom &amp; Jerry"), "Tom & Jerry");
        assert_eq!(strip_html_tags("He said &quot;hello&quot;"), "He said \"hello\"");
        assert_eq!(strip_html_tags("It&apos;s fine"), "It's fine");
        assert_eq!(strip_html_tags("Non&nbsp;breaking"), "Non breaking");

        // Numeric entities (decimal)
        assert_eq!(strip_html_tags("&#60;&#62;"), "<>");
        assert_eq!(strip_html_tags("&#65;&#66;&#67;"), "ABC");

        // Numeric entities (hexadecimal)
        assert_eq!(strip_html_tags("&#x3C;&#x3E;"), "<>");
        assert_eq!(strip_html_tags("&#x41;&#x42;&#x43;"), "ABC");
        assert_eq!(strip_html_tags("&#X41;&#X42;"), "AB"); // uppercase X

        // Mixed content with entities
        assert_eq!(
            strip_html_tags("<p>Price: &lt;$100 &amp; discount</p>"),
            "Price: <$100 & discount"
        );
    }

    #[test]
    fn test_strip_html_cdata() {
        assert_eq!(
            strip_html_tags("<![CDATA[Some raw content]]>"),
            "Some raw content"
        );
        assert_eq!(
            strip_html_tags("<div><![CDATA[Inner CDATA]]></div>"),
            "Inner CDATA"
        );
        assert_eq!(
            strip_html_tags("Before <![CDATA[inside]]> after"),
            "Before inside after"
        );
        // CDATA with special characters
        assert_eq!(
            strip_html_tags("<![CDATA[<script>alert('hi')</script>]]>"),
            "<script>alert('hi')</script>"
        );
    }

    #[test]
    fn test_strip_html_edge_cases() {
        // Unclosed entity (should preserve as-is)
        assert_eq!(strip_html_tags("a & b"), "a & b");
        assert_eq!(strip_html_tags("a &unknown; b"), "a &unknown; b");

        // Empty input
        assert_eq!(strip_html_tags(""), "");

        // Only whitespace
        assert_eq!(strip_html_tags("   "), "");

        // Self-closing tags
        assert_eq!(strip_html_tags("<br/><hr/>text"), "text");

        // Complex FHIR narrative
        let fhir_narrative = r#"<div xmlns="http://www.w3.org/1999/xhtml">
            <p>Patient: John Smith &amp; family</p>
            <p>DOB: &lt;1970-01-15&gt;</p>
        </div>"#;
        assert_eq!(
            strip_html_tags(fhir_narrative),
            "Patient: John Smith & family DOB: <1970-01-15>"
        );
    }

    #[test]
    fn test_decode_html_entity() {
        assert_eq!(decode_html_entity("lt"), Some('<'));
        assert_eq!(decode_html_entity("gt"), Some('>'));
        assert_eq!(decode_html_entity("amp"), Some('&'));
        assert_eq!(decode_html_entity("nbsp"), Some(' '));
        assert_eq!(decode_html_entity("quot"), Some('"'));
        assert_eq!(decode_html_entity("apos"), Some('\''));

        // Decimal numeric
        assert_eq!(decode_html_entity("#65"), Some('A'));
        assert_eq!(decode_html_entity("#97"), Some('a'));

        // Hexadecimal numeric
        assert_eq!(decode_html_entity("#x41"), Some('A'));
        assert_eq!(decode_html_entity("#X41"), Some('A'));
        assert_eq!(decode_html_entity("#x1F600"), Some('😀')); // emoji

        // Unknown
        assert_eq!(decode_html_entity("unknown"), None);
        assert_eq!(decode_html_entity("#invalid"), None);
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
