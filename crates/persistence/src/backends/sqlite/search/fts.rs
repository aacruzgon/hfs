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
    result.split_whitespace().collect::<Vec<_>>().join(" ")
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
    ///
    /// Indexes both `value_string` and `value_token_display` columns
    /// to support both regular string search and :text-advanced modifier
    /// on token display text.
    pub fn create_triggers_sql() -> &'static str {
        r#"
        -- Trigger for INSERT (indexes value_string and value_token_display)
        CREATE TRIGGER IF NOT EXISTS search_index_fts_insert AFTER INSERT ON search_index
        WHEN new.value_string IS NOT NULL OR new.value_token_display IS NOT NULL
        BEGIN
            INSERT INTO search_index_fts(rowid, text_content)
            VALUES (new.rowid, COALESCE(new.value_string, '') || ' ' || COALESCE(new.value_token_display, ''));
        END;

        -- Trigger for DELETE
        CREATE TRIGGER IF NOT EXISTS search_index_fts_delete AFTER DELETE ON search_index
        WHEN old.value_string IS NOT NULL OR old.value_token_display IS NOT NULL
        BEGIN
            INSERT INTO search_index_fts(search_index_fts, rowid, text_content)
            VALUES ('delete', old.rowid, COALESCE(old.value_string, '') || ' ' || COALESCE(old.value_token_display, ''));
        END;

        -- Trigger for UPDATE
        CREATE TRIGGER IF NOT EXISTS search_index_fts_update AFTER UPDATE ON search_index
        WHEN old.value_string IS NOT NULL OR new.value_string IS NOT NULL
             OR old.value_token_display IS NOT NULL OR new.value_token_display IS NOT NULL
        BEGIN
            INSERT INTO search_index_fts(search_index_fts, rowid, text_content)
            VALUES ('delete', old.rowid, COALESCE(old.value_string, '') || ' ' || COALESCE(old.value_token_display, ''));
            INSERT INTO search_index_fts(rowid, text_content)
            VALUES (new.rowid, COALESCE(new.value_string, '') || ' ' || COALESCE(new.value_token_display, ''));
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

    /// Builds an advanced FTS5 query with boolean operator support.
    ///
    /// This supports the `:text-advanced` modifier from FHIR v6.0.0.
    ///
    /// Query syntax:
    /// - `term1 term2` → implicit AND
    /// - `term1 OR term2` → either term
    /// - `"exact phrase"` → phrase match
    /// - `term*` → prefix match
    /// - `-term` or `NOT term` → exclude term
    /// - `term1 NEAR term2` → proximity match (within 10 words)
    /// - `term1 NEAR/5 term2` → proximity match within 5 words
    pub fn build_advanced_query(query: &str, param_num: usize) -> SqlFragment {
        let fts_query = Self::parse_advanced_query(query);
        SqlFragment::with_params(
            format!(
                "rowid IN (SELECT rowid FROM {} WHERE {} MATCH ?{})",
                Self::FTS_TABLE_NAME,
                Self::FTS_TABLE_NAME,
                param_num
            ),
            vec![SqlParam::string(fts_query)],
        )
    }

    /// Parses a user-friendly query into FTS5 syntax.
    ///
    /// Transforms user input into valid FTS5 query syntax:
    /// - Preserves quoted phrases
    /// - Handles OR operator (passed through to FTS5)
    /// - Handles NOT / - prefix (converts to NOT)
    /// - Handles NEAR operator (passed through to FTS5)
    /// - Handles prefix wildcard (term* stays as-is)
    /// - Escapes special characters in regular terms
    /// - Joins remaining terms with implicit AND
    pub fn parse_advanced_query(query: &str) -> String {
        let tokens = Self::tokenize_advanced_query(query);
        Self::tokens_to_fts5(&tokens)
    }

    /// Tokenizes an advanced query, preserving quoted phrases and operators.
    fn tokenize_advanced_query(query: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let chars = query.chars().peekable();
        let mut current = String::new();
        let mut in_quote = false;

        for c in chars {
            match c {
                '"' => {
                    if in_quote {
                        // End of quoted phrase
                        if !current.is_empty() {
                            tokens.push(format!("\"{}\"", current));
                            current.clear();
                        }
                        in_quote = false;
                    } else {
                        // Start of quoted phrase - save current token first
                        if !current.is_empty() {
                            tokens.push(current.clone());
                            current.clear();
                        }
                        in_quote = true;
                    }
                }
                ' ' | '\t' | '\n' if !in_quote => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                }
                _ => {
                    current.push(c);
                }
            }
        }

        // Handle any remaining content
        if !current.is_empty() {
            if in_quote {
                // Unclosed quote - treat as phrase anyway
                tokens.push(format!("\"{}\"", current));
            } else {
                tokens.push(current);
            }
        }

        tokens
    }

    /// Converts parsed tokens to FTS5 query syntax.
    fn tokens_to_fts5(tokens: &[String]) -> String {
        let mut result = Vec::new();
        let mut i = 0;

        while i < tokens.len() {
            let token = &tokens[i];
            let upper = token.to_uppercase();

            // Check for operators
            if upper == "OR" || upper == "AND" {
                // Keep operators as-is
                result.push(upper);
            } else if upper == "NOT" {
                // NOT operator
                result.push("NOT".to_string());
            } else if upper == "NEAR" || upper.starts_with("NEAR/") {
                // NEAR operator (with optional distance)
                result.push(upper);
            } else if token.starts_with('-') && token.len() > 1 {
                // -term becomes NOT term
                result.push("NOT".to_string());
                let term = &token[1..];
                result.push(Self::escape_term_for_fts5(term));
            } else if token.starts_with('"') {
                // Quoted phrase - already formatted, just escape inner content
                let inner = token.trim_matches('"');
                result.push(format!("\"{}\"", Self::escape_fts_query(inner)));
            } else if token.ends_with('*') {
                // Prefix search - escape the base term and add *
                let base = &token[..token.len() - 1];
                if !base.is_empty() {
                    result.push(format!("{}*", Self::escape_term_for_fts5(base)));
                }
            } else {
                // Regular term
                result.push(Self::escape_term_for_fts5(token));
            }
            i += 1;
        }

        // Join tokens with implicit AND between adjacent non-operator terms
        Self::join_with_implicit_and(&result)
    }

    /// Escapes a single term for FTS5 query.
    fn escape_term_for_fts5(term: &str) -> String {
        Self::escape_fts_query(term)
    }

    /// Joins terms with implicit AND between adjacent non-operator terms.
    ///
    /// FTS5 requires explicit AND between terms for conjunction.
    /// This inserts AND between adjacent terms that are not already
    /// separated by an operator (OR, AND, NOT, NEAR).
    fn join_with_implicit_and(terms: &[String]) -> String {
        if terms.is_empty() {
            return String::new();
        }

        let mut result = Vec::new();
        let operators = ["OR", "AND", "NOT"];

        for (i, term) in terms.iter().enumerate() {
            result.push(term.clone());

            // Check if we need to insert AND before the next term
            if i < terms.len() - 1 {
                let next = &terms[i + 1];
                let current_is_op = operators.contains(&term.to_uppercase().as_str())
                    || term.to_uppercase().starts_with("NEAR");
                let next_is_op = operators.contains(&next.to_uppercase().as_str())
                    || next.to_uppercase().starts_with("NEAR");

                // Insert AND if current is not an operator and next is not an operator or NOT
                if !current_is_op && !next_is_op && next.to_uppercase() != "NOT" {
                    result.push("AND".to_string());
                }
            }
        }

        result.join(" ")
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

    // ============================================================================
    // Advanced Query Parser Tests (:text-advanced modifier)
    // ============================================================================

    #[test]
    fn test_parse_advanced_query_simple() {
        assert_eq!(Fts5Search::parse_advanced_query("headache"), "headache");
    }

    #[test]
    fn test_parse_advanced_query_multiple_terms() {
        // Multiple terms should be joined with AND
        assert_eq!(
            Fts5Search::parse_advanced_query("heart attack"),
            "heart AND attack"
        );
    }

    #[test]
    fn test_parse_advanced_query_phrase() {
        assert_eq!(
            Fts5Search::parse_advanced_query("\"heart attack\""),
            "\"heart attack\""
        );
    }

    #[test]
    fn test_parse_advanced_query_or() {
        assert_eq!(
            Fts5Search::parse_advanced_query("headache OR migraine"),
            "headache OR migraine"
        );
    }

    #[test]
    fn test_parse_advanced_query_prefix() {
        assert_eq!(Fts5Search::parse_advanced_query("cardio*"), "cardio*");
    }

    #[test]
    fn test_parse_advanced_query_not_minus() {
        // -term should become NOT term
        assert_eq!(Fts5Search::parse_advanced_query("-surgery"), "NOT surgery");
    }

    #[test]
    fn test_parse_advanced_query_not_keyword() {
        // NOT term should stay as NOT term
        assert_eq!(
            Fts5Search::parse_advanced_query("NOT surgery"),
            "NOT surgery"
        );
    }

    #[test]
    fn test_parse_advanced_query_near() {
        assert_eq!(
            Fts5Search::parse_advanced_query("heart NEAR attack"),
            "heart NEAR attack"
        );
    }

    #[test]
    fn test_parse_advanced_query_near_with_distance() {
        assert_eq!(
            Fts5Search::parse_advanced_query("heart NEAR/5 attack"),
            "heart NEAR/5 attack"
        );
    }

    #[test]
    fn test_parse_advanced_query_complex() {
        // Complex query: heart OR cardiac with exclusion
        assert_eq!(
            Fts5Search::parse_advanced_query("heart OR cardiac -surgery"),
            "heart OR cardiac NOT surgery"
        );
    }

    #[test]
    fn test_parse_advanced_query_mixed() {
        // Mix of phrase, prefix, and boolean
        assert_eq!(
            Fts5Search::parse_advanced_query("\"chest pain\" cardio* OR thoracic"),
            "\"chest pain\" AND cardio* OR thoracic"
        );
    }

    #[test]
    fn test_parse_advanced_query_case_insensitive_operators() {
        // Operators should work case-insensitively
        assert_eq!(
            Fts5Search::parse_advanced_query("heart or cardiac"),
            "heart OR cardiac"
        );
        assert_eq!(
            Fts5Search::parse_advanced_query("pain not chronic"),
            "pain NOT chronic"
        );
    }

    #[test]
    fn test_build_advanced_query() {
        let frag = Fts5Search::build_advanced_query("heart OR cardiac -surgery", 1);

        assert!(frag.sql.contains("search_index_fts"));
        assert!(frag.sql.contains("MATCH"));
        assert_eq!(frag.params.len(), 1);

        // The query should be properly formatted
        if let SqlParam::String(s) = &frag.params[0] {
            assert!(s.contains("OR"));
            assert!(s.contains("NOT"));
        }
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
        assert_eq!(
            strip_html_tags("He said &quot;hello&quot;"),
            "He said \"hello\""
        );
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
