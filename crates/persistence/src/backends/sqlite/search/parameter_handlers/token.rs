//! Token parameter SQL handler.

use crate::types::{SearchModifier, SearchValue};

use super::super::query_builder::{SqlFragment, SqlParam};

/// Handles token parameter SQL generation.
pub struct TokenHandler;

impl TokenHandler {
    /// Builds SQL for a token parameter value.
    ///
    /// Token values can be:
    /// - `code` - matches any system
    /// - `system|code` - matches specific system and code
    /// - `|code` - matches code with no system (empty or null)
    /// - `system|` - matches any code in system
    pub fn build_sql(
        value: &SearchValue,
        modifier: Option<&SearchModifier>,
        param_offset: usize,
    ) -> SqlFragment {
        let param_num = param_offset + 1;

        // Handle :not modifier
        if matches!(modifier, Some(SearchModifier::Not)) {
            let inner = Self::build_sql(
                value,
                None,
                param_offset,
            );
            return SqlFragment::with_params(
                format!("NOT ({})", inner.sql),
                inner.params,
            );
        }

        // Handle :text modifier
        if matches!(modifier, Some(SearchModifier::Text)) {
            // Full-text search on code display text
            return SqlFragment::with_params(
                format!("value_token_code COLLATE NOCASE LIKE '%' || ?{} || '%'", param_num),
                vec![SqlParam::string(&value.value.to_lowercase())],
            );
        }

        // Handle :code-only modifier
        if matches!(modifier, Some(SearchModifier::CodeOnly)) {
            return SqlFragment::with_params(
                format!("value_token_code = ?{}", param_num),
                vec![SqlParam::string(&value.value)],
            );
        }

        // Parse the token value
        let token_value = &value.value;

        if let Some(pipe_pos) = token_value.find('|') {
            let system = &token_value[..pipe_pos];
            let code = &token_value[pipe_pos + 1..];

            if system.is_empty() {
                // |code - match code with no system
                SqlFragment::with_params(
                    format!(
                        "(value_token_system IS NULL OR value_token_system = '') AND value_token_code = ?{}",
                        param_num
                    ),
                    vec![SqlParam::string(code)],
                )
            } else if code.is_empty() {
                // system| - match any code in system
                SqlFragment::with_params(
                    format!("value_token_system = ?{}", param_num),
                    vec![SqlParam::string(system)],
                )
            } else {
                // system|code - exact match
                SqlFragment::with_params(
                    format!(
                        "value_token_system = ?{} AND value_token_code = ?{}",
                        param_num,
                        param_num + 1
                    ),
                    vec![SqlParam::string(system), SqlParam::string(code)],
                )
            }
        } else {
            // code only - match any system
            SqlFragment::with_params(
                format!("value_token_code = ?{}", param_num),
                vec![SqlParam::string(token_value)],
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SearchPrefix;

    #[test]
    fn test_token_code_only() {
        let value = SearchValue::new(SearchPrefix::Eq, "12345");
        let frag = TokenHandler::build_sql(&value, None, 0);

        assert!(frag.sql.contains("value_token_code = ?1"));
        assert_eq!(frag.params.len(), 1);
    }

    #[test]
    fn test_token_system_and_code() {
        let value = SearchValue::new(SearchPrefix::Eq, "http://loinc.org|12345-6");
        let frag = TokenHandler::build_sql(&value, None, 0);

        assert!(frag.sql.contains("value_token_system = ?1"));
        assert!(frag.sql.contains("value_token_code = ?2"));
        assert_eq!(frag.params.len(), 2);
    }

    #[test]
    fn test_token_no_system() {
        let value = SearchValue::new(SearchPrefix::Eq, "|12345");
        let frag = TokenHandler::build_sql(&value, None, 0);

        assert!(frag.sql.contains("IS NULL OR"));
        assert!(frag.sql.contains("value_token_code = ?1"));
    }

    #[test]
    fn test_token_system_only() {
        let value = SearchValue::new(SearchPrefix::Eq, "http://loinc.org|");
        let frag = TokenHandler::build_sql(&value, None, 0);

        assert!(frag.sql.contains("value_token_system = ?1"));
        assert!(!frag.sql.contains("value_token_code"));
    }

    #[test]
    fn test_token_not_modifier() {
        let value = SearchValue::new(SearchPrefix::Eq, "12345");
        let frag = TokenHandler::build_sql(&value, Some(&SearchModifier::Not), 0);

        assert!(frag.sql.starts_with("NOT ("));
    }
}
