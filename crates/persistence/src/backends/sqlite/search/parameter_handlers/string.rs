//! String parameter SQL handler.

use crate::types::{SearchModifier, SearchValue};

use super::super::query_builder::{SqlFragment, SqlParam};

/// Handles string parameter SQL generation.
pub struct StringHandler;

impl StringHandler {
    /// Builds SQL for a string parameter value.
    ///
    /// Default behavior is case-insensitive prefix match.
    pub fn build_sql(
        value: &SearchValue,
        modifier: Option<&SearchModifier>,
        param_offset: usize,
    ) -> SqlFragment {
        let param_num = param_offset + 1;

        match modifier {
            Some(SearchModifier::Exact) => {
                // Exact match (case-sensitive)
                SqlFragment::with_params(
                    format!("value_string = ?{}", param_num),
                    vec![SqlParam::string(&value.value)],
                )
            }
            Some(SearchModifier::Contains) => {
                // Contains (case-insensitive)
                SqlFragment::with_params(
                    format!(
                        "value_string COLLATE NOCASE LIKE '%' || ?{} || '%'",
                        param_num
                    ),
                    vec![SqlParam::string(value.value.to_lowercase())],
                )
            }
            Some(SearchModifier::Text) => {
                // Full-text search - use FTS5 if available, otherwise contains
                SqlFragment::with_params(
                    format!(
                        "value_string COLLATE NOCASE LIKE '%' || ?{} || '%'",
                        param_num
                    ),
                    vec![SqlParam::string(value.value.to_lowercase())],
                )
            }
            _ => {
                // Default: case-insensitive prefix match
                SqlFragment::with_params(
                    format!("value_string COLLATE NOCASE LIKE ?{} || '%'", param_num),
                    vec![SqlParam::string(value.value.to_lowercase())],
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SearchPrefix;

    #[test]
    fn test_string_default() {
        let value = SearchValue::new(SearchPrefix::Eq, "Smith");
        let frag = StringHandler::build_sql(&value, None, 0);

        assert!(frag.sql.contains("COLLATE NOCASE LIKE"));
        assert!(frag.sql.contains("|| '%'"));
        assert_eq!(frag.params.len(), 1);
    }

    #[test]
    fn test_string_exact() {
        let value = SearchValue::new(SearchPrefix::Eq, "Smith");
        let frag = StringHandler::build_sql(&value, Some(&SearchModifier::Exact), 0);

        assert!(frag.sql.contains("= ?1"));
        assert!(!frag.sql.contains("LIKE"));
    }

    #[test]
    fn test_string_contains() {
        let value = SearchValue::new(SearchPrefix::Eq, "smith");
        let frag = StringHandler::build_sql(&value, Some(&SearchModifier::Contains), 0);

        assert!(frag.sql.contains("LIKE '%' ||"));
        assert!(frag.sql.contains("|| '%'"));
    }
}
