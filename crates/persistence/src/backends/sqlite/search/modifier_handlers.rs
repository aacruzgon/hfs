//! Cross-cutting modifier handlers.
//!
//! Handles modifiers that apply across multiple parameter types.

use crate::types::{SearchModifier, SearchParameter};

use super::query_builder::SqlFragment;

/// Handles the :missing modifier for any parameter type.
pub fn build_missing_condition(param: &SearchParameter, is_missing: bool) -> SqlFragment {
    if is_missing {
        // Missing = true: resources with NO index entry for this param
        SqlFragment::new(format!(
            "resource_id NOT IN (SELECT resource_id FROM search_index WHERE tenant_id = ?1 AND resource_type = ?2 AND param_name = '{}')",
            param.name
        ))
    } else {
        // Missing = false: resources WITH an index entry for this param
        SqlFragment::new(format!(
            "resource_id IN (SELECT resource_id FROM search_index WHERE tenant_id = ?1 AND resource_type = ?2 AND param_name = '{}')",
            param.name
        ))
    }
}

/// Checks if a modifier is the :missing modifier.
pub fn is_missing_modifier(modifier: &Option<SearchModifier>) -> bool {
    matches!(modifier, Some(SearchModifier::Missing))
}

/// Extracts the boolean value for :missing modifier.
pub fn get_missing_value(value: &str) -> bool {
    value.to_lowercase() == "true"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SearchParamType, SearchValue};

    #[test]
    fn test_missing_true() {
        let param = SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: Some(SearchModifier::Missing),
            values: vec![SearchValue::eq("true")],
            chain: vec![],
        };

        let frag = build_missing_condition(&param, true);

        assert!(frag.sql.contains("NOT IN"));
        assert!(frag.sql.contains("param_name = 'name'"));
    }

    #[test]
    fn test_missing_false() {
        let param = SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: Some(SearchModifier::Missing),
            values: vec![SearchValue::eq("false")],
            chain: vec![],
        };

        let frag = build_missing_condition(&param, false);

        assert!(!frag.sql.contains("NOT IN"));
        assert!(frag.sql.contains("resource_id IN"));
    }

    #[test]
    fn test_is_missing_modifier() {
        assert!(is_missing_modifier(&Some(SearchModifier::Missing)));
        assert!(!is_missing_modifier(&Some(SearchModifier::Exact)));
        assert!(!is_missing_modifier(&None));
    }

    #[test]
    fn test_get_missing_value() {
        assert!(get_missing_value("true"));
        assert!(get_missing_value("TRUE"));
        assert!(get_missing_value("True"));
        assert!(!get_missing_value("false"));
        assert!(!get_missing_value("False"));
        assert!(!get_missing_value("anything-else"));
    }
}
