//! Composite parameter handler for Elasticsearch.

use serde_json::{Value, json};

use crate::types::SearchParameter;

/// Builds an ES query clause for a composite search parameter.
///
/// Composite parameters combine two or more sub-parameters that must
/// match on the same logical grouping. The value format is `value1$value2`.
pub fn build_clause(param: &SearchParameter, value: &str) -> Option<Value> {
    let name = &param.name;
    let component_values: Vec<&str> = value.split('$').collect();

    if component_values.is_empty() {
        return None;
    }

    // Build a nested query that matches on the composite group
    // Each component must match within the same group_id
    let must_conditions = vec![json!({ "term": { "search_params.composite.name": name } })];

    // For now, composite matching is simplified:
    // We match the composite name, and the actual component matching
    // is handled by the individual parameter type handlers in the query
    Some(json!({
        "nested": {
            "path": "search_params.composite",
            "query": {
                "bool": {
                    "must": must_conditions
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SearchParamType, SearchValue};

    #[test]
    fn test_composite_clause() {
        let param = SearchParameter {
            name: "code-value-quantity".to_string(),
            param_type: SearchParamType::Composite,
            modifier: None,
            values: vec![SearchValue::eq("8867-4$120")],
            chain: vec![],
            components: vec![],
        };
        let clause = build_clause(&param, "8867-4$120");
        assert!(clause.is_some());
    }
}
