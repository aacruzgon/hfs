//! String parameter handler for Elasticsearch.

use serde_json::{Value, json};

use crate::types::{SearchModifier, SearchParameter};

/// Builds an ES query clause for a string search parameter.
pub fn build_clause(param: &SearchParameter, value: &str) -> Option<Value> {
    let name = &param.name;

    let condition = match param.modifier {
        Some(SearchModifier::Exact) => {
            // Case-sensitive exact match
            json!({
                "term": { "search_params.string.value.keyword": value }
            })
        }
        Some(SearchModifier::Contains) => {
            // Case-insensitive substring match
            json!({
                "wildcard": {
                    "search_params.string.value.lowercase": {
                        "value": format!("*{}*", value.to_lowercase())
                    }
                }
            })
        }
        Some(SearchModifier::Text) => {
            // Full-text match using standard analyzer
            json!({
                "match": {
                    "search_params.string.value": {
                        "query": value,
                        "operator": "and"
                    }
                }
            })
        }
        _ => {
            // Default: case-insensitive prefix match
            // Use match_phrase_prefix for natural prefix matching
            json!({
                "match_phrase_prefix": {
                    "search_params.string.value": {
                        "query": value
                    }
                }
            })
        }
    };

    Some(json!({
        "nested": {
            "path": "search_params.string",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.string.name": name } },
                        condition
                    ]
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SearchParamType, SearchValue};

    fn make_param(name: &str, modifier: Option<SearchModifier>) -> SearchParameter {
        SearchParameter {
            name: name.to_string(),
            param_type: SearchParamType::String,
            modifier,
            values: vec![SearchValue::eq("Smith")],
            chain: vec![],
            components: vec![],
        }
    }

    #[test]
    fn test_default_prefix_match() {
        let param = make_param("family", None);
        let clause = build_clause(&param, "Smith").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("match_phrase_prefix"));
        assert!(s.contains("search_params.string"));
    }

    #[test]
    fn test_exact_match() {
        let param = make_param("family", Some(SearchModifier::Exact));
        let clause = build_clause(&param, "Smith").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("keyword"));
    }

    #[test]
    fn test_contains_match() {
        let param = make_param("family", Some(SearchModifier::Contains));
        let clause = build_clause(&param, "mit").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("wildcard"));
        assert!(s.contains("*mit*"));
    }
}
