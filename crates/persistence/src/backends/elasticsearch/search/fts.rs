//! Full-text search query builders for Elasticsearch.
//!
//! Handles `_text` (narrative search) and `_content` (full resource search).

use serde_json::{Value, json};

use crate::types::SearchParameter;

/// Builds an ES query clause for the `_text` parameter.
///
/// Searches the `narrative_text` field extracted from `resource.text.div`.
pub fn build_text_clause(param: &SearchParameter) -> Option<Value> {
    let values: Vec<&str> = param.values.iter().map(|v| v.value.as_str()).collect();
    if values.is_empty() {
        return None;
    }

    let text = values.join(" ");

    Some(json!({
        "match": {
            "narrative_text": {
                "query": text,
                "operator": "and"
            }
        }
    }))
}

/// Builds an ES query clause for the `_content` parameter.
///
/// Searches the `content_text` field which contains all string values from the resource.
pub fn build_content_clause(param: &SearchParameter) -> Option<Value> {
    let values: Vec<&str> = param.values.iter().map(|v| v.value.as_str()).collect();
    if values.is_empty() {
        return None;
    }

    let text = values.join(" ");

    Some(json!({
        "match": {
            "content_text": {
                "query": text,
                "operator": "and"
            }
        }
    }))
}

/// Builds a full-text search on narrative text for the TextSearchProvider.
pub fn build_narrative_query(text: &str) -> Value {
    json!({
        "match": {
            "narrative_text": {
                "query": text,
                "operator": "and"
            }
        }
    })
}

/// Builds a full-text search on content text for the TextSearchProvider.
pub fn build_content_query(content: &str) -> Value {
    json!({
        "match": {
            "content_text": {
                "query": content,
                "operator": "and"
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SearchParamType, SearchValue};

    #[test]
    fn test_text_clause() {
        let param = SearchParameter {
            name: "_text".to_string(),
            param_type: SearchParamType::Special,
            modifier: None,
            values: vec![SearchValue::eq("headache fever")],
            chain: vec![],
            components: vec![],
        };
        let clause = build_text_clause(&param).unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("narrative_text"));
        assert!(s.contains("headache fever"));
    }

    #[test]
    fn test_content_clause() {
        let param = SearchParameter {
            name: "_content".to_string(),
            param_type: SearchParamType::Special,
            modifier: None,
            values: vec![SearchValue::eq("aspirin")],
            chain: vec![],
            components: vec![],
        };
        let clause = build_content_clause(&param).unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("content_text"));
        assert!(s.contains("aspirin"));
    }
}
