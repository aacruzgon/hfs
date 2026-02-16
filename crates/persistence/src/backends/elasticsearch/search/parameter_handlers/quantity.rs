//! Quantity parameter handler for Elasticsearch.

use serde_json::{Value, json};

use crate::types::SearchPrefix;

/// Builds an ES query clause for a quantity search parameter.
///
/// Format: `[prefix]number|system|code` or `[prefix]number|code` or `[prefix]number`
pub fn build_clause(name: &str, value: &str, prefix: SearchPrefix) -> Option<Value> {
    let (num_str, system, code) = parse_quantity_value(value);
    let num: f64 = num_str.parse().ok()?;

    let mut must_conditions = vec![json!({ "term": { "search_params.quantity.name": name } })];

    // Add numeric condition based on prefix
    let num_condition = match prefix {
        SearchPrefix::Eq => {
            let precision = super::number::implicit_range(num_str);
            json!({
                "range": {
                    "search_params.quantity.value": {
                        "gte": num - precision,
                        "lt": num + precision
                    }
                }
            })
        }
        SearchPrefix::Gt => {
            json!({ "range": { "search_params.quantity.value": { "gt": num } } })
        }
        SearchPrefix::Lt => {
            json!({ "range": { "search_params.quantity.value": { "lt": num } } })
        }
        SearchPrefix::Ge => {
            json!({ "range": { "search_params.quantity.value": { "gte": num } } })
        }
        SearchPrefix::Le => {
            json!({ "range": { "search_params.quantity.value": { "lte": num } } })
        }
        SearchPrefix::Ap => {
            let margin = (num * 0.1).abs().max(0.5);
            json!({
                "range": {
                    "search_params.quantity.value": {
                        "gte": num - margin,
                        "lte": num + margin
                    }
                }
            })
        }
        _ => {
            let precision = super::number::implicit_range(num_str);
            json!({
                "range": {
                    "search_params.quantity.value": {
                        "gte": num - precision,
                        "lt": num + precision
                    }
                }
            })
        }
    };
    must_conditions.push(num_condition);

    // Add system/code conditions if specified
    if let Some(sys) = system {
        must_conditions.push(json!({ "term": { "search_params.quantity.system": sys } }));
    }
    if let Some(c) = code {
        must_conditions.push(json!({ "term": { "search_params.quantity.code": c } }));
    }

    Some(json!({
        "nested": {
            "path": "search_params.quantity",
            "query": {
                "bool": {
                    "must": must_conditions
                }
            }
        }
    }))
}

/// Parses a quantity value string into (number, system, code).
///
/// Formats:
/// - `5.4` -> ("5.4", None, None)
/// - `5.4|mg` -> ("5.4", None, Some("mg"))
/// - `5.4|http://unitsofmeasure.org|mg` -> ("5.4", Some("http://..."), Some("mg"))
fn parse_quantity_value(value: &str) -> (&str, Option<&str>, Option<&str>) {
    let parts: Vec<&str> = value.splitn(3, '|').collect();
    match parts.len() {
        1 => (parts[0], None, None),
        2 => (parts[0], None, Some(parts[1])),
        3 => {
            let system = if parts[1].is_empty() {
                None
            } else {
                Some(parts[1])
            };
            let code = if parts[2].is_empty() {
                None
            } else {
                Some(parts[2])
            };
            (parts[0], system, code)
        }
        _ => (value, None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quantity_value() {
        let (n, s, c) = parse_quantity_value("5.4");
        assert_eq!(n, "5.4");
        assert!(s.is_none());
        assert!(c.is_none());

        let (n, s, c) = parse_quantity_value("5.4|http://unitsofmeasure.org|mg");
        assert_eq!(n, "5.4");
        assert_eq!(s, Some("http://unitsofmeasure.org"));
        assert_eq!(c, Some("mg"));
    }

    #[test]
    fn test_quantity_clause() {
        let clause = build_clause(
            "value-quantity",
            "120|http://unitsofmeasure.org|mm[Hg]",
            SearchPrefix::Eq,
        )
        .unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("search_params.quantity"));
        assert!(s.contains("mm[Hg]"));
    }
}
