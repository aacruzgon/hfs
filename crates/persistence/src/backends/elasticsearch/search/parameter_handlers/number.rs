//! Number parameter handler for Elasticsearch.

use serde_json::{Value, json};

use crate::types::SearchPrefix;

/// Builds an ES query clause for a number search parameter.
pub fn build_clause(name: &str, value: &str, prefix: SearchPrefix) -> Option<Value> {
    let num: f64 = value.parse().ok()?;
    let implicit_precision = implicit_range(value);

    let range_condition = match prefix {
        SearchPrefix::Eq => {
            // Implicit precision: 100 matches [99.5, 100.5), 100.0 matches [99.95, 100.05)
            json!({
                "range": {
                    "search_params.number.value": {
                        "gte": num - implicit_precision,
                        "lt": num + implicit_precision
                    }
                }
            })
        }
        SearchPrefix::Ne => {
            return Some(json!({
                "nested": {
                    "path": "search_params.number",
                    "query": {
                        "bool": {
                            "must": [
                                { "term": { "search_params.number.name": name } }
                            ],
                            "must_not": [
                                {
                                    "range": {
                                        "search_params.number.value": {
                                            "gte": num - implicit_precision,
                                            "lt": num + implicit_precision
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }));
        }
        SearchPrefix::Gt => {
            json!({
                "range": { "search_params.number.value": { "gt": num } }
            })
        }
        SearchPrefix::Lt => {
            json!({
                "range": { "search_params.number.value": { "lt": num } }
            })
        }
        SearchPrefix::Ge => {
            json!({
                "range": { "search_params.number.value": { "gte": num } }
            })
        }
        SearchPrefix::Le => {
            json!({
                "range": { "search_params.number.value": { "lte": num } }
            })
        }
        SearchPrefix::Ap => {
            // Approximately ±10%
            let margin = (num * 0.1).abs().max(0.5);
            json!({
                "range": {
                    "search_params.number.value": {
                        "gte": num - margin,
                        "lte": num + margin
                    }
                }
            })
        }
        _ => {
            json!({
                "range": {
                    "search_params.number.value": {
                        "gte": num - implicit_precision,
                        "lt": num + implicit_precision
                    }
                }
            })
        }
    };

    Some(json!({
        "nested": {
            "path": "search_params.number",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.number.name": name } },
                        range_condition
                    ]
                }
            }
        }
    }))
}

/// Determines the implicit precision based on string representation.
///
/// "100" has implicit precision of 0.5 (integer)
/// "100.0" has implicit precision of 0.05
/// "100.00" has implicit precision of 0.005
pub(crate) fn implicit_range(value: &str) -> f64 {
    if let Some(dot_pos) = value.find('.') {
        let decimal_places = value.len() - dot_pos - 1;
        0.5 * 10.0_f64.powi(-(decimal_places as i32))
    } else {
        0.5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_implicit_precision() {
        assert!((implicit_range("100") - 0.5).abs() < f64::EPSILON);
        assert!((implicit_range("100.0") - 0.05).abs() < f64::EPSILON);
        assert!((implicit_range("100.00") - 0.005).abs() < f64::EPSILON);
    }

    #[test]
    fn test_eq_range() {
        let clause = build_clause("length", "100", SearchPrefix::Eq).unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("99.5"));
        assert!(s.contains("100.5"));
    }

    #[test]
    fn test_gt() {
        let clause = build_clause("length", "100", SearchPrefix::Gt).unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("\"gt\":100"));
    }
}
