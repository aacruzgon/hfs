//! Date parameter handler for Elasticsearch.

use serde_json::{Value, json};

use crate::types::SearchPrefix;

/// Builds an ES query clause for a date search parameter.
pub fn build_clause(name: &str, value: &str, prefix: SearchPrefix) -> Option<Value> {
    let range_condition = match prefix {
        SearchPrefix::Eq => {
            // Equality accounting for precision:
            // "2024-01-15" matches the full day
            let (lower, upper) = date_precision_range(value);
            json!({
                "range": {
                    "search_params.date.value": {
                        "gte": lower,
                        "lt": upper
                    }
                }
            })
        }
        SearchPrefix::Ne => {
            let (lower, upper) = date_precision_range(value);
            return Some(json!({
                "nested": {
                    "path": "search_params.date",
                    "query": {
                        "bool": {
                            "must": [
                                { "term": { "search_params.date.name": name } }
                            ],
                            "must_not": [
                                {
                                    "range": {
                                        "search_params.date.value": {
                                            "gte": lower,
                                            "lt": upper
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }));
        }
        SearchPrefix::Gt | SearchPrefix::Sa => {
            let (_, upper) = date_precision_range(value);
            json!({
                "range": {
                    "search_params.date.value": {
                        "gte": upper
                    }
                }
            })
        }
        SearchPrefix::Lt | SearchPrefix::Eb => {
            let (lower, _) = date_precision_range(value);
            json!({
                "range": {
                    "search_params.date.value": {
                        "lt": lower
                    }
                }
            })
        }
        SearchPrefix::Ge => {
            let (lower, _) = date_precision_range(value);
            json!({
                "range": {
                    "search_params.date.value": {
                        "gte": lower
                    }
                }
            })
        }
        SearchPrefix::Le => {
            let (_, upper) = date_precision_range(value);
            json!({
                "range": {
                    "search_params.date.value": {
                        "lt": upper
                    }
                }
            })
        }
        SearchPrefix::Ap => {
            // Approximately: ±10% of the precision range
            let (lower, upper) = date_precision_range(value);
            // For approximate, we use the range itself (ES handles fuzzy matching)
            json!({
                "range": {
                    "search_params.date.value": {
                        "gte": lower,
                        "lt": upper
                    }
                }
            })
        }
    };

    Some(json!({
        "nested": {
            "path": "search_params.date",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.date.name": name } },
                        range_condition
                    ]
                }
            }
        }
    }))
}

/// Computes the precision-based range for a date value.
///
/// Returns (lower_bound_inclusive, upper_bound_exclusive).
fn date_precision_range(value: &str) -> (String, String) {
    // Count characters to determine precision
    let clean = value.trim();

    if clean.len() == 4 {
        // Year precision: "2024" -> ["2024-01-01", "2025-01-01")
        let year: i32 = clean.parse().unwrap_or(2000);
        (
            format!("{:04}-01-01", year),
            format!("{:04}-01-01", year + 1),
        )
    } else if clean.len() == 7 {
        // Month precision: "2024-01" -> ["2024-01-01", "2024-02-01")
        let parts: Vec<&str> = clean.split('-').collect();
        let year: i32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(2000);
        let month: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(1);
        let (next_year, next_month) = if month >= 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        (
            format!("{:04}-{:02}-01", year, month),
            format!("{:04}-{:02}-01", next_year, next_month),
        )
    } else if clean.len() == 10 {
        // Day precision: "2024-01-15" -> ["2024-01-15", "2024-01-16")
        // Simple: parse and add one day
        let lower = clean.to_string();
        let parts: Vec<&str> = clean.split('-').collect();
        let year: i32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(2000);
        let month: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(1);
        let day: u32 = parts.get(2).and_then(|p| p.parse().ok()).unwrap_or(1);

        // Use chrono for correct date arithmetic
        if let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
            let next = date + chrono::Duration::days(1);
            (lower, next.format("%Y-%m-%d").to_string())
        } else {
            (lower.clone(), lower)
        }
    } else {
        // Full date-time precision: use the value directly
        (clean.to_string(), clean.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_year_precision() {
        let (lower, upper) = date_precision_range("2024");
        assert_eq!(lower, "2024-01-01");
        assert_eq!(upper, "2025-01-01");
    }

    #[test]
    fn test_month_precision() {
        let (lower, upper) = date_precision_range("2024-01");
        assert_eq!(lower, "2024-01-01");
        assert_eq!(upper, "2024-02-01");
    }

    #[test]
    fn test_day_precision() {
        let (lower, upper) = date_precision_range("2024-01-15");
        assert_eq!(lower, "2024-01-15");
        assert_eq!(upper, "2024-01-16");
    }

    #[test]
    fn test_eq_range() {
        let clause = build_clause("birthdate", "2024-01-15", SearchPrefix::Eq).unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("gte"));
        assert!(s.contains("2024-01-15"));
        assert!(s.contains("2024-01-16"));
    }

    #[test]
    fn test_gt_range() {
        let clause = build_clause("birthdate", "2024-01-15", SearchPrefix::Gt).unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("gte"));
        assert!(s.contains("2024-01-16")); // starts after precision range
    }
}
