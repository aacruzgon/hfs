//! Quantity parameter SQL handler.

use crate::types::{SearchPrefix, SearchValue};

use super::super::query_builder::{SqlFragment, SqlParam};

/// Handles quantity parameter SQL generation.
pub struct QuantityHandler;

impl QuantityHandler {
    /// Builds SQL for a quantity parameter value.
    ///
    /// Quantity values can be:
    /// - `value` - matches any unit
    /// - `value|unit` - matches specific unit (code)
    /// - `value|system|code` - matches specific system and code
    pub fn build_sql(value: &SearchValue, param_offset: usize) -> SqlFragment {
        let param_num = param_offset + 1;

        // Parse the quantity value: [prefix]number|system|code or [prefix]number|code or [prefix]number
        let quantity_str = &value.value;
        let parts: Vec<&str> = quantity_str.split('|').collect();

        let (num_value, system, code) = match parts.len() {
            1 => {
                // Just a number
                let num: f64 = match parts[0].parse() {
                    Ok(v) => v,
                    Err(_) => return SqlFragment::new("1 = 0"),
                };
                (num, None, None)
            }
            2 => {
                // number|code
                let num: f64 = match parts[0].parse() {
                    Ok(v) => v,
                    Err(_) => return SqlFragment::new("1 = 0"),
                };
                (num, None, Some(parts[1]))
            }
            3 => {
                // number|system|code
                let num: f64 = match parts[0].parse() {
                    Ok(v) => v,
                    Err(_) => return SqlFragment::new("1 = 0"),
                };
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
                (num, system, code)
            }
            _ => return SqlFragment::new("1 = 0"),
        };

        // Build the numeric comparison
        let num_condition = Self::build_numeric_condition(num_value, value.prefix, param_num);

        // Add unit conditions if specified
        if system.is_some() || code.is_some() {
            let mut conditions = vec![num_condition.sql];
            let mut params = num_condition.params;
            let mut next_param = param_num + params.len();

            if let Some(sys) = system {
                conditions.push(format!("value_quantity_system = ?{}", next_param));
                params.push(SqlParam::string(sys));
                next_param += 1;
            }

            if let Some(c) = code {
                conditions.push(format!("value_quantity_unit = ?{}", next_param));
                params.push(SqlParam::string(c));
            }

            SqlFragment::with_params(conditions.join(" AND "), params)
        } else {
            num_condition
        }
    }

    /// Builds the numeric comparison part of the condition.
    fn build_numeric_condition(value: f64, prefix: SearchPrefix, param_num: usize) -> SqlFragment {
        match prefix {
            SearchPrefix::Eq => {
                // Implicit precision range
                let precision = Self::get_implicit_precision(value);
                let half = precision / 2.0;
                SqlFragment::with_params(
                    format!(
                        "value_quantity_value >= ?{} AND value_quantity_value < ?{}",
                        param_num,
                        param_num + 1
                    ),
                    vec![SqlParam::float(value - half), SqlParam::float(value + half)],
                )
            }
            SearchPrefix::Ne => {
                let precision = Self::get_implicit_precision(value);
                let half = precision / 2.0;
                SqlFragment::with_params(
                    format!(
                        "(value_quantity_value < ?{} OR value_quantity_value >= ?{})",
                        param_num,
                        param_num + 1
                    ),
                    vec![SqlParam::float(value - half), SqlParam::float(value + half)],
                )
            }
            SearchPrefix::Gt | SearchPrefix::Sa => SqlFragment::with_params(
                format!("value_quantity_value > ?{}", param_num),
                vec![SqlParam::float(value)],
            ),
            SearchPrefix::Lt | SearchPrefix::Eb => SqlFragment::with_params(
                format!("value_quantity_value < ?{}", param_num),
                vec![SqlParam::float(value)],
            ),
            SearchPrefix::Ge => SqlFragment::with_params(
                format!("value_quantity_value >= ?{}", param_num),
                vec![SqlParam::float(value)],
            ),
            SearchPrefix::Le => SqlFragment::with_params(
                format!("value_quantity_value <= ?{}", param_num),
                vec![SqlParam::float(value)],
            ),
            SearchPrefix::Ap => {
                // +/- 10%
                let margin = (value.abs() * 0.1).max(0.0001);
                SqlFragment::with_params(
                    format!(
                        "value_quantity_value BETWEEN ?{} AND ?{}",
                        param_num,
                        param_num + 1
                    ),
                    vec![
                        SqlParam::float(value - margin),
                        SqlParam::float(value + margin),
                    ],
                )
            }
        }
    }

    /// Gets the implicit precision of a number.
    fn get_implicit_precision(value: f64) -> f64 {
        let s = value.to_string();
        if let Some(dot_pos) = s.find('.') {
            let decimal_places = s.len() - dot_pos - 1;
            10_f64.powi(-(decimal_places as i32))
        } else {
            1.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantity_value_only() {
        let value = SearchValue::new(SearchPrefix::Eq, "5.4");
        let frag = QuantityHandler::build_sql(&value, 0);

        assert!(frag.sql.contains("value_quantity_value"));
        assert!(!frag.sql.contains("value_quantity_unit"));
    }

    #[test]
    fn test_quantity_with_code() {
        let value = SearchValue::new(SearchPrefix::Eq, "5.4|mg");
        let frag = QuantityHandler::build_sql(&value, 0);

        assert!(frag.sql.contains("value_quantity_value"));
        assert!(frag.sql.contains("value_quantity_unit"));
    }

    #[test]
    fn test_quantity_with_system_and_code() {
        let value = SearchValue::new(SearchPrefix::Eq, "5.4|http://unitsofmeasure.org|mg");
        let frag = QuantityHandler::build_sql(&value, 0);

        assert!(frag.sql.contains("value_quantity_value"));
        assert!(frag.sql.contains("value_quantity_system"));
        assert!(frag.sql.contains("value_quantity_unit"));
    }

    #[test]
    fn test_quantity_gt() {
        let value = SearchValue::new(SearchPrefix::Gt, "5.4|mg");
        let frag = QuantityHandler::build_sql(&value, 0);

        assert!(frag.sql.contains("> ?1"));
    }

    #[test]
    fn test_quantity_ap() {
        let value = SearchValue::new(SearchPrefix::Ap, "100");
        let frag = QuantityHandler::build_sql(&value, 0);

        assert!(frag.sql.contains("BETWEEN"));
    }
}
