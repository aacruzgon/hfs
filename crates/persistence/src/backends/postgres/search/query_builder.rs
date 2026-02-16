//! PostgreSQL search query builder.
//!
//! Builds SQL queries for FHIR search operations using PostgreSQL syntax
//! with $N parameter placeholders, ILIKE for case-insensitive matching,
//! and native TIMESTAMPTZ comparisons.

use chrono::{DateTime, Utc};

use crate::types::{
    SearchModifier, SearchParamType, SearchParameter, SearchPrefix, SearchQuery, SearchValue,
};

/// A SQL fragment with associated parameters.
#[derive(Debug, Clone)]
pub struct SqlFragment {
    /// The SQL string with $N placeholders.
    pub sql: String,
    /// The parameter values.
    pub params: Vec<SqlParam>,
}

/// A SQL parameter value.
#[derive(Debug, Clone)]
pub enum SqlParam {
    /// Text parameter.
    Text(String),
    /// Floating point parameter.
    Float(f64),
    /// Integer parameter.
    Integer(i64),
    /// Boolean parameter.
    Bool(bool),
    /// Timestamp parameter.
    Timestamp(DateTime<Utc>),
    /// Null parameter.
    Null,
}

impl SqlParam {
    /// Creates a text parameter.
    pub fn text(s: &str) -> Self {
        SqlParam::Text(s.to_string())
    }
}

impl SqlFragment {
    /// Creates a new fragment with no parameters.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Creates a fragment with parameters.
    pub fn with_params(sql: impl Into<String>, params: Vec<SqlParam>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }

    /// Combines two fragments with AND.
    pub fn and(self, other: SqlFragment) -> SqlFragment {
        SqlFragment {
            sql: format!("({}) AND ({})", self.sql, other.sql),
            params: [self.params, other.params].concat(),
        }
    }

    /// Combines two fragments with OR.
    pub fn or(self, other: SqlFragment) -> SqlFragment {
        SqlFragment {
            sql: format!("({}) OR ({})", self.sql, other.sql),
            params: [self.params, other.params].concat(),
        }
    }
}

/// PostgreSQL search query builder.
pub struct PostgresQueryBuilder;

impl PostgresQueryBuilder {
    /// Builds a search query for finding matching resource IDs.
    ///
    /// Returns a SQL fragment that selects DISTINCT resource_ids from search_index
    /// matching the given search parameters.
    pub fn build_search_query(query: &SearchQuery, param_offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();
        let mut current_offset = param_offset;

        for param in &query.parameters {
            if let Some(condition) = Self::build_parameter_condition(param, current_offset) {
                current_offset += condition.params.len();
                conditions.push(condition);
            }
        }

        if conditions.is_empty() {
            return None;
        }

        // AND all conditions together
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.and(cond);
        }

        Some(combined)
    }

    /// Builds a condition for a single search parameter.
    fn build_parameter_condition(
        param: &SearchParameter,
        param_offset: usize,
    ) -> Option<SqlFragment> {
        if param.values.is_empty() {
            return None;
        }

        // Handle special parameters
        match param.name.as_str() {
            "_id" => return Self::build_id_condition(&param.values, param_offset),
            "_lastUpdated" => {
                return Self::build_last_updated_condition(&param.values, param_offset);
            }
            _ => {}
        }

        // Build conditions based on parameter type
        match param.param_type {
            SearchParamType::String => Self::build_string_condition(param, param_offset),
            SearchParamType::Token => Self::build_token_condition(param, param_offset),
            SearchParamType::Date => Self::build_date_condition(param, param_offset),
            SearchParamType::Number => Self::build_number_condition(param, param_offset),
            SearchParamType::Quantity => Self::build_quantity_condition(param, param_offset),
            SearchParamType::Reference => Self::build_reference_condition(param, param_offset),
            SearchParamType::Uri => Self::build_uri_condition(param, param_offset),
            SearchParamType::Composite => None,
            SearchParamType::Special => None,
        }
    }

    fn build_id_condition(values: &[SearchValue], offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();
        for (i, value) in values.iter().enumerate() {
            let param_num = offset + i + 1;
            conditions.push(SqlFragment::with_params(
                format!("id = ${}", param_num),
                vec![SqlParam::text(&value.value)],
            ));
        }
        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.or(cond);
        }
        Some(combined)
    }

    fn build_last_updated_condition(values: &[SearchValue], offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();
        for (i, value) in values.iter().enumerate() {
            let param_num = offset + i + 1;
            let op = Self::prefix_to_operator(&value.prefix);
            conditions.push(SqlFragment::with_params(
                format!("last_updated {} ${}", op, param_num),
                vec![SqlParam::text(&value.value)],
            ));
        }
        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.and(cond);
        }
        Some(combined)
    }

    fn build_string_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let modifier = param.modifier.as_ref();
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let param_num = offset + i + 1;
            let condition = match modifier {
                Some(SearchModifier::Exact) => SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_string = ${})",
                        param.name, param_num
                    ),
                    vec![SqlParam::text(&value.value)],
                ),
                Some(SearchModifier::Contains) => SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_string ILIKE ${})",
                        param.name, param_num
                    ),
                    vec![SqlParam::text(&format!("%{}%", value.value))],
                ),
                _ => {
                    // Default: starts-with (case-insensitive)
                    SqlFragment::with_params(
                        format!(
                            "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_string ILIKE ${})",
                            param.name, param_num
                        ),
                        vec![SqlParam::text(&format!("{}%", value.value))],
                    )
                }
            };
            conditions.push(condition);
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.or(cond);
        }
        Some(combined)
    }

    fn build_token_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let base_offset = offset + i * 2;
            let condition = if let Some((system, code)) = value.value.split_once('|') {
                if system.is_empty() {
                    // |code - match any system
                    SqlFragment::with_params(
                        format!(
                            "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_token_code = ${})",
                            param.name,
                            base_offset + 1
                        ),
                        vec![SqlParam::text(code)],
                    )
                } else if code.is_empty() {
                    // system| - match any code in system
                    SqlFragment::with_params(
                        format!(
                            "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_token_system = ${})",
                            param.name,
                            base_offset + 1
                        ),
                        vec![SqlParam::text(system)],
                    )
                } else {
                    // system|code - exact match
                    SqlFragment::with_params(
                        format!(
                            "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_token_system = ${} AND value_token_code = ${})",
                            param.name,
                            base_offset + 1,
                            base_offset + 2
                        ),
                        vec![SqlParam::text(system), SqlParam::text(code)],
                    )
                }
            } else {
                // code only - match any system
                SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_token_code = ${})",
                        param.name,
                        base_offset + 1
                    ),
                    vec![SqlParam::text(&value.value)],
                )
            };
            conditions.push(condition);
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.or(cond);
        }
        Some(combined)
    }

    fn build_date_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let param_num = offset + i + 1;
            let op = Self::prefix_to_operator(&value.prefix);
            let timestamp = Self::parse_date_value(&value.value);
            conditions.push(SqlFragment::with_params(
                format!(
                    "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_date {} ${})",
                    param.name, op, param_num
                ),
                vec![SqlParam::Timestamp(timestamp)],
            ));
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.and(cond);
        }
        Some(combined)
    }

    fn build_number_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let param_num = offset + i + 1;
            let op = Self::prefix_to_operator(&value.prefix);
            if let Ok(num) = value.value.parse::<f64>() {
                conditions.push(SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_number {} ${})",
                        param.name, op, param_num
                    ),
                    vec![SqlParam::Float(num)],
                ));
            }
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.and(cond);
        }
        Some(combined)
    }

    fn build_quantity_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let base_offset = offset + i * 2;
            // Parse quantity: [prefix]number|system|code
            let parts: Vec<&str> = value.value.splitn(3, '|').collect();
            if let Some(num_str) = parts.first() {
                if let Ok(num) = num_str.parse::<f64>() {
                    let op = Self::prefix_to_operator(&value.prefix);
                    if parts.len() >= 3 {
                        conditions.push(SqlFragment::with_params(
                            format!(
                                "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_quantity_value {} ${} AND value_quantity_unit = ${})",
                                param.name, op, base_offset + 1, base_offset + 2
                            ),
                            vec![SqlParam::Float(num), SqlParam::text(parts[2])],
                        ));
                    } else {
                        conditions.push(SqlFragment::with_params(
                            format!(
                                "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_quantity_value {} ${})",
                                param.name, op, base_offset + 1
                            ),
                            vec![SqlParam::Float(num)],
                        ));
                    }
                }
            }
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.and(cond);
        }
        Some(combined)
    }

    fn build_reference_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let param_num = offset + i + 1;
            conditions.push(SqlFragment::with_params(
                format!(
                    "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_reference = ${})",
                    param.name, param_num
                ),
                vec![SqlParam::text(&value.value)],
            ));
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.or(cond);
        }
        Some(combined)
    }

    fn build_uri_condition(param: &SearchParameter, offset: usize) -> Option<SqlFragment> {
        let modifier = param.modifier.as_ref();
        let mut conditions = Vec::new();

        for (i, value) in param.values.iter().enumerate() {
            let param_num = offset + i + 1;
            let condition = match modifier {
                Some(SearchModifier::Below) => SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_uri LIKE ${} || '%')",
                        param.name, param_num
                    ),
                    vec![SqlParam::text(&value.value)],
                ),
                Some(SearchModifier::Above) => SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND ${} LIKE value_uri || '%')",
                        param.name, param_num
                    ),
                    vec![SqlParam::text(&value.value)],
                ),
                _ => SqlFragment::with_params(
                    format!(
                        "id IN (SELECT resource_id FROM search_index WHERE tenant_id = $1 AND resource_type = $2 AND param_name = '{}' AND value_uri = ${})",
                        param.name, param_num
                    ),
                    vec![SqlParam::text(&value.value)],
                ),
            };
            conditions.push(condition);
        }

        if conditions.is_empty() {
            return None;
        }
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = combined.or(cond);
        }
        Some(combined)
    }

    /// Converts a FHIR search prefix to a SQL comparison operator.
    fn prefix_to_operator(prefix: &SearchPrefix) -> &'static str {
        match prefix {
            SearchPrefix::Eq => "=",
            SearchPrefix::Ne => "!=",
            SearchPrefix::Gt => ">",
            SearchPrefix::Lt => "<",
            SearchPrefix::Ge => ">=",
            SearchPrefix::Le => "<=",
            SearchPrefix::Sa => ">", // starts after
            SearchPrefix::Eb => "<", // ends before
            SearchPrefix::Ap => "=", // approximately (simplified)
        }
    }

    /// Parses a FHIR date search value into a `DateTime<Utc>`.
    ///
    /// Handles partial dates (year, year-month, date) and full date-times.
    fn parse_date_value(value: &str) -> DateTime<Utc> {
        let normalized = if value.contains('T') {
            if value.contains('+') || value.contains('Z') || value.ends_with("-00:00") {
                value.to_string()
            } else {
                format!("{}+00:00", value)
            }
        } else if value.len() == 10 {
            format!("{}T00:00:00+00:00", value)
        } else if value.len() == 7 {
            format!("{}-01T00:00:00+00:00", value)
        } else if value.len() == 4 {
            format!("{}-01-01T00:00:00+00:00", value)
        } else {
            value.to_string()
        };

        DateTime::parse_from_rfc3339(&normalized)
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|_| normalized.parse::<DateTime<Utc>>())
            .unwrap_or_else(|_| Utc::now())
    }
}
