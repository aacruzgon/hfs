//! Search parameters extractor.
//!
//! Extracts and parses FHIR search parameters from query strings.

use axum::{
    extract::{FromRequestParts, Query},
    http::{StatusCode, request::Parts},
};
use std::collections::HashMap;

/// Axum extractor for FHIR search parameters.
///
/// Parses query string parameters into a structured format for search
/// operations.
///
/// # Example
///
/// ```rust,ignore
/// use helios_rest::extractors::SearchParams;
///
/// async fn search_handler(params: SearchParams) {
///     for (name, value) in params.iter() {
///         println!("{} = {}", name, value);
///     }
/// }
/// ```
#[derive(Debug, Default)]
pub struct SearchParams {
    /// Raw query parameters.
    params: HashMap<String, String>,

    /// Page size (_count).
    count: Option<usize>,

    /// Page offset (_offset).
    offset: Option<usize>,

    /// Sort parameters (_sort).
    sort: Option<Vec<SortParam>>,

    /// Include parameters (_include).
    include: Vec<String>,

    /// Reverse include parameters (_revinclude).
    revinclude: Vec<String>,

    /// Summary mode (_summary).
    summary: Option<String>,

    /// Elements to include (_elements).
    elements: Option<Vec<String>>,

    /// Total mode (_total).
    total: Option<String>,
}

/// A parsed sort parameter.
#[derive(Debug, Clone)]
pub struct SortParam {
    /// The field to sort by.
    pub field: String,
    /// Sort direction (ascending if true).
    pub ascending: bool,
}

impl SearchParams {
    /// Creates empty search params.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates search params from a HashMap.
    pub fn from_map(params: HashMap<String, String>) -> Self {
        let mut result = Self {
            params: params.clone(),
            ..Default::default()
        };

        // Extract system parameters
        if let Some(count) = params.get("_count") {
            result.count = count.parse().ok();
        }

        if let Some(offset) = params.get("_offset") {
            result.offset = offset.parse().ok();
        }

        if let Some(sort) = params.get("_sort") {
            result.sort = Some(parse_sort_params(sort));
        }

        if let Some(include) = params.get("_include") {
            result.include = include.split(',').map(String::from).collect();
        }

        if let Some(revinclude) = params.get("_revinclude") {
            result.revinclude = revinclude.split(',').map(String::from).collect();
        }

        if let Some(summary) = params.get("_summary") {
            result.summary = Some(summary.clone());
        }

        if let Some(elements) = params.get("_elements") {
            result.elements = Some(elements.split(',').map(String::from).collect());
        }

        if let Some(total) = params.get("_total") {
            result.total = Some(total.clone());
        }

        result
    }

    /// Returns an iterator over all parameters.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.params.iter()
    }

    /// Returns an iterator over search parameters (excluding result format/pagination params).
    ///
    /// Excludes: _count, _offset, _cursor, _sort, _total, _summary, _elements,
    ///           _include, _revinclude, _contained, _containedType, _format
    ///
    /// Includes: _id, _lastUpdated, _tag, _profile, _security, _source, _has,
    ///           _list, _text, _content, _filter, _query, _type
    pub fn search_params(&self) -> impl Iterator<Item = (&String, &String)> {
        const EXCLUDED_PARAMS: &[&str] = &[
            "_count",
            "_offset",
            "_cursor",
            "_sort",
            "_total",
            "_summary",
            "_elements",
            "_include",
            "_revinclude",
            "_contained",
            "_containedType",
            "_format",
            "_pretty",
        ];
        self.params
            .iter()
            .filter(|(k, _)| !EXCLUDED_PARAMS.contains(&k.as_str()))
    }

    /// Returns the page size (_count).
    pub fn count(&self) -> Option<usize> {
        self.count
    }

    /// Returns the page offset (_offset).
    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    /// Returns the sort parameters.
    pub fn sort(&self) -> Option<&[SortParam]> {
        self.sort.as_deref()
    }

    /// Returns the include parameters.
    pub fn include(&self) -> &[String] {
        &self.include
    }

    /// Returns the reverse include parameters.
    pub fn revinclude(&self) -> &[String] {
        &self.revinclude
    }

    /// Returns the summary mode.
    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }

    /// Returns the elements to include.
    pub fn elements(&self) -> Option<&[String]> {
        self.elements.as_deref()
    }

    /// Returns the total mode.
    pub fn total(&self) -> Option<&str> {
        self.total.as_deref()
    }

    /// Returns a specific parameter value.
    pub fn get(&self, name: &str) -> Option<&String> {
        self.params.get(name)
    }

    /// Checks if a parameter is present.
    pub fn contains(&self, name: &str) -> bool {
        self.params.contains_key(name)
    }

    /// Returns the raw parameter map.
    pub fn raw_params(&self) -> &HashMap<String, String> {
        &self.params
    }
}

/// Parses sort parameter string into structured params.
fn parse_sort_params(sort: &str) -> Vec<SortParam> {
    sort.split(',')
        .map(|s| {
            let s = s.trim();
            if let Some(field) = s.strip_prefix('-') {
                SortParam {
                    field: field.to_string(),
                    ascending: false,
                }
            } else {
                SortParam {
                    field: s.to_string(),
                    ascending: true,
                }
            }
        })
        .collect()
}

impl<S> FromRequestParts<S> for SearchParams
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(params) = Query::<HashMap<String, String>>::from_request_parts(parts, state)
            .await
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid query parameters"))?;

        Ok(SearchParams::from_map(params))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_map() {
        let mut params = HashMap::new();
        params.insert("_count".to_string(), "10".to_string());
        params.insert("_offset".to_string(), "20".to_string());
        params.insert("name".to_string(), "Smith".to_string());

        let search = SearchParams::from_map(params);

        assert_eq!(search.count(), Some(10));
        assert_eq!(search.offset(), Some(20));
        assert_eq!(search.get("name"), Some(&"Smith".to_string()));
    }

    #[test]
    fn test_sort_params() {
        let mut params = HashMap::new();
        params.insert("_sort".to_string(), "name,-date".to_string());

        let search = SearchParams::from_map(params);
        let sort = search.sort().unwrap();

        assert_eq!(sort.len(), 2);
        assert_eq!(sort[0].field, "name");
        assert!(sort[0].ascending);
        assert_eq!(sort[1].field, "date");
        assert!(!sort[1].ascending);
    }

    #[test]
    fn test_search_params_filter() {
        let mut params = HashMap::new();
        params.insert("_count".to_string(), "10".to_string());
        params.insert("name".to_string(), "Smith".to_string());
        params.insert("active".to_string(), "true".to_string());

        let search = SearchParams::from_map(params);
        let search_only: Vec<_> = search.search_params().collect();

        assert_eq!(search_only.len(), 2);
    }
}
