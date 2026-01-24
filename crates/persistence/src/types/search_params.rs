//! FHIR search parameter types.
//!
//! This module defines types for representing FHIR search parameters,
//! including parameter types, modifiers, and prefixes.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// FHIR search parameter types.
///
/// See: https://build.fhir.org/search.html#ptypes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchParamType {
    /// A simple string, like a name or description.
    String,
    /// A search against a URI.
    Uri,
    /// A search for a number.
    Number,
    /// A search for a date, dateTime, or period.
    Date,
    /// A quantity, with a number and units.
    Quantity,
    /// A code from a code system or value set.
    Token,
    /// A reference to another resource.
    Reference,
    /// A composite search parameter that combines others.
    Composite,
    /// Special search parameters (_id, _lastUpdated, etc.).
    Special,
}

impl fmt::Display for SearchParamType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchParamType::String => write!(f, "string"),
            SearchParamType::Uri => write!(f, "uri"),
            SearchParamType::Number => write!(f, "number"),
            SearchParamType::Date => write!(f, "date"),
            SearchParamType::Quantity => write!(f, "quantity"),
            SearchParamType::Token => write!(f, "token"),
            SearchParamType::Reference => write!(f, "reference"),
            SearchParamType::Composite => write!(f, "composite"),
            SearchParamType::Special => write!(f, "special"),
        }
    }
}

impl FromStr for SearchParamType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "string" => Ok(SearchParamType::String),
            "uri" => Ok(SearchParamType::Uri),
            "number" => Ok(SearchParamType::Number),
            "date" => Ok(SearchParamType::Date),
            "quantity" => Ok(SearchParamType::Quantity),
            "token" => Ok(SearchParamType::Token),
            "reference" => Ok(SearchParamType::Reference),
            "composite" => Ok(SearchParamType::Composite),
            "special" => Ok(SearchParamType::Special),
            _ => Err(format!("unknown search parameter type: {}", s)),
        }
    }
}

/// Search modifiers that can be applied to search parameters.
///
/// See: https://build.fhir.org/search.html#modifiers
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchModifier {
    /// Exact string match (string parameters).
    Exact,
    /// Contains substring (string parameters).
    Contains,
    /// Text search (token parameters).
    Text,
    /// Negation - exclude matches.
    Not,
    /// Match if value is missing.
    Missing,
    /// Match codes above in hierarchy (token parameters).
    Above,
    /// Match codes below in hierarchy (token parameters).
    Below,
    /// Match codes in a value set (token parameters).
    In,
    /// Match codes not in a value set (token parameters).
    NotIn,
    /// Match on identifier (reference parameters).
    Identifier,
    /// Specify reference type (reference parameters).
    Type(String),
    /// Match on type (token parameters for polymorphic elements).
    OfType,
    /// Match on code only (token parameters).
    CodeOnly,
    /// Iterate through results (_include modifier).
    Iterate,
}

impl fmt::Display for SearchModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchModifier::Exact => write!(f, "exact"),
            SearchModifier::Contains => write!(f, "contains"),
            SearchModifier::Text => write!(f, "text"),
            SearchModifier::Not => write!(f, "not"),
            SearchModifier::Missing => write!(f, "missing"),
            SearchModifier::Above => write!(f, "above"),
            SearchModifier::Below => write!(f, "below"),
            SearchModifier::In => write!(f, "in"),
            SearchModifier::NotIn => write!(f, "not-in"),
            SearchModifier::Identifier => write!(f, "identifier"),
            SearchModifier::Type(t) => write!(f, "{}", t),
            SearchModifier::OfType => write!(f, "ofType"),
            SearchModifier::CodeOnly => write!(f, "code"),
            SearchModifier::Iterate => write!(f, "iterate"),
        }
    }
}

impl SearchModifier {
    /// Parses a modifier string, returning None for unknown modifiers.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "exact" => Some(SearchModifier::Exact),
            "contains" => Some(SearchModifier::Contains),
            "text" => Some(SearchModifier::Text),
            "not" => Some(SearchModifier::Not),
            "missing" => Some(SearchModifier::Missing),
            "above" => Some(SearchModifier::Above),
            "below" => Some(SearchModifier::Below),
            "in" => Some(SearchModifier::In),
            "not-in" => Some(SearchModifier::NotIn),
            "identifier" => Some(SearchModifier::Identifier),
            "oftype" => Some(SearchModifier::OfType),
            "code" => Some(SearchModifier::CodeOnly),
            "iterate" => Some(SearchModifier::Iterate),
            _ => {
                // Check if it's a resource type modifier
                if s.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                    Some(SearchModifier::Type(s.to_string()))
                } else {
                    None
                }
            }
        }
    }

    /// Returns true if this modifier is valid for the given parameter type.
    pub fn is_valid_for(&self, param_type: SearchParamType) -> bool {
        match self {
            SearchModifier::Exact | SearchModifier::Contains => {
                param_type == SearchParamType::String
            }
            SearchModifier::Text => param_type == SearchParamType::Token,
            SearchModifier::Not => true,     // Valid for all types
            SearchModifier::Missing => true, // Valid for all types
            SearchModifier::Above
            | SearchModifier::Below
            | SearchModifier::In
            | SearchModifier::NotIn => {
                param_type == SearchParamType::Token || param_type == SearchParamType::Uri
            }
            SearchModifier::Identifier | SearchModifier::Type(_) => {
                param_type == SearchParamType::Reference
            }
            SearchModifier::OfType => param_type == SearchParamType::Token,
            SearchModifier::CodeOnly => param_type == SearchParamType::Token,
            SearchModifier::Iterate => false, // Only for _include/_revinclude
        }
    }
}

/// Comparison prefixes for search parameters.
///
/// See: https://build.fhir.org/search.html#prefix
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SearchPrefix {
    /// Equal (default).
    #[default]
    Eq,
    /// Not equal.
    Ne,
    /// Greater than.
    Gt,
    /// Less than.
    Lt,
    /// Greater than or equal.
    Ge,
    /// Less than or equal.
    Le,
    /// Starts after.
    Sa,
    /// Ends before.
    Eb,
    /// Approximately equal.
    Ap,
}

impl fmt::Display for SearchPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchPrefix::Eq => write!(f, "eq"),
            SearchPrefix::Ne => write!(f, "ne"),
            SearchPrefix::Gt => write!(f, "gt"),
            SearchPrefix::Lt => write!(f, "lt"),
            SearchPrefix::Ge => write!(f, "ge"),
            SearchPrefix::Le => write!(f, "le"),
            SearchPrefix::Sa => write!(f, "sa"),
            SearchPrefix::Eb => write!(f, "eb"),
            SearchPrefix::Ap => write!(f, "ap"),
        }
    }
}

impl FromStr for SearchPrefix {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "eq" => Ok(SearchPrefix::Eq),
            "ne" => Ok(SearchPrefix::Ne),
            "gt" => Ok(SearchPrefix::Gt),
            "lt" => Ok(SearchPrefix::Lt),
            "ge" => Ok(SearchPrefix::Ge),
            "le" => Ok(SearchPrefix::Le),
            "sa" => Ok(SearchPrefix::Sa),
            "eb" => Ok(SearchPrefix::Eb),
            "ap" => Ok(SearchPrefix::Ap),
            _ => Err(format!("unknown search prefix: {}", s)),
        }
    }
}

impl SearchPrefix {
    /// Extracts a prefix from the beginning of a value string.
    ///
    /// Returns the prefix and the remaining value.
    pub fn extract(value: &str) -> (Self, &str) {
        if value.len() >= 2 {
            let prefix = &value[..2];
            if let Ok(p) = prefix.parse() {
                return (p, &value[2..]);
            }
        }
        (SearchPrefix::Eq, value)
    }

    /// Returns true if this prefix is valid for the given parameter type.
    pub fn is_valid_for(&self, param_type: SearchParamType) -> bool {
        match self {
            SearchPrefix::Eq | SearchPrefix::Ne => true,
            SearchPrefix::Gt | SearchPrefix::Lt | SearchPrefix::Ge | SearchPrefix::Le => {
                matches!(
                    param_type,
                    SearchParamType::Number | SearchParamType::Date | SearchParamType::Quantity
                )
            }
            SearchPrefix::Sa | SearchPrefix::Eb => param_type == SearchParamType::Date,
            SearchPrefix::Ap => {
                matches!(
                    param_type,
                    SearchParamType::Number | SearchParamType::Date | SearchParamType::Quantity
                )
            }
        }
    }
}

/// A parsed search parameter with its value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchParameter {
    /// The parameter name (e.g., "name", "identifier").
    pub name: String,

    /// The parameter type.
    pub param_type: SearchParamType,

    /// Modifier, if any.
    pub modifier: Option<SearchModifier>,

    /// The search value(s). Multiple values are ORed.
    pub values: Vec<SearchValue>,

    /// Chained parameters (e.g., patient.name=Smith).
    pub chain: Vec<ChainedParameter>,
}

/// A single search value with optional prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchValue {
    /// The comparison prefix.
    pub prefix: SearchPrefix,

    /// The value to search for.
    pub value: String,
}

impl SearchValue {
    /// Creates a new search value with the given prefix and value.
    pub fn new(prefix: SearchPrefix, value: impl Into<String>) -> Self {
        Self {
            prefix,
            value: value.into(),
        }
    }

    /// Creates a search value with the default (eq) prefix.
    pub fn eq(value: impl Into<String>) -> Self {
        Self::new(SearchPrefix::Eq, value)
    }

    /// Parses a value string, extracting any prefix.
    pub fn parse(s: &str) -> Self {
        let (prefix, value) = SearchPrefix::extract(s);
        Self::new(prefix, value)
    }
}

/// A chained search parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainedParameter {
    /// The reference parameter being chained through.
    pub reference_param: String,

    /// Optional type modifier on the reference.
    pub target_type: Option<String>,

    /// The target parameter on the referenced resource.
    pub target_param: String,
}

/// A reverse chained parameter (_has).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReverseChainedParameter {
    /// The resource type that references this resource.
    pub source_type: String,

    /// The reference parameter on the source type.
    pub reference_param: String,

    /// The search parameter on the source type.
    pub search_param: String,

    /// The search value.
    pub value: SearchValue,
}

/// Include directive for _include and _revinclude.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncludeDirective {
    /// The type of include.
    pub include_type: IncludeType,

    /// The source resource type.
    pub source_type: String,

    /// The search parameter (reference) to follow.
    pub search_param: String,

    /// Optional target resource type filter.
    pub target_type: Option<String>,

    /// Whether to iterate (follow includes of included resources).
    pub iterate: bool,
}

/// Type of include operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncludeType {
    /// Forward include (_include).
    Include,
    /// Reverse include (_revinclude).
    Revinclude,
}

/// Sort direction for _sort parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortDirection {
    /// Ascending order.
    #[default]
    Ascending,
    /// Descending order.
    Descending,
}

/// A sort directive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortDirective {
    /// The parameter to sort by.
    pub parameter: String,
    /// The sort direction.
    pub direction: SortDirection,
}

impl SortDirective {
    /// Parses a sort parameter value (e.g., "-date" for descending).
    pub fn parse(s: &str) -> Self {
        if let Some(stripped) = s.strip_prefix('-') {
            Self {
                parameter: stripped.to_string(),
                direction: SortDirection::Descending,
            }
        } else {
            Self {
                parameter: s.to_string(),
                direction: SortDirection::Ascending,
            }
        }
    }
}

/// A complete search query with all parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchQuery {
    /// The resource type being searched.
    pub resource_type: String,

    /// Standard search parameters.
    pub parameters: Vec<SearchParameter>,

    /// Reverse chain parameters (_has).
    pub reverse_chains: Vec<ReverseChainedParameter>,

    /// Include directives.
    pub includes: Vec<IncludeDirective>,

    /// Sort directives.
    pub sort: Vec<SortDirective>,

    /// Result count limit (_count).
    pub count: Option<u32>,

    /// Offset for pagination.
    pub offset: Option<u32>,

    /// Cursor for keyset pagination.
    pub cursor: Option<String>,

    /// Whether to include total count (_total).
    pub total: Option<TotalMode>,

    /// Summary mode (_summary).
    pub summary: Option<SummaryMode>,

    /// Elements to include (_elements).
    pub elements: Vec<String>,

    /// Raw query parameters for debugging.
    pub raw_params: HashMap<String, Vec<String>>,
}

/// Mode for _total parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TotalMode {
    /// No total.
    None,
    /// Estimated total.
    Estimate,
    /// Accurate total.
    Accurate,
}

/// Mode for _summary parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SummaryMode {
    /// Return summary elements only.
    True,
    /// Return full resource.
    False,
    /// Return text narrative only.
    Text,
    /// Return data elements only (no text).
    Data,
    /// Return count only.
    Count,
}

impl SearchQuery {
    /// Creates a new search query for the given resource type.
    pub fn new(resource_type: impl Into<String>) -> Self {
        Self {
            resource_type: resource_type.into(),
            ..Default::default()
        }
    }

    /// Adds a search parameter.
    pub fn with_parameter(mut self, param: SearchParameter) -> Self {
        self.parameters.push(param);
        self
    }

    /// Adds an include directive.
    pub fn with_include(mut self, include: IncludeDirective) -> Self {
        self.includes.push(include);
        self
    }

    /// Adds a sort directive.
    pub fn with_sort(mut self, sort: SortDirective) -> Self {
        self.sort.push(sort);
        self
    }

    /// Sets the count limit.
    pub fn with_count(mut self, count: u32) -> Self {
        self.count = Some(count);
        self
    }

    /// Sets the cursor for keyset pagination.
    pub fn with_cursor(mut self, cursor: String) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Returns true if this query uses any features that require special backend support.
    pub fn requires_advanced_features(&self) -> bool {
        // Chained parameters
        if self.parameters.iter().any(|p| !p.chain.is_empty()) {
            return true;
        }

        // Reverse chains
        if !self.reverse_chains.is_empty() {
            return true;
        }

        // Includes
        if !self.includes.is_empty() {
            return true;
        }

        // Terminology modifiers
        if self.parameters.iter().any(|p| {
            matches!(
                p.modifier,
                Some(SearchModifier::Above)
                    | Some(SearchModifier::Below)
                    | Some(SearchModifier::In)
                    | Some(SearchModifier::NotIn)
            )
        }) {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_param_type_display() {
        assert_eq!(SearchParamType::String.to_string(), "string");
        assert_eq!(SearchParamType::Token.to_string(), "token");
        assert_eq!(SearchParamType::Reference.to_string(), "reference");
    }

    #[test]
    fn test_search_param_type_parse() {
        assert_eq!(
            "string".parse::<SearchParamType>().unwrap(),
            SearchParamType::String
        );
        assert_eq!(
            "TOKEN".parse::<SearchParamType>().unwrap(),
            SearchParamType::Token
        );
    }

    #[test]
    fn test_search_modifier_parse() {
        assert_eq!(SearchModifier::parse("exact"), Some(SearchModifier::Exact));
        assert_eq!(
            SearchModifier::parse("contains"),
            Some(SearchModifier::Contains)
        );
        assert_eq!(
            SearchModifier::parse("Patient"),
            Some(SearchModifier::Type("Patient".to_string()))
        );
        assert_eq!(SearchModifier::parse("unknown"), None);
    }

    #[test]
    fn test_search_modifier_validity() {
        assert!(SearchModifier::Exact.is_valid_for(SearchParamType::String));
        assert!(!SearchModifier::Exact.is_valid_for(SearchParamType::Token));
        assert!(SearchModifier::Text.is_valid_for(SearchParamType::Token));
        assert!(SearchModifier::Not.is_valid_for(SearchParamType::String));
        assert!(SearchModifier::Not.is_valid_for(SearchParamType::Token));
    }

    #[test]
    fn test_search_prefix_extract() {
        assert_eq!(
            SearchPrefix::extract("gt2020-01-01"),
            (SearchPrefix::Gt, "2020-01-01")
        );
        assert_eq!(
            SearchPrefix::extract("2020-01-01"),
            (SearchPrefix::Eq, "2020-01-01")
        );
        assert_eq!(SearchPrefix::extract("le100"), (SearchPrefix::Le, "100"));
    }

    #[test]
    fn test_search_prefix_validity() {
        assert!(SearchPrefix::Gt.is_valid_for(SearchParamType::Number));
        assert!(SearchPrefix::Gt.is_valid_for(SearchParamType::Date));
        assert!(!SearchPrefix::Gt.is_valid_for(SearchParamType::String));
        assert!(SearchPrefix::Sa.is_valid_for(SearchParamType::Date));
        assert!(!SearchPrefix::Sa.is_valid_for(SearchParamType::Number));
    }

    #[test]
    fn test_search_value_parse() {
        let value = SearchValue::parse("gt100");
        assert_eq!(value.prefix, SearchPrefix::Gt);
        assert_eq!(value.value, "100");

        let value2 = SearchValue::parse("Smith");
        assert_eq!(value2.prefix, SearchPrefix::Eq);
        assert_eq!(value2.value, "Smith");
    }

    #[test]
    fn test_sort_directive_parse() {
        let asc = SortDirective::parse("date");
        assert_eq!(asc.parameter, "date");
        assert_eq!(asc.direction, SortDirection::Ascending);

        let desc = SortDirective::parse("-date");
        assert_eq!(desc.parameter, "date");
        assert_eq!(desc.direction, SortDirection::Descending);
    }

    #[test]
    fn test_search_query_builder() {
        let query = SearchQuery::new("Patient")
            .with_count(10)
            .with_sort(SortDirective::parse("-_lastUpdated"));

        assert_eq!(query.resource_type, "Patient");
        assert_eq!(query.count, Some(10));
        assert_eq!(query.sort.len(), 1);
    }

    #[test]
    fn test_requires_advanced_features() {
        let simple = SearchQuery::new("Patient");
        assert!(!simple.requires_advanced_features());

        let with_include = SearchQuery::new("Patient").with_include(IncludeDirective {
            include_type: IncludeType::Include,
            source_type: "Patient".to_string(),
            search_param: "organization".to_string(),
            target_type: None,
            iterate: false,
        });
        assert!(with_include.requires_advanced_features());
    }
}
