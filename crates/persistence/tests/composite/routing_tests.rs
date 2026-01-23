//! Tests for query routing logic in composite storage.
//!
//! This module tests query decomposition and routing decisions
//! for directing queries to appropriate backends based on capabilities.

use std::collections::HashSet;

use helios_persistence::core::BackendCapability;
use helios_persistence::types::{
    IncludeDirective, IncludeType, SearchParamType, SearchParameter, SearchPrefix, SearchQuery,
    SearchValue, SortDirective,
};

// ============================================================================
// Query Feature Detection Tests
// ============================================================================

/// Test detection of basic search features.
#[test]
fn test_detect_basic_search_features() {
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "name".to_string(),
        param_type: SearchParamType::String,
        modifier: None,
        values: vec![SearchValue::string("Smith")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::BasicSearch));
    assert!(features.contains(&QueryFeature::StringSearch));
}

/// Test detection of date search features.
#[test]
fn test_detect_date_search_features() {
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "birthdate".to_string(),
        param_type: SearchParamType::Date,
        modifier: None,
        values: vec![SearchValue::date(SearchPrefix::Gt, "1990-01-01")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::DateSearch));
}

/// Test detection of token search features.
#[test]
fn test_detect_token_search_features() {
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "code".to_string(),
        param_type: SearchParamType::Token,
        modifier: None,
        values: vec![SearchValue::token("http://loinc.org", "8867-4")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::TokenSearch));
}

/// Test detection of reference search features.
#[test]
fn test_detect_reference_search_features() {
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: None,
        values: vec![SearchValue::reference("Patient/123")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::ReferenceSearch));
}

/// Test detection of chained search features.
#[test]
fn test_detect_chained_search_features() {
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: None,
        values: vec![SearchValue::string("Smith")],
        chain: vec!["Patient".to_string(), "name".to_string()],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::ChainedSearch));
}

/// Test detection of reverse chained search (_has).
#[test]
fn test_detect_reverse_chained_features() {
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "_has".to_string(),
        param_type: SearchParamType::Special,
        modifier: None,
        values: vec![SearchValue::string("Observation:subject:code=8867-4")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::ReverseChaining));
}

/// Test detection of _include features.
#[test]
fn test_detect_include_features() {
    let query = SearchQuery::new("Observation").with_include(IncludeDirective {
        include_type: IncludeType::Include,
        source_type: "Observation".to_string(),
        search_param: "subject".to_string(),
        target_type: Some("Patient".to_string()),
        iterate: false,
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::Include));
}

/// Test detection of _revinclude features.
#[test]
fn test_detect_revinclude_features() {
    let query = SearchQuery::new("Patient").with_include(IncludeDirective {
        include_type: IncludeType::Revinclude,
        source_type: "Observation".to_string(),
        search_param: "subject".to_string(),
        target_type: Some("Patient".to_string()),
        iterate: false,
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::Revinclude));
}

/// Test detection of full-text search (_text, _content).
#[test]
fn test_detect_fulltext_search_features() {
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "_text".to_string(),
        param_type: SearchParamType::String,
        modifier: None,
        values: vec![SearchValue::string("cardiac patient")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::FullTextSearch));
}

/// Test detection of terminology search (code:below).
#[test]
fn test_detect_terminology_search_features() {
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "code".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some("below".to_string()),
        values: vec![SearchValue::token("http://loinc.org", "8867-4")],
        chain: vec![],
    });

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::TerminologySearch));
}

/// Test detection of sorting features.
#[test]
fn test_detect_sorting_features() {
    let query =
        SearchQuery::new("Patient").with_sort(SortDirective::parse("-_lastUpdated"));

    let features = detect_query_features(&query);

    assert!(features.contains(&QueryFeature::Sorting));
}

// ============================================================================
// Query Routing Tests
// ============================================================================

/// Test routing a simple query to primary backend.
#[test]
fn test_route_simple_query_to_primary() {
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "_id".to_string(),
        param_type: SearchParamType::Token,
        modifier: None,
        values: vec![SearchValue::eq("patient-123")],
        chain: vec![],
    });

    let routing = route_query(&query);

    assert_eq!(routing.primary_backend, BackendType::Primary);
    assert!(routing.auxiliary_backends.is_empty());
}

/// Test routing chained search to graph backend.
#[test]
fn test_route_chained_search_to_graph() {
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "subject".to_string(),
        param_type: SearchParamType::Reference,
        modifier: None,
        values: vec![SearchValue::string("Smith")],
        chain: vec!["Patient".to_string(), "name".to_string()],
    });

    let routing = route_query(&query);

    // Chained searches benefit from graph backend
    assert!(
        routing.primary_backend == BackendType::Graph
            || routing.auxiliary_backends.contains(&BackendType::Graph)
    );
}

/// Test routing full-text search to search backend.
#[test]
fn test_route_fulltext_to_search_backend() {
    let query = SearchQuery::new("Patient").with_parameter(SearchParameter {
        name: "_text".to_string(),
        param_type: SearchParamType::String,
        modifier: None,
        values: vec![SearchValue::string("chronic heart failure")],
        chain: vec![],
    });

    let routing = route_query(&query);

    // Full-text searches should use search backend
    assert!(
        routing.primary_backend == BackendType::Search
            || routing.auxiliary_backends.contains(&BackendType::Search)
    );
}

/// Test routing terminology search to terminology service.
#[test]
fn test_route_terminology_to_terminology_service() {
    let query = SearchQuery::new("Observation").with_parameter(SearchParameter {
        name: "code".to_string(),
        param_type: SearchParamType::Token,
        modifier: Some("below".to_string()),
        values: vec![SearchValue::token("http://loinc.org", "8867-4")],
        chain: vec![],
    });

    let routing = route_query(&query);

    // Terminology expansion should involve terminology service
    assert!(routing.auxiliary_backends.contains(&BackendType::Terminology));
}

/// Test routing complex query to multiple backends.
#[test]
fn test_route_complex_query_to_multiple_backends() {
    // Complex query: chained search + full-text + terminology + _include
    let query = SearchQuery::new("Observation")
        .with_parameter(SearchParameter {
            name: "subject".to_string(),
            param_type: SearchParamType::Reference,
            modifier: Some("contains".to_string()),
            values: vec![SearchValue::string("smith")],
            chain: vec!["Patient".to_string(), "name".to_string()],
        })
        .with_parameter(SearchParameter {
            name: "_text".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::string("cardiac")],
            chain: vec![],
        })
        .with_parameter(SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: Some("below".to_string()),
            values: vec![SearchValue::token("http://loinc.org", "8867-4")],
            chain: vec![],
        })
        .with_include(IncludeDirective {
            include_type: IncludeType::Include,
            source_type: "Observation".to_string(),
            search_param: "subject".to_string(),
            target_type: Some("Patient".to_string()),
            iterate: false,
        });

    let routing = route_query(&query);

    // Complex query should involve multiple backends
    assert!(!routing.auxiliary_backends.is_empty());
}

// ============================================================================
// Capability Matching Tests
// ============================================================================

/// Test matching query features to backend capabilities.
#[test]
fn test_match_features_to_capabilities() {
    let features = HashSet::from([
        QueryFeature::BasicSearch,
        QueryFeature::StringSearch,
        QueryFeature::Sorting,
    ]);

    let required_caps = features_to_capabilities(&features);

    assert!(required_caps.contains(&BackendCapability::BasicSearch));
    assert!(required_caps.contains(&BackendCapability::Sorting));
}

/// Test that graph-specific features require graph capabilities.
#[test]
fn test_graph_features_require_graph_capabilities() {
    let features = HashSet::from([
        QueryFeature::ChainedSearch,
        QueryFeature::ReverseChaining,
    ]);

    let required_caps = features_to_capabilities(&features);

    assert!(required_caps.contains(&BackendCapability::ChainedSearch));
    assert!(required_caps.contains(&BackendCapability::ReverseChaining));
}

/// Test that full-text features require search capabilities.
#[test]
fn test_fulltext_features_require_search_capabilities() {
    let features = HashSet::from([QueryFeature::FullTextSearch]);

    let required_caps = features_to_capabilities(&features);

    assert!(required_caps.contains(&BackendCapability::FullTextSearch));
}

// ============================================================================
// Query Decomposition Tests
// ============================================================================

/// Test decomposing a complex query into backend-specific parts.
#[test]
fn test_decompose_query() {
    let query = SearchQuery::new("Observation")
        .with_parameter(SearchParameter {
            name: "code".to_string(),
            param_type: SearchParamType::Token,
            modifier: None,
            values: vec![SearchValue::token("http://loinc.org", "8867-4")],
            chain: vec![],
        })
        .with_parameter(SearchParameter {
            name: "_text".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::string("cardiac")],
            chain: vec![],
        });

    let parts = decompose_query(&query);

    // Should have at least primary part
    assert!(!parts.is_empty());

    // Token search can go to primary
    let primary_part = parts.iter().find(|p| p.backend_type == BackendType::Primary);
    assert!(primary_part.is_some());

    // Full-text should go to search backend
    let search_part = parts.iter().find(|p| p.backend_type == BackendType::Search);
    assert!(search_part.is_some());
}

/// Test that decomposition preserves all query parameters.
#[test]
fn test_decomposition_preserves_parameters() {
    let query = SearchQuery::new("Patient")
        .with_parameter(SearchParameter {
            name: "name".to_string(),
            param_type: SearchParamType::String,
            modifier: None,
            values: vec![SearchValue::string("Smith")],
            chain: vec![],
        })
        .with_parameter(SearchParameter {
            name: "birthdate".to_string(),
            param_type: SearchParamType::Date,
            modifier: None,
            values: vec![SearchValue::date(SearchPrefix::Gt, "1990-01-01")],
            chain: vec![],
        });

    let parts = decompose_query(&query);

    // Count total parameters across all parts
    let total_params: usize = parts.iter().map(|p| p.parameters.len()).sum();

    // Should account for all original parameters (may be distributed)
    assert!(total_params >= 2);
}

// ============================================================================
// Helper Types and Functions (would be in actual implementation)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum QueryFeature {
    BasicSearch,
    StringSearch,
    TokenSearch,
    DateSearch,
    ReferenceSearch,
    NumberSearch,
    QuantitySearch,
    ChainedSearch,
    ReverseChaining,
    Include,
    Revinclude,
    FullTextSearch,
    TerminologySearch,
    Sorting,
    CursorPagination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BackendType {
    Primary,
    Search,
    Graph,
    Terminology,
    Archive,
}

#[derive(Debug)]
struct QueryRouting {
    primary_backend: BackendType,
    auxiliary_backends: HashSet<BackendType>,
}

#[derive(Debug)]
struct QueryPart {
    backend_type: BackendType,
    parameters: Vec<SearchParameter>,
    feature: QueryFeature,
}

fn detect_query_features(query: &SearchQuery) -> HashSet<QueryFeature> {
    let mut features = HashSet::new();

    // Always has basic search
    if !query.parameters().is_empty() || !query.includes().is_empty() {
        features.insert(QueryFeature::BasicSearch);
    }

    for param in query.parameters() {
        // Detect parameter type features
        match param.param_type {
            SearchParamType::String => {
                features.insert(QueryFeature::StringSearch);
            }
            SearchParamType::Token => {
                features.insert(QueryFeature::TokenSearch);
            }
            SearchParamType::Date => {
                features.insert(QueryFeature::DateSearch);
            }
            SearchParamType::Reference => {
                features.insert(QueryFeature::ReferenceSearch);
            }
            SearchParamType::Number => {
                features.insert(QueryFeature::NumberSearch);
            }
            SearchParamType::Quantity => {
                features.insert(QueryFeature::QuantitySearch);
            }
            _ => {}
        }

        // Detect chained search
        if !param.chain.is_empty() {
            features.insert(QueryFeature::ChainedSearch);
        }

        // Detect reverse chaining
        if param.name == "_has" {
            features.insert(QueryFeature::ReverseChaining);
        }

        // Detect full-text search
        if param.name == "_text" || param.name == "_content" {
            features.insert(QueryFeature::FullTextSearch);
        }

        // Detect terminology search
        if param.modifier.as_deref() == Some("below")
            || param.modifier.as_deref() == Some("above")
            || param.modifier.as_deref() == Some("in")
            || param.modifier.as_deref() == Some("not-in")
        {
            features.insert(QueryFeature::TerminologySearch);
        }
    }

    // Detect include/revinclude
    for include in query.includes() {
        match include.include_type {
            IncludeType::Include => {
                features.insert(QueryFeature::Include);
            }
            IncludeType::Revinclude => {
                features.insert(QueryFeature::Revinclude);
            }
        }
    }

    // Detect sorting
    if !query.sorts().is_empty() {
        features.insert(QueryFeature::Sorting);
    }

    features
}

fn route_query(query: &SearchQuery) -> QueryRouting {
    let features = detect_query_features(query);

    let mut routing = QueryRouting {
        primary_backend: BackendType::Primary,
        auxiliary_backends: HashSet::new(),
    };

    // Route chained search to graph
    if features.contains(&QueryFeature::ChainedSearch)
        || features.contains(&QueryFeature::ReverseChaining)
    {
        routing.auxiliary_backends.insert(BackendType::Graph);
    }

    // Route full-text to search
    if features.contains(&QueryFeature::FullTextSearch) {
        routing.auxiliary_backends.insert(BackendType::Search);
    }

    // Route terminology to terminology service
    if features.contains(&QueryFeature::TerminologySearch) {
        routing.auxiliary_backends.insert(BackendType::Terminology);
    }

    routing
}

fn features_to_capabilities(features: &HashSet<QueryFeature>) -> HashSet<BackendCapability> {
    let mut caps = HashSet::new();

    for feature in features {
        match feature {
            QueryFeature::BasicSearch
            | QueryFeature::StringSearch
            | QueryFeature::TokenSearch
            | QueryFeature::ReferenceSearch
            | QueryFeature::NumberSearch
            | QueryFeature::QuantitySearch => {
                caps.insert(BackendCapability::BasicSearch);
            }
            QueryFeature::DateSearch => {
                caps.insert(BackendCapability::DateSearch);
            }
            QueryFeature::ChainedSearch => {
                caps.insert(BackendCapability::ChainedSearch);
            }
            QueryFeature::ReverseChaining => {
                caps.insert(BackendCapability::ReverseChaining);
            }
            QueryFeature::Include => {
                caps.insert(BackendCapability::Include);
            }
            QueryFeature::Revinclude => {
                caps.insert(BackendCapability::Revinclude);
            }
            QueryFeature::FullTextSearch => {
                caps.insert(BackendCapability::FullTextSearch);
            }
            QueryFeature::TerminologySearch => {
                caps.insert(BackendCapability::TerminologySearch);
            }
            QueryFeature::Sorting => {
                caps.insert(BackendCapability::Sorting);
            }
            QueryFeature::CursorPagination => {
                caps.insert(BackendCapability::CursorPagination);
            }
        }
    }

    caps
}

fn decompose_query(query: &SearchQuery) -> Vec<QueryPart> {
    let mut parts = Vec::new();
    let mut primary_params = Vec::new();
    let mut search_params = Vec::new();

    for param in query.parameters() {
        // Full-text goes to search backend
        if param.name == "_text" || param.name == "_content" {
            search_params.push(param.clone());
        } else {
            primary_params.push(param.clone());
        }
    }

    if !primary_params.is_empty() {
        parts.push(QueryPart {
            backend_type: BackendType::Primary,
            parameters: primary_params,
            feature: QueryFeature::BasicSearch,
        });
    }

    if !search_params.is_empty() {
        parts.push(QueryPart {
            backend_type: BackendType::Search,
            parameters: search_params,
            feature: QueryFeature::FullTextSearch,
        });
    }

    parts
}
