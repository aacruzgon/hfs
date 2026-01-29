//! JSON test specification loader.
//!
//! Loads and parses JSON test specifications for declarative testing.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

/// A test specification file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestSpec {
    /// Name of the test suite.
    pub name: String,

    /// Description of the test suite.
    #[serde(default)]
    pub description: String,

    /// Tags for filtering tests.
    #[serde(default)]
    pub tags: Vec<String>,

    /// Backend configuration for this spec.
    #[serde(default)]
    pub backends: BackendFilter,

    /// FHIR versions this spec applies to.
    #[serde(default)]
    pub fhir_versions: Vec<String>,

    /// Setup resources to create before tests.
    #[serde(default)]
    pub setup: TestSetup,

    /// The individual test cases.
    #[serde(default)]
    pub tests: Vec<TestCase>,
}

/// Backend filter configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BackendFilter {
    /// Backends to include (empty = all).
    #[serde(default)]
    pub include: Vec<String>,

    /// Backends to exclude.
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl BackendFilter {
    /// Checks if a backend should be used.
    pub fn should_run(&self, backend: &str) -> bool {
        // If include is specified and non-empty, backend must be in it
        if !self.include.is_empty() && !self.include.contains(&backend.to_string()) {
            if !self.include.contains(&"all".to_string()) {
                return false;
            }
        }

        // Backend must not be in exclude list
        !self.exclude.contains(&backend.to_string())
    }
}

/// Test setup configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TestSetup {
    /// Resources to create before tests.
    #[serde(default)]
    pub resources: Vec<Value>,
}

/// A single test case.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestCase {
    /// Test name.
    pub name: String,

    /// Test description.
    #[serde(default)]
    pub description: String,

    /// Tags for filtering.
    #[serde(default)]
    pub tags: Vec<String>,

    /// Whether this test is skipped.
    #[serde(default)]
    pub skip: bool,

    /// The request to make.
    pub request: TestRequest,

    /// Expected response.
    pub expect: TestExpectation,
}

/// A test request.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestRequest {
    /// HTTP method.
    pub method: String,

    /// Request path.
    pub path: String,

    /// Request headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Request body (for POST/PUT/PATCH).
    #[serde(default)]
    pub body: Option<Value>,
}

/// Expected response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestExpectation {
    /// Expected status code.
    pub status: u16,

    /// Expected headers (with matchers).
    #[serde(default)]
    pub headers: HashMap<String, HeaderMatcher>,

    /// Expected body (partial match).
    #[serde(default)]
    pub body: Option<Value>,
}

/// Header value matcher.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum HeaderMatcher {
    /// Exact match.
    Exact(String),
    /// Pattern match.
    Pattern {
        #[serde(rename = "matches")]
        pattern: String,
    },
    /// Contains match.
    Contains { contains: String },
}

impl HeaderMatcher {
    /// Checks if a header value matches.
    pub fn matches(&self, value: &str) -> bool {
        match self {
            HeaderMatcher::Exact(expected) => value == expected,
            HeaderMatcher::Pattern { pattern } => regex::Regex::new(pattern)
                .map(|re| re.is_match(value))
                .unwrap_or(false),
            HeaderMatcher::Contains { contains } => value.contains(contains),
        }
    }
}

/// Loads a test spec from a file.
pub fn load_spec(path: &Path) -> Result<TestSpec, SpecLoadError> {
    let content =
        std::fs::read_to_string(path).map_err(|e| SpecLoadError::IoError(e.to_string()))?;

    serde_json::from_str(&content).map_err(|e| SpecLoadError::ParseError(e.to_string()))
}

/// Discovers all spec files in a directory.
pub fn discover_specs(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut specs = Vec::new();

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().map(|e| e == "json").unwrap_or(false) {
                specs.push(path);
            } else if path.is_dir() {
                specs.extend(discover_specs(&path));
            }
        }
    }

    specs
}

/// Error loading a spec file.
#[derive(Debug)]
pub enum SpecLoadError {
    /// IO error reading the file.
    IoError(String),
    /// JSON parsing error.
    ParseError(String),
}

impl std::fmt::Display for SpecLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecLoadError::IoError(msg) => write!(f, "IO error: {}", msg),
            SpecLoadError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for SpecLoadError {}

/// Result of running a test spec.
#[derive(Debug)]
pub struct SpecResult {
    /// The spec name.
    pub spec_name: String,
    /// Results for each test case.
    pub test_results: Vec<TestCaseResult>,
}

impl SpecResult {
    /// Returns true if all tests passed.
    pub fn all_passed(&self) -> bool {
        self.test_results.iter().all(|r| r.passed)
    }

    /// Returns the number of passed tests.
    pub fn passed_count(&self) -> usize {
        self.test_results.iter().filter(|r| r.passed).count()
    }

    /// Returns the number of failed tests.
    pub fn failed_count(&self) -> usize {
        self.test_results.iter().filter(|r| !r.passed).count()
    }
}

/// Result of a single test case.
#[derive(Debug)]
pub struct TestCaseResult {
    /// Test name.
    pub test_name: String,
    /// Whether the test passed.
    pub passed: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_filter_include() {
        let filter = BackendFilter {
            include: vec!["sqlite".to_string()],
            exclude: vec![],
        };

        assert!(filter.should_run("sqlite"));
        assert!(!filter.should_run("postgres"));
    }

    #[test]
    fn test_backend_filter_exclude() {
        let filter = BackendFilter {
            include: vec![],
            exclude: vec!["neo4j".to_string()],
        };

        assert!(filter.should_run("sqlite"));
        assert!(filter.should_run("postgres"));
        assert!(!filter.should_run("neo4j"));
    }

    #[test]
    fn test_backend_filter_all() {
        let filter = BackendFilter {
            include: vec!["all".to_string()],
            exclude: vec!["neo4j".to_string()],
        };

        assert!(filter.should_run("sqlite"));
        assert!(filter.should_run("postgres"));
        assert!(!filter.should_run("neo4j"));
    }

    #[test]
    fn test_header_matcher_exact() {
        let matcher = HeaderMatcher::Exact("application/fhir+json".to_string());
        assert!(matcher.matches("application/fhir+json"));
        assert!(!matcher.matches("application/json"));
    }

    #[test]
    fn test_header_matcher_contains() {
        let matcher = HeaderMatcher::Contains {
            contains: "fhir+json".to_string(),
        };
        assert!(matcher.matches("application/fhir+json"));
        assert!(matcher.matches("application/fhir+json; charset=utf-8"));
        assert!(!matcher.matches("application/json"));
    }

    #[test]
    fn test_header_matcher_pattern() {
        let matcher = HeaderMatcher::Pattern {
            pattern: r#"W/"\d+""#.to_string(),
        };
        assert!(matcher.matches("W/\"1\""));
        assert!(matcher.matches("W/\"123\""));
        assert!(!matcher.matches("W/\"abc\""));
    }
}
