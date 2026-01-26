//! Configuration analysis for composite storage.
//!
//! This module provides analysis of composite storage configurations,
//! identifying issues, gaps, and opportunities for optimization.

use std::collections::{HashMap, HashSet};

use crate::composite::{BackendRole, CompositeConfig, QueryAnalyzer, QueryFeature, QueryRouter};
use crate::core::{BackendCapability, BackendKind};
use crate::types::SearchQuery;

/// Analyzer for composite storage configurations.
pub struct ConfigurationAnalyzer {
    /// Query analyzer for capability detection.
    query_analyzer: QueryAnalyzer,
}

impl ConfigurationAnalyzer {
    /// Creates a new configuration analyzer.
    pub fn new() -> Self {
        Self {
            query_analyzer: QueryAnalyzer::new(),
        }
    }

    /// Analyzes a configuration and returns detailed results.
    pub fn analyze(&self, config: &CompositeConfig) -> AnalysisResult {
        let capability_coverage = self.analyze_capability_coverage(config);
        let gap_analysis = self.analyze_gaps(config, &capability_coverage);
        let redundancy_report = self.analyze_redundancy(config);
        let issues = self.find_issues(config, &capability_coverage, &redundancy_report);
        let recommendations = self.generate_recommendations(config, &issues, &gap_analysis);

        AnalysisResult {
            is_valid: issues.iter().all(|i| i.severity != IssueSeverity::Error),
            capability_coverage,
            gap_analysis,
            redundancy_report,
            issues,
            recommendations,
        }
    }

    /// Validates a configuration for correctness.
    pub fn validate(&self, config: &CompositeConfig) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Check for primary backend
        let primaries: Vec<_> = config
            .backends
            .iter()
            .filter(|b| b.role == BackendRole::Primary)
            .collect();

        if primaries.is_empty() {
            errors.push("Configuration must have exactly one primary backend".to_string());
        } else if primaries.len() > 1 {
            errors.push(format!(
                "Configuration has {} primary backends (expected 1)",
                primaries.len()
            ));
        }

        // Check for duplicate backend IDs
        let mut seen_ids = HashSet::new();
        for backend in &config.backends {
            if !seen_ids.insert(&backend.id) {
                errors.push(format!("Duplicate backend ID: {}", backend.id));
            }
        }

        // Check for valid failover targets
        let backend_ids: HashSet<_> = config.backends.iter().map(|b| &b.id).collect();
        for backend in &config.backends {
            if let Some(ref failover) = backend.failover_to {
                if !backend_ids.contains(failover) {
                    errors.push(format!(
                        "Backend '{}' has invalid failover target: '{}'",
                        backend.id, failover
                    ));
                }
            }
        }

        // Check for circular failover references
        if let Some(cycle) = self.find_failover_cycle(config) {
            errors.push(format!("Circular failover chain detected: {}", cycle));
        }

        // Warnings for common issues
        if config.backends.iter().filter(|b| b.enabled).count() == 1 {
            warnings.push("Only one backend enabled - no redundancy".to_string());
        }

        // Check for disabled backends that are failover targets
        for backend in &config.backends {
            if let Some(ref failover) = backend.failover_to {
                if let Some(target) = config.backends.iter().find(|b| &b.id == failover) {
                    if !target.enabled {
                        warnings.push(format!(
                            "Failover target '{}' for backend '{}' is disabled",
                            failover, backend.id
                        ));
                    }
                }
            }
        }

        ValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
        }
    }

    /// Simulates query routing for a given query.
    pub fn simulate_query(&self, query: &SearchQuery, config: &CompositeConfig) -> QuerySimulation {
        let analysis = self.query_analyzer.analyze(query);
        let router = QueryRouter::new(config.clone());
        let routing = router.route(query);

        let estimated_cost = match &routing {
            Ok(decision) => {
                // Estimate based on complexity and target backend
                let base_cost = match decision.primary_target.as_str() {
                    t if config
                        .backends
                        .iter()
                        .find(|b| b.id == t)
                        .map(|b| b.kind == BackendKind::Sqlite)
                        .unwrap_or(false) =>
                    {
                        1.0
                    }
                    t if config
                        .backends
                        .iter()
                        .find(|b| b.id == t)
                        .map(|b| b.kind == BackendKind::Elasticsearch)
                        .unwrap_or(false) =>
                    {
                        2.0
                    }
                    _ => 1.5,
                };
                base_cost * (1.0 + analysis.complexity_score as f64 * 0.1)
            }
            Err(_) => 10.0, // High cost for failed routing
        };

        QuerySimulation {
            query_features: analysis.features.iter().cloned().collect(),
            complexity_score: analysis.complexity_score,
            routing_decision: routing.as_ref().map(|d| d.primary_target.clone()).ok(),
            auxiliary_targets: routing
                .as_ref()
                .map(|d| d.auxiliary_targets.values().cloned().collect())
                .unwrap_or_default(),
            estimated_cost,
            routing_error: routing.as_ref().err().map(|e| format!("{:?}", e)),
        }
    }

    /// Analyzes capability coverage across all backends.
    fn analyze_capability_coverage(&self, config: &CompositeConfig) -> CapabilityCoverage {
        let mut covered_capabilities = HashSet::new();
        let mut capability_backends: HashMap<BackendCapability, Vec<String>> = HashMap::new();

        for backend in &config.backends {
            if !backend.enabled {
                continue;
            }

            let capabilities = backend.effective_capabilities();
            for cap in capabilities {
                covered_capabilities.insert(cap);
                capability_backends
                    .entry(cap)
                    .or_default()
                    .push(backend.id.clone());
            }
        }

        // All potentially desirable capabilities
        let all_capabilities: HashSet<_> = [
            BackendCapability::Crud,
            BackendCapability::Versioning,
            BackendCapability::BasicSearch,
            BackendCapability::InstanceHistory,
            BackendCapability::TypeHistory,
            BackendCapability::Transactions,
            BackendCapability::ChainedSearch,
            BackendCapability::FullTextSearch,
            BackendCapability::TerminologySearch,
            BackendCapability::Include,
            BackendCapability::Revinclude,
        ]
        .into_iter()
        .collect();

        let missing_capabilities: HashSet<_> = all_capabilities
            .difference(&covered_capabilities)
            .cloned()
            .collect();

        CapabilityCoverage {
            covered: covered_capabilities,
            missing: missing_capabilities,
            capability_backends,
            coverage_percentage: 0.0, // Calculated below
        }
    }

    /// Analyzes gaps in the configuration.
    fn analyze_gaps(&self, _config: &CompositeConfig, coverage: &CapabilityCoverage) -> GapAnalysis {
        let mut feature_gaps = Vec::new();

        // Check for missing critical capabilities
        let critical = [BackendCapability::Crud, BackendCapability::BasicSearch];

        for cap in critical {
            if coverage.missing.contains(&cap) {
                feature_gaps.push(FeatureGap {
                    capability: cap,
                    impact: GapImpact::High,
                    suggestion: format!("Add a backend that supports {:?}", cap),
                });
            }
        }

        // Check for missing advanced features
        let advanced = [
            BackendCapability::ChainedSearch,
            BackendCapability::FullTextSearch,
            BackendCapability::TerminologySearch,
        ];

        for cap in advanced {
            if coverage.missing.contains(&cap) {
                feature_gaps.push(FeatureGap {
                    capability: cap,
                    impact: GapImpact::Medium,
                    suggestion: format!("Consider adding a specialized backend for {:?}", cap),
                });
            }
        }

        // Estimate completeness
        let total_features = critical.len() + advanced.len();
        let covered_features = total_features - feature_gaps.len();
        let completeness = covered_features as f64 / total_features as f64;

        GapAnalysis {
            feature_gaps,
            completeness_score: completeness,
            recommendations: Vec::new(), // Filled later
        }
    }

    /// Analyzes redundancy in the configuration.
    fn analyze_redundancy(&self, config: &CompositeConfig) -> RedundancyReport {
        let mut overlapping_capabilities: HashMap<BackendCapability, Vec<String>> = HashMap::new();
        let mut redundant_backends = Vec::new();

        // Find capabilities provided by multiple backends
        for backend in &config.backends {
            if !backend.enabled {
                continue;
            }

            let capabilities = backend.effective_capabilities();
            for cap in capabilities {
                overlapping_capabilities
                    .entry(cap)
                    .or_default()
                    .push(backend.id.clone());
            }
        }

        // Filter to only those with multiple providers
        overlapping_capabilities.retain(|_, v| v.len() > 1);

        // Find backends that are fully redundant (all capabilities covered by others)
        for backend in &config.backends {
            if !backend.enabled || backend.role == BackendRole::Primary {
                continue;
            }

            let capabilities = backend.effective_capabilities();
            let all_covered = capabilities.iter().all(|cap| {
                overlapping_capabilities
                    .get(cap)
                    .map(|backends| backends.iter().any(|b| b != &backend.id))
                    .unwrap_or(false)
            });

            if all_covered && !capabilities.is_empty() {
                redundant_backends.push(RedundantBackend {
                    backend_id: backend.id.clone(),
                    covered_by: overlapping_capabilities
                        .values()
                        .flatten()
                        .filter(|b| *b != &backend.id)
                        .cloned()
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect(),
                    reason: "All capabilities provided by other backends".to_string(),
                });
            }
        }

        // Calculate redundancy score
        let total_capability_assignments: usize =
            overlapping_capabilities.values().map(|v| v.len()).sum();
        let unique_capabilities = overlapping_capabilities.len();
        let redundancy_score = if unique_capabilities > 0 {
            (total_capability_assignments - unique_capabilities) as f64
                / total_capability_assignments as f64
        } else {
            0.0
        };

        RedundancyReport {
            overlapping_capabilities,
            redundant_backends,
            redundancy_score,
        }
    }

    /// Finds issues in the configuration.
    fn find_issues(
        &self,
        config: &CompositeConfig,
        coverage: &CapabilityCoverage,
        redundancy: &RedundancyReport,
    ) -> Vec<ConfigurationIssue> {
        let mut issues = Vec::new();

        // Critical: Missing primary
        let primary_count = config
            .backends
            .iter()
            .filter(|b| b.role == BackendRole::Primary)
            .count();

        if primary_count == 0 {
            issues.push(ConfigurationIssue {
                severity: IssueSeverity::Error,
                category: IssueCategory::MissingRequirement,
                message: "No primary backend configured".to_string(),
                suggestion: Some("Add a backend with role Primary".to_string()),
            });
        } else if primary_count > 1 {
            issues.push(ConfigurationIssue {
                severity: IssueSeverity::Error,
                category: IssueCategory::Configuration,
                message: "Multiple primary backends configured".to_string(),
                suggestion: Some("Only one backend should have role Primary".to_string()),
            });
        }

        // Missing critical capabilities
        for cap in &coverage.missing {
            let severity = match cap {
                BackendCapability::Crud => IssueSeverity::Error,
                BackendCapability::BasicSearch => IssueSeverity::Warning,
                _ => IssueSeverity::Info,
            };

            issues.push(ConfigurationIssue {
                severity,
                category: IssueCategory::MissingCapability,
                message: format!("Missing capability: {:?}", cap),
                suggestion: Some(format!("Add a backend that supports {:?}", cap)),
            });
        }

        // High redundancy warning
        if redundancy.redundancy_score > 0.5 {
            issues.push(ConfigurationIssue {
                severity: IssueSeverity::Warning,
                category: IssueCategory::Redundancy,
                message: format!(
                    "High redundancy detected ({:.0}%)",
                    redundancy.redundancy_score * 100.0
                ),
                suggestion: Some("Consider consolidating backends".to_string()),
            });
        }

        issues
    }

    /// Generates recommendations based on analysis.
    fn generate_recommendations(
        &self,
        _config: &CompositeConfig,
        issues: &[ConfigurationIssue],
        gap_analysis: &GapAnalysis,
    ) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        // Recommendations from issues
        for issue in issues {
            if let Some(ref suggestion) = issue.suggestion {
                let priority = match issue.severity {
                    IssueSeverity::Error => RecommendationPriority::Critical,
                    IssueSeverity::Warning => RecommendationPriority::High,
                    IssueSeverity::Info => RecommendationPriority::Medium,
                };

                recommendations.push(Recommendation {
                    priority,
                    title: format!("Fix: {}", issue.message),
                    description: suggestion.clone(),
                    impact: format!("Resolves {:?} issue", issue.category),
                });
            }
        }

        // Recommendations from gaps
        for gap in &gap_analysis.feature_gaps {
            recommendations.push(Recommendation {
                priority: match gap.impact {
                    GapImpact::High => RecommendationPriority::High,
                    GapImpact::Medium => RecommendationPriority::Medium,
                    GapImpact::Low => RecommendationPriority::Low,
                },
                title: format!("Add support for {:?}", gap.capability),
                description: gap.suggestion.clone(),
                impact: format!("Enables {:?} operations", gap.capability),
            });
        }

        recommendations
    }

    /// Finds circular failover chains.
    fn find_failover_cycle(&self, config: &CompositeConfig) -> Option<String> {
        for backend in &config.backends {
            let mut visited = HashSet::new();
            let mut current = backend.id.clone();

            while let Some(next) = config
                .backends
                .iter()
                .find(|b| b.id == current)
                .and_then(|b| b.failover_to.clone())
            {
                if !visited.insert(current.clone()) {
                    return Some(format!("{} -> {}", current, next));
                }
                current = next;
            }
        }
        None
    }
}

impl Default for ConfigurationAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of configuration analysis.
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Whether the configuration is valid.
    pub is_valid: bool,

    /// Capability coverage analysis.
    pub capability_coverage: CapabilityCoverage,

    /// Gap analysis.
    pub gap_analysis: GapAnalysis,

    /// Redundancy report.
    pub redundancy_report: RedundancyReport,

    /// Configuration issues found.
    pub issues: Vec<ConfigurationIssue>,

    /// Recommendations for improvement.
    pub recommendations: Vec<Recommendation>,
}

/// Result of configuration validation.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the configuration is valid.
    pub is_valid: bool,

    /// Validation errors.
    pub errors: Vec<String>,

    /// Validation warnings.
    pub warnings: Vec<String>,
}

/// Analysis of capability coverage.
#[derive(Debug, Clone)]
pub struct CapabilityCoverage {
    /// Capabilities covered by at least one backend.
    pub covered: HashSet<BackendCapability>,

    /// Capabilities not covered by any backend.
    pub missing: HashSet<BackendCapability>,

    /// Map of capability to backends providing it.
    pub capability_backends: HashMap<BackendCapability, Vec<String>>,

    /// Overall coverage percentage.
    pub coverage_percentage: f64,
}

/// Analysis of capability gaps.
#[derive(Debug, Clone)]
pub struct GapAnalysis {
    /// Feature gaps found.
    pub feature_gaps: Vec<FeatureGap>,

    /// Completeness score (0.0 to 1.0).
    pub completeness_score: f64,

    /// Recommendations for filling gaps.
    pub recommendations: Vec<String>,
}

/// A feature gap in the configuration.
#[derive(Debug, Clone)]
pub struct FeatureGap {
    /// Missing capability.
    pub capability: BackendCapability,

    /// Impact of missing this capability.
    pub impact: GapImpact,

    /// Suggestion for addressing the gap.
    pub suggestion: String,
}

/// Impact level of a feature gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapImpact {
    /// Critical missing feature.
    High,
    /// Important but not critical.
    Medium,
    /// Nice to have.
    Low,
}

/// Report on configuration redundancy.
#[derive(Debug, Clone)]
pub struct RedundancyReport {
    /// Capabilities with multiple providers.
    pub overlapping_capabilities: HashMap<BackendCapability, Vec<String>>,

    /// Potentially redundant backends.
    pub redundant_backends: Vec<RedundantBackend>,

    /// Overall redundancy score (0.0 to 1.0).
    pub redundancy_score: f64,
}

/// A potentially redundant backend.
#[derive(Debug, Clone)]
pub struct RedundantBackend {
    /// Backend identifier.
    pub backend_id: String,

    /// Backends that cover this one's capabilities.
    pub covered_by: Vec<String>,

    /// Reason for redundancy.
    pub reason: String,
}

/// A configuration issue.
#[derive(Debug, Clone)]
pub struct ConfigurationIssue {
    /// Issue severity.
    pub severity: IssueSeverity,

    /// Issue category.
    pub category: IssueCategory,

    /// Issue message.
    pub message: String,

    /// Suggestion for fixing the issue.
    pub suggestion: Option<String>,
}

/// Severity of a configuration issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueSeverity {
    /// Configuration will not work.
    Error,
    /// Configuration may have problems.
    Warning,
    /// Informational finding.
    Info,
}

/// Category of configuration issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueCategory {
    /// Missing requirement.
    MissingRequirement,
    /// Missing capability.
    MissingCapability,
    /// Configuration error.
    Configuration,
    /// Redundancy issue.
    Redundancy,
}

/// A recommendation for improvement.
#[derive(Debug, Clone)]
pub struct Recommendation {
    /// Priority of the recommendation.
    pub priority: RecommendationPriority,

    /// Recommendation title.
    pub title: String,

    /// Detailed description.
    pub description: String,

    /// Expected impact.
    pub impact: String,
}

/// Priority of a recommendation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecommendationPriority {
    /// Must address immediately.
    Critical,
    /// Should address soon.
    High,
    /// Worth considering.
    Medium,
    /// Nice to have.
    Low,
}

/// Result of query simulation.
#[derive(Debug, Clone)]
pub struct QuerySimulation {
    /// Detected query features.
    pub query_features: Vec<QueryFeature>,

    /// Query complexity score.
    pub complexity_score: u8,

    /// Primary routing target.
    pub routing_decision: Option<String>,

    /// Auxiliary routing targets.
    pub auxiliary_targets: Vec<String>,

    /// Estimated query cost.
    pub estimated_cost: f64,

    /// Routing error (if any).
    pub routing_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composite::{BackendEntry, CompositeConfigBuilder};

    #[test]
    fn test_analyzer_creation() {
        let analyzer = ConfigurationAnalyzer::new();
        // Just verify we can create an analyzer and run analysis
        let _analysis = analyzer
            .query_analyzer
            .analyze(&SearchQuery::new("Patient"));
    }

    #[test]
    fn test_validation_no_primary() {
        let analyzer = ConfigurationAnalyzer::new();
        let config = CompositeConfig {
            backends: vec![BackendEntry::new(
                "secondary",
                BackendRole::Search,
                BackendKind::Elasticsearch,
            )],
            ..Default::default()
        };

        let result = analyzer.validate(&config);
        assert!(!result.is_valid);
        assert!(result.errors.iter().any(|e| e.contains("primary")));
    }

    #[test]
    fn test_validation_valid_config() {
        let analyzer = ConfigurationAnalyzer::new();
        let config = CompositeConfigBuilder::new()
            .primary("sqlite", BackendKind::Sqlite)
            .build()
            .unwrap();

        let result = analyzer.validate(&config);
        assert!(result.is_valid);
    }

    #[test]
    fn test_analysis_capability_coverage() {
        let analyzer = ConfigurationAnalyzer::new();
        let config = CompositeConfigBuilder::new()
            .primary("sqlite", BackendKind::Sqlite)
            .search_backend("es", BackendKind::Elasticsearch)
            .build()
            .unwrap();

        let result = analyzer.analyze(&config);
        assert!(!result.capability_coverage.covered.is_empty());
    }

    #[test]
    fn test_query_simulation() {
        let analyzer = ConfigurationAnalyzer::new();
        let config = CompositeConfigBuilder::new()
            .primary("sqlite", BackendKind::Sqlite)
            .build()
            .unwrap();

        let query = SearchQuery::new("Patient");
        let simulation = analyzer.simulate_query(&query, &config);

        assert!(simulation.estimated_cost > 0.0);
    }
}
