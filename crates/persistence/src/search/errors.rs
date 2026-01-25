//! Search-specific error types.
//!
//! This module provides error types for search parameter operations:
//! - Loading and parsing SearchParameter resources
//! - Registry operations
//! - FHIRPath extraction
//! - Value conversion
//! - Reindexing operations

use std::fmt;

use serde::{Deserialize, Serialize};

/// Error during SearchParameter loading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoaderError {
    /// Invalid SearchParameter resource structure.
    InvalidResource {
        /// Description of what was invalid.
        message: String,
        /// URL of the problematic parameter, if known.
        url: Option<String>,
    },

    /// Missing required field in SearchParameter.
    MissingField {
        /// Name of the missing field.
        field: String,
        /// URL of the parameter.
        url: Option<String>,
    },

    /// Invalid FHIRPath expression in SearchParameter.
    InvalidExpression {
        /// The invalid expression.
        expression: String,
        /// Parser error message.
        error: String,
    },

    /// Failed to read embedded parameters.
    EmbeddedLoadFailed {
        /// FHIR version attempted.
        version: String,
        /// Error message.
        message: String,
    },

    /// Failed to read config file.
    ConfigLoadFailed {
        /// Path to the config file.
        path: String,
        /// Error message.
        message: String,
    },

    /// Storage error when loading stored parameters.
    StorageError {
        /// Error message.
        message: String,
    },
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoaderError::InvalidResource { message, url } => {
                if let Some(url) = url {
                    write!(f, "Invalid SearchParameter '{}': {}", url, message)
                } else {
                    write!(f, "Invalid SearchParameter: {}", message)
                }
            }
            LoaderError::MissingField { field, url } => {
                if let Some(url) = url {
                    write!(
                        f,
                        "SearchParameter '{}' missing required field '{}'",
                        url, field
                    )
                } else {
                    write!(f, "SearchParameter missing required field '{}'", field)
                }
            }
            LoaderError::InvalidExpression { expression, error } => {
                write!(f, "Invalid FHIRPath expression '{}': {}", expression, error)
            }
            LoaderError::EmbeddedLoadFailed { version, message } => {
                write!(
                    f,
                    "Failed to load embedded {} parameters: {}",
                    version, message
                )
            }
            LoaderError::ConfigLoadFailed { path, message } => {
                write!(f, "Failed to load config from '{}': {}", path, message)
            }
            LoaderError::StorageError { message } => {
                write!(f, "Storage error loading parameters: {}", message)
            }
        }
    }
}

impl std::error::Error for LoaderError {}

/// Error during registry operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegistryError {
    /// Parameter with this URL already exists.
    DuplicateUrl {
        /// The duplicate URL.
        url: String,
    },

    /// Parameter not found in registry.
    NotFound {
        /// The URL or code that was not found.
        identifier: String,
    },

    /// Invalid parameter definition.
    InvalidDefinition {
        /// Description of the problem.
        message: String,
    },

    /// Registry is locked/read-only.
    Locked {
        /// Reason for the lock.
        reason: String,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::DuplicateUrl { url } => {
                write!(f, "SearchParameter with URL '{}' already exists", url)
            }
            RegistryError::NotFound { identifier } => {
                write!(f, "SearchParameter '{}' not found", identifier)
            }
            RegistryError::InvalidDefinition { message } => {
                write!(f, "Invalid SearchParameter definition: {}", message)
            }
            RegistryError::Locked { reason } => {
                write!(f, "Registry is locked: {}", reason)
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// Error during value extraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtractionError {
    /// FHIRPath evaluation failed.
    EvaluationFailed {
        /// The parameter name.
        param_name: String,
        /// The FHIRPath expression.
        expression: String,
        /// Error message.
        error: String,
    },

    /// Value conversion failed.
    ConversionFailed {
        /// The parameter name.
        param_name: String,
        /// The expected type.
        expected_type: String,
        /// What was actually found.
        actual_value: String,
    },

    /// Unsupported value type for indexing.
    UnsupportedType {
        /// The parameter name.
        param_name: String,
        /// The unsupported type.
        value_type: String,
    },

    /// Resource is not a valid JSON object.
    InvalidResource {
        /// Description of the problem.
        message: String,
    },

    /// FHIRPath expression evaluation error.
    FhirPathError {
        /// The FHIRPath expression that failed.
        expression: String,
        /// The error message from the evaluator.
        message: String,
    },

    /// Generic value conversion error.
    ConversionError {
        /// Description of the conversion error.
        message: String,
    },
}

impl fmt::Display for ExtractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtractionError::EvaluationFailed {
                param_name,
                expression,
                error,
            } => {
                write!(
                    f,
                    "Failed to evaluate '{}' for parameter '{}': {}",
                    expression, param_name, error
                )
            }
            ExtractionError::ConversionFailed {
                param_name,
                expected_type,
                actual_value,
            } => {
                write!(
                    f,
                    "Cannot convert '{}' to {} for parameter '{}'",
                    actual_value, expected_type, param_name
                )
            }
            ExtractionError::UnsupportedType {
                param_name,
                value_type,
            } => {
                write!(
                    f,
                    "Unsupported value type '{}' for parameter '{}'",
                    value_type, param_name
                )
            }
            ExtractionError::InvalidResource { message } => {
                write!(f, "Invalid resource: {}", message)
            }
            ExtractionError::FhirPathError {
                expression,
                message,
            } => {
                write!(f, "FHIRPath error evaluating '{}': {}", expression, message)
            }
            ExtractionError::ConversionError { message } => {
                write!(f, "Conversion error: {}", message)
            }
        }
    }
}

impl std::error::Error for ExtractionError {}

/// Error during reindex operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReindexError {
    /// Reindex job not found.
    JobNotFound {
        /// The job ID.
        job_id: String,
    },

    /// Reindex job already running.
    AlreadyRunning {
        /// The existing job ID.
        existing_job_id: String,
    },

    /// Failed to process resource during reindex.
    ProcessingFailed {
        /// Resource type.
        resource_type: String,
        /// Resource ID.
        resource_id: String,
        /// Error message.
        error: String,
    },

    /// Storage error during reindex.
    StorageError {
        /// Error message.
        message: String,
    },

    /// Reindex was cancelled.
    Cancelled {
        /// Job ID that was cancelled.
        job_id: String,
    },
}

impl fmt::Display for ReindexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReindexError::JobNotFound { job_id } => {
                write!(f, "Reindex job '{}' not found", job_id)
            }
            ReindexError::AlreadyRunning { existing_job_id } => {
                write!(
                    f,
                    "Reindex already running with job ID '{}'",
                    existing_job_id
                )
            }
            ReindexError::ProcessingFailed {
                resource_type,
                resource_id,
                error,
            } => {
                write!(
                    f,
                    "Failed to reindex {}/{}: {}",
                    resource_type, resource_id, error
                )
            }
            ReindexError::StorageError { message } => {
                write!(f, "Storage error during reindex: {}", message)
            }
            ReindexError::Cancelled { job_id } => {
                write!(f, "Reindex job '{}' was cancelled", job_id)
            }
        }
    }
}

impl std::error::Error for ReindexError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loader_error_display() {
        let err = LoaderError::MissingField {
            field: "expression".to_string(),
            url: Some("http://example.org/SearchParameter/test".to_string()),
        };
        assert!(err.to_string().contains("expression"));
        assert!(err.to_string().contains("test"));
    }

    #[test]
    fn test_registry_error_display() {
        let err = RegistryError::DuplicateUrl {
            url: "http://example.org/sp".to_string(),
        };
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_extraction_error_display() {
        let err = ExtractionError::EvaluationFailed {
            param_name: "name".to_string(),
            expression: "Patient.name".to_string(),
            error: "syntax error".to_string(),
        };
        assert!(err.to_string().contains("name"));
        assert!(err.to_string().contains("Patient.name"));
    }

    #[test]
    fn test_reindex_error_display() {
        let err = ReindexError::ProcessingFailed {
            resource_type: "Patient".to_string(),
            resource_id: "123".to_string(),
            error: "database error".to_string(),
        };
        assert!(err.to_string().contains("Patient/123"));
    }
}
