/// Error types for FHIR serialization and deserialization.
#[derive(Debug)]
pub enum SerdeError {
    /// JSON serialization or deserialization error
    Json(serde_json::Error),

    /// XML serialization or deserialization error
    Xml(quick_xml::Error),

    /// Custom error message
    Custom(String),
}

impl std::fmt::Display for SerdeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerdeError::Json(e) => write!(f, "JSON error: {}", e),
            SerdeError::Xml(e) => write!(f, "XML error: {}", e),
            SerdeError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for SerdeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SerdeError::Json(e) => Some(e),
            SerdeError::Xml(e) => Some(e),
            SerdeError::Custom(_) => None,
        }
    }
}

impl From<serde_json::Error> for SerdeError {
    fn from(err: serde_json::Error) -> Self {
        SerdeError::Json(err)
    }
}

impl From<quick_xml::Error> for SerdeError {
    fn from(err: quick_xml::Error) -> Self {
        SerdeError::Xml(err)
    }
}

impl From<String> for SerdeError {
    fn from(msg: String) -> Self {
        SerdeError::Custom(msg)
    }
}

impl From<&str> for SerdeError {
    fn from(msg: &str) -> Self {
        SerdeError::Custom(msg.to_string())
    }
}

/// Result type alias for FHIR serialization operations
pub type Result<T> = std::result::Result<T, SerdeError>;
