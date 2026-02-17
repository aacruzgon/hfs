/// Error types for FHIR serialization and deserialization.
#[derive(Debug)]
pub enum SerdeError {
    /// JSON serialization or deserialization error
    Json(serde_json::Error),

    /// XML serialization or deserialization error
    #[cfg(feature = "xml")]
    Xml(quick_xml::Error),

    /// IO error during serialization/deserialization
    Io(std::io::Error),

    /// Custom error message
    Custom(String),
}

impl std::fmt::Display for SerdeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerdeError::Json(e) => write!(f, "JSON error: {}", e),
            #[cfg(feature = "xml")]
            SerdeError::Xml(e) => write!(f, "XML error: {}", e),
            SerdeError::Io(e) => write!(f, "IO error: {}", e),
            SerdeError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for SerdeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SerdeError::Json(e) => Some(e),
            #[cfg(feature = "xml")]
            SerdeError::Xml(e) => Some(e),
            SerdeError::Io(e) => Some(e),
            SerdeError::Custom(_) => None,
        }
    }
}

impl From<serde_json::Error> for SerdeError {
    fn from(err: serde_json::Error) -> Self {
        SerdeError::Json(err)
    }
}

#[cfg(feature = "xml")]
impl From<quick_xml::Error> for SerdeError {
    fn from(err: quick_xml::Error) -> Self {
        SerdeError::Xml(err)
    }
}

impl From<std::io::Error> for SerdeError {
    fn from(err: std::io::Error) -> Self {
        SerdeError::Io(err)
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

// Implement serde::ser::Error for serialization
impl serde::ser::Error for SerdeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        SerdeError::Custom(msg.to_string())
    }
}

// Implement serde::de::Error for deserialization (will be needed in Phase 4)
impl serde::de::Error for SerdeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        SerdeError::Custom(msg.to_string())
    }
}

/// Result type alias for FHIR serialization operations
pub type Result<T> = std::result::Result<T, SerdeError>;
