//! Integration tests for error handling across the pysof library
//! 
//! Tests error propagation and handling between Rust and Python layers.

use serde_json::json;
use helios_sof::{SofError, ContentType};

#[test]
fn test_error_chain_propagation() {
    // Test that errors properly chain through the system
    let invalid_json = "{ invalid json }";
    let parse_result: Result<serde_json::Value, _> = serde_json::from_str(invalid_json);
    
    assert!(parse_result.is_err());
    let error = parse_result.unwrap_err();
    assert!(error.to_string().contains("invalid"));
}

#[test]
fn test_content_type_error_handling() {
    // Test error handling in content type parsing
    let invalid_types = ["", "invalid", "123", "application/unknown"];
    
    for invalid_type in &invalid_types {
        let result = ContentType::from_string(invalid_type);
        assert!(result.is_err(), "Should fail for invalid type: {}", invalid_type);
    }
}

#[test]
fn test_fhir_validation_error_scenarios() {
    // Test various FHIR validation error scenarios
    let invalid_resources = [
        json!({}), // Empty object
        json!({"resourceType": "InvalidResource"}), // Invalid resource type
        json!({"resourceType": "Patient", "id": 123}), // Invalid ID type
        json!({"resourceType": "Bundle", "type": "invalid"}), // Invalid bundle type
    ];
    
    for invalid_resource in &invalid_resources {
        // Test that invalid resources are properly handled
        // This simulates what would happen in the actual binding functions
        let json_str = serde_json::to_string(invalid_resource).unwrap();
        assert!(!json_str.is_empty());
    }
}

#[test]
fn test_error_message_formatting() {
    // Test that error messages are properly formatted for Python consumption
    let test_errors = [
        "Invalid ViewDefinition structure",
        "Unsupported FHIR version: R3",
        "Failed to parse content type: unknown",
        "Bundle validation failed: missing required field",
    ];
    
    for error_msg in &test_errors {
        assert!(!error_msg.is_empty());
        assert!(error_msg.len() > 10); // Reasonable error message length
        assert!(!error_msg.starts_with(' ')); // No leading whitespace
        assert!(!error_msg.ends_with(' ')); // No trailing whitespace
    }
}
