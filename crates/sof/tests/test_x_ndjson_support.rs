//! Test to verify that 'application/x-ndjson' is supported according to SQL-on-FHIR specification

use helios_sof::ContentType;

#[test]
fn test_x_ndjson_mime_type_support() {
    // Test that 'application/x-ndjson' is accepted
    let result = ContentType::from_string("application/x-ndjson");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ContentType::NdJson);

    // Test that 'application/ndjson' is still accepted for backward compatibility
    let result = ContentType::from_string("application/ndjson");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ContentType::NdJson);

    // Test that shortened format name 'ndjson' is still accepted
    let result = ContentType::from_string("ndjson");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ContentType::NdJson);
}
