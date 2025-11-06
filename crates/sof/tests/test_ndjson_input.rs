use helios_sof::data_source::{DataSource, UniversalDataSource};
use helios_sof::SofBundle;
use std::io::Write;
use tempfile::NamedTempFile;

/// Test loading NDJSON file with multiple Patient resources
#[tokio::test]
async fn test_ndjson_multiple_patients() {
    let data_source = UniversalDataSource::new();

    // Create NDJSON content with multiple patients
    let ndjson_content = r#"{"resourceType": "Patient", "id": "patient-1", "gender": "male"}
{"resourceType": "Patient", "id": "patient-2", "gender": "female"}
{"resourceType": "Patient", "id": "patient-3", "gender": "other"}"#;

    // Create temporary .ndjson file
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    // Rename to have .ndjson extension
    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());

    // Load and verify
    let result = data_source.load(&file_url).await;
    assert!(result.is_ok(), "Failed to load NDJSON file: {:?}", result);

    #[cfg(feature = "R4")]
    match result.unwrap() {
        SofBundle::R4(bundle) => {
            let entries = bundle.entry.as_ref().unwrap();
            assert_eq!(entries.len(), 3, "Expected 3 resources in bundle");
        }
        #[cfg(feature = "R4B")]
        SofBundle::R4B(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R5")]
        SofBundle::R5(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R6")]
        SofBundle::R6(_) => panic!("Expected R4 bundle"),
    }

    // Cleanup
    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test loading NDJSON with mixed resource types
#[tokio::test]
async fn test_ndjson_mixed_resources() {
    let data_source = UniversalDataSource::new();

    let ndjson_content = r#"{"resourceType": "Patient", "id": "p1"}
{"resourceType": "Observation", "id": "obs1", "status": "final", "code": {"text": "Test"}}
{"resourceType": "Condition", "id": "cond1", "clinicalStatus": {"text": "active"}, "code": {"text": "Test"}}"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());
    let result = data_source.load(&file_url).await;

    assert!(result.is_ok(), "Failed to load mixed NDJSON: {:?}", result);

    #[cfg(feature = "R4")]
    match result.unwrap() {
        SofBundle::R4(bundle) => {
            assert_eq!(bundle.entry.as_ref().unwrap().len(), 3);
        }
        #[cfg(feature = "R4B")]
        SofBundle::R4B(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R5")]
        SofBundle::R5(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R6")]
        SofBundle::R6(_) => panic!("Expected R4 bundle"),
    }

    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test NDJSON with empty lines (should be ignored)
#[tokio::test]
async fn test_ndjson_with_empty_lines() {
    let data_source = UniversalDataSource::new();

    let ndjson_content = r#"{"resourceType": "Patient", "id": "p1"}

{"resourceType": "Patient", "id": "p2"}

{"resourceType": "Patient", "id": "p3"}
"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());
    let result = data_source.load(&file_url).await;

    assert!(result.is_ok());

    #[cfg(feature = "R4")]
    match result.unwrap() {
        SofBundle::R4(bundle) => {
            // Empty lines should be ignored
            assert_eq!(bundle.entry.as_ref().unwrap().len(), 3);
        }
        #[cfg(feature = "R4B")]
        SofBundle::R4B(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R5")]
        SofBundle::R5(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R6")]
        SofBundle::R6(_) => panic!("Expected R4 bundle"),
    }

    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test content-based NDJSON detection (no .ndjson extension)
#[tokio::test]
async fn test_ndjson_content_detection() {
    let data_source = UniversalDataSource::new();

    // Create NDJSON content but use .json extension
    let ndjson_content = r#"{"resourceType": "Patient", "id": "p1"}
{"resourceType": "Patient", "id": "p2"}"#;

    let mut temp_file = NamedTempFile::with_suffix(".json").unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let file_url = format!("file://{}", temp_file.path().to_string_lossy());
    let result = data_source.load(&file_url).await;

    // Should succeed via content-based fallback
    assert!(result.is_ok(), "Content-based NDJSON detection failed");

    #[cfg(feature = "R4")]
    match result.unwrap() {
        SofBundle::R4(bundle) => {
            assert_eq!(bundle.entry.as_ref().unwrap().len(), 2);
        }
        #[cfg(feature = "R4B")]
        SofBundle::R4B(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R5")]
        SofBundle::R5(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R6")]
        SofBundle::R6(_) => panic!("Expected R4 bundle"),
    }
}

/// Test NDJSON with some invalid lines (should warn but process valid ones)
#[tokio::test]
async fn test_ndjson_partial_invalid() {
    let data_source = UniversalDataSource::new();

    let ndjson_content = r#"{"resourceType": "Patient", "id": "p1"}
{invalid json line}
{"resourceType": "Patient", "id": "p2"}
{"missing": "resourceType"}
{"resourceType": "Patient", "id": "p3"}"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());
    let result = data_source.load(&file_url).await;

    // Should succeed with 3 valid resources (and warnings for 2 invalid lines)
    assert!(result.is_ok());

    #[cfg(feature = "R4")]
    match result.unwrap() {
        SofBundle::R4(bundle) => {
            assert_eq!(bundle.entry.as_ref().unwrap().len(), 3);
        }
        #[cfg(feature = "R4B")]
        SofBundle::R4B(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R5")]
        SofBundle::R5(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R6")]
        SofBundle::R6(_) => panic!("Expected R4 bundle"),
    }

    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test NDJSON with all invalid lines (should fail)
#[tokio::test]
async fn test_ndjson_all_invalid() {
    let data_source = UniversalDataSource::new();

    let ndjson_content = r#"{invalid json}
{also: invalid}
not even json"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());
    let result = data_source.load(&file_url).await;

    // Should fail - no valid resources
    assert!(result.is_err());

    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test empty NDJSON file
#[tokio::test]
async fn test_ndjson_empty_file() {
    let data_source = UniversalDataSource::new();

    let ndjson_content = "";

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());
    let result = data_source.load(&file_url).await;

    // Should fail - empty content
    assert!(result.is_err());

    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test NDJSON file with only whitespace
#[tokio::test]
async fn test_ndjson_only_whitespace() {
    let data_source = UniversalDataSource::new();

    let ndjson_content = "   \n  \n   \n";

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file_mut().write_all(ndjson_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let temp_path = temp_file.path().to_path_buf();
    let ndjson_path = temp_path.with_extension("ndjson");
    std::fs::copy(&temp_path, &ndjson_path).unwrap();

    let file_url = format!("file://{}", ndjson_path.to_string_lossy());
    let result = data_source.load(&file_url).await;

    // Should fail - no content
    assert!(result.is_err());

    let _ = std::fs::remove_file(&ndjson_path);
}

/// Test that regular JSON Bundle still works (regression test)
#[tokio::test]
async fn test_json_bundle_still_works() {
    let data_source = UniversalDataSource::new();

    let bundle_json = r#"{
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{
            "resource": {
                "resourceType": "Patient",
                "id": "test-patient"
            }
        }]
    }"#;

    let mut temp_file = NamedTempFile::with_suffix(".json").unwrap();
    temp_file.as_file_mut().write_all(bundle_json.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let file_url = format!("file://{}", temp_file.path().to_string_lossy());
    let result = data_source.load(&file_url).await;

    assert!(result.is_ok(), "Regular JSON Bundle loading broken");

    #[cfg(feature = "R4")]
    match result.unwrap() {
        SofBundle::R4(bundle) => {
            assert_eq!(bundle.entry.as_ref().unwrap().len(), 1);
        }
        #[cfg(feature = "R4B")]
        SofBundle::R4B(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R5")]
        SofBundle::R5(_) => panic!("Expected R4 bundle"),
        #[cfg(feature = "R6")]
        SofBundle::R6(_) => panic!("Expected R4 bundle"),
    }
}
