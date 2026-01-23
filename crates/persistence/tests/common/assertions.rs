//! FHIR-specific assertion helpers for testing.
//!
//! This module provides assertion macros and helper functions for testing
//! FHIR resources and storage operations.

use serde_json::Value;

use helios_persistence::error::{ConcurrencyError, ResourceError, StorageError, TenantError};
use helios_persistence::types::StoredResource;

/// Asserts that a StoredResource matches expected values.
///
/// # Arguments
///
/// * `resource` - The stored resource to check
/// * `resource_type` - Expected resource type
/// * `id` - Expected resource ID
///
/// # Panics
///
/// Panics if the resource doesn't match expected values.
pub fn assert_resource_matches(resource: &StoredResource, resource_type: &str, id: &str) {
    assert_eq!(
        resource.resource_type(),
        resource_type,
        "Resource type mismatch: expected {}, got {}",
        resource_type,
        resource.resource_type()
    );
    assert_eq!(
        resource.id(),
        id,
        "Resource ID mismatch: expected {}, got {}",
        id,
        resource.id()
    );
}

/// Asserts that a resource has the expected version.
pub fn assert_version(resource: &StoredResource, expected_version: &str) {
    assert_eq!(
        resource.version_id(),
        expected_version,
        "Version mismatch: expected {}, got {}",
        expected_version,
        resource.version_id()
    );
}

/// Asserts that a resource is not deleted.
pub fn assert_not_deleted(resource: &StoredResource) {
    assert!(
        !resource.is_deleted(),
        "Expected resource to not be deleted, but it was"
    );
}

/// Asserts that a resource is deleted.
pub fn assert_deleted(resource: &StoredResource) {
    assert!(
        resource.is_deleted(),
        "Expected resource to be deleted, but it was not"
    );
}

/// Asserts that the resource content contains a specific field value.
pub fn assert_content_field(resource: &StoredResource, field: &str, expected: &Value) {
    let actual = resource.content().get(field);
    assert_eq!(
        actual,
        Some(expected),
        "Content field '{}' mismatch: expected {:?}, got {:?}",
        field,
        expected,
        actual
    );
}

/// Asserts that a JSON path in the resource content has a specific value.
///
/// # Arguments
///
/// * `resource` - The stored resource
/// * `path` - JSON pointer path (e.g., "/name/0/family")
/// * `expected` - Expected value
pub fn assert_content_path(resource: &StoredResource, path: &str, expected: &Value) {
    let actual = resource.content().pointer(path);
    assert_eq!(
        actual,
        Some(expected),
        "Content path '{}' mismatch: expected {:?}, got {:?}",
        path,
        expected,
        actual
    );
}

/// Asserts that a result is an error of a specific type.
pub fn assert_storage_error<T>(result: Result<T, StorageError>, expected_variant: &str) {
    match result {
        Ok(_) => panic!("Expected error '{}', but got Ok", expected_variant),
        Err(e) => {
            let error_string = format!("{:?}", e);
            assert!(
                error_string.contains(expected_variant),
                "Expected error containing '{}', got {:?}",
                expected_variant,
                e
            );
        }
    }
}

/// Asserts that a result is a ResourceError::NotFound.
pub fn assert_not_found<T>(result: Result<T, StorageError>) {
    match result {
        Ok(_) => panic!("Expected NotFound error, but got Ok"),
        Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
        Err(e) => panic!("Expected NotFound error, got {:?}", e),
    }
}

/// Asserts that a result is a ResourceError::AlreadyExists.
pub fn assert_already_exists<T>(result: Result<T, StorageError>) {
    match result {
        Ok(_) => panic!("Expected AlreadyExists error, but got Ok"),
        Err(StorageError::Resource(ResourceError::AlreadyExists { .. })) => {}
        Err(e) => panic!("Expected AlreadyExists error, got {:?}", e),
    }
}

/// Asserts that a result is a ResourceError::Gone.
pub fn assert_gone<T>(result: Result<T, StorageError>) {
    match result {
        Ok(_) => panic!("Expected Gone error, but got Ok"),
        Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
        Err(e) => panic!("Expected Gone error, got {:?}", e),
    }
}

/// Asserts that a result is a ConcurrencyError::VersionConflict.
pub fn assert_version_conflict<T>(result: Result<T, StorageError>) {
    match result {
        Ok(_) => panic!("Expected VersionConflict error, but got Ok"),
        Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. })) => {}
        Err(e) => panic!("Expected VersionConflict error, got {:?}", e),
    }
}

/// Asserts that a result is a TenantError::AccessDenied.
pub fn assert_access_denied<T>(result: Result<T, StorageError>) {
    match result {
        Ok(_) => panic!("Expected AccessDenied error, but got Ok"),
        Err(StorageError::Tenant(TenantError::AccessDenied { .. })) => {}
        Err(e) => panic!("Expected AccessDenied error, got {:?}", e),
    }
}

/// Asserts that a result is a TenantError::OperationNotPermitted.
pub fn assert_operation_not_permitted<T>(result: Result<T, StorageError>) {
    match result {
        Ok(_) => panic!("Expected OperationNotPermitted error, but got Ok"),
        Err(StorageError::Tenant(TenantError::OperationNotPermitted { .. })) => {}
        Err(e) => panic!("Expected OperationNotPermitted error, got {:?}", e),
    }
}

/// Asserts that a search result contains a specific resource.
pub fn assert_search_contains(resources: &[StoredResource], resource_type: &str, id: &str) {
    let found = resources
        .iter()
        .any(|r| r.resource_type() == resource_type && r.id() == id);
    assert!(
        found,
        "Search results should contain {}/{}",
        resource_type, id
    );
}

/// Asserts that a search result does not contain a specific resource.
pub fn assert_search_not_contains(resources: &[StoredResource], resource_type: &str, id: &str) {
    let found = resources
        .iter()
        .any(|r| r.resource_type() == resource_type && r.id() == id);
    assert!(
        !found,
        "Search results should not contain {}/{}",
        resource_type, id
    );
}

/// Asserts that a search result has the expected count.
pub fn assert_search_count(resources: &[StoredResource], expected: usize) {
    assert_eq!(
        resources.len(),
        expected,
        "Expected {} search results, got {}",
        expected,
        resources.len()
    );
}

/// Asserts that resources are sorted by a field in ascending order.
pub fn assert_sorted_asc(resources: &[StoredResource], field_path: &str) {
    let values: Vec<Option<&Value>> = resources
        .iter()
        .map(|r| r.content().pointer(field_path))
        .collect();

    for i in 1..values.len() {
        if let (Some(prev), Some(curr)) = (values[i - 1], values[i]) {
            assert!(
                prev <= curr,
                "Resources not sorted ascending by '{}': {:?} > {:?}",
                field_path,
                prev,
                curr
            );
        }
    }
}

/// Asserts that resources are sorted by a field in descending order.
pub fn assert_sorted_desc(resources: &[StoredResource], field_path: &str) {
    let values: Vec<Option<&Value>> = resources
        .iter()
        .map(|r| r.content().pointer(field_path))
        .collect();

    for i in 1..values.len() {
        if let (Some(prev), Some(curr)) = (values[i - 1], values[i]) {
            assert!(
                prev >= curr,
                "Resources not sorted descending by '{}': {:?} < {:?}",
                field_path,
                prev,
                curr
            );
        }
    }
}

/// Asserts that all resources in the result are of the expected type.
pub fn assert_all_resource_type(resources: &[StoredResource], expected_type: &str) {
    for resource in resources {
        assert_eq!(
            resource.resource_type(),
            expected_type,
            "Expected all resources to be type '{}', found '{}'",
            expected_type,
            resource.resource_type()
        );
    }
}

/// Asserts that all resources belong to the expected tenant.
pub fn assert_all_tenant(
    resources: &[StoredResource],
    expected_tenant: &helios_persistence::tenant::TenantId,
) {
    for resource in resources {
        assert_eq!(
            resource.tenant_id(),
            expected_tenant,
            "Expected all resources to belong to tenant '{}', found '{}'",
            expected_tenant.as_str(),
            resource.tenant_id().as_str()
        );
    }
}

/// Assertion macro for checking resource matches expected type and id.
#[macro_export]
macro_rules! assert_resource {
    ($resource:expr, $type:expr, $id:expr) => {
        $crate::common::assertions::assert_resource_matches(&$resource, $type, $id);
    };
    ($resource:expr, $type:expr, $id:expr, version: $version:expr) => {
        $crate::common::assertions::assert_resource_matches(&$resource, $type, $id);
        $crate::common::assertions::assert_version(&$resource, $version);
    };
}

/// Assertion macro for checking search results.
#[macro_export]
macro_rules! assert_search {
    ($results:expr, count: $count:expr) => {
        $crate::common::assertions::assert_search_count(&$results, $count);
    };
    ($results:expr, contains: $type:expr, $id:expr) => {
        $crate::common::assertions::assert_search_contains(&$results, $type, $id);
    };
    ($results:expr, not_contains: $type:expr, $id:expr) => {
        $crate::common::assertions::assert_search_not_contains(&$results, $type, $id);
    };
}

/// Assertion macro for checking errors.
#[macro_export]
macro_rules! assert_error {
    ($result:expr, not_found) => {
        $crate::common::assertions::assert_not_found($result);
    };
    ($result:expr, already_exists) => {
        $crate::common::assertions::assert_already_exists($result);
    };
    ($result:expr, gone) => {
        $crate::common::assertions::assert_gone($result);
    };
    ($result:expr, version_conflict) => {
        $crate::common::assertions::assert_version_conflict($result);
    };
    ($result:expr, access_denied) => {
        $crate::common::assertions::assert_access_denied($result);
    };
    ($result:expr, operation_not_permitted) => {
        $crate::common::assertions::assert_operation_not_permitted($result);
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use helios_persistence::tenant::TenantId;
    use serde_json::json;

    fn create_test_resource(id: &str, version: &str) -> StoredResource {
        StoredResource::from_storage(
            "Patient",
            id,
            version,
            TenantId::new("test"),
            json!({"resourceType": "Patient", "id": id, "name": [{"family": "Test"}]}),
            chrono::Utc::now(),
            chrono::Utc::now(),
            None,
        )
    }

    #[test]
    fn test_assert_resource_matches() {
        let resource = create_test_resource("123", "1");
        assert_resource_matches(&resource, "Patient", "123");
    }

    #[test]
    fn test_assert_version() {
        let resource = create_test_resource("123", "2");
        assert_version(&resource, "2");
    }

    #[test]
    fn test_assert_not_deleted() {
        let resource = create_test_resource("123", "1");
        assert_not_deleted(&resource);
    }

    #[test]
    fn test_assert_content_field() {
        let resource = create_test_resource("123", "1");
        assert_content_field(&resource, "resourceType", &json!("Patient"));
    }

    #[test]
    fn test_assert_content_path() {
        let resource = create_test_resource("123", "1");
        assert_content_path(&resource, "/name/0/family", &json!("Test"));
    }

    #[test]
    fn test_assert_search_count() {
        let resources = vec![
            create_test_resource("1", "1"),
            create_test_resource("2", "1"),
        ];
        assert_search_count(&resources, 2);
    }

    #[test]
    fn test_assert_search_contains() {
        let resources = vec![
            create_test_resource("1", "1"),
            create_test_resource("2", "1"),
        ];
        assert_search_contains(&resources, "Patient", "1");
        assert_search_not_contains(&resources, "Patient", "3");
    }

    #[test]
    fn test_assert_all_resource_type() {
        let resources = vec![
            create_test_resource("1", "1"),
            create_test_resource("2", "1"),
        ];
        assert_all_resource_type(&resources, "Patient");
    }

    #[test]
    fn test_assert_not_found_error() {
        let result: Result<(), StorageError> = Err(StorageError::Resource(ResourceError::NotFound {
            resource_type: "Patient".to_string(),
            id: "123".to_string(),
        }));
        assert_not_found(result);
    }

    #[test]
    fn test_assert_already_exists_error() {
        let result: Result<(), StorageError> =
            Err(StorageError::Resource(ResourceError::AlreadyExists {
                resource_type: "Patient".to_string(),
                id: "123".to_string(),
            }));
        assert_already_exists(result);
    }
}
