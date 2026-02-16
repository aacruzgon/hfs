/// Tests for GitHub issue #31: Tokio stack overflow when deserializing
/// resources with `contained` resources.
///
/// The Resource enum has 150+ variants, making it very large. When a resource
/// like Observation has a `contained: Option<Vec<Resource>>` field,
/// deserialization can overflow the stack — especially on Tokio worker threads
/// which have a default 2MB stack.
use helios_fhir::r4::*;

const OBSERVATION_WITH_CONTAINED: &str = r##"{
  "resourceType": "Observation",
  "contained": [
    {
      "resourceType": "Patient",
      "id": "db2b7000-672c-43e3-8447-e43dcc166950"
    }
  ],
  "text": {
    "status": "generated",
    "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">Complete Resource Test</div>"
  },
  "status": "final",
  "code": {
    "coding": [{
      "system": "http://loinc.org",
      "code": "29463-7",
      "display": "Body Weight"
    }]
  },
  "subject": {
    "reference": "#db2b7000-672c-43e3-8447-e43dcc166950"
  },
  "valueQuantity": {
    "value": 85.5,
    "unit": "kg"
  }
}"##;

/// Verify that the Resource enum size is reasonable (< 1KB after boxing).
/// Before the fix, it was several hundred KB due to the largest variant being
/// inlined. After boxing, each variant is just a pointer (8 bytes on 64-bit).
#[test]
fn test_resource_enum_size_is_reasonable() {
    let size = std::mem::size_of::<Resource>();
    // After boxing, each variant should be Box<T> = 8 bytes + tag + alignment.
    // We allow up to 1KB as a generous upper bound.
    assert!(
        size <= 1024,
        "Resource enum is {} bytes — expected <= 1024 after boxing variants. \
         Large enum size causes stack overflows on Tokio worker threads (2MB default stack).",
        size
    );
}

/// Deserialize the exact JSON from issue #31 on a thread with a 2MB stack
/// (Tokio worker thread default). This reliably reproduces the stack overflow
/// when Resource variants are not boxed.
#[test]
fn test_deserialize_contained_resource_small_stack() {
    // 2MB = Tokio's default worker thread stack size
    let stack_size = 2 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name("small-stack-test".into())
        .stack_size(stack_size)
        .spawn(|| {
            let resource: Resource = serde_json::from_str(OBSERVATION_WITH_CONTAINED)
                .expect("Failed to deserialize Observation with contained Patient");

            // Verify the deserialized structure
            if let Resource::Observation(obs) = &resource {
                assert!(obs.contained.is_some());
                let contained = obs.contained.as_ref().unwrap();
                assert_eq!(contained.len(), 1);
                assert!(matches!(&contained[0], Resource::Patient(_)));
            } else {
                panic!("Expected Observation resource, got {:?}", resource);
            }
        })
        .expect("Failed to spawn thread");

    handle.join().expect(
        "Thread panicked — likely a stack overflow. \
         The Resource enum is too large for a 2MB stack (Tokio default).",
    );
}

/// Deserialize a Resource directly (not via contained) should also work
/// on a small stack.
#[test]
fn test_deserialize_resource_directly_small_stack() {
    let json = r#"{
      "resourceType": "Patient",
      "id": "example",
      "name": [{"family": "Smith", "given": ["John"]}]
    }"#;

    let stack_size = 2 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name("small-stack-direct".into())
        .stack_size(stack_size)
        .spawn(move || {
            let resource: Resource =
                serde_json::from_str(json).expect("Failed to deserialize Patient resource");
            assert!(matches!(resource, Resource::Patient(_)));
        })
        .expect("Failed to spawn thread");

    handle
        .join()
        .expect("Thread panicked — likely a stack overflow deserializing Resource.");
}
