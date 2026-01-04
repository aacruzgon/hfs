use helios_serde::Result;
use helios_serde::xml::{from_xml_str, to_xml_string};
use serde::{Deserialize, Serialize};

#[cfg(feature = "R4")]
#[test]
fn test_xml_deserialize_r4_resource() -> Result<()> {
    // Test with a simple resource without nested complex types
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<Patient xmlns="http://hl7.org/fhir">
    <id value="example"/>
    <active value="true"/>
</Patient>"#;

    let result = from_xml_str::<helios_fhir::r4::Resource>(xml);
    match &result {
        Ok(_) => println!("Parse succeeded!"),
        Err(e) => println!("Parse error: {}", e),
    }
    assert!(result.is_ok());
    Ok(())
}

#[cfg(feature = "R4")]
#[test]
fn test_xml_deserialize_participant_type_single_entry() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<AppointmentResponse xmlns="http://hl7.org/fhir">
    <participantType>
        <coding>
            <system value="http://terminology.hl7.org/CodeSystem/v3-ParticipationType"/>
            <code value="ADM"/>
        </coding>
    </participantType>
</AppointmentResponse>"#;

    let result = from_xml_str::<helios_fhir::r4::AppointmentResponse>(xml);
    assert!(result.is_ok(), "Failed to parse XML: {:?}", result);
    let response = result?;
    let participant_type = response.participant_type.expect("participantType missing");
    assert_eq!(participant_type.len(), 1);
    assert!(
        participant_type[0].coding.is_some(),
        "coding entries missing"
    );
    Ok(())
}

#[cfg(feature = "R4")]
#[test]
fn test_xml_deserialize_r4_appointment_response_example() -> Result<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("serde crate has parent directory")
        .join("fhir")
        .join("tests")
        .join("data")
        .join("xml")
        .join("R4")
        .join("appointmentresponse-example-req(exampleresp).xml");

    let xml = std::fs::read_to_string(&path)?;
    let response = from_xml_str::<helios_fhir::r4::AppointmentResponse>(&xml)?;
    println!("participant_type = {:?}", response.participant_type);
    assert!(response.participant_type.is_some());
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct SimpleResource {
    #[serde(rename = "resourceType")]
    resource_type: String,
    id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Name {
    family: Option<String>,
    given: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PatientResource {
    #[serde(rename = "resourceType")]
    resource_type: String,
    id: Option<String>,
    name: Option<Vec<Name>>,
}

#[test]
fn test_xml_serialize_simple_resource() -> Result<()> {
    let resource = SimpleResource {
        resource_type: "Patient".to_string(),
        id: Some("example".to_string()),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output:\n{}", xml);

    // For now, just check it doesn't crash
    assert!(!xml.is_empty());

    Ok(())
}

#[test]
fn test_xml_serialize_nested_struct() -> Result<()> {
    let resource = PatientResource {
        resource_type: "Patient".to_string(),
        id: Some("example".to_string()),
        name: Some(vec![Name {
            family: Some("Doe".to_string()),
            given: Some(vec!["John".to_string()]),
        }]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (nested):\n{}", xml);

    // For now, just check it doesn't crash
    assert!(!xml.is_empty());
    assert!(xml.contains("<Patient xmlns=\"http://hl7.org/fhir\">"));
    assert!(xml.contains("<id value=\"example\"/>"));
    assert!(xml.contains("<family value=\"Doe\"/>"));

    Ok(())
}

// Old split-field pattern (for JSON). No longer used in XML tests.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PrimitiveExtension {
    id: Option<String>,
}

// Element structure for XML primitives with metadata
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct BirthDateElement {
    #[serde(rename = "value")]
    value: Option<String>,
    id: Option<String>,
}

// Split-field pattern for serialization (used in real FHIR models)
#[derive(Serialize, Debug)]
struct ResourceForSerialization {
    #[serde(rename = "resourceType")]
    resource_type: String,
    #[serde(rename = "birthDate")]
    birth_date: Option<String>,
    #[serde(rename = "_birthDate")]
    birth_date_ext: Option<PrimitiveExtension>,
}

// New XML-compatible structure using PrimitiveOrElement for deserialization
#[derive(Deserialize, Debug, PartialEq)]
struct ResourceWithExtension {
    #[serde(rename = "resourceType")]
    resource_type: String,
    #[serde(rename = "birthDate")]
    birth_date: Option<helios_serde_support::PrimitiveOrElement<serde_json::Value, BirthDateElement>>,
}

#[test]
fn test_xml_serialize_with_primitive_extension() -> Result<()> {
    let resource = ResourceForSerialization {
        resource_type: "Patient".to_string(),
        birth_date: Some("1974-12-25".to_string()),
        birth_date_ext: Some(PrimitiveExtension {
            id: Some("bd1".to_string()),
        }),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (with extension):\n{}", xml);

    // Check the field/_field pattern is merged correctly
    assert!(xml.contains("<birthDate"));
    assert!(xml.contains("id=\"bd1\""));
    assert!(xml.contains("value=\"1974-12-25\""));

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithArrays {
    #[serde(rename = "resourceType")]
    resource_type: String,
    given: Option<Vec<String>>,
}

#[test]
fn test_xml_serialize_array_of_primitives() -> Result<()> {
    let resource = ResourceWithArrays {
        resource_type: "Patient".to_string(),
        given: Some(vec!["Alice".to_string(), "Marie".to_string()]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (array):\n{}", xml);

    // Check array elements are repeated
    assert!(xml.contains("<given value=\"Alice\"/>"));
    assert!(xml.contains("<given value=\"Marie\"/>"));

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithComplexArray {
    #[serde(rename = "resourceType")]
    resource_type: String,
    name: Option<Vec<Name>>,
}

#[test]
fn test_xml_serialize_array_of_complex() -> Result<()> {
    let resource = ResourceWithComplexArray {
        resource_type: "Patient".to_string(),
        name: Some(vec![
            Name {
                family: Some("Smith".to_string()),
                given: Some(vec!["John".to_string()]),
            },
            Name {
                family: Some("Doe".to_string()),
                given: None,
            },
        ]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (complex array):\n{}", xml);

    // Check multiple name elements
    assert!(xml.contains("<family value=\"Smith\"/>"));
    assert!(xml.contains("<family value=\"Doe\"/>"));

    Ok(())
}

#[test]
fn test_xml_serialize_empty_optional() -> Result<()> {
    let resource = SimpleResource {
        resource_type: "Patient".to_string(),
        id: None, // Empty optional
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (empty optional):\n{}", xml);

    // Empty optionals should not appear
    assert!(!xml.contains("<id"));
    assert!(xml.contains("<Patient"));

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithArrayExtensions {
    #[serde(rename = "resourceType")]
    resource_type: String,
    given: Option<Vec<Option<String>>>,
    #[serde(rename = "_given")]
    given_ext: Option<Vec<Option<PrimitiveExtension>>>,
}

#[test]
fn test_xml_serialize_array_with_extensions() -> Result<()> {
    let resource = ResourceWithArrayExtensions {
        resource_type: "Patient".to_string(),
        given: Some(vec![
            Some("Alice".to_string()),
            None,
            Some("Marie".to_string()),
        ]),
        given_ext: Some(vec![
            None,
            Some(PrimitiveExtension {
                id: Some("g1".to_string()),
            }),
            Some(PrimitiveExtension {
                id: Some("g2".to_string()),
            }),
        ]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (array with extensions):\n{}", xml);

    // Check that array elements are merged correctly with extension data
    // Alice without id
    assert!(xml.contains("<given value=\"Alice\"/>"));
    // Second element only has extension (no value)
    assert!(xml.contains("<given id=\"g1\"/>"));
    // Marie with extension
    assert!(xml.contains("<given id=\"g2\" value=\"Marie\"/>"));

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Text {
    status: String,
    div: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithNarrative {
    #[serde(rename = "resourceType")]
    resource_type: String,
    text: Option<Text>,
}

#[test]
fn test_xml_serialize_with_narrative() -> Result<()> {
    let resource = ResourceWithNarrative {
        resource_type: "Patient".to_string(),
        text: Some(Text {
            status: "generated".to_string(),
            div: "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>Test content</p></div>"
                .to_string(),
        }),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (with narrative):\n{}", xml);

    // Check that text element is present
    assert!(xml.contains("<text>"));
    // Check that status is present
    assert!(xml.contains("<status value=\"generated\"/>"));
    // Check that div is present with XHTML namespace and content as raw XML
    assert!(xml.contains("<div xmlns=\"http://www.w3.org/1999/xhtml\">"));
    assert!(xml.contains("<p>Test content</p>"));
    assert!(xml.contains("</div>"));

    Ok(())
}

#[test]
fn test_xml_roundtrip_with_narrative() -> Result<()> {
    let original = ResourceWithNarrative {
        resource_type: "Patient".to_string(),
        text: Some(Text {
            status: "generated".to_string(),
            div: "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>Test content</p></div>"
                .to_string(),
        }),
    };

    let xml = to_xml_string(&original)?;
    println!("Serialized XML: {}", xml);

    let deserialized: ResourceWithNarrative = from_xml_str(&xml)?;

    assert_eq!(original, deserialized);

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithEmptyArray {
    #[serde(rename = "resourceType")]
    resource_type: String,
    given: Option<Vec<String>>,
}

#[test]
fn test_xml_serialize_empty_array() -> Result<()> {
    let resource = ResourceWithEmptyArray {
        resource_type: "Patient".to_string(),
        given: Some(vec![]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (empty array):\n{}", xml);

    // Empty arrays should not produce any elements
    assert!(!xml.contains("<given"));
    assert!(xml.contains("<Patient"));

    Ok(())
}

#[test]
fn test_xml_serialize_none_array() -> Result<()> {
    let resource = ResourceWithEmptyArray {
        resource_type: "Patient".to_string(),
        given: None,
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (None array):\n{}", xml);

    // None arrays should not produce any elements
    assert!(!xml.contains("<given"));
    assert!(xml.contains("<Patient"));

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithNullElements {
    #[serde(rename = "resourceType")]
    resource_type: String,
    given: Option<Vec<Option<String>>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MiniCoding {
    system: Option<String>,
    code: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MiniParticipantType {
    coding: Option<Vec<MiniCoding>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MiniAppointmentResponse {
    #[serde(rename = "resourceType")]
    resource_type: String,
    #[serde(rename = "participantType")]
    participant_type: Option<Vec<MiniParticipantType>>,
}

#[test]
fn test_xml_serialize_array_all_nulls() -> Result<()> {
    let resource = ResourceWithNullElements {
        resource_type: "Patient".to_string(),
        given: Some(vec![None, None, None]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (array all nulls):\n{}", xml);

    // Array with all null values should not produce any elements
    assert!(!xml.contains("<given"));
    assert!(xml.contains("<Patient"));

    Ok(())
}

#[test]
fn test_xml_serialize_array_mixed_nulls() -> Result<()> {
    let resource = ResourceWithNullElements {
        resource_type: "Patient".to_string(),
        given: Some(vec![
            None,
            Some("John".to_string()),
            None,
            Some("Doe".to_string()),
        ]),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (array mixed nulls):\n{}", xml);

    // Only non-null elements should appear
    assert!(xml.contains("<given value=\"John\"/>"));
    assert!(xml.contains("<given value=\"Doe\"/>"));
    // Should have exactly 2 given elements
    let count = xml.matches("<given").count();
    assert_eq!(count, 2);

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Extension {
    url: String,
    #[serde(rename = "valueString", skip_serializing_if = "Option::is_none")]
    value_string: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PrimitiveExtensionWithContent {
    id: Option<String>,
    extension: Option<Vec<Extension>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithComplexExtension {
    #[serde(rename = "resourceType")]
    resource_type: String,
    #[serde(rename = "birthDate")]
    birth_date: Option<String>,
    #[serde(rename = "_birthDate")]
    birth_date_ext: Option<PrimitiveExtensionWithContent>,
}

#[test]
fn test_xml_serialize_with_complex_extension() -> Result<()> {
    let resource = ResourceWithComplexExtension {
        resource_type: "Patient".to_string(),
        birth_date: Some("1974-12-25".to_string()),
        birth_date_ext: Some(PrimitiveExtensionWithContent {
            id: Some("bd1".to_string()),
            extension: Some(vec![Extension {
                url: "http://example.org/fhir/StructureDefinition/text".to_string(),
                value_string: Some("Estimated date".to_string()),
            }]),
        }),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (with complex extension):\n{}", xml);

    // Check the field/_field pattern is merged correctly
    assert!(xml.contains("<birthDate"));
    assert!(xml.contains("id=\"bd1\""));
    assert!(xml.contains("value=\"1974-12-25\""));
    // Check that extension element is present with url
    assert!(xml.contains("<extension"));
    assert!(xml.contains("url=\"http://example.org/fhir/StructureDefinition/text\""));
    // Check that valueString is present inside extension
    assert!(xml.contains("<valueString value=\"Estimated date\""));

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ExtensionWithDecimal {
    url: String,
    #[serde(rename = "valueDecimal", skip_serializing_if = "Option::is_none")]
    value_decimal: Option<rust_decimal::Decimal>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PrimitiveExtensionWithDecimal {
    extension: Option<Vec<ExtensionWithDecimal>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ResourceWithDecimalExtension {
    #[serde(rename = "resourceType")]
    resource_type: String,
    value: Option<String>,
    #[serde(rename = "_value")]
    value_ext: Option<PrimitiveExtensionWithDecimal>,
}

#[test]
fn test_xml_serialize_decimal_precision() -> Result<()> {
    use rust_decimal_macros::dec;

    // Use a decimal value that would lose precision with f64
    // This decimal has more precision than f64 can represent
    let precise_decimal = dec!(123456789.123456789123456789);

    let resource = ResourceWithDecimalExtension {
        resource_type: "Observation".to_string(),
        value: Some("test".to_string()),
        value_ext: Some(PrimitiveExtensionWithDecimal {
            extension: Some(vec![ExtensionWithDecimal {
                url: "http://example.org/precision".to_string(),
                value_decimal: Some(precise_decimal),
            }]),
        }),
    };

    let xml = to_xml_string(&resource)?;
    println!("XML output (decimal precision):\n{}", xml);

    // Check that the decimal value is serialized with full precision
    // rust_decimal preserves the exact decimal representation
    assert!(xml.contains("valueDecimal"));
    assert!(xml.contains("value=\"123456789.123456789123456789\""));

    Ok(())
}

// ====================
// Deserialization Tests
// ====================

#[test]
fn test_xml_deserialize_simple_resource() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?><Patient xmlns="http://hl7.org/fhir"><id value="example"/></Patient>"#;

    let resource: SimpleResource = from_xml_str(xml)?;

    assert_eq!(resource.resource_type, "Patient");
    assert_eq!(resource.id, Some("example".to_string()));

    Ok(())
}

#[test]
fn test_xml_roundtrip_simple() -> Result<()> {
    let original = SimpleResource {
        resource_type: "Patient".to_string(),
        id: Some("example".to_string()),
    };

    // Serialize
    let xml = to_xml_string(&original)?;

    // Deserialize
    let deserialized: SimpleResource = from_xml_str(&xml)?;

    // Should match
    assert_eq!(original, deserialized);

    Ok(())
}

#[test]
fn test_xml_deserialize_nested_struct() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <Patient xmlns="http://hl7.org/fhir">
        <id value="example"/>
        <name>
            <family value="Doe"/>
            <given value="John"/>
        </name>
    </Patient>"#;

    let resource: PatientResource = from_xml_str(xml)?;

    assert_eq!(resource.resource_type, "Patient");
    assert_eq!(resource.id, Some("example".to_string()));
    assert_eq!(resource.name.as_ref().unwrap().len(), 1);
    assert_eq!(
        resource.name.as_ref().unwrap()[0].family,
        Some("Doe".to_string())
    );
    assert_eq!(
        resource.name.as_ref().unwrap()[0].given.as_ref().unwrap()[0],
        "John"
    );

    Ok(())
}

#[test]
fn test_xml_roundtrip_nested_struct() -> Result<()> {
    let original = PatientResource {
        resource_type: "Patient".to_string(),
        id: Some("example".to_string()),
        name: Some(vec![Name {
            family: Some("Doe".to_string()),
            given: Some(vec!["John".to_string()]),
        }]),
    };

    let xml = to_xml_string(&original)?;
    let deserialized: PatientResource = from_xml_str(&xml)?;

    assert_eq!(original, deserialized);

    Ok(())
}

#[test]
fn test_xml_deserialize_array_of_primitives() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <Patient xmlns="http://hl7.org/fhir">
        <given value="Alice"/>
        <given value="Marie"/>
    </Patient>"#;

    let resource: ResourceWithArrays = from_xml_str(xml)?;

    assert_eq!(resource.resource_type, "Patient");
    assert_eq!(resource.given.as_ref().unwrap().len(), 2);
    assert_eq!(resource.given.as_ref().unwrap()[0], "Alice");
    assert_eq!(resource.given.as_ref().unwrap()[1], "Marie");

    Ok(())
}

#[test]
fn test_xml_roundtrip_array_of_primitives() -> Result<()> {
    let original = ResourceWithArrays {
        resource_type: "Patient".to_string(),
        given: Some(vec!["Alice".to_string(), "Marie".to_string()]),
    };

    let xml = to_xml_string(&original)?;
    let deserialized: ResourceWithArrays = from_xml_str(&xml)?;

    assert_eq!(original, deserialized);

    Ok(())
}

#[test]
fn test_xml_deserialize_primitive_extension() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <Patient xmlns="http://hl7.org/fhir">
        <birthDate id="bd1" value="1974-12-25"/>
    </Patient>"#;

    let resource: ResourceWithExtension = from_xml_str(xml)?;

    assert_eq!(resource.resource_type, "Patient");

    // Check that birthDate was deserialized as an Element with both value and id
    let birth_date = resource.birth_date.as_ref().expect("birthDate should be present");
    match birth_date {
        helios_serde_support::PrimitiveOrElement::Element(elem) => {
            assert_eq!(elem.value, Some("1974-12-25".to_string()));
            assert_eq!(elem.id, Some("bd1".to_string()));
        }
        helios_serde_support::PrimitiveOrElement::Primitive(val) => {
            panic!("Expected Element variant, got Primitive: {:?}", val);
        }
    }

    Ok(())
}

#[test]
fn test_xml_deserialize_primitive_extension_non_self_closing() -> Result<()> {
    // Test Start element (not self-closing) with attributes
    // Same as test_xml_deserialize_primitive_extension but with </birthDate> instead of />
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <Patient xmlns="http://hl7.org/fhir">
        <birthDate id="bd1" value="1974-12-25"></birthDate>
    </Patient>"#;

    let resource: ResourceWithExtension = from_xml_str(xml)?;

    assert_eq!(resource.resource_type, "Patient");

    // Check that birthDate was deserialized as an Element with both value and id
    let birth_date = resource.birth_date.as_ref().expect("birthDate should be present");
    match birth_date {
        helios_serde_support::PrimitiveOrElement::Element(elem) => {
            assert_eq!(elem.value, Some("1974-12-25".to_string()));
            assert_eq!(elem.id, Some("bd1".to_string()));
        }
        _ => panic!("Expected Element variant, got Primitive"),
    }

    Ok(())
}

#[test]
fn test_xml_deserialize_participant_type_array() -> Result<()> {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <AppointmentResponse xmlns="http://hl7.org/fhir">
        <participantType>
            <coding>
                <system value="http://example.org"/>
                <code value="ATND"/>
            </coding>
        </participantType>
    </AppointmentResponse>"#;

    let resource: MiniAppointmentResponse = from_xml_str(xml)?;
    assert_eq!(resource.resource_type, "AppointmentResponse");
    assert!(
        resource
            .participant_type
            .as_ref()
            .unwrap()
            .first()
            .unwrap()
            .coding
            .as_ref()
            .unwrap()
            .first()
            .unwrap()
            .code
            .as_ref()
            .unwrap()
            == "ATND"
    );
    Ok(())
}

#[test]
fn test_minimal_singlevec_xml() -> Result<()> {
    use helios_serde_support::SingleOrVec;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestResource {
        #[serde(default, rename = "item")]
        items: SingleOrVec<TestItem>,
    }

    #[derive(Debug, Deserialize, PartialEq, Clone)]
    struct TestItem {
        #[serde(rename = "linkId")]
        link_id: String,
        #[serde(default, rename = "text")]
        text: Option<String>,
    }

    // Test with single item
    let xml_single = r#"<?xml version="1.0"?><TestResource xmlns="http://test"><item><linkId value="q1"/></item></TestResource>"#;
    let result_single = from_xml_str::<TestResource>(xml_single)?;
    assert_eq!(result_single.items.as_ref().len(), 1);

    // Test with multiple items
    let xml_multi = r#"<?xml version="1.0"?><TestResource xmlns="http://test"><item><linkId value="q1"/></item><item><linkId value="q2"/></item></TestResource>"#;
    let result_multi = from_xml_str::<TestResource>(xml_multi)?;
    assert_eq!(result_multi.items.as_ref().len(), 2);

    Ok(())
}
