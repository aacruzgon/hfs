use helios_hfs_serde::xml::to_xml_string;
use helios_hfs_serde::Result;
use serde::Serialize;

#[derive(Serialize)]
struct SimpleResource {
    #[serde(rename = "resourceType")]
    resource_type: String,
    id: Option<String>,
}

#[derive(Serialize)]
struct Name {
    family: Option<String>,
    given: Option<Vec<String>>,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
struct PrimitiveExtension {
    id: Option<String>,
}

#[derive(Serialize)]
struct ResourceWithExtension {
    #[serde(rename = "resourceType")]
    resource_type: String,
    #[serde(rename = "birthDate")]
    birth_date: Option<String>,
    #[serde(rename = "_birthDate")]
    birth_date_ext: Option<PrimitiveExtension>,
}

#[test]
fn test_xml_serialize_with_primitive_extension() -> Result<()> {
    let resource = ResourceWithExtension {
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

#[derive(Serialize)]
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

#[derive(Serialize)]
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

#[derive(Serialize)]
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
        given: Some(vec![Some("Alice".to_string()), None, Some("Marie".to_string())]),
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

#[derive(Serialize)]
struct Text {
    status: String,
    div: String,
}

#[derive(Serialize)]
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
            div: "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>Test content</p></div>".to_string(),
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

#[derive(Serialize)]
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

#[derive(Serialize)]
struct ResourceWithNullElements {
    #[serde(rename = "resourceType")]
    resource_type: String,
    given: Option<Vec<Option<String>>>,
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
        given: Some(vec![None, Some("John".to_string()), None, Some("Doe".to_string())]),
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

#[derive(Serialize)]
struct Extension {
    url: String,
    #[serde(rename = "valueString", skip_serializing_if = "Option::is_none")]
    value_string: Option<String>,
}

#[derive(Serialize)]
struct PrimitiveExtensionWithContent {
    id: Option<String>,
    extension: Option<Vec<Extension>>,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
struct ExtensionWithDecimal {
    url: String,
    #[serde(rename = "valueDecimal", skip_serializing_if = "Option::is_none")]
    value_decimal: Option<rust_decimal::Decimal>,
}

#[derive(Serialize)]
struct PrimitiveExtensionWithDecimal {
    extension: Option<Vec<ExtensionWithDecimal>>,
}

#[derive(Serialize)]
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
