//! XML tests for the FhirSerde derive macro, Element<T,E>, DecimalElement,
//! Timing/Extension roundtrips, and flatten — ported from test_json_serde.rs.
#![cfg(all(feature = "xml", feature = "R4"))]

use helios_fhir::r4::*;
use helios_fhir::{DecimalElement, PreciseDecimal, PrecisionDate};
use helios_fhir_macro::FhirSerde;
use helios_serde::xml::{from_xml_str, to_xml_string};
use rust_decimal as _;
use rust_decimal_macros::dec;
use serde::Deserialize;

// =============================================================================
// FhirSerde Macro Tests
// =============================================================================

#[derive(Debug, PartialEq, FhirSerde)]
struct FhirSerdeTestStruct {
    #[fhir_serde(rename = "resourceType")]
    resource_type: helios_fhir::r4::String,

    name1: helios_fhir::r4::String,
    name2: Option<helios_fhir::r4::String>,

    #[fhir_serde(rename = "birthDate1")]
    birth_date1: Date,
    #[fhir_serde(rename = "birthDate2")]
    birth_date2: Option<Date>,

    #[fhir_serde(rename = "isActive1")]
    is_active1: Boolean,
    #[fhir_serde(rename = "isActive2")]
    is_active2: Option<Boolean>,

    decimal1: Decimal,
    decimal2: Option<Decimal>,

    money1: Money,
    money2: Option<Money>,

    given: Option<Vec<helios_fhir::r4::String>>,
}

fn make_default_test_struct(name: &str) -> FhirSerdeTestStruct {
    let decimal = Decimal::new(dec!(123.45));
    FhirSerdeTestStruct {
        resource_type: "TestResource".to_string().into(),
        name1: name.to_string().into(),
        name2: None,
        birth_date1: Date {
            id: None,
            extension: None,
            value: Some(PrecisionDate::parse("1970-03-30").unwrap()),
        },
        birth_date2: None,
        is_active1: true.into(),
        is_active2: None,
        decimal1: decimal.clone(),
        decimal2: None,
        money1: Money {
            id: None,
            extension: None,
            value: Some(decimal),
            currency: None,
        },
        money2: None,
        given: None,
    }
}

fn default_extension() -> Extension {
    Extension {
        id: None,
        extension: None,
        url: "http://example.com/ext".to_string().into(),
        value: Some(ExtensionValue::String(helios_fhir::r4::String {
            id: None,
            extension: None,
            value: Some("ext-val".to_string()),
        })),
    }
}

#[test]
fn test_xml_fhir_serde_serialize() {
    // Case 1: Only primitive value for birthDate1
    let s1 = make_default_test_struct("Test1");
    let xml1 = to_xml_string(&s1).unwrap();
    assert!(xml1.contains("<TestResource"));
    assert!(xml1.contains("<name1 value=\"Test1\"/>"));
    assert!(xml1.contains("<birthDate1 value=\"1970-03-30\"/>"));
    assert!(xml1.contains("<isActive1 value=\"true\"/>"));
    assert!(xml1.contains("<decimal1 value=\"123.45\"/>"));
    assert!(xml1.contains("<money1>"));
    assert!(xml1.contains("<value value=\"123.45\"/>"));

    // Case 2: Only extension for birthDate1 (id only, no primitive value)
    let s2 = FhirSerdeTestStruct {
        birth_date1: Date {
            id: Some("bd-id".to_string()),
            extension: None,
            value: None,
        },
        ..make_default_test_struct("Test2")
    };
    let xml2 = to_xml_string(&s2).unwrap();
    assert!(xml2.contains("<birthDate1 id=\"bd-id\""), "XML: {}", xml2);

    // Case 3: Both primitive value and extension for birthDate1
    let s3 = FhirSerdeTestStruct {
        birth_date1: Date {
            id: Some("bd-id-3".to_string()),
            extension: None,
            value: Some(PrecisionDate::parse("1970-03-30").unwrap()),
        },
        ..make_default_test_struct("Test3")
    };
    let xml3 = to_xml_string(&s3).unwrap();
    assert!(
        xml3.contains("id=\"bd-id-3\"") && xml3.contains("value=\"1970-03-30\""),
        "birthDate1 should have both id and value. XML: {}",
        xml3
    );

    // Case 4: isActive1 with extension only (no primitive value)
    let s4 = FhirSerdeTestStruct {
        is_active1: Boolean {
            id: None,
            extension: Some(vec![Extension {
                id: None,
                extension: None,
                url: "http://example.com/flag".to_string().into(),
                value: Some(ExtensionValue::Boolean(Boolean {
                    id: None,
                    extension: None,
                    value: Some(true),
                })),
            }]),
            value: None,
        },
        ..make_default_test_struct("Test4")
    };
    let xml4 = to_xml_string(&s4).unwrap();
    assert!(
        xml4.contains("<isActive1>") || xml4.contains("<isActive1 "),
        "isActive1 should be present. XML: {}",
        xml4
    );
    assert!(
        xml4.contains("<extension"),
        "Extension should be present. XML: {}",
        xml4
    );

    // Case 5: All optional fields are None
    let s5 = make_default_test_struct("Test5");
    let xml5 = to_xml_string(&s5).unwrap();
    assert!(!xml5.contains("<name2"));
    assert!(!xml5.contains("<birthDate2"));
    assert!(!xml5.contains("<isActive2"));
    assert!(!xml5.contains("<decimal2"));
    assert!(!xml5.contains("<money2"));
    assert!(!xml5.contains("<given"));

    // Case 6: Decimal with id
    let s6 = FhirSerdeTestStruct {
        decimal1: Decimal {
            id: Some("dec-id".to_string()),
            extension: None,
            value: Some(PreciseDecimal::from_parts(
                Some(dec!(123.45)),
                "123.45".to_string(),
            )),
        },
        decimal2: Some(Decimal {
            id: None,
            extension: None,
            value: Some(PreciseDecimal::from_parts(
                Some(dec!(98.7)),
                "98.7".to_string(),
            )),
        }),
        ..make_default_test_struct("Test6")
    };
    let xml6 = to_xml_string(&s6).unwrap();
    assert!(
        xml6.contains("id=\"dec-id\"") && xml6.contains("value=\"123.45\""),
        "decimal1 should have id and value. XML: {}",
        xml6
    );
    assert!(
        xml6.contains("<decimal2 value=\"98.7\""),
        "decimal2 should be present. XML: {}",
        xml6
    );

    // Case 7: Money serialization (always complex)
    let s7 = FhirSerdeTestStruct {
        money1: Money {
            id: Some("money-id".to_string().into()),
            extension: None,
            value: Some(Decimal {
                id: None,
                extension: None,
                value: Some(PreciseDecimal::from_parts(
                    Some(dec!(100.50)),
                    "100.50".to_string(),
                )),
            }),
            currency: Some(Code {
                id: None,
                extension: None,
                value: Some("USD".to_string()),
            }),
        },
        money2: Some(Money {
            id: None,
            extension: Some(vec![default_extension()]),
            value: Some(Decimal {
                id: None,
                extension: None,
                value: Some(PreciseDecimal::from_parts(
                    Some(dec!(200)),
                    "200".to_string(),
                )),
            }),
            currency: None,
        }),
        ..make_default_test_struct("Test7")
    };
    let xml7 = to_xml_string(&s7).unwrap();
    assert!(xml7.contains("<money1"), "XML: {}", xml7);
    assert!(xml7.contains("<currency value=\"USD\""), "XML: {}", xml7);
    assert!(xml7.contains("<money2"), "XML: {}", xml7);

    // Case 8: Vec<String> with mixed values/extensions
    let s8 = FhirSerdeTestStruct {
        given: Some(vec![
            helios_fhir::r4::String {
                id: None,
                extension: None,
                value: Some("Peter".to_string()),
            },
            helios_fhir::r4::String {
                id: Some("given-id-2".to_string()),
                extension: None,
                value: Some("James".to_string()),
            },
            helios_fhir::r4::String {
                id: None,
                extension: Some(vec![default_extension()]),
                value: None,
            },
            helios_fhir::r4::String {
                id: Some("given-id-4".to_string()),
                extension: Some(vec![default_extension()]),
                value: Some("Smith".to_string()),
            },
        ]),
        ..make_default_test_struct("Test8")
    };
    let xml8 = to_xml_string(&s8).unwrap();
    assert!(xml8.contains("Peter"), "XML: {}", xml8);
    assert!(xml8.contains("James"), "XML: {}", xml8);

    // Case 9: Vec<String> with only primitives
    let s9 = FhirSerdeTestStruct {
        given: Some(vec![
            helios_fhir::r4::String {
                id: None,
                extension: None,
                value: Some("Alice".to_string()),
            },
            helios_fhir::r4::String {
                id: None,
                extension: None,
                value: Some("Bob".to_string()),
            },
        ]),
        ..make_default_test_struct("Test9")
    };
    let xml9 = to_xml_string(&s9).unwrap();
    assert!(xml9.contains("<given value=\"Alice\"/>"));
    assert!(xml9.contains("<given value=\"Bob\"/>"));

    // Case 10: Vec<String> with only extensions/ids
    let s10 = FhirSerdeTestStruct {
        given: Some(vec![
            helios_fhir::r4::String {
                id: Some("g1".to_string()),
                extension: None,
                value: None,
            },
            helios_fhir::r4::String {
                id: None,
                extension: Some(vec![default_extension()]),
                value: None,
            },
        ]),
        ..make_default_test_struct("Test10")
    };
    let xml10 = to_xml_string(&s10).unwrap();
    assert!(
        xml10.contains("<given") && xml10.contains("id=\"g1\""),
        "XML: {}",
        xml10
    );

    // Case 11: Vec<String> with null value in middle
    let s11 = FhirSerdeTestStruct {
        given: Some(vec![
            helios_fhir::r4::String {
                id: None,
                extension: None,
                value: Some("First".to_string()),
            },
            helios_fhir::r4::String {
                id: Some("g-null".to_string()),
                extension: None,
                value: None,
            },
            helios_fhir::r4::String {
                id: None,
                extension: None,
                value: Some("Last".to_string()),
            },
        ]),
        ..make_default_test_struct("Test11")
    };
    let xml11 = to_xml_string(&s11).unwrap();
    assert!(xml11.contains("First"));
    assert!(xml11.contains("Last"));
    assert!(xml11.contains("id=\"g-null\""), "XML: {}", xml11);
}

#[test]
fn test_xml_fhir_serde_deserialize() {
    // Case 1: Only primitive value for birthDate1
    let xml1 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test1"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s1: FhirSerdeTestStruct = from_xml_str(xml1).unwrap();
    assert_eq!(s1.name1.value, Some("Test1".to_string()));
    assert_eq!(
        s1.birth_date1.value,
        Some(PrecisionDate::parse("1970-03-30").unwrap())
    );
    assert_eq!(s1.birth_date1.id, None);
    assert_eq!(s1.is_active1.value, Some(true));
    assert_eq!(s1.birth_date2, None);

    // Case 2: Only extension for birthDate1 (id, no value)
    let xml2 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test2"/><birthDate1 id="bd-id"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s2: FhirSerdeTestStruct = from_xml_str(xml2).unwrap();
    assert_eq!(s2.birth_date1.id, Some("bd-id".to_string()));
    assert_eq!(s2.birth_date1.value, None);

    // Case 3: Both primitive value and extension for birthDate1
    let xml3 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test3"/><birthDate1 id="bd-id-3" value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s3: FhirSerdeTestStruct = from_xml_str(xml3).unwrap();
    assert_eq!(s3.birth_date1.id, Some("bd-id-3".to_string()));
    assert_eq!(
        s3.birth_date1.value,
        Some(PrecisionDate::parse("1970-03-30").unwrap())
    );

    // Case 4: isActive1 with extension
    let xml4 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test4"/><birthDate1 value="1970-03-30"/><isActive1><extension url="http://example.com/flag"><valueBoolean value="true"/></extension></isActive1><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s4: FhirSerdeTestStruct = from_xml_str(xml4).unwrap();
    assert!(s4.is_active1.extension.is_some());
    assert_eq!(s4.is_active1.value, None);

    // Case 5: Null primitive but extension exists
    let xml5 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test5"/><birthDate1 id="bd-null"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s5: FhirSerdeTestStruct = from_xml_str(xml5).unwrap();
    assert_eq!(s5.birth_date1.id, Some("bd-null".to_string()));
    assert_eq!(s5.birth_date1.value, None);

    // Case 6: Decimal (primitive and extension)
    let xml6 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test6"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 id="dec-id" value="123.45"/><decimal2 value="98.7"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s6: FhirSerdeTestStruct = from_xml_str(xml6).unwrap();
    assert_eq!(s6.decimal1.id, Some("dec-id".to_string()));
    assert_eq!(
        s6.decimal1.value.as_ref().and_then(|pd| pd.value()),
        Some(dec!(123.45))
    );
    assert!(s6.decimal2.is_some());

    // Case 7: Money deserialization
    let xml7 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test7"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><id value="money-id"/><value value="100.50"/><currency value="USD"/></money1><money2><extension url="http://example.com/ext"><valueString value="ext-val"/></extension><value value="200"/></money2></TestResource>"#;
    let s7: FhirSerdeTestStruct = from_xml_str(xml7).unwrap();
    assert!(s7.money1.id.is_some());
    assert!(s7.money1.currency.is_some());
    assert_eq!(
        s7.money1
            .currency
            .as_ref()
            .and_then(|c| c.value.as_ref())
            .map(|s| s.as_str()),
        Some("USD")
    );
    assert!(s7.money2.is_some());

    // Case 8: Vec<String> with mixed values/extensions
    let xml8 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test8"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1><given value="Peter"/><given id="given-id-2" value="James"/><given><extension url="http://example.com/ext"><valueString value="ext-val"/></extension></given><given id="given-id-4" value="Smith"><extension url="http://example.com/ext"><valueString value="ext-val"/></extension></given></TestResource>"#;
    let s8: FhirSerdeTestStruct = from_xml_str(xml8).unwrap();
    let given = s8.given.as_ref().unwrap();
    assert_eq!(given.len(), 4);
    assert_eq!(given[0].value, Some("Peter".to_string()));
    assert_eq!(given[0].id, None);
    assert_eq!(given[1].value, Some("James".to_string()));
    assert_eq!(given[1].id, Some("given-id-2".to_string()));
    assert_eq!(given[2].value, None);
    assert!(given[2].extension.is_some());
    assert_eq!(given[3].value, Some("Smith".to_string()));
    assert_eq!(given[3].id, Some("given-id-4".to_string()));
    assert!(given[3].extension.is_some());

    // Case 9: Vec<String> with only primitives
    let xml9 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test9"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1><given value="Alice"/><given value="Bob"/></TestResource>"#;
    let s9: FhirSerdeTestStruct = from_xml_str(xml9).unwrap();
    let given9 = s9.given.as_ref().unwrap();
    assert_eq!(given9.len(), 2);
    assert_eq!(given9[0].value, Some("Alice".to_string()));
    assert_eq!(given9[1].value, Some("Bob".to_string()));

    // Case 10: Vec<String> with only extensions/ids
    let xml10 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test10"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1><given id="g1"/><given><extension url="http://example.com/ext"><valueString value="ext-val"/></extension></given></TestResource>"#;
    let s10: FhirSerdeTestStruct = from_xml_str(xml10).unwrap();
    let given10 = s10.given.as_ref().unwrap();
    assert_eq!(given10.len(), 2);
    assert_eq!(given10[0].id, Some("g1".to_string()));
    assert_eq!(given10[0].value, None);
    assert!(given10[1].extension.is_some());
    assert_eq!(given10[1].value, None);

    // Case 11: Vec<String> with null value in middle
    let xml11 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test11"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1><given value="First"/><given id="g-null"/><given value="Last"/></TestResource>"#;
    let s11: FhirSerdeTestStruct = from_xml_str(xml11).unwrap();
    let given11 = s11.given.as_ref().unwrap();
    assert_eq!(given11.len(), 3);
    assert_eq!(given11[0].value, Some("First".to_string()));
    assert_eq!(given11[1].id, Some("g-null".to_string()));
    assert_eq!(given11[1].value, None);
    assert_eq!(given11[2].value, Some("Last".to_string()));

    // Case 12: Single-element given
    let xml12 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test12"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1><given value="OnlyOne"/></TestResource>"#;
    let s12: FhirSerdeTestStruct = from_xml_str(xml12).unwrap();
    let given12 = s12.given.as_ref().unwrap();
    assert_eq!(given12.len(), 1);
    assert_eq!(given12[0].value, Some("OnlyOne".to_string()));

    // Case 13: All optional fields present
    let xml13 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test13"/><name2 value="Optional"/><birthDate1 value="1970-03-30"/><birthDate2 value="1980-06-15"/><isActive1 value="true"/><isActive2 value="false"/><decimal1 value="123.45"/><decimal2 value="678.90"/><money1><value value="123.45"/></money1><money2><value value="99.99"/></money2></TestResource>"#;
    let s13: FhirSerdeTestStruct = from_xml_str(xml13).unwrap();
    assert_eq!(
        s13.name2.as_ref().unwrap().value,
        Some("Optional".to_string())
    );
    assert!(s13.birth_date2.is_some());
    assert!(s13.is_active2.is_some());
    assert_eq!(s13.is_active2.as_ref().unwrap().value, Some(false));
    assert!(s13.decimal2.is_some());
    assert!(s13.money2.is_some());

    // Case 14: Empty optional fields
    let xml14 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test14"/><birthDate1 value="1970-03-30"/><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s14: FhirSerdeTestStruct = from_xml_str(xml14).unwrap();
    assert_eq!(s14.name2, None);
    assert_eq!(s14.birth_date2, None);
    assert_eq!(s14.is_active2, None);
    assert_eq!(s14.decimal2, None);
    assert_eq!(s14.money2, None);
    assert_eq!(s14.given, None);

    // Case 15: Extension with both id and extension on birthDate1
    let xml15 = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><name1 value="Test15"/><birthDate1 id="bd-ext" value="1970-03-30"><extension url="http://example.com/note"><valueString value="some note"/></extension></birthDate1><isActive1 value="true"/><decimal1 value="123.45"/><money1><value value="123.45"/></money1></TestResource>"#;
    let s15: FhirSerdeTestStruct = from_xml_str(xml15).unwrap();
    assert_eq!(s15.birth_date1.id, Some("bd-ext".to_string()));
    assert_eq!(
        s15.birth_date1.value,
        Some(PrecisionDate::parse("1970-03-30").unwrap())
    );
    assert!(s15.birth_date1.extension.is_some());
    let ext = s15.birth_date1.extension.as_ref().unwrap();
    assert_eq!(ext.len(), 1);
    assert_eq!(
        ext[0].url.value,
        Some("http://example.com/note".to_string())
    );
}

#[test]
fn test_xml_fhir_serde_roundtrip() {
    // Case 1: Simple primitive values
    let s1 = make_default_test_struct("RoundTrip1");
    let xml1 = to_xml_string(&s1).unwrap();
    let rt1: FhirSerdeTestStruct = from_xml_str(&xml1).unwrap();
    assert_eq!(s1.name1, rt1.name1);
    assert_eq!(s1.birth_date1, rt1.birth_date1);
    assert_eq!(s1.is_active1, rt1.is_active1);

    // Case 2: With extensions on fields
    let s2 = FhirSerdeTestStruct {
        birth_date1: Date {
            id: Some("bd-rt".to_string()),
            extension: None,
            value: Some(PrecisionDate::parse("1970-03-30").unwrap()),
        },
        ..make_default_test_struct("RoundTrip2")
    };
    let xml2 = to_xml_string(&s2).unwrap();
    let rt2: FhirSerdeTestStruct = from_xml_str(&xml2).unwrap();
    assert_eq!(s2.birth_date1.id, rt2.birth_date1.id);
    assert_eq!(s2.birth_date1.value, rt2.birth_date1.value);

    // Case 3: With Vec<String> containing extensions
    let s3 = FhirSerdeTestStruct {
        given: Some(vec![
            helios_fhir::r4::String {
                id: None,
                extension: None,
                value: Some("Alice".to_string()),
            },
            helios_fhir::r4::String {
                id: Some("g-id".to_string()),
                extension: None,
                value: Some("Bob".to_string()),
            },
        ]),
        ..make_default_test_struct("RoundTrip3")
    };
    let xml3 = to_xml_string(&s3).unwrap();
    let rt3: FhirSerdeTestStruct = from_xml_str(&xml3).unwrap();
    let given_orig = s3.given.as_ref().unwrap();
    let given_rt = rt3.given.as_ref().unwrap();
    assert_eq!(given_orig.len(), given_rt.len());
    assert_eq!(given_orig[0].value, given_rt[0].value);
    assert_eq!(given_orig[1].value, given_rt[1].value);
    assert_eq!(given_orig[1].id, given_rt[1].id);

    // Case 4: With optional fields present
    let s4 = FhirSerdeTestStruct {
        name2: Some("OptName".to_string().into()),
        birth_date2: Some(Date {
            id: None,
            extension: None,
            value: Some(PrecisionDate::parse("1980-06-15").unwrap()),
        }),
        is_active2: Some(Boolean {
            id: None,
            extension: None,
            value: Some(false),
        }),
        ..make_default_test_struct("RoundTrip4")
    };
    let xml4 = to_xml_string(&s4).unwrap();
    let rt4: FhirSerdeTestStruct = from_xml_str(&xml4).unwrap();
    assert_eq!(
        s4.name2.as_ref().unwrap().value,
        rt4.name2.as_ref().unwrap().value
    );
    assert_eq!(s4.birth_date2, rt4.birth_date2);
    assert_eq!(s4.is_active2, rt4.is_active2);

    // Case 5: Money fields roundtrip
    let s5 = FhirSerdeTestStruct {
        money1: Money {
            id: Some("m-id".to_string().into()),
            extension: None,
            value: Some(Decimal {
                id: None,
                extension: None,
                value: Some(PreciseDecimal::from_parts(
                    Some(dec!(55.50)),
                    "55.50".to_string(),
                )),
            }),
            currency: Some(Code {
                id: None,
                extension: None,
                value: Some("EUR".to_string()),
            }),
        },
        ..make_default_test_struct("RoundTrip5")
    };
    let xml5 = to_xml_string(&s5).unwrap();
    let rt5: FhirSerdeTestStruct = from_xml_str(&xml5).unwrap();
    assert!(rt5.money1.id.is_some());
    assert_eq!(
        rt5.money1
            .currency
            .as_ref()
            .and_then(|c| c.value.as_ref())
            .map(|s| s.as_str()),
        Some("EUR")
    );
}

// =============================================================================
// Element<T, E> Tests
// =============================================================================

// XML requires a root element from a FhirSerde struct with resourceType.
// Element types are always used as fields in practice, so we test them that way.
#[derive(Debug, PartialEq, FhirSerde)]
struct ElementTestWrapper {
    #[fhir_serde(rename = "resourceType")]
    resource_type: helios_fhir::r4::String,

    #[fhir_serde(rename = "stringField")]
    string_field: helios_fhir::r4::String,

    #[fhir_serde(rename = "intField")]
    int_field: Option<Integer>,

    #[fhir_serde(rename = "boolField")]
    bool_field: Option<Boolean>,
}

#[test]
fn test_xml_serialize_element_primitive() {
    let wrapper = ElementTestWrapper {
        resource_type: "TestResource".to_string().into(),
        string_field: helios_fhir::r4::String {
            id: None,
            extension: None,
            value: Some("test_value".to_string()),
        },
        int_field: Some(Integer {
            id: None,
            extension: None,
            value: Some(123),
        }),
        bool_field: Some(Boolean {
            id: None,
            extension: None,
            value: Some(true),
        }),
    };
    let xml = to_xml_string(&wrapper).unwrap();
    assert!(
        xml.contains("<stringField value=\"test_value\""),
        "XML: {}",
        xml
    );
    assert!(xml.contains("<intField value=\"123\""), "XML: {}", xml);
    assert!(xml.contains("<boolField value=\"true\""), "XML: {}", xml);
}

#[test]
fn test_xml_serialize_element_object() {
    let wrapper = ElementTestWrapper {
        resource_type: "TestResource".to_string().into(),
        string_field: helios_fhir::r4::String {
            id: Some("elem-id".to_string()),
            extension: Some(vec![Extension {
                id: None,
                extension: None,
                url: "http://example.com/ext1".to_string().into(),
                value: Some(ExtensionValue::Boolean(Boolean {
                    id: None,
                    extension: None,
                    value: Some(true),
                })),
            }]),
            value: Some("test_value".to_string()),
        },
        int_field: None,
        bool_field: None,
    };
    let xml = to_xml_string(&wrapper).unwrap();
    assert!(xml.contains("id=\"elem-id\""), "XML: {}", xml);
    assert!(xml.contains("value=\"test_value\""), "XML: {}", xml);
    assert!(xml.contains("<extension"), "XML: {}", xml);

    // Element with id and extension but no value
    let wrapper_no_value = ElementTestWrapper {
        resource_type: "TestResource".to_string().into(),
        string_field: helios_fhir::r4::String {
            id: Some("elem-id-no-val".to_string()),
            extension: Some(vec![Extension {
                id: None,
                extension: None,
                url: "http://example.com/ext3".to_string().into(),
                value: Some(ExtensionValue::Integer(Integer {
                    id: None,
                    extension: None,
                    value: Some(123),
                })),
            }]),
            value: None,
        },
        int_field: None,
        bool_field: None,
    };
    let xml_no_value = to_xml_string(&wrapper_no_value).unwrap();
    assert!(
        xml_no_value.contains("id=\"elem-id-no-val\""),
        "XML: {}",
        xml_no_value
    );
}

#[test]
fn test_xml_deserialize_element_primitive() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><stringField value="test_value"/><intField value="123"/><boolField value="true"/></TestResource>"#;
    let wrapper: ElementTestWrapper = from_xml_str(xml).unwrap();
    assert_eq!(wrapper.string_field.id, None);
    assert_eq!(wrapper.string_field.extension, None);
    assert_eq!(wrapper.string_field.value, Some("test_value".to_string()));
    assert_eq!(wrapper.int_field.as_ref().unwrap().value, Some(123));
    assert_eq!(wrapper.bool_field.as_ref().unwrap().value, Some(true));
}

#[test]
fn test_xml_deserialize_element_object() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><stringField id="elem-id" value="test_value"><extension url="http://example.com/ext1"><valueBoolean value="true"/></extension></stringField></TestResource>"#;
    let wrapper: ElementTestWrapper = from_xml_str(xml).unwrap();
    assert_eq!(wrapper.string_field.id, Some("elem-id".to_string()));
    assert_eq!(wrapper.string_field.value, Some("test_value".to_string()));
    assert!(wrapper.string_field.extension.is_some());
    let ext = wrapper.string_field.extension.as_ref().unwrap();
    assert_eq!(ext.len(), 1);
    assert_eq!(
        ext[0].url.value,
        Some("http://example.com/ext1".to_string())
    );

    // Element with missing value
    let xml_no_value = r#"<?xml version="1.0" encoding="UTF-8"?><TestResource xmlns="http://hl7.org/fhir"><stringField id="elem-id-no-val"><extension url="http://example.com/ext3"><valueInteger value="123"/></extension></stringField></TestResource>"#;
    let wrapper_no_value: ElementTestWrapper = from_xml_str(xml_no_value).unwrap();
    assert_eq!(
        wrapper_no_value.string_field.id,
        Some("elem-id-no-val".to_string())
    );
    assert_eq!(wrapper_no_value.string_field.value, None);
    assert!(wrapper_no_value.string_field.extension.is_some());
}

// =============================================================================
// DecimalElement Tests
// =============================================================================

#[derive(Debug, PartialEq, FhirSerde)]
struct DecimalTestWrapper {
    #[fhir_serde(rename = "resourceType")]
    resource_type: helios_fhir::r4::String,

    #[fhir_serde(rename = "decimalField")]
    decimal_field: Decimal,
}

#[test]
fn test_xml_decimal_element_value_present() {
    let element = DecimalElement::<Extension> {
        id: None,
        extension: None,
        value: Some(PreciseDecimal::from_parts(
            Some(dec!(1050.00)),
            "1050.00".to_string(),
        )),
    };
    let xml = to_xml_string(&element).unwrap();
    assert!(xml.contains("1050.00"), "XML: {}", xml);
}

#[test]
fn test_xml_decimal_element_value_absent() {
    let wrapper = DecimalTestWrapper {
        resource_type: "TestResource".to_string().into(),
        decimal_field: Decimal {
            id: Some("test-id-123".to_string()),
            extension: None,
            value: None,
        },
    };
    let xml = to_xml_string(&wrapper).unwrap();
    assert!(
        xml.contains("<decimalField") && xml.contains("id=\"test-id-123\""),
        "XML: {}",
        xml
    );
}

#[test]
fn test_xml_decimal_element_all_fields() {
    let wrapper = DecimalTestWrapper {
        resource_type: "TestResource".to_string().into(),
        decimal_field: Decimal {
            id: Some("all-fields-present".to_string()),
            extension: Some(vec![Extension {
                id: None,
                extension: None,
                url: "http://example.com/ext1".to_string().into(),
                value: Some(ExtensionValue::Boolean(Boolean {
                    id: None,
                    extension: None,
                    value: Some(true),
                })),
            }]),
            value: Some(PreciseDecimal::from_parts(
                Some(dec!(-987.654321)),
                "-987.654321".to_string(),
            )),
        },
    };
    let xml = to_xml_string(&wrapper).unwrap();
    assert!(xml.contains("id=\"all-fields-present\""), "XML: {}", xml);
    assert!(xml.contains("-987.654321"), "XML: {}", xml);
    assert!(xml.contains("<extension"), "XML: {}", xml);
}

#[test]
fn test_xml_roundtrip_decimal_serialization() {
    let element = DecimalElement::<Extension> {
        id: None,
        extension: None,
        value: Some(PreciseDecimal::from_parts(
            Some(dec!(123.456)),
            "123.456".to_string(),
        )),
    };
    let xml = to_xml_string(&element).unwrap();
    let deserialized: DecimalElement<Extension> = from_xml_str(&xml).unwrap();
    assert_eq!(
        deserialized.value.as_ref().map(|pd| pd.original_string()),
        Some("123.456"),
        "Original string should be preserved through XML roundtrip"
    );
}

#[test]
fn test_xml_decimal_with_trailing_zeros() {
    let test_cases = [("3.0", dec!(3.0)), ("3.00", dec!(3.00))];
    for (input_str, decimal_val) in test_cases {
        let element = DecimalElement::<Extension> {
            id: None,
            extension: None,
            value: Some(PreciseDecimal::from_parts(
                Some(decimal_val),
                input_str.to_string(),
            )),
        };
        let xml = to_xml_string(&element).unwrap();
        assert!(
            xml.contains(input_str),
            "XML should contain '{}': {}",
            input_str,
            xml
        );

        let deserialized: DecimalElement<Extension> = from_xml_str(&xml).unwrap();
        assert_eq!(
            deserialized.value.as_ref().map(|pd| pd.original_string()),
            Some(input_str),
            "Trailing zeros should be preserved for '{}'",
            input_str
        );
    }
}

#[test]
fn test_xml_decimal_out_of_range() {
    let xml_inputs = [
        ("1E-22", true),
        ("1.000000000000000000E-245", false),
        ("-1.000000000000000000E+245", false),
    ];
    for (input_str, should_have_value) in xml_inputs {
        let element = DecimalElement::<Extension> {
            id: None,
            extension: None,
            value: Some(PreciseDecimal::from_parts(
                if should_have_value {
                    input_str.parse::<rust_decimal::Decimal>().ok()
                } else {
                    None
                },
                input_str.to_string(),
            )),
        };
        let xml = to_xml_string(&element).unwrap();
        assert!(
            xml.contains(input_str),
            "XML should contain '{}': {}",
            input_str,
            xml
        );
    }
}

// =============================================================================
// Timing and Extension Roundtrip Tests
// =============================================================================

#[derive(Debug, PartialEq, FhirSerde)]
struct TimingTestStruct {
    #[fhir_serde(rename = "resourceType")]
    resource_type: helios_fhir::r4::String,

    #[fhir_serde(rename = "timingTiming")]
    timing_timing: Option<Timing>,
}

#[test]
fn test_xml_timing_roundtrip() {
    let timing_struct = TimingTestStruct {
        resource_type: "TestResource".to_string().into(),
        timing_timing: Some(Timing {
            id: None,
            extension: None,
            modifier_extension: None,
            event: Some(vec![DateTime {
                id: None,
                extension: Some(vec![Extension {
                    id: None,
                    extension: None,
                    url: "http://hl7.org/fhir/StructureDefinition/cqf-expression"
                        .to_string()
                        .into(),
                    value: Some(ExtensionValue::Expression(Expression {
                        id: None,
                        extension: None,
                        description: None,
                        name: None,
                        language: Code {
                            id: None,
                            extension: None,
                            value: Some("text/cql".to_string()),
                        },
                        expression: Some(helios_fhir::r4::String {
                            id: None,
                            extension: None,
                            value: Some("Now()".to_string()),
                        }),
                        reference: None,
                    })),
                }]),
                value: None,
            }]),
            repeat: None,
            code: None,
        }),
    };

    let xml = to_xml_string(&timing_struct).unwrap();
    assert!(xml.contains("<timingTiming"), "XML: {}", xml);
    assert!(xml.contains("cqf-expression"), "XML: {}", xml);

    let deserialized: TimingTestStruct = from_xml_str(&xml).unwrap();
    let timing = deserialized.timing_timing.as_ref().unwrap();
    let events = timing.event.as_ref().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].value, None);
    assert!(events[0].extension.is_some());
    let ext = events[0].extension.as_ref().unwrap();
    assert_eq!(ext.len(), 1);
    assert_eq!(
        ext[0].url.value,
        Some("http://hl7.org/fhir/StructureDefinition/cqf-expression".to_string())
    );
}

#[derive(Debug, PartialEq, FhirSerde)]
struct ExtensionTestWrapper {
    #[fhir_serde(rename = "resourceType")]
    resource_type: helios_fhir::r4::String,

    extension: Option<Vec<Extension>>,
}

#[test]
fn test_xml_extension_with_primitive_extension() {
    let wrapper = ExtensionTestWrapper {
        resource_type: "TestResource".to_string().into(),
        extension: Some(vec![Extension {
            id: None,
            extension: None,
            url: "http://hl7.org/fhir/StructureDefinition/codesystem-concept-comments"
                .to_string()
                .into(),
            value: Some(ExtensionValue::String(helios_fhir::r4::String {
                id: None,
                extension: Some(vec![Extension {
                    id: None,
                    extension: Some(vec![
                        Extension {
                            id: None,
                            extension: None,
                            url: "lang".to_string().into(),
                            value: Some(ExtensionValue::Code(Code {
                                id: None,
                                extension: None,
                                value: Some("nl".to_string()),
                            })),
                        },
                        Extension {
                            id: None,
                            extension: None,
                            url: "content".to_string().into(),
                            value: Some(ExtensionValue::String(helios_fhir::r4::String {
                                id: None,
                                extension: None,
                                value: Some("Dutch translation".to_string()),
                            })),
                        },
                    ]),
                    url: "http://hl7.org/fhir/StructureDefinition/translation"
                        .to_string()
                        .into(),
                    value: None,
                }]),
                value: Some("Retained for backwards compatibility only".to_string()),
            })),
        }]),
    };

    let xml = to_xml_string(&wrapper).unwrap();
    assert!(xml.contains("codesystem-concept-comments"), "XML: {}", xml);
    assert!(
        xml.contains("Retained for backwards compatibility only"),
        "XML: {}",
        xml
    );

    let deserialized: ExtensionTestWrapper = from_xml_str(&xml).unwrap();
    let ext = deserialized.extension.as_ref().unwrap();
    assert_eq!(ext.len(), 1);
    assert_eq!(
        ext[0].url.value,
        Some("http://hl7.org/fhir/StructureDefinition/codesystem-concept-comments".to_string())
    );

    match &ext[0].value {
        Some(ExtensionValue::String(s)) => {
            assert_eq!(
                s.value,
                Some("Retained for backwards compatibility only".to_string())
            );
            // Nested extensions on primitive values (_valueString pattern)
            // are serialized as child elements of <valueString>
            if let Some(nested) = &s.extension {
                assert_eq!(nested.len(), 1);
                assert_eq!(
                    nested[0].url.value,
                    Some("http://hl7.org/fhir/StructureDefinition/translation".to_string())
                );
            }
        }
        other => panic!("Expected String extension value, got: {:?}", other),
    }
}

// =============================================================================
// Flatten Test
// =============================================================================

#[derive(Debug, PartialEq, FhirSerde, Default)]
struct FlattenTestStruct {
    #[fhir_serde(rename = "resourceType")]
    resource_type: helios_fhir::r4::String,

    name: helios_fhir::r4::String,

    #[fhir_serde(flatten)]
    nested: NestedStruct,
}

#[derive(Debug, PartialEq, FhirSerde, Default)]
struct NestedStruct {
    field1: helios_fhir::r4::String,
    field2: Integer,
}

#[test]
fn test_xml_flatten_serialization() {
    let test_struct = FlattenTestStruct {
        resource_type: "TestResource".to_string().into(),
        name: "Test".to_string().into(),
        nested: NestedStruct {
            field1: "Nested".to_string().into(),
            field2: 42.into(),
        },
    };

    let xml = to_xml_string(&test_struct).unwrap();
    assert!(xml.contains("<name value=\"Test\""), "XML: {}", xml);
    assert!(
        xml.contains("<field1 value=\"Nested\""),
        "field1 should be flattened. XML: {}",
        xml
    );
    assert!(
        xml.contains("<field2 value=\"42\""),
        "field2 should be flattened. XML: {}",
        xml
    );
    assert!(
        !xml.contains("<nested"),
        "No nested wrapper element. XML: {}",
        xml
    );
}
