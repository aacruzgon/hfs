///! JSON serialization wrapper functions for FHIR resources.
///!
///! This module provides thin wrappers around `serde_json` functions,
///! allowing FHIR resources to be serialized and deserialized using
///! the existing `FhirSerde` derive macro implementations.
use crate::error::Result;
use serde::{Deserialize, Serialize};

/// Deserialize a FHIR resource from a JSON string.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::from_json_str;
/// use helios_fhir::r4::Patient;
///
/// let json = r#"{"resourceType": "Patient", "id": "example"}"#;
/// let patient: Patient = from_json_str(json)?;
/// ```
pub fn from_json_str<'a, T>(s: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    Ok(serde_json::from_str(s)?)
}

/// Serialize a FHIR resource to a JSON string.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::to_json_string;
/// use helios_fhir::r4::Patient;
///
/// let patient = Patient::default();
/// let json = to_json_string(&patient)?;
/// ```
pub fn to_json_string<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    Ok(serde_json::to_string(value)?)
}

/// Serialize a FHIR resource to a pretty-printed JSON string.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::to_json_string_pretty;
/// use helios_fhir::r4::Patient;
///
/// let patient = Patient::default();
/// let json = to_json_string_pretty(&patient)?;
/// ```
pub fn to_json_string_pretty<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    Ok(serde_json::to_string_pretty(value)?)
}

/// Deserialize a FHIR resource from a JSON byte slice.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::from_json_slice;
/// use helios_fhir::r4::Patient;
///
/// let json_bytes = br#"{"resourceType": "Patient", "id": "example"}"#;
/// let patient: Patient = from_json_slice(json_bytes)?;
/// ```
pub fn from_json_slice<'a, T>(v: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    Ok(serde_json::from_slice(v)?)
}

/// Serialize a FHIR resource to a JSON byte vector.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::to_json_vec;
/// use helios_fhir::r4::Patient;
///
/// let patient = Patient::default();
/// let json_bytes = to_json_vec(&patient)?;
/// ```
pub fn to_json_vec<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    Ok(serde_json::to_vec(value)?)
}

/// Serialize a FHIR resource to a `serde_json::Value`.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::to_json_value;
/// use helios_fhir::r4::Patient;
///
/// let patient = Patient::default();
/// let json_value = to_json_value(&patient)?;
/// ```
pub fn to_json_value<T>(value: &T) -> Result<serde_json::Value>
where
    T: Serialize + ?Sized,
{
    Ok(serde_json::to_value(value)?)
}

/// Deserialize a FHIR resource from a `serde_json::Value`.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::json::from_json_value;
/// use helios_fhir::r4::Patient;
/// use serde_json::json;
///
/// let json_value = json!({"resourceType": "Patient", "id": "example"});
/// let patient: Patient = from_json_value(json_value)?;
/// ```
pub fn from_json_value<T>(value: serde_json::Value) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    Ok(serde_json::from_value(value)?)
}
