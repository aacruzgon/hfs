//! XML deserialization implementation using custom serde::Deserializer.
//!
//! This module implements streaming deserialization from FHIR XML to resources
//! by providing a custom `Deserializer` that reads quick-xml events and reconstructs
//! FHIR JSON patterns on-the-fly.

use crate::error::{Result, SerdeError};
use quick_xml::Reader;
use quick_xml::events::{BytesText, Event};
use serde::de::{self, Deserialize, DeserializeSeed, IntoDeserializer, SeqAccess, Visitor};
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::io::BufRead;

/// Deserialize a FHIR resource from an XML string.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::xml::from_xml_str;
/// use helios_fhir::r4::Patient;
///
/// let xml = r#"<?xml version="1.0"?>
/// <Patient xmlns="http://hl7.org/fhir">
///   <id value="example"/>
/// </Patient>"#;
/// let patient: Patient = from_xml_str(xml)?;
/// ```
pub fn from_xml_str<'a, T>(xml: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut deserializer = XmlDeserializer::new(reader);
    T::deserialize(&mut deserializer)
}

/// Deserialize a FHIR resource from XML bytes.
pub fn from_xml_slice<'a, T>(xml: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    let xml_str = std::str::from_utf8(xml)
        .map_err(|e| SerdeError::Custom(format!("Invalid UTF-8: {}", e)))?;
    from_xml_str(xml_str)
}

/// Deserialize a FHIR resource from an XML reader.
pub fn from_xml_reader<R: BufRead, T: de::DeserializeOwned>(reader: R) -> Result<T> {
    let mut xml_reader = Reader::from_reader(reader);
    xml_reader.config_mut().trim_text(true);

    let mut deserializer = XmlDeserializer::new(xml_reader);
    T::deserialize(&mut deserializer)
}

/// XML Deserializer that reads quick-xml events.
struct XmlDeserializer<R: BufRead> {
    reader: Reader<R>,
    /// Buffer for reading events
    buf: Vec<u8>,
    /// Buffered events that have been peeked / unread
    buffered_events: VecDeque<Event<'static>>,
    /// Current element name being deserialized (for resourceType reconstruction)
    current_element_name: String,
    /// Pending container element name for resourceType injection
    pending_element_name: Option<String>,
    /// Whether the pending element represents a FHIR resource (needs resourceType key)
    pending_is_resource: bool,
    /// Stack of element names representing the current XML path
    element_stack: Vec<String>,
    /// Pending attributes from an empty element that should be provided as struct fields
    pending_attributes: Vec<(String, String)>,
}

impl<R: BufRead> XmlDeserializer<R> {
    fn new(reader: Reader<R>) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            buffered_events: VecDeque::new(),
            current_element_name: String::new(),
            pending_element_name: None,
            pending_is_resource: false,
            element_stack: Vec::new(),
            pending_attributes: Vec::new(),
        }
    }

    /// Peek at the next event without consuming it
    fn peek_event(&mut self) -> Result<Option<&Event<'static>>> {
        if self.buffered_events.is_empty() {
            self.buf.clear();
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Eof) => return Ok(None),
                Ok(event) => {
                    self.buffered_events.push_back(event.into_owned());
                }
                Err(e) => {
                    return Err(SerdeError::Custom(format!("XML parse error: {}", e)));
                }
            }
        }
        Ok(self.buffered_events.front())
    }

    /// Get the next event, using peeked if available
    fn next_event(&mut self) -> Result<Event<'static>> {
        if let Some(event) = self.buffered_events.pop_front() {
            return Ok(event);
        }

        self.buf.clear();
        match self.reader.read_event_into(&mut self.buf) {
            Ok(event) => Ok(event.into_owned()),
            Err(e) => Err(SerdeError::Custom(format!("XML parse error: {}", e))),
        }
    }

    fn push_front_event(&mut self, event: Event<'static>) {
        self.buffered_events.push_front(event);
    }

    fn deserialize_wrapped_resource<'de, V>(
        &mut self,
        visitor: V,
        wrapper_name: &str,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        loop {
            match self.peek_event()? {
                Some(Event::Start(_)) => break,
                Some(Event::End(end)) => {
                    if end.name().as_ref() == wrapper_name.as_bytes() {
                        self.next_event()?;
                        return Err(SerdeError::Custom(format!(
                            "{} element missing resource",
                            wrapper_name
                        )));
                    } else {
                        self.next_event()?;
                        continue;
                    }
                }
                Some(Event::Text(text)) => {
                    if is_whitespace_text(&text)? {
                        self.next_event()?;
                        continue;
                    } else {
                        return Err(SerdeError::Custom(format!(
                            "Unexpected text inside <{}>",
                            wrapper_name
                        )));
                    }
                }
                Some(Event::Comment(_))
                | Some(Event::CData(_))
                | Some(Event::PI(_))
                | Some(Event::Decl(_))
                | Some(Event::DocType(_))
                | Some(Event::GeneralRef(_)) => {
                    self.next_event()?;
                    continue;
                }
                Some(Event::Empty(_)) => {
                    return Err(SerdeError::Custom(format!(
                        "{} resource cannot be an empty element",
                        wrapper_name
                    )));
                }
                Some(Event::Eof) | None => {
                    return Err(SerdeError::Custom(format!(
                        "Unexpected EOF inside <{}>",
                        wrapper_name
                    )));
                }
            }
        }

        if let Some(Event::Start(start)) = self.peek_event()? {
            let resource_name = String::from_utf8_lossy(start.name().as_ref()).to_string();
            self.current_element_name = resource_name.clone();
            self.pending_element_name = Some(resource_name);
            self.pending_is_resource = true;
            // Consume the resource start event so standard map logic can run
            self.next_event()?;
            let value = <&mut Self as de::Deserializer<'de>>::deserialize_map(self, visitor)?;
            self.consume_specific_end(wrapper_name.as_bytes())?;
            Ok(value)
        } else {
            Err(SerdeError::Custom(format!(
                "Expected resource element inside <{}>",
                wrapper_name
            )))
        }
    }

    fn consume_specific_end(&mut self, expected: &[u8]) -> Result<()> {
        loop {
            match self.peek_event()? {
                Some(Event::End(end)) => {
                    if end.name().as_ref() == expected {
                        self.next_event()?;
                        return Ok(());
                    } else {
                        self.next_event()?;
                        continue;
                    }
                }
                Some(Event::Text(text)) => {
                    if is_whitespace_text(&text)? {
                        self.next_event()?;
                        continue;
                    } else {
                        return Err(SerdeError::Custom(format!(
                            "Unexpected text before closing </{}>",
                            String::from_utf8_lossy(expected)
                        )));
                    }
                }
                Some(Event::Comment(_))
                | Some(Event::CData(_))
                | Some(Event::PI(_))
                | Some(Event::Decl(_))
                | Some(Event::DocType(_))
                | Some(Event::GeneralRef(_)) => {
                    self.next_event()?;
                    continue;
                }
                Some(Event::Eof) | None => {
                    return Err(SerdeError::Custom(format!(
                        "Unexpected EOF before closing </{}>",
                        String::from_utf8_lossy(expected)
                    )));
                }
                Some(_) => {
                    return Err(SerdeError::Custom(format!(
                        "Unexpected event before closing </{}>",
                        String::from_utf8_lossy(expected)
                    )));
                }
            }
        }
    }
}

impl<R: BufRead> XmlDeserializer<R> {
    fn path_matches_suffix(&self, suffix: &[&str]) -> bool {
        if suffix.len() > self.element_stack.len() {
            return false;
        }
        let start = self.element_stack.len() - suffix.len();
        self.element_stack[start..]
            .iter()
            .map(|segment| segment.as_str())
            .zip(suffix.iter().copied())
            .all(|(current, expected)| current == expected)
    }

    fn is_named_resource_container(
        &self,
        parent_suffix: &[&str],
        child_name: &str,
        element_name: &str,
    ) -> bool {
        element_name == child_name && self.path_matches_suffix(parent_suffix)
    }

    fn is_resource_container(&self, element_name: &str) -> bool {
        if element_name == "contained" {
            return true;
        }

        self.is_named_resource_container(&["Bundle", "entry"], "resource", element_name)
            || self.is_named_resource_container(
                &["Bundle", "entry", "response"],
                "outcome",
                element_name,
            )
            || self.is_named_resource_container(
                &["Parameters", "parameter"],
                "resource",
                element_name,
            )
    }
}

fn event_has_value_attribute(e: &quick_xml::events::BytesStart) -> bool {
    for attr in e.attributes() {
        if let Ok(attr) = attr {
            if attr.key.as_ref() == b"value" {
                return true;
            }
        }
    }
    false
}

/// Check if an element has attributes other than "value" and xmlns namespaces.
/// If it does, it should be deserialized as a struct to preserve all attributes.
fn event_has_non_value_attributes(e: &quick_xml::events::BytesStart) -> bool {
    for attr in e.attributes() {
        if let Ok(attr) = attr {
            let key = attr.key.as_ref();
            // Ignore "value" and xmlns namespace declarations
            if key != b"value" && !key.starts_with(b"xmlns") {
                return true;
            }
        }
    }
    false
}

impl<'de, 'a, R: BufRead> de::Deserializer<'de> for &'a mut XmlDeserializer<R> {
    type Error = SerdeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.current_element_name == "div" {
            return self.deserialize_string(visitor);
        }
        loop {
            let next_event = self.peek_event()?;
            match next_event {
                Some(Event::Empty(e)) => {
                    // Check if element has attributes other than value (like id, extension, etc.)
                    let has_non_value_attrs = event_has_non_value_attributes(e);

                    if event_has_value_attribute(e) && !has_non_value_attrs {
                        // Only has value attribute - extract just the value
                        let value = self.get_value_attribute()?;
                        return visitor.visit_string(value);
                    } else {
                        // Has other attributes or no value attribute - deserialize as struct/map
                        let element_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                        let end_event = Event::End(e.to_end()).into_owned();

                        // Extract all attributes into a temporary vector (before any mutating operations)
                        let mut attrs = Vec::new();
                        for attr in e.attributes() {
                            let attr = attr.map_err(|e| {
                                SerdeError::Custom(format!("Failed to parse attribute: {}", e))
                            })?;
                            let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
                            let value = String::from_utf8_lossy(&attr.value).to_string();
                            // Skip xmlns namespace declarations
                            if !key.starts_with("xmlns") {
                                attrs.push((key, value));
                            }
                        }

                        // Now we can mutate self
                        if self.is_resource_container(&element_name) {
                            return Err(SerdeError::Custom(format!(
                                "{} resource cannot be an empty element",
                                element_name
                            )));
                        }

                        // Store the extracted attributes
                        self.pending_attributes = attrs;

                        // Treat `<foo/>` as an empty element with no children by synthesizing an end event
                        // Consume the empty event we just inspected
                        self.next_event()?;
                        self.pending_element_name = Some(element_name);
                        self.pending_is_resource = self.element_stack.is_empty();
                        // Push the synthetic end event so map access knows when to stop
                        self.push_front_event(end_event);
                        return self.deserialize_map(visitor);
                    }
                }
                Some(Event::Start(e)) => {
                    // Check if element has attributes other than value (like id, extension, etc.)
                    let has_non_value_attrs = event_has_non_value_attributes(e);

                    if event_has_value_attribute(e) && !has_non_value_attrs {
                        // Only has value attribute - extract just the value
                        let value = self.get_value_attribute()?;
                        return visitor.visit_string(value);
                    } else {
                        let element_name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                        // Extract all attributes into a temporary vector (before any mutating operations)
                        let mut attrs = Vec::new();
                        for attr in e.attributes() {
                            let attr = attr.map_err(|e| {
                                SerdeError::Custom(format!("Failed to parse attribute: {}", e))
                            })?;
                            let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
                            let value = String::from_utf8_lossy(&attr.value).to_string();
                            // Skip xmlns namespace declarations
                            if !key.starts_with("xmlns") {
                                attrs.push((key, value));
                            }
                        }

                        // Store the extracted attributes
                        self.pending_attributes = attrs;

                        self.next_event()?;
                        if self.is_resource_container(&element_name) {
                            return self.deserialize_wrapped_resource(visitor, &element_name);
                        }
                        self.pending_element_name = Some(element_name);
                        self.pending_is_resource = self.element_stack.is_empty();
                        return self.deserialize_map(visitor);
                    }
                }
                Some(Event::Text(text)) => {
                    let text_value = String::from_utf8_lossy(text.as_ref()).trim().to_string();
                    self.next_event()?; // consume the text event
                    if text_value.is_empty() {
                        return visitor.visit_unit();
                    } else {
                        return visitor.visit_string(text_value);
                    }
                }
                Some(Event::End(_)) => {
                    self.next_event()?; // consume the end event
                    return visitor.visit_unit();
                }
                Some(Event::Comment(_)) | Some(Event::CData(_)) | Some(Event::PI(_)) => {
                    self.next_event()?; // skip non-data events
                    continue;
                }
                Some(Event::Decl(_)) | Some(Event::DocType(_)) | Some(Event::GeneralRef(_)) => {
                    self.next_event()?; // skip and continue
                    continue;
                }
                Some(Event::Eof) => {
                    self.next_event()?;
                    return Err(SerdeError::Custom("Unexpected EOF".to_string()));
                }
                None => {
                    return Err(SerdeError::Custom("Unexpected EOF".to_string()));
                }
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let b = match value_str.as_str() {
            "true" => true,
            "false" => false,
            _ => {
                return Err(SerdeError::Custom(format!(
                    "Invalid boolean value: {}",
                    value_str
                )));
            }
        };
        visitor.visit_bool(b)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid i8: {}", e)))?;
        visitor.visit_i8(n)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid i16: {}", e)))?;
        visitor.visit_i16(n)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid i32: {}", e)))?;
        visitor.visit_i32(n)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid i64: {}", e)))?;
        visitor.visit_i64(n)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid u8: {}", e)))?;
        visitor.visit_u8(n)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid u16: {}", e)))?;
        visitor.visit_u16(n)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid u32: {}", e)))?;
        visitor.visit_u32(n)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid u64: {}", e)))?;
        visitor.visit_u64(n)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid f32: {}", e)))?;
        visitor.visit_f32(n)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let n = value_str
            .parse()
            .map_err(|e| SerdeError::Custom(format!("Invalid f64: {}", e)))?;
        visitor.visit_f64(n)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value_str = self.get_value_attribute()?;
        let c = value_str
            .chars()
            .next()
            .ok_or_else(|| SerdeError::Custom("Empty character value".to_string()))?;
        visitor.visit_char(c)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Special case: "div" field contains raw XHTML content
        if self.current_element_name == "div" {
            let inner_xml = self.get_inner_xml()?;
            return visitor.visit_str(&inner_xml);
        }

        let value_str = self.get_value_attribute()?;
        visitor.visit_str(&value_str)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Special case: "div" field contains raw XHTML content
        if self.current_element_name == "div" {
            let inner_xml = self.get_inner_xml()?;
            return visitor.visit_string(inner_xml);
        }

        let value_str = self.get_value_attribute()?;
        visitor.visit_string(value_str)
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(SerdeError::Custom(
            "Bytes not supported in FHIR XML".to_string(),
        ))
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(SerdeError::Custom(
            "Byte buf not supported in FHIR XML".to_string(),
        ))
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Options are always Some if we're deserializing them
        // (None values don't appear in XML)
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Sequences are handled by looking ahead for repeated elements
        visitor.visit_seq(ElementSeqAccess::new(self))
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(SerdeError::Custom(
            "Tuples not supported in FHIR XML".to_string(),
        ))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(SerdeError::Custom(
            "Tuple structs not supported in FHIR XML".to_string(),
        ))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Maps/structs deserialize the same way in XML
        visitor.visit_map(ElementMapAccess::new(self))
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // If we already have a pending element (e.g., from an empty element like <foo/>),
        // use that directly without looking for a new Start event
        if self.pending_element_name.is_some() {
            return visitor.visit_map(ElementMapAccess::new(self));
        }

        // Skip to the start element (handle XML declaration, etc.)
        loop {
            match self.next_event()? {
                Event::Start(e) => {
                    // Store the resource type as a field
                    let element_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    self.pending_element_name = Some(element_name);
                    self.pending_is_resource = self.element_stack.is_empty();
                    // Pass control to map access to read fields
                    return visitor.visit_map(ElementMapAccess::new(self));
                }
                Event::Decl(_) | Event::Comment(_) | Event::DocType(_) => continue,
                Event::Eof => {
                    return Err(SerdeError::Custom(format!(
                        "Unexpected EOF while looking for {}",
                        name
                    )));
                }
                _ => continue,
            }
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = JsonValue::deserialize(self)?;
        value
            .into_deserializer()
            .deserialize_enum(name, variants, visitor)
            .map_err(|err| SerdeError::Custom(err.to_string()))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Return the current element name as an identifier
        visitor.visit_str(&self.current_element_name)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Skip the current element
        self.skip_element()?;
        visitor.visit_unit()
    }
}

impl<R: BufRead> XmlDeserializer<R> {
    /// Get the value from the "value" attribute of the current element
    fn get_value_attribute(&mut self) -> Result<String> {
        // Read the current element
        let event = self.next_event()?;
        match event {
            Event::Empty(e) => {
                // Look for value attribute
                for attr in e.attributes() {
                    let attr = attr.map_err(|e| {
                        SerdeError::Custom(format!("Failed to parse attribute: {}", e))
                    })?;
                    if attr.key.as_ref() == b"value" {
                        let value = String::from_utf8_lossy(&attr.value).to_string();
                        return Ok(value);
                    }
                }
                Err(SerdeError::Custom("No value attribute found".to_string()))
            }
            Event::Start(e) => {
                // Look for value attribute
                for attr in e.attributes() {
                    let attr = attr.map_err(|e| {
                        SerdeError::Custom(format!("Failed to parse attribute: {}", e))
                    })?;
                    if attr.key.as_ref() == b"value" {
                        let value = String::from_utf8_lossy(&attr.value).to_string();
                        // Skip to the end tag
                        self.skip_to_end_element()?;
                        return Ok(value);
                    }
                }
                Err(SerdeError::Custom("No value attribute found".to_string()))
            }
            _ => Err(SerdeError::Custom(
                "Expected element with value attribute".to_string(),
            )),
        }
    }

    /// Skip to the matching end element
    fn skip_to_end_element(&mut self) -> Result<()> {
        let mut depth = 1;
        loop {
            match self.next_event()? {
                Event::Start(_) => depth += 1,
                Event::End(_) => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Event::Eof => {
                    return Err(SerdeError::Custom("Unexpected EOF".to_string()));
                }
                _ => {}
            }
        }
    }

    /// Skip the current element entirely
    fn skip_element(&mut self) -> Result<()> {
        let event = self.next_event()?;
        if matches!(event, Event::Start(_)) {
            self.skip_to_end_element()?;
        }
        Ok(())
    }

    /// Get the raw inner XML content of the current element (for XHTML div)
    fn get_inner_xml(&mut self) -> Result<String> {
        use quick_xml::Writer;
        use std::io::Cursor;

        // Read the opening tag
        let event = self.next_event()?;
        let mut result = Vec::new();
        let mut writer = Writer::new(Cursor::new(&mut result));

        match event {
            Event::Start(e) => {
                // Write the start tag with attributes
                writer
                    .write_event(Event::Start(e.clone()))
                    .map_err(|e| SerdeError::Custom(format!("Failed to write XML: {}", e)))?;

                // Read and write all content until matching end tag
                let mut depth = 1;
                loop {
                    let inner_event = self.next_event()?;
                    match &inner_event {
                        Event::Start(_) => depth += 1,
                        Event::End(_) => {
                            depth -= 1;
                            if depth == 0 {
                                writer.write_event(inner_event.clone()).map_err(|e| {
                                    SerdeError::Custom(format!("Failed to write XML: {}", e))
                                })?;
                                break;
                            }
                        }
                        Event::Eof => {
                            return Err(SerdeError::Custom(
                                "Unexpected EOF in inner XML".to_string(),
                            ));
                        }
                        _ => {}
                    }

                    writer
                        .write_event(inner_event.clone())
                        .map_err(|e| SerdeError::Custom(format!("Failed to write XML: {}", e)))?;
                }
            }
            Event::Empty(e) => {
                // Self-closing tag
                writer
                    .write_event(Event::Empty(e))
                    .map_err(|e| SerdeError::Custom(format!("Failed to write XML: {}", e)))?;
            }
            _ => {
                return Err(SerdeError::Custom(
                    "Expected element for inner XML".to_string(),
                ));
            }
        }

        String::from_utf8(result)
            .map_err(|e| SerdeError::Custom(format!("Invalid UTF-8 in XML: {}", e)))
    }
}


/// MapAccess implementation for deserializing XML elements as struct fields
struct ElementMapAccess<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    /// Element name (for resourceType field)
    element_name: Option<String>,
    /// Whether this map pushed a new element onto the stack
    pushed_element: bool,
    /// Whether this element should emit a synthetic resourceType field
    should_emit_resource_type: bool,
    /// Whether we've emitted resourceType yet
    emitted_resource_type: bool,
    /// Whether the next value should be the resourceType value
    emit_resource_type_value: bool,
    pending_field_name: Option<String>,
    /// Attributes from the current element being deserialized
    attributes: Vec<(String, String)>,
    /// Current attribute index for providing attributes as fields
    attribute_index: usize,
    /// Whether the next value should be an attribute value
    emit_attribute_value: bool,
}

impl<'a, R: BufRead> ElementMapAccess<'a, R> {
    fn new(de: &'a mut XmlDeserializer<R>) -> Self {
        let element_name = de.pending_element_name.take();
        let should_emit_resource_type =
            element_name.is_some() && std::mem::take(&mut de.pending_is_resource);
        let pushed_element = match &element_name {
            Some(name) => {
                de.element_stack.push(name.clone());
                true
            }
            None => false,
        };
        // Take ownership of attributes so nested deserializations don't see them
        let attributes = std::mem::take(&mut de.pending_attributes);
        Self {
            de,
            element_name,
            pushed_element,
            should_emit_resource_type,
            emitted_resource_type: false,
            emit_resource_type_value: false,
            pending_field_name: None,
            attributes,
            attribute_index: 0,
            emit_attribute_value: false,
        }
    }


    fn consume_insignificant_events(&mut self) -> Result<()> {
        loop {
            let should_consume = match self.de.peek_event()? {
                Some(Event::Text(text)) => is_whitespace_text(text)?,
                Some(Event::Comment(_)) | Some(Event::PI(_)) => true,
                _ => false,
            };
            if !should_consume {
                break;
            }
            self.de.next_event()?;
        }
        Ok(())
    }

}

impl<'a, R: BufRead> Drop for ElementMapAccess<'a, R> {
    fn drop(&mut self) {
        if self.pushed_element {
            self.de.element_stack.pop();
        }
    }
}
impl<'de, 'a, R: BufRead + 'a> de::MapAccess<'de> for ElementMapAccess<'a, R> {
    type Error = SerdeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: de::DeserializeSeed<'de>,
    {
        // First, emit resourceType if we have an element name
        if self.should_emit_resource_type && !self.emitted_resource_type {
            self.emitted_resource_type = true;
            self.emit_resource_type_value = true;
            let key = "resourceType".to_string();
            self.de.current_element_name = key.clone();
            self.pending_field_name = Some(key.clone());
            return seed.deserialize(key.into_deserializer()).map(Some);
        }

        // Second, emit any pending attributes as fields
        if self.attribute_index < self.attributes.len() {
            let (key, _) = &self.attributes[self.attribute_index];
            self.emit_attribute_value = true;
            self.pending_field_name = Some(key.clone());
            return seed.deserialize(key.clone().into_deserializer()).map(Some);
        }

        // Finally, read next child element
        loop {
            let event = self.de.next_event()?;
            match event {
                Event::Start(e) => {
                    let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    self.de.current_element_name = name.clone();
                    self.de.push_front_event(Event::Start(e));
                    self.pending_field_name = Some(name.clone());
                    return seed.deserialize(name.into_deserializer()).map(Some);
                }
                Event::Empty(e) => {
                    let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    self.de.current_element_name = name.clone();
                    self.de.push_front_event(Event::Empty(e));
                    self.pending_field_name = Some(name.clone());
                    return seed.deserialize(name.into_deserializer()).map(Some);
                }
                Event::End(_) => return Ok(None),
                Event::Eof => {
                    return Ok(None);
                }
                Event::Text(_) | Event::Comment(_) | Event::CData(_) => {
                    continue;
                }
                _ => continue,
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: de::DeserializeSeed<'de>,
    {
        // Special case: resourceType value
        if self.emit_resource_type_value {
            self.emit_resource_type_value = false;
            let name = self.element_name.as_ref().unwrap().clone();
            self.pending_field_name = None;
            return seed.deserialize(de::value::StrDeserializer::<SerdeError>::new(&name));
        }

        // Special case: attribute value
        if self.emit_attribute_value {
            self.emit_attribute_value = false;
            let (_, value) = &self.attributes[self.attribute_index];
            self.attribute_index += 1;
            self.pending_field_name = None;
            return seed.deserialize(de::value::StrDeserializer::<SerdeError>::new(value));
        }

        let field_name = self
            .pending_field_name
            .take()
            .unwrap_or_else(|| self.de.current_element_name.clone());

        // Check if field exists
        self.consume_insignificant_events()?;
        let has_field = match self.de.peek_event()? {
            Some(Event::Start(e)) | Some(Event::Empty(e)) => {
                e.name().as_ref() == field_name.as_bytes()
            }
            _ => false,
        };

        if !has_field {
            return Err(SerdeError::Custom(format!("field {} missing", field_name)));
        }

        // Set context for field deserialization
        self.de.current_element_name = field_name.clone();

        // Use FieldValueDeserializer for all fields to handle both single and multiple occurrences.
        // We can't detect SingleOrVec fields by type name because serde wraps everything in
        // PhantomData<Content> when deserializing untagged enums (which FHIR resources use).
        // This adds overhead but ensures correct deserialization of repeating elements.
        let field_deser = FieldValueDeserializer::new(self.de, field_name.clone())?;
        seed.deserialize(field_deser).map_err(|err| {
            SerdeError::Custom(format!("field {}: {}", field_name, err))
        })
    }
}

/// Deserializer for field values that handles both single and sequence cases.
///
/// To support untagged enums like SingleOrVec with streaming XML, we buffer
/// all occurrences of the current field, count them, then replay the buffered
/// events during deserialization. This allows us to decide upfront whether to
/// deserialize as a single value or a sequence.
struct FieldValueDeserializer<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    field_name: String,
    buffered_events: Vec<quick_xml::events::Event<'static>>,
    occurrence_count: usize,
}

impl<'a, R: BufRead> FieldValueDeserializer<'a, R> {
    fn new(de: &'a mut XmlDeserializer<R>, field_name: String) -> Result<Self> {
        let mut buffered_events = Vec::new();

        // Skip whitespace and comments before first occurrence
        loop {
            match de.peek_event()? {
                Some(Event::Text(text)) => {
                    if !is_whitespace_text(&text)? {
                        break;
                    }
                    let event = de.next_event()?;
                    buffered_events.push(event.into_owned());
                }
                Some(Event::Comment(_)) | Some(Event::PI(_)) => {
                    let event = de.next_event()?;
                    buffered_events.push(event.into_owned());
                }
                _ => break,
            }
        }

        // Buffer the first occurrence (must exist since XML drives deserialization)
        let start_event = de.next_event()?;
        let is_empty = matches!(start_event, Event::Empty(_));
        buffered_events.push(start_event.into_owned());

        if !is_empty {
            // Buffer until we find the matching end tag
            let mut depth = 1;
            loop {
                let event = de.next_event()?;

                match &event {
                    Event::Eof => {
                        return Err(SerdeError::Custom(format!(
                            "Unexpected EOF while reading field {}", field_name
                        )));
                    }
                    Event::Start(_) => {
                        depth += 1;
                        buffered_events.push(event.into_owned());
                    }
                    Event::End(_) => {
                        depth -= 1;
                        buffered_events.push(event.into_owned());
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {
                        buffered_events.push(event.into_owned());
                    }
                }
            }
        }

        // Now check if there's a second occurrence (peek ahead after skipping whitespace)
        loop {
            match de.peek_event()? {
                Some(Event::Text(text)) => {
                    if !is_whitespace_text(&text)? {
                        break;
                    }
                    de.next_event()?; // consume whitespace but don't buffer
                }
                Some(Event::Comment(_)) | Some(Event::PI(_)) => {
                    de.next_event()?; // consume but don't buffer
                }
                _ => break,
            }
        }

        // Check if there's a second occurrence
        let has_second_occurrence = match de.peek_event()? {
            Some(Event::Start(e)) | Some(Event::Empty(e)) => {
                e.name().as_ref() == field_name.as_bytes()
            }
            _ => false,
        };

        let occurrence_count = if has_second_occurrence { 2 } else { 1 };

        Ok(Self {
            de,
            field_name,
            buffered_events,
            occurrence_count,
        })
    }
}

impl<'de, 'a, R: BufRead> de::Deserializer<'de> for FieldValueDeserializer<'a, R> {
    type Error = SerdeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.occurrence_count == 1 {
            // Single occurrence - deserialize directly (not as sequence)
            // Push buffered events back for the single element
            for event in self.buffered_events.into_iter().rev() {
                self.de.buffered_events.push_front(event);
            }
            self.de.current_element_name = self.field_name.clone();
            self.de.deserialize_any(visitor)
        } else {
            // Multiple occurrences (2+) - provide sequence interface
            // Push buffered first occurrence back, then stream the rest
            for event in self.buffered_events.into_iter().rev() {
                self.de.buffered_events.push_front(event);
            }
            let seq = BufferedFieldSeqAccess {
                de: self.de,
                field_name: self.field_name,
            };
            visitor.visit_seq(seq)
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // When deserialize_seq is explicitly called, always provide sequence interface
        // This handles the case where untagged enum tries Vec variant first

        // Push buffered events back into deserializer's buffered_events (in reverse order)
        for event in self.buffered_events.into_iter().rev() {
            self.de.buffered_events.push_front(event);
        }

        // One or more elements - provide sequence interface
        let seq = BufferedFieldSeqAccess {
            de: self.de,
            field_name: self.field_name,
        };
        visitor.visit_seq(seq)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // For Option<T> fields, we need to check if there's actually a field occurrence.
        // If there is, wrap it in Some(). If not, return None.

        if self.occurrence_count > 0 {
            // We have at least one occurrence - call visit_some with self as the deserializer
            visitor.visit_some(self)
        } else {
            // No occurrences - this shouldn't happen since we buffer at least one occurrence
            // in the constructor, but handle it gracefully
            visitor.visit_none()
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// Sequence accessor for field occurrences.
///
/// Streams all occurrences of a field by checking if the next element matches the field name.
struct BufferedFieldSeqAccess<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    field_name: String,
}

impl<'de, 'a, R: BufRead> SeqAccess<'de> for BufferedFieldSeqAccess<'a, R> {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        // Skip whitespace and comments
        loop {
            match self.de.peek_event()? {
                Some(Event::Text(text)) => {
                    if !is_whitespace_text(&text)? {
                        break;
                    }
                    self.de.next_event()?;
                }
                Some(Event::Comment(_)) | Some(Event::PI(_)) => {
                    self.de.next_event()?;
                }
                _ => break,
            }
        }

        // Check if there's another element with the same field name
        let has_next = match self.de.peek_event()? {
            Some(Event::Start(e)) | Some(Event::Empty(e)) => {
                e.name().as_ref() == self.field_name.as_bytes()
            }
            _ => false,
        };

        if !has_next {
            return Ok(None);
        }

        // Deserialize this occurrence
        self.de.current_element_name = self.field_name.clone();
        seed.deserialize(&mut *self.de).map(Some)
    }
}

fn is_whitespace_text(text: &BytesText) -> Result<bool> {
    Ok(text
        .as_ref()
        .iter()
        .all(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t')))
}

/// Custom deserializer for extension data that handles Option
/// SeqAccess implementation for deserializing repeated XML elements as arrays
struct ElementSeqAccess<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    element_name: Option<String>,
}

impl<'a, R: BufRead> ElementSeqAccess<'a, R> {
    fn new(de: &'a mut XmlDeserializer<R>) -> Self {
        Self {
            de,
            element_name: None,
        }
    }
}

impl<'de, 'a, R: BufRead + 'a> de::SeqAccess<'de> for ElementSeqAccess<'a, R> {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        loop {
            match self.de.peek_event()? {
                Some(Event::Start(e)) | Some(Event::Empty(e)) => {
                    let name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                    if self.element_name.is_none() {
                        self.element_name = Some(name.clone());
                    }

                    if self.element_name.as_ref() == Some(&name) {
                        return seed.deserialize(&mut *self.de).map(Some);
                    } else {
                        return Ok(None);
                    }
                }
                Some(Event::End(_)) | Some(Event::Eof) | None => return Ok(None),
                _ => {
                    self.de.next_event()?;
                }
            }
        }
    }
}


