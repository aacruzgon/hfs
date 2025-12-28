//! XML deserialization implementation using custom serde::Deserializer.
//!
//! This module implements streaming deserialization from FHIR XML to resources
//! by providing a custom `Deserializer` that reads quick-xml events and reconstructs
//! FHIR JSON patterns on-the-fly.

use crate::error::{Result, SerdeError};
use quick_xml::Reader;
use quick_xml::events::{BytesText, Event};
use serde::de::{self, Deserialize, DeserializeSeed, IntoDeserializer, Visitor};
use serde_json::Value as JsonValue;
use serde_path_to_error::{Deserializer as PathDeserializer, Track};
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
    let mut track = Track::new();
    let path_deserializer = PathDeserializer::new(&mut deserializer, &mut track);
    T::deserialize(path_deserializer).map_err(move |err| {
        let path = track.path();
        SerdeError::Custom(format!("{} at {}", err, path))
    })
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
    let mut track = Track::new();
    let path_deserializer = PathDeserializer::new(&mut deserializer, &mut track);
    T::deserialize(path_deserializer).map_err(move |err| {
        let path = track.path();
        SerdeError::Custom(format!("{} at {}", err, path))
    })
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
    /// Stack of element names representing the current XML path
    element_stack: Vec<String>,
}

impl<R: BufRead> XmlDeserializer<R> {
    fn new(reader: Reader<R>) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            buffered_events: VecDeque::new(),
            current_element_name: String::new(),
            pending_element_name: None,
            element_stack: Vec::new(),
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
                    if event_has_value_attribute(e) {
                        let value = self.get_value_attribute()?;
                        return visitor.visit_string(value);
                    } else {
                        let element_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                        let end_event = Event::End(e.to_end()).into_owned();
                        if self.is_resource_container(&element_name) {
                            return Err(SerdeError::Custom(format!(
                                "{} resource cannot be an empty element",
                                element_name
                            )));
                        }
                        // Treat `<foo/>` as an empty element with no children by synthesizing an end event
                        // Consume the empty event we just inspected
                        self.next_event()?;
                        self.pending_element_name = Some(element_name);
                        // Push the synthetic end event so map access knows when to stop
                        self.push_front_event(end_event);
                        return self.deserialize_map(visitor);
                    }
                }
                Some(Event::Start(e)) => {
                    if event_has_value_attribute(e) {
                        let value = self.get_value_attribute()?;
                        return visitor.visit_string(value);
                    } else {
                        let element_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                        self.next_event()?;
                        if self.is_resource_container(&element_name) {
                            return self.deserialize_wrapped_resource(visitor, &element_name);
                        }
                        self.pending_element_name = Some(element_name);
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
        // Skip to the start element (handle XML declaration, etc.)
        loop {
            match self.next_event()? {
                Event::Start(e) => {
                    // Store the resource type as a field
                    let element_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    self.pending_element_name = Some(element_name);
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

/// Collected occurrence with element stack context
#[derive(Clone)]
struct CollectedOccurrence {
    events: Vec<Event<'static>>,
    element_stack: Vec<String>,
}

/// MapAccess implementation for deserializing XML elements as struct fields
struct ElementMapAccess<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    /// Element name (for resourceType field)
    element_name: Option<String>,
    /// Whether this map pushed a new element onto the stack
    pushed_element: bool,
    /// Whether we've emitted resourceType yet
    emitted_resource_type: bool,
    /// Whether the next value should be the resourceType value
    emit_resource_type_value: bool,
    pending_field_name: Option<String>,
}

impl<'a, R: BufRead> ElementMapAccess<'a, R> {
    fn new(de: &'a mut XmlDeserializer<R>) -> Self {
        let element_name = de.pending_element_name.take();
        let pushed_element = match &element_name {
            Some(name) => {
                de.element_stack.push(name.clone());
                true
            }
            None => false,
        };
        Self {
            de,
            element_name,
            pushed_element,
            emitted_resource_type: false,
            emit_resource_type_value: false,
            pending_field_name: None,
        }
    }

    fn collect_field_occurrences(
        &mut self,
        field_name: &str,
    ) -> Result<VecDeque<CollectedOccurrence>> {
        let mut occurrences = VecDeque::new();
        loop {
            self.consume_insignificant_events()?;
            let matches_field = match self.de.peek_event()? {
                Some(Event::Start(e)) | Some(Event::Empty(e)) => {
                    e.name().as_ref() == field_name.as_bytes()
                }
                _ => false,
            };
            if !matches_field {
                break;
            }
            let element_stack = self.de.element_stack.clone();
            let events = self.collect_single_occurrence(field_name)?;
            occurrences.push_back(CollectedOccurrence {
                events,
                element_stack,
            });
        }
        Ok(occurrences)
    }

    fn collect_single_occurrence(&mut self, field_name: &str) -> Result<Vec<Event<'static>>> {
        let mut events = Vec::new();
        let first = self.de.next_event()?;
        match &first {
            Event::Start(e) => {
                if e.name().as_ref() != field_name.as_bytes() {
                    return Err(SerdeError::Custom(format!(
                        "Unexpected element <{}> while collecting <{}>",
                        String::from_utf8_lossy(e.name().as_ref()),
                        field_name
                    )));
                }
                events.push(first.clone());
                let mut depth = 1;
                while depth > 0 {
                    let event = self.de.next_event()?;
                    match &event {
                        Event::Start(_) => depth += 1,
                        Event::End(_) => depth -= 1,
                        Event::Eof => {
                            return Err(SerdeError::Custom(
                                "Unexpected EOF while collecting element".to_string(),
                            ));
                        }
                        _ => {}
                    }
                    events.push(event);
                }
            }
            Event::Empty(e) => {
                if e.name().as_ref() != field_name.as_bytes() {
                    return Err(SerdeError::Custom(format!(
                        "Unexpected element <{}> while collecting <{}>",
                        String::from_utf8_lossy(e.name().as_ref()),
                        field_name
                    )));
                }
                events.push(first);
            }
            other => {
                return Err(SerdeError::Custom(format!(
                    "Unexpected event {:?} while collecting <{}>",
                    other, field_name
                )));
            }
        }
        Ok(events)
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

    fn deserialize_occurrence_to_json(
        &mut self,
        field_name: &str,
        occurrence: CollectedOccurrence,
        force_value_object: bool,
    ) -> Result<JsonValue> {
        self.de.pending_element_name.replace(field_name.to_string());
        self.de.current_element_name = field_name.to_string();
        let saved_stack = std::mem::replace(&mut self.de.element_stack, occurrence.element_stack);
        replay_events(self.de, occurrence.events.clone());
        let mut value = JsonValue::deserialize(&mut *self.de)
            .map_err(|err| SerdeError::Custom(err.to_string()))?;
        self.de.element_stack = saved_stack;
        if force_value_object {
            if let Some(first_event) = occurrence.events.first() {
                let mut value_attr: Option<JsonValue> = None;
                let mut id_attr: Option<String> = None;
                let mut attrs_iter = None;
                match first_event {
                    Event::Start(e) => attrs_iter = Some(e.attributes()),
                    Event::Empty(e) => attrs_iter = Some(e.attributes()),
                    _ => {}
                }
                if let Some(iter) = attrs_iter {
                    for attr in iter {
                        let attr = attr.map_err(|e| {
                            SerdeError::Custom(format!("Failed to parse attribute: {}", e))
                        })?;
                        match attr.key.as_ref() {
                            b"value" => {
                                let attr_str = String::from_utf8_lossy(&attr.value).to_string();
                                value_attr = Some(JsonValue::String(attr_str));
                            }
                            b"id" => {
                                id_attr = Some(String::from_utf8_lossy(&attr.value).to_string());
                            }
                            _ => {}
                        }
                    }
                }

                if value_attr.is_some() || id_attr.is_some() {
                    let mut obj = match value {
                        JsonValue::Object(map) => map,
                        JsonValue::Null => serde_json::Map::new(),
                        other => {
                            let mut map = serde_json::Map::new();
                            map.insert("value".to_string(), other);
                            map
                        }
                    };

                    if let Some(val) = value_attr {
                        obj.insert("value".to_string(), val);
                    }
                    if let Some(id) = id_attr {
                        obj.insert("id".to_string(), JsonValue::String(id));
                    }
                    value = JsonValue::Object(obj);
                }
            }
        }
        Ok(value)
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
        if let Some(ref _name) = self.element_name {
            if !self.emitted_resource_type {
                self.emitted_resource_type = true;
                self.emit_resource_type_value = true;
                let key = "resourceType".to_string();
                self.de.current_element_name = key.clone();
                self.pending_field_name = Some(key.clone());
                return seed.deserialize(key.into_deserializer()).map(Some);
            }
        }

        // Read next element
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

        let field_name = self
            .pending_field_name
            .take()
            .unwrap_or_else(|| self.de.current_element_name.clone());
        let occurrences = self.collect_field_occurrences(&field_name)?;
        if occurrences.is_empty() {
            return Err(SerdeError::Custom(format!(
                "field {} missing XML data",
                field_name
            )));
        }
        let seed_type = std::any::type_name::<V>();
        let force_single_or_vec = seed_type.contains("deserialize_single_or_vec_option")
            || seed_type.contains("deserialize_single_or_vec<")
            || seed_type.ends_with("deserialize_single_or_vec")
            || seed_type.contains("SingleOrVecHelper");
        let force_value_object = seed_type.contains("PrimitiveOrElementHelper");
        let needs_json_value = force_single_or_vec
            || seed_type.contains("serde::__private::de::content")
            || force_value_object;

        if needs_json_value {
            let mut values = Vec::new();
            for occurrence in occurrences {
                let value = self.deserialize_occurrence_to_json(
                    &field_name,
                    occurrence,
                    force_value_object,
                )?;
                values.push(value);
            }
            let json_value = if values.len() == 1 {
                values.into_iter().next().unwrap()
            } else {
                JsonValue::Array(values)
            };
            return seed
                .deserialize(json_value.into_deserializer())
                .map_err(|err| SerdeError::Custom(err.to_string()));
        }

        let field_deserializer =
            FieldValueDeserializer::new(self.de, field_name.clone(), occurrences);
        match seed.deserialize(field_deserializer) {
            Ok(value) => Ok(value),
            Err(err) => Err(SerdeError::Custom(format!("field {}: {}", field_name, err))),
        }
    }
}

struct FieldValueDeserializer<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    field_name: String,
    occurrences: VecDeque<CollectedOccurrence>,
}

impl<'a, R: BufRead> FieldValueDeserializer<'a, R> {
    fn new(
        de: &'a mut XmlDeserializer<R>,
        field_name: String,
        occurrences: VecDeque<CollectedOccurrence>,
    ) -> Self {
        Self {
            de,
            field_name,
            occurrences,
        }
    }

    fn deserialize_scalar<F, T>(mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut XmlDeserializer<R>) -> Result<T>,
    {
        let occurrence = self.occurrences.pop_front().ok_or_else(|| {
            SerdeError::Custom(format!("field {} missing value", self.field_name))
        })?;
        self.de
            .pending_element_name
            .replace(self.field_name.clone());
        self.de.current_element_name = self.field_name.clone();
        let saved_stack = std::mem::replace(&mut self.de.element_stack, occurrence.element_stack);
        replay_events(self.de, occurrence.events);
        let result = f(self.de);
        self.de.element_stack = saved_stack;
        result
    }
}

impl<'de, 'a, R: BufRead> de::Deserializer<'de> for FieldValueDeserializer<'a, R> {
    type Error = SerdeError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.occurrences.len() > 1 {
            let occurrences = std::mem::take(&mut self.occurrences);
            let seq = FieldSeqAccess::new(self.de, self.field_name.clone(), occurrences);
            visitor.visit_seq(seq)
        } else {
            let occurrence = self.occurrences.pop_front().ok_or_else(|| {
                SerdeError::Custom(format!("field {} missing value", self.field_name))
            })?;
            self.de
                .pending_element_name
                .replace(self.field_name.clone());
            self.de.current_element_name = self.field_name.clone();
            let saved_stack =
                std::mem::replace(&mut self.de.element_stack, occurrence.element_stack);
            replay_events(self.de, occurrence.events);
            let result = self.de.deserialize_any(visitor);
            self.de.element_stack = saved_stack;
            result
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_bool(visitor))
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_i8(visitor))
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_i16(visitor))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_i32(visitor))
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_i64(visitor))
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_u8(visitor))
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_u16(visitor))
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_u32(visitor))
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_u64(visitor))
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_f32(visitor))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_f64(visitor))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_char(visitor))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_str(visitor))
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_string(visitor))
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_bytes(visitor))
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_scalar(|de| de.deserialize_byte_buf(visitor))
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let seq = FieldSeqAccess::new(self.de, self.field_name.clone(), self.occurrences);
        visitor.visit_seq(seq)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.occurrences.is_empty() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    serde::forward_to_deserialize_any! {
        i128 u128 unit unit_struct newtype_struct tuple tuple_struct map struct enum identifier ignored_any
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

fn replay_events<R: BufRead>(de: &mut XmlDeserializer<R>, mut events: Vec<Event<'static>>) {
    while let Some(event) = events.pop() {
        de.push_front_event(event);
    }
}

struct FieldSeqAccess<'a, R: BufRead> {
    de: &'a mut XmlDeserializer<R>,
    field_name: String,
    occurrences: VecDeque<CollectedOccurrence>,
}

impl<'a, R: BufRead> FieldSeqAccess<'a, R> {
    fn new(
        de: &'a mut XmlDeserializer<R>,
        field_name: String,
        occurrences: VecDeque<CollectedOccurrence>,
    ) -> Self {
        Self {
            de,
            field_name,
            occurrences,
        }
    }
}

impl<'de, 'a, R: BufRead> de::SeqAccess<'de> for FieldSeqAccess<'a, R> {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, SerdeError>
    where
        T: DeserializeSeed<'de>,
    {
        if let Some(occurrence) = self.occurrences.pop_front() {
            self.de
                .pending_element_name
                .replace(self.field_name.clone());
            self.de.current_element_name = self.field_name.clone();
            let saved_stack =
                std::mem::replace(&mut self.de.element_stack, occurrence.element_stack.clone());
            replay_events(self.de, occurrence.events);
            let result = seed.deserialize(&mut *self.de).map(Some);
            self.de.element_stack = saved_stack;
            result
        } else {
            Ok(None)
        }
    }
}
