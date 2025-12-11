//! XML serialization implementation using custom serde::Serializer.
//!
//! This module implements streaming serialization from FHIR resources to XML
//! by providing a custom `Serializer` that writes directly to quick-xml as
//! the FhirSerde macro traverses the resource structure.

use crate::error::{Result, SerdeError};
use crate::xml::utils;
use quick_xml::Writer;
use quick_xml::events::{BytesEnd, BytesStart, Event};
use serde::ser::{self, Serialize};
use std::io::Write;

/// Serialize a FHIR resource to an XML string.
///
/// # Examples
///
/// ```ignore
/// use helios_serde::xml::to_xml_string;
/// use helios_fhir::r4::Patient;
///
/// let patient = Patient::default();
/// let xml = to_xml_string(&patient)?;
/// ```
pub fn to_xml_string<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let mut buffer = Vec::new();
    to_xml_writer(value, &mut buffer)?;
    Ok(String::from_utf8(buffer).map_err(|e| SerdeError::Custom(e.to_string()))?)
}

/// Serialize a FHIR resource to an XML byte vector.
pub fn to_xml_vec<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let mut buffer = Vec::new();
    to_xml_writer(value, &mut buffer)?;
    Ok(buffer)
}

/// Serialize a FHIR resource to an XML writer.
pub fn to_xml_writer<T, W>(value: &T, writer: W) -> Result<()>
where
    T: Serialize + ?Sized,
    W: Write,
{
    let mut serializer = XmlSerializer::new(writer);
    value.serialize(&mut serializer)?;
    serializer.finish()?;
    Ok(())
}

/// Buffer for pending field/_field pairs that need to be merged.
#[derive(Debug)]
enum PendingField {
    /// Single field buffer for non-array fields
    Single(FieldBuffer),
    /// Array field buffer for array fields
    Array(ArrayFieldBuffer),
}

impl PendingField {
    fn name(&self) -> &str {
        match self {
            PendingField::Single(f) => &f.name,
            PendingField::Array(a) => &a.name,
        }
    }
}

/// Buffer for a single field/_field pair.
#[derive(Debug, Default)]
struct FieldBuffer {
    /// The field name (without underscore)
    name: String,

    /// The primitive value (from the non-underscore field)
    value: Option<String>,

    /// The id attribute (from _field)
    id: Option<String>,

    /// Extension elements (from _field)
    extensions: Vec<ExtensionElement>,
}

/// Buffer for pending array field/_field pairs.
#[derive(Debug)]
struct ArrayFieldBuffer {
    /// The field name (without underscore)
    name: String,

    /// Array of values from the main field
    values: Vec<Option<String>>,

    /// Array of extension data from the _field
    /// Index-aligned with values
    extension_data: Vec<Option<ArrayExtensionData>>,
}

/// Extension data for a single array element.
#[derive(Debug, Clone)]
struct ArrayExtensionData {
    id: Option<String>,
    extensions: Vec<ExtensionElement>,
}

/// Value[x] type for extensions.
#[derive(Debug, Clone)]
enum ExtensionValue {
    /// Primitive value (field name, value as string)
    Primitive(String, String),
    /// Complex value (field name, pre-serialized XML content)
    Complex(String, String),
}

/// Content of an extension element.
/// Extensions can contain a value[x] element and/or nested extensions.
#[derive(Debug, Clone)]
struct ExtensionContent {
    /// Element ID
    id: Option<String>,

    /// Extension value (value[x] in FHIR)
    value: Option<ExtensionValue>,

    /// Nested extensions (extensions within extensions)
    extension: Vec<ExtensionElement>,
}

impl ExtensionContent {
    fn is_empty(&self) -> bool {
        self.id.is_none() && self.value.is_none() && self.extension.is_empty()
    }
}

/// Represents an extension element to be serialized.
#[derive(Debug, Clone)]
struct ExtensionElement {
    url: String,
    /// Extension content (fields beyond url)
    /// If None, the extension has no content beyond the URL
    content: Option<ExtensionContent>,
}

impl FieldBuffer {
    fn new(name: String) -> Self {
        Self {
            name,
            value: None,
            id: None,
            extensions: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.value.is_none() && self.id.is_none() && self.extensions.is_empty()
    }
}

/// XML Serializer that writes directly to quick-xml.
pub struct XmlSerializer<W: Write> {
    writer: Writer<W>,
    pending_field: Option<PendingField>,
    namespace_written: bool,
    xml_declaration_written: bool,
    open_resource_stack: Vec<String>,
}

impl<W: Write> XmlSerializer<W> {
    /// Creates a new XML serializer.
    pub fn new(writer: W) -> Self {
        Self {
            writer: Writer::new(writer),
            pending_field: None,
            namespace_written: false,
            xml_declaration_written: false,
            open_resource_stack: Vec::new(),
        }
    }

    /// Finishes serialization and flushes the writer.
    pub fn finish(mut self) -> Result<()> {
        // Flush any pending field
        if let Some(pending) = self.pending_field.take() {
            match pending {
                PendingField::Single(field) => self.write_field(field)?,
                PendingField::Array(array) => self.write_array_field(array)?,
            }
        }

        // Close any remaining open resource elements (should only trigger for root)
        while let Some(root_name) = self.open_resource_stack.pop() {
            self.write_end_element(&root_name)?;
        }

        Ok(())
    }

    /// Writes a field element to XML.
    fn write_field(&mut self, field: FieldBuffer) -> Result<()> {
        if field.is_empty() {
            return Ok(());
        }

        // Special handling for div elements (XHTML content)
        if utils::is_div_element(&field.name) {
            return self.write_div_element(&field);
        }

        let mut element = BytesStart::new(&field.name);

        // Add id attribute if present
        if let Some(id) = &field.id {
            element.push_attribute(("id", id.as_str()));
        }

        // Add value attribute if present
        if let Some(value) = &field.value {
            element.push_attribute(("value", value.as_str()));
        }

        // If no extensions, write empty element
        if field.extensions.is_empty() {
            self.writer.write_event(Event::Empty(element))?;
        } else {
            // Write start element
            self.writer.write_event(Event::Start(element))?;

            // Write extension children
            for ext in field.extensions {
                self.write_extension(&ext)?;
            }

            // Write end element
            let end = BytesEnd::new(&field.name);
            self.writer.write_event(Event::End(end))?;
        }

        Ok(())
    }

    /// Writes an array field where values and extension data are merged.
    fn write_array_field(&mut self, array_field: ArrayFieldBuffer) -> Result<()> {
        // Determine the maximum length (in case arrays are different lengths)
        let max_len = array_field
            .values
            .len()
            .max(array_field.extension_data.len());

        // Write each element, merging value with extension data
        for i in 0..max_len {
            let value = array_field.values.get(i).and_then(|v| v.as_ref());
            let ext_data = array_field.extension_data.get(i).and_then(|e| e.as_ref());

            // Check if extension data has any actual content
            let has_ext_content = ext_data
                .map(|e| e.id.is_some() || !e.extensions.is_empty())
                .unwrap_or(false);

            // Skip if both value and extension data are None/empty
            if value.is_none() && !has_ext_content {
                continue;
            }

            let mut element = BytesStart::new(&array_field.name);

            // Track if we added any attributes
            let mut has_attributes = false;

            // Add id attribute from extension data if present
            if let Some(ext) = ext_data {
                if let Some(id) = &ext.id {
                    element.push_attribute(("id", id.as_str()));
                    has_attributes = true;
                }
            }

            // Add value attribute if present
            if let Some(val) = value {
                element.push_attribute(("value", val.as_str()));
                has_attributes = true;
            }

            // Check if we have extension children
            let has_extensions = ext_data.map(|e| !e.extensions.is_empty()).unwrap_or(false);

            // Only write element if it has attributes or extension children
            if !has_attributes && !has_extensions {
                continue;
            }

            if !has_extensions {
                // No extension children - write empty element
                self.writer.write_event(Event::Empty(element))?;
            } else {
                // Write start element
                self.writer.write_event(Event::Start(element))?;

                // Write extension children
                if let Some(ext) = ext_data {
                    for extension in &ext.extensions {
                        self.write_extension(extension)?;
                    }
                }

                // Write end element
                let end = BytesEnd::new(&array_field.name);
                self.writer.write_event(Event::End(end))?;
            }
        }

        Ok(())
    }

    /// Writes a div element with XHTML content as raw XML.
    fn write_div_element(&mut self, field: &FieldBuffer) -> Result<()> {
        use quick_xml::Reader;

        if let Some(xml_content) = &field.value {
            // Parse the XML content using quick-xml
            let mut reader = Reader::from_str(xml_content);
            reader.config_mut().trim_text(false); // Preserve whitespace

            // Copy events from reader to writer
            loop {
                match reader.read_event() {
                    Ok(Event::Eof) => break,
                    Ok(event) => {
                        // Clone the event and write it
                        self.writer.write_event(event)?;
                    }
                    Err(e) => {
                        // XML parsing failed - propagate the error
                        return Err(SerdeError::Custom(format!(
                            "Failed to parse XHTML div content: {}",
                            e
                        )));
                    }
                }
            }
            Ok(())
        } else {
            // No value, just skip
            Ok(())
        }
    }

    /// Writes an extension element with proper content serialization.
    fn write_extension(&mut self, ext: &ExtensionElement) -> Result<()> {
        let mut ext_element = BytesStart::new("extension");
        ext_element.push_attribute(("url", ext.url.as_str()));

        if let Some(content) = &ext.content {
            if content.is_empty() {
                // Extension has no actual content - write as empty element
                self.writer.write_event(Event::Empty(ext_element))?;
                return Ok(());
            }

            // Extension has content - write start tag, then serialize content, then end tag
            self.writer.write_event(Event::Start(ext_element))?;

            // Write ID attribute if present
            if let Some(id) = &content.id {
                self.write_simple_element("id", id)?;
            }

            // Write value[x] if present
            if let Some(value) = &content.value {
                self.write_extension_value(value)?;
            }

            // Write nested extensions
            for nested_ext in &content.extension {
                self.write_extension(nested_ext)?;
            }

            self.writer
                .write_event(Event::End(BytesEnd::new("extension")))?;
        } else {
            // Extension has no content - write as empty element
            self.writer.write_event(Event::Empty(ext_element))?;
        }

        Ok(())
    }

    /// Writes an extension value[x] element.
    fn write_extension_value(&mut self, ext_value: &ExtensionValue) -> Result<()> {
        match ext_value {
            ExtensionValue::Primitive(field_name, value) => {
                // Primitive - write as <fieldName value="..."/>
                self.write_simple_element(field_name, value)?;
            }
            ExtensionValue::Complex(field_name, xml_content) => {
                // Complex - write start tag, content, end tag
                self.write_start_element(field_name, false)?;

                // Write the pre-serialized XML content directly
                use quick_xml::Reader;
                let mut reader = Reader::from_str(xml_content);
                reader.config_mut().trim_text(true);

                loop {
                    match reader.read_event() {
                        Ok(Event::Eof) => break,
                        Ok(event) => {
                            self.writer.write_event(event)?;
                        }
                        Err(e) => {
                            return Err(SerdeError::Custom(format!(
                                "Failed to parse complex extension value XML: {}",
                                e
                            )));
                        }
                    }
                }

                // Write end tag
                self.writer
                    .write_event(Event::End(BytesEnd::new(field_name)))?;
            }
        }

        Ok(())
    }

    /// Writes a simple element with just a value attribute.
    fn write_simple_element(&mut self, name: &str, value: &str) -> Result<()> {
        let mut element = BytesStart::new(name);
        element.push_attribute(("value", value));

        self.writer.write_event(Event::Empty(element))?;

        Ok(())
    }

    /// Writes the XML declaration if not already written.
    fn write_xml_declaration(&mut self) -> Result<()> {
        if !self.xml_declaration_written {
            self.writer
                .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
                    "1.0",
                    Some("UTF-8"),
                    None,
                )))?;
            self.xml_declaration_written = true;
        }
        Ok(())
    }

    /// Writes the start of an element (for complex types).
    fn write_start_element(&mut self, name: &str, add_namespace: bool) -> Result<()> {
        // Write XML declaration before first element
        if !self.xml_declaration_written {
            self.write_xml_declaration()?;
        }

        let mut element = BytesStart::new(name);

        // Add FHIR namespace to root resource element
        if add_namespace && !self.namespace_written {
            element.push_attribute(("xmlns", utils::FHIR_NAMESPACE));
            self.namespace_written = true;
        }

        self.writer.write_event(Event::Start(element))?;

        Ok(())
    }

    /// Writes the end of an element.
    fn write_end_element(&mut self, name: &str) -> Result<()> {
        let end = BytesEnd::new(name);
        self.writer.write_event(Event::End(end))?;

        Ok(())
    }

    fn start_resource_element(&mut self, name: &str) -> Result<()> {
        let add_namespace = self.open_resource_stack.is_empty();
        self.write_start_element(name, add_namespace)?;
        self.open_resource_stack.push(name.to_string());
        Ok(())
    }

    fn end_resource_element(&mut self) -> Result<()> {
        if let Some(name) = self.open_resource_stack.pop() {
            self.write_end_element(&name)?;
        }
        Ok(())
    }
}

// Implement serde::Serializer trait
impl<'a, W: Write> ser::Serializer for &'a mut XmlSerializer<W> {
    type Ok = ();
    type Error = SerdeError;

    type SerializeSeq = SeqSerializer<'a, W>;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = MapSerializer<'a, W>;
    type SerializeStruct = MapSerializer<'a, W>;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.serialize_str(utils::bool_to_string(v))
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, _v: &str) -> Result<()> {
        // This method is only called for "bare" strings (not wrapped in structs/options)
        // This happens in arrays of primitives or at the top level (which we don't support)
        // For arrays, we should write a simple element
        // For now, return an error as this shouldn't happen in FHIR serialization
        Err(SerdeError::Custom(
            "Direct string serialization not supported - strings should be wrapped in fields"
                .to_string(),
        ))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Err(SerdeError::Custom(
            "Bytes not supported in FHIR XML".to_string(),
        ))
    }

    fn serialize_none(self) -> Result<()> {
        // Skip null values
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(SeqSerializer { serializer: self })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(MapSerializer {
            serializer: self,
            current_key: None,
            resource_element_open: false,
        })
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(MapSerializer {
            serializer: self,
            current_key: None,
            resource_element_open: false,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(self)
    }
}

/// Serializer for sequences (arrays).
///
/// Note: This is currently unused as arrays are handled directly in MapSerializer
/// through NamedSeqSerializer. May be removed in future cleanup.
pub struct SeqSerializer<'a, W: Write> {
    serializer: &'a mut XmlSerializer<W>,
}

impl<'a, W: Write> ser::SerializeSeq for SeqSerializer<'a, W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

/// Serializer for named sequences (arrays with a known element name).
/// This is used when serializing FHIR arrays where each element should be
/// written as a repeated XML element with the same name.
struct NamedSeqSerializer<'a, W: Write> {
    serializer: &'a mut XmlSerializer<W>,
    element_name: String,
}

impl<'a, W: Write> ser::Serializer for &'a mut NamedSeqSerializer<'a, W> {
    type Ok = ();
    type Error = SerdeError;
    type SerializeSeq = NamedSeqElements<'a, 'a, W>;
    type SerializeTuple = ser::Impossible<(), SerdeError>;
    type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
    type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
    type SerializeMap = ser::Impossible<(), SerdeError>;
    type SerializeStruct = ser::Impossible<(), SerdeError>;
    type SerializeStructVariant = ser::Impossible<(), SerdeError>;

    fn serialize_bool(self, _v: bool) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_i8(self, _v: i8) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_i16(self, _v: i16) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_i32(self, _v: i32) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_i64(self, _v: i64) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_f32(self, _v: f32) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_f64(self, _v: f64) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_char(self, _v: char) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_str(self, _v: &str) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_none(self) -> Result<()> {
        // None is skipped
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        Err(SerdeError::Custom("Expected sequence".to_string()))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(NamedSeqElements { parent: self })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(SerdeError::Custom("Tuples not supported".to_string()))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(SerdeError::Custom(
            "Tuple structs not supported".to_string(),
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(SerdeError::Custom(
            "Tuple variants not supported".to_string(),
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(SerdeError::Custom(
            "Maps not supported in sequences".to_string(),
        ))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(SerdeError::Custom(
            "Structs not supported in sequences".to_string(),
        ))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(SerdeError::Custom(
            "Struct variants not supported".to_string(),
        ))
    }
}

/// Helper for serializing sequence elements with a known element name.
struct NamedSeqElements<'a, 'b, W: Write> {
    parent: &'a mut NamedSeqSerializer<'b, W>,
}

impl<'a, 'b, W: Write> ser::SerializeSeq for NamedSeqElements<'a, 'b, W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        // Check if element is None and skip it
        if is_none_value(value)? {
            return Ok(());
        }

        // For each element, check if it's a primitive or complex type
        if let Some(val_str) = try_serialize_as_primitive(value)? {
            // Primitive - write as <elementName value="..."/>
            self.parent
                .serializer
                .write_simple_element(&self.parent.element_name, &val_str)?;
        } else {
            // Complex type - write as <elementName>...</elementName>
            self.parent
                .serializer
                .write_start_element(&self.parent.element_name, false)?;
            value.serialize(&mut *self.parent.serializer)?;
            self.parent
                .serializer
                .write_end_element(&self.parent.element_name)?;
        }
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

/// Extracts extension array from a value using custom serializer.
/// Returns a vector of ExtensionElement objects.
fn extract_extensions<T: ?Sized + Serialize>(value: &T) -> Result<Vec<ExtensionElement>> {
    struct ExtensionArrayExtractor {
        extensions: Vec<ExtensionElement>,
    }

    impl<'a> ser::Serializer for &'a mut ExtensionArrayExtractor {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ExtensionSeqExtractor<'a>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ser::Impossible<(), SerdeError>;
        type SerializeStruct = ser::Impossible<(), SerdeError>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Ok(())
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Ok(())
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Ok(())
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Ok(())
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Ok(())
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Ok(())
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Ok(())
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Ok(())
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Ok(())
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Ok(())
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Ok(())
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Ok(())
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Ok(())
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Ok(())
        }
        fn serialize_none(self) -> Result<()> {
            Ok(())
        }
        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Ok(ExtensionSeqExtractor { parent: self })
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            Err(SerdeError::Custom("Unexpected tuple".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(SerdeError::Custom("Unexpected tuple struct".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(SerdeError::Custom("Unexpected tuple variant".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Err(SerdeError::Custom("Unexpected map".to_string()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Err(SerdeError::Custom("Unexpected struct".to_string()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Unexpected struct variant".to_string()))
        }
    }

    struct ExtensionSeqExtractor<'a> {
        parent: &'a mut ExtensionArrayExtractor,
    }

    impl<'a> ser::SerializeSeq for ExtensionSeqExtractor<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
            // Each element should be an extension object
            let ext = extract_single_extension(value)?;
            if let Some(ext) = ext {
                self.parent.extensions.push(ext);
            }
            Ok(())
        }

        fn end(self) -> Result<()> {
            Ok(())
        }
    }

    let mut extractor = ExtensionArrayExtractor {
        extensions: Vec::new(),
    };
    value.serialize(&mut extractor)?;
    Ok(extractor.extensions)
}

/// Extracts a single extension object.
fn extract_single_extension<T: ?Sized + Serialize>(value: &T) -> Result<Option<ExtensionElement>> {
    #[derive(Default)]
    struct SingleExtensionExtractor {
        url: Option<String>,
        id: Option<String>,
        value: Option<ExtensionValue>,
        nested_extensions: Vec<ExtensionElement>,
        current_key: Option<String>,
    }

    impl<'a> ser::Serializer for &'a mut SingleExtensionExtractor {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ser::Impossible<(), SerdeError>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = SingleExtensionMapExtractor<'a>;
        type SerializeStruct = SingleExtensionMapExtractor<'a>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Ok(())
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Ok(())
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Ok(())
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Ok(())
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Ok(())
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Ok(())
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Ok(())
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Ok(())
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Ok(())
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Ok(())
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Ok(())
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Ok(())
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Ok(())
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Ok(())
        }
        fn serialize_none(self) -> Result<()> {
            Ok(())
        }
        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Err(SerdeError::Custom("Unexpected seq".to_string()))
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            Err(SerdeError::Custom("Unexpected tuple".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(SerdeError::Custom("Unexpected tuple struct".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(SerdeError::Custom("Unexpected tuple variant".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Ok(SingleExtensionMapExtractor { parent: self })
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Ok(SingleExtensionMapExtractor { parent: self })
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Unexpected struct variant".to_string()))
        }
    }

    struct SingleExtensionMapExtractor<'a> {
        parent: &'a mut SingleExtensionExtractor,
    }

    impl<'a> ser::SerializeStruct for SingleExtensionMapExtractor<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_field<T: ?Sized + Serialize>(
            &mut self,
            key: &'static str,
            value: &T,
        ) -> Result<()> {
            self.parent.current_key = Some(key.to_string());

            if key == "url" {
                // Extract URL as string
                let mut url_extractor = StringExtractor { value: None };
                value.serialize(&mut url_extractor)?;
                self.parent.url = url_extractor.value;
            } else if key == "id" {
                // Extract ID as string
                let mut id_extractor = StringExtractor { value: None };
                value.serialize(&mut id_extractor)?;
                self.parent.id = id_extractor.value;
            } else if key.starts_with("value") && key != "value" {
                // Extract value[x] field
                self.parent.value = extract_extension_value(key, value)?;
            } else if key == "extension" {
                // Extract nested extensions
                self.parent.nested_extensions = extract_extensions(value)?;
            }

            self.parent.current_key = None;
            Ok(())
        }

        fn end(self) -> Result<()> {
            Ok(())
        }
    }

    impl<'a> ser::SerializeMap for SingleExtensionMapExtractor<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
            let mut key_extractor = StringExtractor { value: None };
            key.serialize(&mut key_extractor)?;
            self.parent.current_key = key_extractor.value;
            Ok(())
        }

        fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
            if let Some(ref key) = self.parent.current_key {
                if key == "url" {
                    let mut url_extractor = StringExtractor { value: None };
                    value.serialize(&mut url_extractor)?;
                    self.parent.url = url_extractor.value;
                } else if key == "id" {
                    let mut id_extractor = StringExtractor { value: None };
                    value.serialize(&mut id_extractor)?;
                    self.parent.id = id_extractor.value;
                } else if key.starts_with("value") && key != "value" {
                    self.parent.value = extract_extension_value(key, value)?;
                } else if key == "extension" {
                    self.parent.nested_extensions = extract_extensions(value)?;
                }
            }
            self.parent.current_key = None;
            Ok(())
        }

        fn end(self) -> Result<()> {
            Ok(())
        }
    }

    let mut extractor = SingleExtensionExtractor::default();
    value.serialize(&mut extractor)?;

    // Only create extension if URL is present
    if let Some(url) = extractor.url {
        let content = if extractor.id.is_none()
            && extractor.value.is_none()
            && extractor.nested_extensions.is_empty()
        {
            None
        } else {
            Some(ExtensionContent {
                id: extractor.id,
                value: extractor.value,
                extension: extractor.nested_extensions,
            })
        };

        Ok(Some(ExtensionElement { url, content }))
    } else {
        Ok(None)
    }
}

/// Extracts an extension value[x] field and constructs the appropriate enum variant.
fn extract_extension_value<T: ?Sized + Serialize>(
    field_name: &str,
    value: &T,
) -> Result<Option<ExtensionValue>> {
    // Try to extract as primitive first (string, number, boolean)
    let mut primitive_extractor = PrimitiveValueExtractor { value: None };
    value.serialize(&mut primitive_extractor)?;

    if let Some(primitive_value) = primitive_extractor.value {
        // Primitive type - store as Primitive variant
        Ok(Some(ExtensionValue::Primitive(
            field_name.to_string(),
            primitive_value,
        )))
    } else {
        // Complex value - serialize to XML string and store as Complex variant
        let xml = to_xml_string(value)?;
        // Strip the XML declaration and root wrapper if present
        let content = xml
            .lines()
            .skip(1) // Skip XML declaration
            .collect::<Vec<_>>()
            .join("\n");

        Ok(Some(ExtensionValue::Complex(
            field_name.to_string(),
            content,
        )))
    }
}

/// Helper struct to extract primitive values as strings.
struct PrimitiveValueExtractor {
    value: Option<String>,
}

impl<'a> ser::Serializer for &'a mut PrimitiveValueExtractor {
    type Ok = ();
    type Error = SerdeError;
    type SerializeSeq = ser::Impossible<(), SerdeError>;
    type SerializeTuple = ser::Impossible<(), SerdeError>;
    type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
    type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
    type SerializeMap = ser::Impossible<(), SerdeError>;
    type SerializeStruct = ser::Impossible<(), SerdeError>;
    type SerializeStructVariant = ser::Impossible<(), SerdeError>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.value = Some(if v { "true" } else { "false" }.to_string());
        Ok(())
    }
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_i16(self, v: i16) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_i32(self, v: i32) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_u16(self, v: u16) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_u32(self, v: u32) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Err(SerdeError::Custom("Bytes not supported".to_string()))
    }
    fn serialize_none(self) -> Result<()> {
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        Ok(())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        Ok(())
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(SerdeError::Custom("Not a primitive value".to_string()))
    }
}

/// Helper struct to extract string values.
struct StringExtractor {
    value: Option<String>,
}

impl<'a> ser::Serializer for &'a mut StringExtractor {
    type Ok = ();
    type Error = SerdeError;
    type SerializeSeq = ser::Impossible<(), SerdeError>;
    type SerializeTuple = ser::Impossible<(), SerdeError>;
    type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
    type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
    type SerializeMap = ser::Impossible<(), SerdeError>;
    type SerializeStruct = ser::Impossible<(), SerdeError>;
    type SerializeStructVariant = ser::Impossible<(), SerdeError>;

    fn serialize_bool(self, _v: bool) -> Result<()> {
        Ok(())
    }
    fn serialize_i8(self, _v: i8) -> Result<()> {
        Ok(())
    }
    fn serialize_i16(self, _v: i16) -> Result<()> {
        Ok(())
    }
    fn serialize_i32(self, _v: i32) -> Result<()> {
        Ok(())
    }
    fn serialize_i64(self, _v: i64) -> Result<()> {
        Ok(())
    }
    fn serialize_u8(self, _v: u8) -> Result<()> {
        Ok(())
    }
    fn serialize_u16(self, _v: u16) -> Result<()> {
        Ok(())
    }
    fn serialize_u32(self, _v: u32) -> Result<()> {
        Ok(())
    }
    fn serialize_u64(self, _v: u64) -> Result<()> {
        Ok(())
    }
    fn serialize_f32(self, _v: f32) -> Result<()> {
        Ok(())
    }
    fn serialize_f64(self, _v: f64) -> Result<()> {
        Ok(())
    }
    fn serialize_char(self, _v: char) -> Result<()> {
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Ok(())
    }
    fn serialize_none(self) -> Result<()> {
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        Ok(())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        Ok(())
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(SerdeError::Custom("Expected string".to_string()))
    }
}

/// Extracts id and extension fields from an _field value.
/// Returns (id, extensions) tuple.
fn extract_extension_fields<T: ?Sized + Serialize>(
    value: &T,
) -> Result<(Option<String>, Vec<ExtensionElement>)> {
    #[derive(Default)]
    struct ExtensionFieldExtractor {
        id: Option<String>,
        extensions: Vec<ExtensionElement>,
        current_key: Option<String>,
    }

    impl<'a> ser::Serializer for &'a mut ExtensionFieldExtractor {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ser::Impossible<(), SerdeError>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ExtensionFieldMapSerializer<'a>;
        type SerializeStruct = ExtensionFieldMapSerializer<'a>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Ok(())
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Ok(())
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Ok(())
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Ok(())
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Ok(())
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Ok(())
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Ok(())
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Ok(())
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Ok(())
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Ok(())
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Ok(())
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Ok(())
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Ok(())
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Ok(())
        }
        fn serialize_none(self) -> Result<()> {
            Ok(())
        }
        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Err(SerdeError::Custom("Unexpected sequence".to_string()))
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            Err(SerdeError::Custom("Unexpected tuple".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(SerdeError::Custom("Unexpected tuple struct".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(SerdeError::Custom("Unexpected tuple variant".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Ok(ExtensionFieldMapSerializer { extractor: self })
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Ok(ExtensionFieldMapSerializer { extractor: self })
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Unexpected struct variant".to_string()))
        }
    }

    struct ExtensionFieldMapSerializer<'a> {
        extractor: &'a mut ExtensionFieldExtractor,
    }

    impl<'a> ser::SerializeMap for ExtensionFieldMapSerializer<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
            // Capture the key as a string
            if let Some(key_str) = try_serialize_as_primitive(key)? {
                self.extractor.current_key = Some(key_str);
            }
            Ok(())
        }

        fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
            if let Some(key) = self.extractor.current_key.take() {
                if key == "id" {
                    // Extract id field
                    if let Some(id_str) = try_serialize_as_primitive(value)? {
                        self.extractor.id = Some(id_str);
                    }
                } else if key == "extension" {
                    // Extract extension array
                    self.extractor.extensions = extract_extensions(value)?;
                }
            }
            Ok(())
        }

        fn end(self) -> Result<()> {
            Ok(())
        }
    }

    impl<'a> ser::SerializeStruct for ExtensionFieldMapSerializer<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_field<T: ?Sized + Serialize>(
            &mut self,
            key: &'static str,
            value: &T,
        ) -> Result<()> {
            <Self as ser::SerializeMap>::serialize_key(self, key)?;
            <Self as ser::SerializeMap>::serialize_value(self, value)
        }

        fn end(self) -> Result<()> {
            <Self as ser::SerializeMap>::end(self)
        }
    }

    let mut extractor = ExtensionFieldExtractor::default();
    value.serialize(&mut extractor)?;
    Ok((extractor.id, extractor.extensions))
}

/// Extracts primitive values from an array.
/// Returns a vector of optional string values (Some for primitives, None for non-primitives).
fn extract_array_values<T: ?Sized + Serialize>(value: &T) -> Result<Vec<Option<String>>> {
    struct ArrayValueCollector {
        values: Vec<Option<String>>,
    }

    impl<'a> ser::Serializer for &'a mut ArrayValueCollector {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ArrayValueSeqCollector<'a>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ser::Impossible<(), SerdeError>;
        type SerializeStruct = ser::Impossible<(), SerdeError>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_none(self) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Ok(ArrayValueSeqCollector { parent: self })
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
    }

    struct ArrayValueSeqCollector<'a> {
        parent: &'a mut ArrayValueCollector,
    }

    impl<'a> ser::SerializeSeq for ArrayValueSeqCollector<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
            // Try to serialize as primitive
            let val = try_serialize_as_primitive(value)?;
            self.parent.values.push(val);
            Ok(())
        }

        fn end(self) -> Result<()> {
            Ok(())
        }
    }

    let mut collector = ArrayValueCollector { values: Vec::new() };
    value.serialize(&mut collector)?;
    Ok(collector.values)
}

/// Extracts extension data from an array of extension elements (_field array).
/// Returns a vector of optional ArrayExtensionData.
fn extract_array_extension_data<T: ?Sized + Serialize>(
    value: &T,
) -> Result<Vec<Option<ArrayExtensionData>>> {
    struct ArrayExtensionCollector {
        extension_data: Vec<Option<ArrayExtensionData>>,
    }

    impl<'a> ser::Serializer for &'a mut ArrayExtensionCollector {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ArrayExtensionSeqCollector<'a>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ser::Impossible<(), SerdeError>;
        type SerializeStruct = ser::Impossible<(), SerdeError>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_none(self) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Ok(ArrayExtensionSeqCollector { parent: self })
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Expected array".to_string()))
        }
    }

    struct ArrayExtensionSeqCollector<'a> {
        parent: &'a mut ArrayExtensionCollector,
    }

    impl<'a> ser::SerializeSeq for ArrayExtensionSeqCollector<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
            // Check if element is None
            if is_none_value(value)? {
                self.parent.extension_data.push(None);
            } else {
                // Extract id and extensions from this element
                let (id, extensions) = extract_extension_fields(value)?;
                self.parent
                    .extension_data
                    .push(Some(ArrayExtensionData { id, extensions }));
            }
            Ok(())
        }

        fn end(self) -> Result<()> {
            Ok(())
        }
    }

    let mut collector = ArrayExtensionCollector {
        extension_data: Vec::new(),
    };
    value.serialize(&mut collector)?;
    Ok(collector.extension_data)
}

/// Checks if a value is None.
fn is_none_value<T: ?Sized + Serialize>(value: &T) -> Result<bool> {
    struct NoneDetector(bool);

    impl<'a> ser::Serializer for &'a mut NoneDetector {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ser::Impossible<(), SerdeError>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ser::Impossible<(), SerdeError>;
        type SerializeStruct = ser::Impossible<(), SerdeError>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Ok(())
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Ok(())
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Ok(())
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Ok(())
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Ok(())
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Ok(())
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Ok(())
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Ok(())
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Ok(())
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Ok(())
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Ok(())
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Ok(())
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Ok(())
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Ok(())
        }
        fn serialize_none(self) -> Result<()> {
            self.0 = true;
            Err(SerdeError::Custom("None detected".to_string()))
        }
        fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<()> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_unit(self) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _value: &T,
        ) -> Result<()> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<()> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Not none".to_string()))
        }
    }

    let mut detector = NoneDetector(false);
    match value.serialize(&mut detector) {
        Ok(()) => Ok(false),
        Err(e) if e.to_string().contains("None detected") => Ok(true),
        Err(e) if e.to_string().contains("Not none") => Ok(false),
        Err(e) => Err(e),
    }
}

/// Checks if a value is an array/sequence.
fn is_array_value<T: ?Sized + Serialize>(value: &T) -> Result<bool> {
    struct ArrayDetector(bool);

    impl<'a> ser::Serializer for &'a mut ArrayDetector {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ser::Impossible<(), SerdeError>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ser::Impossible<(), SerdeError>;
        type SerializeStruct = ser::Impossible<(), SerdeError>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, _v: bool) -> Result<()> {
            Ok(())
        }
        fn serialize_i8(self, _v: i8) -> Result<()> {
            Ok(())
        }
        fn serialize_i16(self, _v: i16) -> Result<()> {
            Ok(())
        }
        fn serialize_i32(self, _v: i32) -> Result<()> {
            Ok(())
        }
        fn serialize_i64(self, _v: i64) -> Result<()> {
            Ok(())
        }
        fn serialize_u8(self, _v: u8) -> Result<()> {
            Ok(())
        }
        fn serialize_u16(self, _v: u16) -> Result<()> {
            Ok(())
        }
        fn serialize_u32(self, _v: u32) -> Result<()> {
            Ok(())
        }
        fn serialize_u64(self, _v: u64) -> Result<()> {
            Ok(())
        }
        fn serialize_f32(self, _v: f32) -> Result<()> {
            Ok(())
        }
        fn serialize_f64(self, _v: f64) -> Result<()> {
            Ok(())
        }
        fn serialize_char(self, _v: char) -> Result<()> {
            Ok(())
        }
        fn serialize_str(self, _v: &str) -> Result<()> {
            Ok(())
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            Ok(())
        }
        fn serialize_none(self) -> Result<()> {
            Ok(())
        }
        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Ok(())
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<()> {
            Ok(())
        }
        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            self.0 = true;
            Err(SerdeError::Custom("Array detected".to_string()))
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            self.0 = true;
            Err(SerdeError::Custom("Array detected".to_string()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            self.0 = true;
            Err(SerdeError::Custom("Array detected".to_string()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            self.0 = true;
            Err(SerdeError::Custom("Array detected".to_string()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            Err(SerdeError::Custom("Not an array".to_string()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            Err(SerdeError::Custom("Not an array".to_string()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(SerdeError::Custom("Not an array".to_string()))
        }
    }

    let mut detector = ArrayDetector(false);
    match value.serialize(&mut detector) {
        Ok(()) => Ok(false),
        Err(e) if e.to_string().contains("Array detected") => Ok(true),
        Err(e) if e.to_string().contains("Not an array") => Ok(false),
        Err(e) => Err(e),
    }
}

/// Attempts to serialize a value as a primitive (string, number, boolean).
/// Returns Some(string) if successful, None if it's a complex type.
fn try_serialize_as_primitive<T: ?Sized + Serialize>(value: &T) -> Result<Option<String>> {
    const RAW_VALUE_TOKEN: &str = "$serde_json::private::RawValue";
    const SERDE_JSON_NUMBER_TOKEN: &str = "$serde_json::private::Number";

    struct PrimitiveCapture(Option<String>);

    enum PrimitiveStructKind {
        RawValue,
        JsonNumber,
    }

    struct PrimitiveStruct<'a> {
        capture: &'a mut PrimitiveCapture,
        kind: PrimitiveStructKind,
        has_value: bool,
    }

    impl<'a> ser::Serializer for &'a mut PrimitiveCapture {
        type Ok = ();
        type Error = SerdeError;
        type SerializeSeq = ser::Impossible<(), SerdeError>;
        type SerializeTuple = ser::Impossible<(), SerdeError>;
        type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
        type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
        type SerializeMap = ser::Impossible<(), SerdeError>;
        type SerializeStruct = PrimitiveStruct<'a>;
        type SerializeStructVariant = ser::Impossible<(), SerdeError>;

        fn serialize_bool(self, v: bool) -> Result<()> {
            self.0 = Some(utils::bool_to_string(v).to_string());
            Ok(())
        }

        fn serialize_i8(self, v: i8) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_i16(self, v: i16) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_i32(self, v: i32) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_i64(self, v: i64) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_u8(self, v: u8) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_u16(self, v: u16) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_u32(self, v: u32) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_u64(self, v: u64) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_f32(self, v: f32) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_f64(self, v: f64) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_char(self, v: char) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_str(self, v: &str) -> Result<()> {
            self.0 = Some(v.to_string());
            Ok(())
        }

        fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
            // Not a primitive
            Ok(())
        }

        fn serialize_none(self) -> Result<()> {
            // None is skipped
            Ok(())
        }

        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
            value.serialize(self)
        }

        fn serialize_unit(self) -> Result<()> {
            Ok(())
        }

        fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
            Ok(())
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            variant: &'static str,
        ) -> Result<()> {
            self.0 = Some(variant.to_string());
            Ok(())
        }

        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }

        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            value: &T,
        ) -> Result<()> {
            value.serialize(self)
        }

        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            // Not a primitive
            Err(SerdeError::Custom("Not a primitive".to_string()))
        }

        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
            // Not a primitive
            Err(SerdeError::Custom("Not a primitive".to_string()))
        }

        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            // Not a primitive
            Err(SerdeError::Custom("Not a primitive".to_string()))
        }

        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            // Not a primitive
            Err(SerdeError::Custom("Not a primitive".to_string()))
        }

        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
            // Not a primitive
            Err(SerdeError::Custom("Not a primitive".to_string()))
        }

        fn serialize_struct(
            self,
            name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct> {
            match name {
                RAW_VALUE_TOKEN => Ok(PrimitiveStruct {
                    capture: self,
                    kind: PrimitiveStructKind::RawValue,
                    has_value: false,
                }),
                SERDE_JSON_NUMBER_TOKEN => Ok(PrimitiveStruct {
                    capture: self,
                    kind: PrimitiveStructKind::JsonNumber,
                    has_value: false,
                }),
                _ => Err(SerdeError::Custom("Not a primitive".to_string())),
            }
        }

        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant> {
            // Not a primitive
            Err(SerdeError::Custom("Not a primitive".to_string()))
        }
    }

    impl<'a> ser::SerializeStruct for PrimitiveStruct<'a> {
        type Ok = ();
        type Error = SerdeError;

        fn serialize_field<T: ?Sized + Serialize>(
            &mut self,
            key: &'static str,
            value: &T,
        ) -> Result<()> {
            let expected_key = match self.kind {
                PrimitiveStructKind::RawValue => RAW_VALUE_TOKEN,
                PrimitiveStructKind::JsonNumber => SERDE_JSON_NUMBER_TOKEN,
            };

            if key != expected_key {
                return Err(SerdeError::Custom(format!(
                    "Unexpected field while serializing {expected_key}"
                )));
            }

            if self.has_value {
                return Err(SerdeError::Custom(format!(
                    "Duplicate field while serializing {expected_key}"
                )));
            }

            let mut inner = PrimitiveCapture(None);
            value.serialize(&mut inner)?;

            if let Some(raw) = inner.0 {
                self.capture.0 = Some(raw);
                self.has_value = true;
                Ok(())
            } else {
                Err(SerdeError::Custom(format!(
                    "{expected_key} did not serialize to a primitive string"
                )))
            }
        }

        fn end(self) -> Result<()> {
            if self.has_value {
                Ok(())
            } else {
                let missing = match self.kind {
                    PrimitiveStructKind::RawValue => "RawValue",
                    PrimitiveStructKind::JsonNumber => "Number",
                };
                Err(SerdeError::Custom(format!(
                    "{missing} missing inner value during serialization"
                )))
            }
        }
    }

    let mut capture = PrimitiveCapture(None);
    match value.serialize(&mut capture) {
        Ok(()) => Ok(capture.0),
        Err(e) if e.to_string().contains("Not a primitive") => Ok(None),
        Err(e) => Err(e),
    }
}

/// Serializer for maps (structs/objects).
pub struct MapSerializer<'a, W: Write> {
    serializer: &'a mut XmlSerializer<W>,
    current_key: Option<String>,
    resource_element_open: bool,
}

impl<'a, W: Write> ser::SerializeMap for MapSerializer<'a, W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        // Keys in FHIR JSON are always strings
        // We use a simple serializer to capture the key string
        struct KeyCapture(Option<String>);

        impl<'a> ser::Serializer for &'a mut KeyCapture {
            type Ok = ();
            type Error = SerdeError;
            type SerializeSeq = ser::Impossible<(), SerdeError>;
            type SerializeTuple = ser::Impossible<(), SerdeError>;
            type SerializeTupleStruct = ser::Impossible<(), SerdeError>;
            type SerializeTupleVariant = ser::Impossible<(), SerdeError>;
            type SerializeMap = ser::Impossible<(), SerdeError>;
            type SerializeStruct = ser::Impossible<(), SerdeError>;
            type SerializeStructVariant = ser::Impossible<(), SerdeError>;

            fn serialize_str(self, v: &str) -> Result<()> {
                self.0 = Some(v.to_string());
                Ok(())
            }

            fn serialize_bool(self, _v: bool) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_i8(self, _v: i8) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_i16(self, _v: i16) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_i32(self, _v: i32) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_i64(self, _v: i64) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_u8(self, _v: u8) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_u16(self, _v: u16) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_u32(self, _v: u32) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_u64(self, _v: u64) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_f32(self, _v: f32) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_f64(self, _v: f64) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_char(self, _v: char) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_none(self) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_unit(self) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_unit_variant(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
            ) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_newtype_struct<T: ?Sized + Serialize>(
                self,
                _name: &'static str,
                _value: &T,
            ) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_newtype_variant<T: ?Sized + Serialize>(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                _value: &T,
            ) -> Result<()> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_tuple_struct(
                self,
                _name: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeTupleStruct> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_tuple_variant(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeTupleVariant> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_struct(
                self,
                _name: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeStruct> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
            fn serialize_struct_variant(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeStructVariant> {
                Err(SerdeError::Custom("Keys must be strings".to_string()))
            }
        }

        let mut capture = KeyCapture(None);
        key.serialize(&mut capture)?;
        self.current_key = capture.0;
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let key = self
            .current_key
            .take()
            .ok_or_else(|| SerdeError::Custom("No key for value".to_string()))?;

        // Special handling for resourceType field at root level
        if key == "resourceType" && !self.resource_element_open {
            // Get the resource type value (should be a string)
            if let Some(resource_type) = try_serialize_as_primitive(value)? {
                self.serializer.start_resource_element(&resource_type)?;
                self.resource_element_open = true;
                return Ok(());
            } else {
                return Err(SerdeError::Custom(
                    "resourceType must be a string".to_string(),
                ));
            }
        }

        // Check if this is an extension field (starts with underscore)
        if utils::is_extension_field(&key) {
            // Strip underscore to get base field name
            let base_name = utils::strip_underscore(&key);

            // Check if the extension value is an array
            let is_ext_array = is_array_value(value)?;

            // Check if we have a pending field with this base name
            if let Some(pending) = self.serializer.pending_field.take() {
                if pending.name() == base_name {
                    match pending {
                        PendingField::Single(mut field) => {
                            if !is_ext_array {
                                // Merge single extension data into existing pending field
                                let (id, extensions) = extract_extension_fields(value)?;
                                if let Some(id_val) = id {
                                    field.id = Some(id_val);
                                }
                                if !extensions.is_empty() {
                                    field.extensions.extend(extensions);
                                }
                                self.serializer.pending_field = Some(PendingField::Single(field));
                                return Ok(());
                            } else {
                                // Had single field, now got array extension - convert to array
                                let ext_data = extract_array_extension_data(value)?;
                                let mut values = vec![field.value];
                                // Pad values to match extension data length
                                while values.len() < ext_data.len() {
                                    values.push(None);
                                }
                                self.serializer.pending_field =
                                    Some(PendingField::Array(ArrayFieldBuffer {
                                        name: base_name.to_string(),
                                        values,
                                        extension_data: ext_data,
                                    }));
                                return Ok(());
                            }
                        }
                        PendingField::Array(mut array) => {
                            if is_ext_array {
                                // Merge array extension data
                                let ext_data = extract_array_extension_data(value)?;
                                array.extension_data = ext_data;
                                self.serializer.pending_field = Some(PendingField::Array(array));
                                return Ok(());
                            } else {
                                // Had array, got single extension - shouldn't happen but handle it
                                self.serializer.pending_field = Some(PendingField::Array(array));
                                return Ok(());
                            }
                        }
                    }
                } else {
                    // Different field name - write the pending one
                    match pending {
                        PendingField::Single(field) => self.serializer.write_field(field)?,
                        PendingField::Array(array) => self.serializer.write_array_field(array)?,
                    }
                }
            }

            // No matching pending field - create new one for extensions only
            if is_ext_array {
                // Array extension data
                let ext_data = extract_array_extension_data(value)?;
                self.serializer.pending_field = Some(PendingField::Array(ArrayFieldBuffer {
                    name: base_name.to_string(),
                    values: Vec::new(),
                    extension_data: ext_data,
                }));
            } else {
                // Single extension data
                let mut pending = FieldBuffer::new(base_name.to_string());
                let (id, extensions) = extract_extension_fields(value)?;
                pending.id = id;
                pending.extensions = extensions;
                self.serializer.pending_field = Some(PendingField::Single(pending));
            }
            Ok(())
        } else {
            // Regular field - flush any pending field that doesn't match
            if let Some(pending) = self.serializer.pending_field.take() {
                if pending.name() != key {
                    // Different field - write the pending one
                    match pending {
                        PendingField::Single(field) => self.serializer.write_field(field)?,
                        PendingField::Array(array) => self.serializer.write_array_field(array)?,
                    }
                } else {
                    // Same field name from _field - put it back
                    self.serializer.pending_field = Some(pending);
                }
            }

            // Check if we have a pending field for this key (from _field)
            if let Some(pending) = self.serializer.pending_field.take() {
                if pending.name() == key {
                    match pending {
                        PendingField::Single(mut field) => {
                            // Add value to existing pending single field
                            // Try to serialize as primitive first
                            if let Some(val_str) = try_serialize_as_primitive(value)? {
                                field.value = Some(val_str);
                                self.serializer.pending_field = Some(PendingField::Single(field));
                                return Ok(());
                            }
                            // Complex type - not supported in field buffer
                            return Err(SerdeError::Custom(
                                "Complex types not yet supported in field buffer".to_string(),
                            ));
                        }
                        PendingField::Array(mut array) => {
                            // Add values to existing pending array field
                            let values = extract_array_values(value)?;
                            array.values = values;
                            self.serializer.pending_field = Some(PendingField::Array(array));
                            return Ok(());
                        }
                    }
                } else {
                    // Different field - shouldn't happen, put it back
                    self.serializer.pending_field = Some(pending);
                }
            }

            // No pending field - try to serialize as primitive
            if let Some(val_str) = try_serialize_as_primitive(value)? {
                let mut pending = FieldBuffer::new(key.clone());
                pending.value = Some(val_str);
                self.serializer.pending_field = Some(PendingField::Single(pending));
                return Ok(());
            }

            // Not a primitive - could be None, array, or complex type
            // Check if it's None first by trying to detect it
            if is_none_value(value)? {
                // None value - skip it entirely
                return Ok(());
            }

            // Check if it's an array by trying to serialize it
            let is_array = is_array_value(value)?;

            if is_array {
                // Array - flush any pending field first
                if let Some(pending) = self.serializer.pending_field.take() {
                    match pending {
                        PendingField::Single(field) => self.serializer.write_field(field)?,
                        PendingField::Array(array) => self.serializer.write_array_field(array)?,
                    }
                }

                // Extract array values to determine if it's primitives or complex objects
                let values = extract_array_values(value)?;

                // Check if all values are None (meaning complex objects)
                let has_primitive = values.iter().any(|v| v.is_some());

                if has_primitive {
                    // Array of primitives - buffer it (extension data might come later)
                    self.serializer.pending_field = Some(PendingField::Array(ArrayFieldBuffer {
                        name: key.clone(),
                        values,
                        extension_data: Vec::new(),
                    }));
                    Ok(())
                } else {
                    // Array of complex objects (or all nulls) - write directly using NamedSeqSerializer
                    // The NamedSeqSerializer will skip None elements
                    let mut seq_serializer = NamedSeqSerializer {
                        serializer: self.serializer,
                        element_name: key.clone(),
                    };

                    value.serialize(&mut seq_serializer)?;
                    Ok(())
                }
            } else {
                // Complex type (struct/object) - write directly as nested element
                // Flush any pending field first
                if let Some(pending) = self.serializer.pending_field.take() {
                    match pending {
                        PendingField::Single(field) => self.serializer.write_field(field)?,
                        PendingField::Array(array) => self.serializer.write_array_field(array)?,
                    }
                }

                // Write start element for this field
                let is_resource = utils::is_resource_name(&key);
                self.serializer.write_start_element(&key, is_resource)?;

                // Serialize the complex value (will recursively use our serializer)
                value.serialize(&mut *self.serializer)?;

                // Write end element
                self.serializer.write_end_element(&key)?;

                Ok(())
            }
        }
    }

    fn end(self) -> Result<()> {
        // Flush any pending field
        if let Some(pending) = self.serializer.pending_field.take() {
            match pending {
                PendingField::Single(field) => self.serializer.write_field(field)?,
                PendingField::Array(array) => self.serializer.write_array_field(array)?,
            }
        }

        if self.resource_element_open {
            self.serializer.end_resource_element()?;
        }
        Ok(())
    }
}

impl<'a, W: Write> ser::SerializeStruct for MapSerializer<'a, W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        <Self as ser::SerializeMap>::serialize_key(self, key)?;
        <Self as ser::SerializeMap>::serialize_value(self, value)
    }

    fn end(self) -> Result<()> {
        <Self as ser::SerializeMap>::end(self)
    }
}

// Stub implementations for unsupported types
impl<'a, W: Write> ser::SerializeTuple for &'a mut XmlSerializer<W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(SerdeError::Custom("Tuples not supported".to_string()))
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W: Write> ser::SerializeTupleStruct for &'a mut XmlSerializer<W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(SerdeError::Custom(
            "Tuple structs not supported".to_string(),
        ))
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W: Write> ser::SerializeTupleVariant for &'a mut XmlSerializer<W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(SerdeError::Custom(
            "Tuple variants not supported".to_string(),
        ))
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, W: Write> ser::SerializeStructVariant for &'a mut XmlSerializer<W> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(SerdeError::Custom(
            "Struct variants not supported".to_string(),
        ))
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}
