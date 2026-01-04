// Serde traits used in custom Deserialize implementations

/// Helper that accepts either a single value or an array when deserializing.
///
/// FHIR allows most repeatable elements to appear either once or multiple times
/// depending on the instance's actual cardinality. While JSON carries enough
/// structure (`[]` vs scalar) so serde can infer that automatically, the XML
/// stream does not embed the schema-driven cardinality constraints. During
/// XML deserialization we therefore wrap every field with a `min > 0` upper
/// bound in `SingleOrVec` so we can accept both the single-element case and
/// the repeated-element case without schema knowledge at parse time.
#[derive(Clone, Debug, PartialEq)]
pub struct SingleOrVec<T>(Vec<T>);

impl<T> AsRef<[T]> for SingleOrVec<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        &self.0
    }
}

impl<T> From<SingleOrVec<T>> for Vec<T> {
    #[inline]
    fn from(wrapper: SingleOrVec<T>) -> Self {
        wrapper.0
    }
}

impl<T> Default for SingleOrVec<T> {
    #[inline]
    fn default() -> Self {
        SingleOrVec(Vec::new())
    }
}

impl<'de, T> serde::Deserialize<'de> for SingleOrVec<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[inline]
        fn deserialize_single_value<'de, D, T>(deserializer: D) -> Result<T, D::Error>
        where
            D: serde::Deserializer<'de>,
            T: serde::Deserialize<'de>,
        {
            T::deserialize(OptionFriendlyDeserializer(deserializer))
        }

        /// Wraps an inner deserializer and forces scalar values to behave like `Some(_)`.
        struct OptionFriendlyDeserializer<D>(D);

        impl<'de, D> serde::Deserializer<'de> for OptionFriendlyDeserializer<D>
        where
            D: serde::Deserializer<'de>,
        {
            type Error = D::Error;

            #[inline]
            fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                self.0.deserialize_any(visitor)
            }

            #[inline]
            fn deserialize_enum<V>(
                self,
                name: &'static str,
                variants: &'static [&'static str],
                visitor: V,
            ) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                self.0.deserialize_enum(name, variants, visitor)
            }

            #[inline]
            fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                visitor.visit_some(self.0)
            }

            serde::forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
                bytes byte_buf unit unit_struct newtype_struct seq tuple tuple_struct
                map struct identifier ignored_any
            }
        }
        struct SingleOrVecVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> serde::de::Visitor<'de> for SingleOrVecVisitor<T>
        where
            T: serde::Deserialize<'de>,
        {
            type Value = SingleOrVec<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a single value or a sequence")
            }

            // High performance path for JSON arrays or repeated XML tags
            #[inline]
            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let values = serde::Deserialize::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )?;
                Ok(SingleOrVec(values))
            }

            // Path for single XML elements (map = object with fields)
            #[inline]
            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let value =
                    deserialize_single_value(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(SingleOrVec(vec![value]))
            }

            // Path for JSON scalars or XML text-only elements
            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = deserialize_single_value(serde::de::value::StrDeserializer::new(v))?;
                Ok(SingleOrVec(vec![value]))
            }

            #[inline]
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = deserialize_single_value(serde::de::value::BoolDeserializer::new(v))?;
                Ok(SingleOrVec(vec![value]))
            }

            #[inline]
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = deserialize_single_value(serde::de::value::I64Deserializer::new(v))?;
                Ok(SingleOrVec(vec![value]))
            }

            #[inline]
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = deserialize_single_value(serde::de::value::U64Deserializer::new(v))?;
                Ok(SingleOrVec(vec![value]))
            }

            #[inline]
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = deserialize_single_value(serde::de::value::F64Deserializer::new(v))?;
                Ok(SingleOrVec(vec![value]))
            }
        }

        deserializer.deserialize_any(SingleOrVecVisitor(std::marker::PhantomData))
    }
}

/// Accepts either JSON primitive values or XML element structures with metadata.
///
/// **JSON Format**: Primitive values come through as scalars, metadata merged from `_field` by macro.
///   - `"birthDate": "1970-03-30"` → `Primitive("1970-03-30")` (String directly)
///   - Metadata in `_field` is handled separately by the generated macro code
///
/// **XML Format**: All primitives are elements with inline metadata, no `_field` exists.
///   - `<birthDate value="1970-03-30"/>` → `Element(Element { value: Some(...), id: None, ... })`
///   - `<birthDate id="x" value="...">` → `Element(Element { value, id, ... })`
///   - `<birthDate id="x" value="..."><extension>...</extension></birthDate>` → `Element` with full metadata
///
/// The untagged enum lets serde choose the variant based on the incoming data structure:
/// - JSON scalars match `Primitive` variant (deserialized directly into final primitive type)
/// - XML element structures (objects with value/id/extension) match `Element` variant
/// This eliminates the need for XML-to-JSON conversion.
///
/// # Type Parameters
/// - `P`: Primitive type (the final deserialized type, e.g. `String`, `i32`, `bool`)
/// - `E`: Element type (struct containing value and metadata fields)
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(untagged)]
pub enum PrimitiveOrElement<P, E> {
    // Try Element first (more specific - requires object structure)
    Element(E),
    // Fall back to Primitive (catch-all for JSON scalars)
    Primitive(P),
}

/// Helper struct for serializing id and extension metadata for FHIR primitives.
///
/// In FHIR JSON, primitive values can have associated metadata stored in a parallel
/// `_fieldName` object containing an `id` and/or `extension` array.
///
/// This helper is used during serialization to output only the id/extension metadata
/// while the primitive value itself is serialized separately.
///
/// # Type Parameters
/// - `'a`: Lifetime of the borrowed data
/// - `E`: Extension type (varies by FHIR version: R4, R4B, R5, R6)
///
/// # Example
/// ```json
/// {
///   "status": "active",
///   "_status": {
///     "id": "status-1",
///     "extension": [...]
///   }
/// }
/// ```
#[derive(serde::Serialize)]
pub struct IdAndExtensionHelper<'a, E> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension: &'a Option<Vec<E>>,
}

/// Helper struct for deserializing id and extension metadata for FHIR primitives.
///
/// This is the owned version of `IdAndExtensionHelper`, used during deserialization
/// to capture id and extension data from the `_fieldName` JSON object.
///
/// # Type Parameters
/// - `E`: Extension type (varies by FHIR version: R4, R4B, R5, R6)
#[derive(Clone, serde::Deserialize, Default)]
pub struct IdAndExtensionOwned<E> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension: Option<Vec<E>>,
}
