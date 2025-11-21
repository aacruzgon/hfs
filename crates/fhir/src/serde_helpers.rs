use serde::{
    Serialize,
    ser::{self, Serializer},
};

#[derive(Clone, Copy)]
struct ContentFound;

impl std::fmt::Debug for ContentFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ContentFound")
    }
}

impl std::fmt::Display for ContentFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("non-empty content detected")
    }
}

impl std::error::Error for ContentFound {}

impl ser::Error for ContentFound {
    fn custom<T: std::fmt::Display>(_: T) -> Self {
        ContentFound
    }
}

struct ContentDetector;

struct SequenceDetector<'a> {
    detector: &'a mut ContentDetector,
}

impl<'a> ser::SerializeSeq for SequenceDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.detector)
    }

    fn end(self) -> Result<(), Self::Error> {
        Err(ContentFound)
    }
}

impl<'a> ser::SerializeTuple for SequenceDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.detector)
    }

    fn end(self) -> Result<(), Self::Error> {
        Err(ContentFound)
    }
}

impl<'a> ser::SerializeTupleStruct for SequenceDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.detector)
    }

    fn end(self) -> Result<(), Self::Error> {
        Err(ContentFound)
    }
}

impl<'a> ser::SerializeTupleVariant for SequenceDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.detector)
    }

    fn end(self) -> Result<(), Self::Error> {
        Err(ContentFound)
    }
}

struct MapDetector<'a> {
    _detector: &'a mut ContentDetector,
}

impl<'a> ser::SerializeMap for MapDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_key<T>(&mut self, _: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(ContentFound)
    }

    fn serialize_value<T>(&mut self, _: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Ok(())
    }

    fn serialize_entry<K, V>(&mut self, _: &K, _: &V) -> Result<(), Self::Error>
    where
        K: ?Sized + Serialize,
        V: ?Sized + Serialize,
    {
        Err(ContentFound)
    }

    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct StructDetector<'a> {
    detector: &'a mut ContentDetector,
    saw_field: bool,
}

impl<'a> ser::SerializeStruct for StructDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match value.serialize(&mut *self.detector) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.saw_field = true;
                Err(err)
            }
        }
    }

    fn end(self) -> Result<(), Self::Error> {
        if self.saw_field {
            Err(ContentFound)
        } else {
            Ok(())
        }
    }
}

impl<'a> ser::SerializeStructVariant for StructDetector<'a> {
    type Ok = ();
    type Error = ContentFound;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match value.serialize(&mut *self.detector) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.saw_field = true;
                Err(err)
            }
        }
    }

    fn end(self) -> Result<(), Self::Error> {
        Err(ContentFound)
    }
}

impl<'a> Serializer for &'a mut ContentDetector {
    type Ok = ();
    type Error = ContentFound;
    type SerializeSeq = SequenceDetector<'a>;
    type SerializeTuple = SequenceDetector<'a>;
    type SerializeTupleStruct = SequenceDetector<'a>;
    type SerializeTupleVariant = SequenceDetector<'a>;
    type SerializeMap = MapDetector<'a>;
    type SerializeStruct = StructDetector<'a>;
    type SerializeStructVariant = StructDetector<'a>;

    fn serialize_bool(self, _value: bool) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_i8(self, _value: i8) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_i16(self, _value: i16) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_i32(self, _value: i32) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_i64(self, _value: i64) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_i128(self, _value: i128) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_u8(self, _value: u8) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_u16(self, _value: u16) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_u32(self, _value: u32) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_u64(self, _value: u64) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_u128(self, _value: u128) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_f32(self, _value: f32) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_f64(self, _value: f64) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_char(self, _value: char) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_str(self, _value: &str) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_none(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<(), Self::Error> {
        Err(ContentFound)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<(), Self::Error>
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
        _value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(ContentFound)
    }

    fn serialize_seq(self, _length: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SequenceDetector { detector: self })
    }

    fn serialize_tuple(self, _length: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(SequenceDetector { detector: self })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _length: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(SequenceDetector { detector: self })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _length: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SequenceDetector { detector: self })
    }

    fn serialize_map(self, _length: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapDetector { _detector: self })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _length: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(StructDetector {
            detector: self,
            saw_field: false,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _length: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(ContentFound)
    }
}

/// Internal helper function for the FhirSerde macro.
/// Do not use directly - this is an implementation detail.
#[doc(hidden)]
pub fn has_non_empty_content<T>(value: &T) -> bool
where
    T: Serialize,
{
    let mut detector = ContentDetector;
    value.serialize(&mut detector).is_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[test]
    fn test_primitives_have_content() {
        // Boolean
        assert!(has_non_empty_content(&true));
        assert!(has_non_empty_content(&false));

        // Integers
        assert!(has_non_empty_content(&42i8));
        assert!(has_non_empty_content(&42i16));
        assert!(has_non_empty_content(&42i32));
        assert!(has_non_empty_content(&42i64));
        assert!(has_non_empty_content(&42i128));
        assert!(has_non_empty_content(&0i32));

        // Unsigned integers
        assert!(has_non_empty_content(&42u8));
        assert!(has_non_empty_content(&42u16));
        assert!(has_non_empty_content(&42u32));
        assert!(has_non_empty_content(&42u64));
        assert!(has_non_empty_content(&42u128));

        // Floats
        assert!(has_non_empty_content(&2.5f32));
        assert!(has_non_empty_content(&2.5f64));
        assert!(has_non_empty_content(&0.0f64));

        // Char
        assert!(has_non_empty_content(&'a'));

        // String
        assert!(has_non_empty_content(&"hello"));
        assert!(has_non_empty_content(&""));
        assert!(has_non_empty_content(&String::from("world")));

        // Bytes (as Vec)
        let bytes = vec![1u8, 2u8, 3u8];
        assert!(has_non_empty_content(&bytes));
    }

    #[test]
    fn test_option_types() {
        // None has no content
        let none: Option<i32> = None;
        assert!(!has_non_empty_content(&none));

        // Some with content
        assert!(has_non_empty_content(&Some(42)));
        assert!(has_non_empty_content(&Some("text")));

        // Some wrapping None (nested)
        let nested_none: Option<Option<i32>> = Some(None);
        assert!(!has_non_empty_content(&nested_none));
    }

    #[test]
    fn test_unit_types() {
        // Unit has no content
        assert!(!has_non_empty_content(&()));
    }

    #[test]
    fn test_sequences() {
        // Empty vec is considered to have content (SequenceDetector::end() always returns Err)
        let empty: Vec<i32> = vec![];
        assert!(has_non_empty_content(&empty));

        // Non-empty vec has content
        assert!(has_non_empty_content(&vec![1, 2, 3]));

        // Array
        assert!(has_non_empty_content(&[1, 2, 3]));
    }

    #[test]
    fn test_tuples() {
        // Tuples with content
        assert!(has_non_empty_content(&(1, 2)));
        assert!(has_non_empty_content(&(1, "text", 2.5)));

        // Empty tuple is unit type
        assert!(!has_non_empty_content(&()));
    }

    #[test]
    fn test_maps() {
        use std::collections::HashMap;

        // Empty map has no content
        let empty: HashMap<String, i32> = HashMap::new();
        assert!(!has_non_empty_content(&empty));

        // Non-empty map has content (serialize_key returns error)
        let mut map = HashMap::new();
        map.insert("key", 42);
        assert!(has_non_empty_content(&map));
    }

    #[derive(Serialize)]
    struct EmptyStruct;

    #[derive(Serialize)]
    struct StructWithAllNone {
        field1: Option<i32>,
        field2: Option<String>,
    }

    #[derive(Serialize)]
    struct StructWithSomeContent {
        field1: Option<i32>,
        field2: Option<String>,
    }

    #[derive(Serialize)]
    struct StructWithContent {
        value: i32,
    }

    #[derive(Serialize)]
    struct NewtypeStruct(i32);

    #[derive(Serialize)]
    struct NewtypeStructEmpty(Option<i32>);

    #[test]
    fn test_structs() {
        // Empty struct has no content
        assert!(!has_non_empty_content(&EmptyStruct));

        // Struct with all None fields has no content
        let all_none = StructWithAllNone {
            field1: None,
            field2: None,
        };
        assert!(!has_non_empty_content(&all_none));

        // Struct with at least one Some field has content
        let some_content = StructWithSomeContent {
            field1: Some(42),
            field2: None,
        };
        assert!(has_non_empty_content(&some_content));

        // Struct with regular fields has content
        let with_content = StructWithContent { value: 42 };
        assert!(has_non_empty_content(&with_content));

        // Newtype struct with content
        assert!(has_non_empty_content(&NewtypeStruct(42)));

        // Newtype struct with None has no content
        assert!(!has_non_empty_content(&NewtypeStructEmpty(None)));

        // Newtype struct with Some has content
        assert!(has_non_empty_content(&NewtypeStructEmpty(Some(42))));
    }

    #[derive(Serialize)]
    #[allow(clippy::enum_variant_names)]
    enum TestEnum {
        UnitVariant,
        NewtypeVariant(i32),
        TupleVariant(i32, String),
        StructVariant { field: i32 },
    }

    #[test]
    fn test_enum_variants() {
        // Unit variant has content
        assert!(has_non_empty_content(&TestEnum::UnitVariant));

        // Newtype variant has content
        assert!(has_non_empty_content(&TestEnum::NewtypeVariant(42)));

        // Tuple variant has content
        assert!(has_non_empty_content(&TestEnum::TupleVariant(
            42,
            "test".to_string()
        )));

        // Struct variant has content
        assert!(has_non_empty_content(&TestEnum::StructVariant {
            field: 42
        }));
    }

    #[derive(Serialize)]
    struct NestedStruct {
        inner: Option<InnerStruct>,
    }

    #[derive(Serialize)]
    struct InnerStruct {
        value: Option<i32>,
    }

    #[test]
    fn test_nested_structures() {
        // Nested struct with all None
        let nested_none = NestedStruct { inner: None };
        assert!(!has_non_empty_content(&nested_none));

        // Nested struct with Some but inner is None
        let nested_some_none = NestedStruct {
            inner: Some(InnerStruct { value: None }),
        };
        assert!(!has_non_empty_content(&nested_some_none));

        // Nested struct with actual content
        let nested_content = NestedStruct {
            inner: Some(InnerStruct { value: Some(42) }),
        };
        assert!(has_non_empty_content(&nested_content));
    }

    #[derive(Serialize)]
    struct ComplexStruct {
        opt_vec: Option<Vec<i32>>,
        opt_string: Option<String>,
        nested: Option<NestedStruct>,
    }

    #[test]
    fn test_complex_combinations() {
        // All None
        let all_none = ComplexStruct {
            opt_vec: None,
            opt_string: None,
            nested: None,
        };
        assert!(!has_non_empty_content(&all_none));

        // Empty vec is considered content (sequences always trigger ContentFound in end())
        let empty_vec = ComplexStruct {
            opt_vec: Some(vec![]),
            opt_string: None,
            nested: None,
        };
        assert!(has_non_empty_content(&empty_vec));

        // Non-empty vec
        let with_vec = ComplexStruct {
            opt_vec: Some(vec![1, 2, 3]),
            opt_string: None,
            nested: None,
        };
        assert!(has_non_empty_content(&with_vec));

        // With string
        let with_string = ComplexStruct {
            opt_vec: None,
            opt_string: Some("text".to_string()),
            nested: None,
        };
        assert!(has_non_empty_content(&with_string));
    }

    #[test]
    fn test_content_found_error_traits() {
        use serde::ser::Error as SerError;

        let err = ContentFound;

        // Test Debug
        assert_eq!(format!("{:?}", err), "ContentFound");

        // Test Display
        assert_eq!(format!("{}", err), "non-empty content detected");

        // Test Error trait (just verify it compiles)
        let _: &dyn std::error::Error = &err;

        // Test ser::Error trait
        let custom_err = <ContentFound as SerError>::custom("test message");
        assert_eq!(format!("{:?}", custom_err), "ContentFound");
    }

    #[derive(Serialize)]
    struct TupleStruct(i32, String);

    #[test]
    fn test_tuple_struct() {
        assert!(has_non_empty_content(&TupleStruct(42, "test".to_string())));
    }

    #[test]
    fn test_empty_string_has_content() {
        // Even empty strings are considered content
        assert!(has_non_empty_content(&""));
        assert!(has_non_empty_content(&String::new()));
    }
}
