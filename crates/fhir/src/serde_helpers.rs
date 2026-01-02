use serde::Deserialize;

/// Helper that accepts either a single value or an array when deserializing.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum SingleOrVec<T> {
    Vec(Vec<T>),
    Single(T),
}

impl<T> SingleOrVec<T> {
    pub fn into_vec(self) -> Vec<T> {
        match self {
            SingleOrVec::Single(value) => vec![value],
            SingleOrVec::Vec(values) => values,
        }
    }
}

impl<T> Default for SingleOrVec<T> {
    fn default() -> Self {
        SingleOrVec::Vec(Vec::new())
    }
}

/// Helper that captures either a raw primitive JSON value or a deserialized element.
#[derive(Clone, Debug, PartialEq)]
pub enum PrimitiveOrElement<T> {
    Primitive(serde_json::Value),
    Element(T),
}

impl<'de, T> Deserialize<'de> for PrimitiveOrElement<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_object() {
            let element = T::deserialize(value).map_err(|err| serde::de::Error::custom(err))?;
            Ok(PrimitiveOrElement::Element(element))
        } else {
            Ok(PrimitiveOrElement::Primitive(value))
        }
    }
}
