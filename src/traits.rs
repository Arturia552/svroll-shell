use serde_json::Value;

pub trait DeserializeOrParse {
    fn deserialize_or_parse(s: String) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;
}

impl DeserializeOrParse for String {
    fn deserialize_or_parse(s: String) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(s)
    }
}

impl DeserializeOrParse for Value {
    fn deserialize_or_parse(s: String) -> Result<Self, Box<dyn std::error::Error>> {
        serde_json::from_str(&s).map_err(Into::into)
    }
}
