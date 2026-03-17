use std::io::Cursor;

use arrow::datatypes::Schema;

pub fn schema_to_ipc(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();
    let cursor = Cursor::new(&mut buf);
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(cursor, schema).unwrap();
    writer.finish().unwrap();
    buf
}

pub fn schema_from_ipc(bytes: &[u8]) -> Schema {
    let cursor = Cursor::new(bytes);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
    reader.schema().as_ref().clone()
}

pub mod b64_bytes {
    use base64::{engine::general_purpose, Engine as _};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        general_purpose::STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)
    }
}

