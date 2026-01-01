use anyhow::{anyhow, Result};
use bytes::Bytes;
use datafusion::common::ScalarValue;
use prost::Message;
use serde_with::{DeserializeAs, SerializeAs};

pub fn scalar_value_to_bytes(value: &ScalarValue) -> Result<Vec<u8>> {
    let proto: datafusion_proto_common::protobuf_common::ScalarValue = value
        .try_into()
        .map_err(|e: datafusion_proto_common::ToProtoError| anyhow!(e.to_string()))?;

    let mut buf = Vec::new();
    proto
        .encode(&mut buf)
        .map_err(|e| anyhow!("failed to encode ScalarValue protobuf: {e}"))?;
    Ok(buf)
}

pub fn scalar_value_from_bytes(bytes: &[u8]) -> Result<ScalarValue> {
    let proto = datafusion_proto_common::protobuf_common::ScalarValue::decode(Bytes::copy_from_slice(bytes))
        .map_err(|e| anyhow!("failed to decode ScalarValue protobuf: {e}"))?;

    let value: ScalarValue = (&proto)
        .try_into()
        .map_err(|e: datafusion_proto_common::FromProtoError| anyhow!(e.to_string()))?;
    Ok(value)
}

pub struct ScalarValueAsBytes;

impl SerializeAs<ScalarValue> for ScalarValueAsBytes {
    fn serialize_as<S>(value: &ScalarValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = scalar_value_to_bytes(value).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> DeserializeAs<'de, ScalarValue> for ScalarValueAsBytes {
    fn deserialize_as<D>(deserializer: D) -> Result<ScalarValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
        scalar_value_from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}
