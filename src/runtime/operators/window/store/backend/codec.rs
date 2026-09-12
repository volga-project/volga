use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use std::io::Cursor;

pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    FileReader::try_new(Cursor::new(bytes), None)?
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("empty Arrow IPC payload"))
}

pub fn encode_batches(batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>> {
    batches.iter().map(encode_batch).collect()
}

pub fn decode_batches(bytes: &[Vec<u8>]) -> Result<Vec<RecordBatch>> {
    bytes.iter().map(|b| decode_batch(b)).collect()
}

pub fn encode_val<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

pub fn decode_val<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(bincode::deserialize(bytes)?)
}
