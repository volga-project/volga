use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor as IoCursor;

pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| anyhow!("ipc write: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| anyhow!("ipc write batch: {e}"))?;
        writer.finish().map_err(|e| anyhow!("ipc finish: {e}"))?;
    }
    Ok(buf)
}

pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader =
        StreamReader::try_new(IoCursor::new(bytes), None).map_err(|e| anyhow!("ipc read: {e}"))?;
    let batch = reader
        .next()
        .ok_or_else(|| anyhow!("empty ipc stream"))?
        .map_err(|e| anyhow!("ipc batch: {e}"))?;
    Ok(batch)
}

