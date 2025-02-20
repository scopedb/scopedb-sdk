use std::io::Cursor;

use arrow::{
    array::RecordBatch,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use base64::{Engine, prelude::BASE64_STANDARD};

use crate::error::Error;

pub fn encode_arrow(data: &[RecordBatch]) -> Result<String, Error> {
    assert!(!data.is_empty());

    let schema = data[0].schema();

    let mut buf = Vec::new();

    let mut arrow_writer = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| Error::Internal(format!("failed to create stream writer: {e}")))?;
    for rb in data {
        arrow_writer
            .write(rb)
            .map_err(|e| Error::Internal(format!("failed to write record batch: {e}")))?;
    }
    arrow_writer
        .finish()
        .map_err(|e| Error::Internal(format!("failed to finish stream writer: {e}")))?;

    Ok(BASE64_STANDARD.encode(&buf))
}

pub fn decode_arrow(buf: &str) -> Result<Vec<RecordBatch>, Error> {
    let binary = BASE64_STANDARD
        .decode(buf)
        .map_err(|e| Error::Internal(format!("failed to decode base64: {e}")))?;
    let mut buf = Cursor::new(binary);
    let arrow_reader = StreamReader::try_new(&mut buf, None)
        .map_err(|e| Error::Internal(format!("failed to create stream reader: {e}")))?;

    let results = arrow_reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::Internal(format!("failed to decode record batches: {e}")))?;

    Ok(results)
}
