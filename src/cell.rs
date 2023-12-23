use anyhow::Context;

use crate::{decode_varint, record::Record};

#[derive(Debug, Clone)]
pub struct Cell {
    npayload: i64,
    rowid: i64,
    record: Record,
}

impl Cell {
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut idx = 0;

        let (npayload, bytes_read) =
            decode_varint(&bytes[idx..idx + 9]).context("decode varint for payload size")?;
        idx += bytes_read;

        let (rowid, bytes_read) =
            decode_varint(&bytes[idx..idx + 9]).context("decode varint for payload size")?;
        idx += bytes_read;

        //let end = if npayload as usize > bytes.len() {
        //    bytes.len()
        //} else {
        //    idx + npayload as usize
        //};
        let end = idx + npayload as usize;
        let payload = &bytes[idx..end];
        let record = Record::new(payload)?;

        Ok(Self {
            npayload,
            rowid,
            record,
        })
    }

    pub fn record(&self) -> &Record {
        &self.record
    }
}
