use crate::{decode_varint, record::Record};

pub struct Cell {
    npayload: i64,
    rowid: i64,
    record: Record,
}

impl Cell {
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut idx = 0;

        let (npayload, bytes_read) = decode_varint(&bytes[idx..])?;
        idx += bytes_read;

        let (rowid, bytes_read) = decode_varint(&bytes[idx..])?;
        idx += bytes_read;

        let end = if npayload as usize > bytes.len() {
            bytes.len()
        } else {
            idx + npayload as usize
        };

        let payload = bytes[idx..end].to_vec();
        let record = Record::new(&payload)?;

        Ok(Self {
            npayload,
            rowid,
            record,
        })
    }
}
