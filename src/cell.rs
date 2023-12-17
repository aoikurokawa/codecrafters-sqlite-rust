use crate::read_varint;

pub struct Cell {
    npayload: i64,
    rowid: i64,
    payload: Vec<u8>,
}

impl Cell {
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut idx = 0;

        let (npayload, bytes_read) = read_varint(&bytes[idx..])?;
        idx += bytes_read;

        let (rowid, bytes_read) = read_varint(&bytes[idx..])?;
        idx += bytes_read;

        let end = if npayload as usize > bytes.len() {
            bytes.len()
        } else {
            idx + npayload as usize
        };

        let payload = bytes[idx..end].to_vec();

        Ok(Self {
            npayload,
            rowid,
            payload,
        })
    }
}
