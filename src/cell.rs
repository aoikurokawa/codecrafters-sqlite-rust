use anyhow::{bail, Context};

use crate::{decode_varint, page::PageType, record::Record};

#[derive(Debug, Clone)]
pub struct Cell {
    pub page_number_left_child: Option<u32>,
    pub npayload: Option<i64>,
    pub rowid: Option<i64>,
    pub record: Option<Record>,
    pub page_number_first_overflow: Option<u32>,
}

impl Cell {
    pub fn from_bytes(page_type: &PageType, offset: usize, bytes: &[u8]) -> anyhow::Result<Self> {
        let mut idx = offset;

        match page_type {
            PageType::LeafTable => {
                let (npayload, bytes_read) = decode_varint(&bytes[idx..idx + 9])
                    .context("decode varint for payload size")?;
                idx += bytes_read;

                let (rowid, bytes_read) = decode_varint(&bytes[idx..idx + 9])
                    .context("decode varint for payload size")?;
                idx += bytes_read;
                //let end = if npayload as usize > bytes.len() {
                //    bytes.len()
                //} else {
                //    idx + npayload as usize
                //};
                let end = idx + npayload as usize;
                let payload = &bytes[idx..end];
                let record = Record::new(payload)?;

                let page_number_first_overflow = if bytes.len() > end + 4 {
                    let num = u32::from_be_bytes([
                        bytes[end],
                        bytes[end + 1],
                        bytes[end + 2],
                        bytes[end + 3],
                    ]);
                    Some(num)
                } else {
                    None
                };

                Ok(Self {
                    page_number_left_child: None,
                    npayload: Some(npayload),
                    rowid: Some(rowid),
                    record: Some(record),
                    page_number_first_overflow,
                })
            }
            PageType::InteriorTable => {
                let page_number_left_child = Some(u32::from_be_bytes([
                    bytes[idx],
                    bytes[idx + 1],
                    bytes[idx + 2],
                    bytes[idx + 3],
                ]));
                idx += 4;

                let (rowid, _bytes_read) =
                    decode_varint(&bytes[idx..]).context("decode varint for payload size")?;

                Ok(Self {
                    page_number_left_child,
                    npayload: None,
                    rowid: Some(rowid),
                    record: None,
                    page_number_first_overflow: None,
                })
            }
            PageType::LeafIndex => {
                let (npayload, bytes_read) =
                    decode_varint(&bytes[idx..]).context("decode varint for payload size")?;
                idx += bytes_read;

                let end = idx + npayload as usize;
                let payload = &bytes[idx..end];
                let record = Record::new(payload).context("create new record")?;

                Ok(Self {
                    page_number_left_child: None,
                    npayload: Some(npayload),
                    rowid: None,
                    record: Some(record),
                    page_number_first_overflow: None,
                })
            }
            PageType::InteriorIndex => {
                let page_number_left_child = Some(u32::from_be_bytes([
                    bytes[idx],
                    bytes[idx + 1],
                    bytes[idx + 2],
                    bytes[idx + 3],
                ]));
                idx += 4;

                let (npayload, bytes_read) =
                    decode_varint(&bytes[idx..]).context("decode varint for payload size")?;
                idx += bytes_read;

                let end = idx + npayload as usize;
                let payload = &bytes[idx..end];
                let record = Record::new(payload).context("create new record")?;

                let page_number_first_overflow = if bytes.len() > end + 4 {
                    let num = u32::from_be_bytes([
                        bytes[end],
                        bytes[end + 1],
                        bytes[end + 2],
                        bytes[end + 3],
                    ]);
                    Some(num)
                } else {
                    None
                };

                Ok(Self {
                    page_number_left_child,
                    npayload: Some(npayload),
                    rowid: None,
                    record: Some(record),
                    page_number_first_overflow,
                })
            }
            PageType::PageError => {
                bail!("can not read cell")
            }
        }
    }

    // pub fn record(&self) -> &Record {
    //     &self.record
    // }
}
