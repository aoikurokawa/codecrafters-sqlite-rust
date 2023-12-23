use anyhow::{bail, Context};

use crate::{database::DbHeader, decode_varint, record::Record};

#[derive(Debug, Clone)]
pub struct Page {
    pub(crate) db_header: Option<DbHeader>,
    pub(crate) btree_header: BTreePageHeader,
    pub(crate) buffer: Vec<u8>,
    pub cell_offsets: Vec<u16>,
}

impl Page {
    pub fn new(idx: usize, header: DbHeader, b_tree_page: &[u8]) -> Self {
        let mut db_header = None;
        let btree_header;
        let mut buffer = vec![];
        buffer.extend_from_slice(b_tree_page);

        if idx == 0 {
            db_header = Some(header.clone());
            btree_header = BTreePageHeader::new(&b_tree_page[100..112]).unwrap();
            buffer.drain(0..100);
        } else {
            btree_header = BTreePageHeader::new(&b_tree_page[0..12]).unwrap();
        }

        let header_size: usize = match btree_header.page_type {
            PageType::InteriorIndex | PageType::InteriorTable => 12,
            _ => 8,
        };

        let ncells = btree_header.ncells as usize;
        let mut cell_offsets = vec![0; ncells];
        for i in 0..ncells {
            let offset = header_size + i * 2;
            cell_offsets[i] = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        }

        Self {
            db_header,
            btree_header,
            buffer: b_tree_page.to_vec(),
            cell_offsets,
        }
    }

    // pub fn cell(&self, i: usize) -> Option<Cell> {
    //     self.cells.get(i).cloned()
    // }

    pub fn read_cell(&self, i: u16) -> anyhow::Result<Record> {
        if i >= self.btree_header.ncells {
            bail!("Cell index out of range");
        }

        let offset = self.cell_offsets[i as usize] as usize;

        match self.btree_header.page_type {
            PageType::LeafTable => {
                let mut idx = offset;

                let (npayload, bytes_read) = decode_varint(&self.buffer[idx..idx + 9])
                    .context("decode varint for payload size")?;
                idx += bytes_read;

                let (_rowid, bytes_read) = decode_varint(&self.buffer[idx..idx + 9])
                    .context("decode varint for payload size")?;
                idx += bytes_read;

                let end = idx + npayload as usize;
                let payload = &self.buffer[idx..end];
                let record = Record::new(payload).context("create new record")?;

                Ok(record)
            }
            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    /// The one-byte flag at offset 0 indicating the b-tree page type
    page_type: PageType,

    freeblock_offset: u16,
    pub ncells: u16,
    cells_start: u16,
    nfragemented_free: u8,
    right_most_pointer: u32,
}

impl BTreePageHeader {
    pub fn new(header: &[u8]) -> anyhow::Result<Self> {
        let page_type = PageType::try_from(u8::from_be_bytes([header[0]]))?;

        Ok(Self {
            page_type,
            freeblock_offset: u16::from_be_bytes([header[1], header[2]]),
            ncells: u16::from_be_bytes([header[3], header[4]]),
            cells_start: u16::from_be_bytes([header[5], header[6]]),
            nfragemented_free: u8::from_be_bytes([header[7]]),
            right_most_pointer: u32::from_be_bytes([header[8], header[9], header[10], header[11]]),
        })
    }

    pub fn ncells(&self) -> u16 {
        self.ncells
    }
}

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum PageType {
    /// A value of 2 (0x02) means the page is an interior index b-tree page
    InteriorIndex = 2,

    /// A value of 5 (0x05) means the page is an interior table b-tree page
    InteriorTable = 5,

    /// A value of 10 (0x0a) means the page is a leaf index b-tree page
    LeafIndex = 10,

    /// A value of 13 (0x0d) means the page is a leaf table b-tree page
    LeafTable = 13,
}

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::InteriorIndex),
            5 => Ok(Self::InteriorTable),
            10 => Ok(Self::LeafIndex),
            13 => Ok(Self::LeafTable),
            num => Err(anyhow::Error::msg(format!(
                "No corresponding PageType for value: {num}"
            ))),
        }
    }
}
