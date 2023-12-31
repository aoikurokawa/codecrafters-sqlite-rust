use anyhow::{bail, Context};

use crate::{database::DbHeader, decode_varint, record::Record};

#[derive(Debug, Clone)]
pub struct Page {
    pub db_header: Option<DbHeader>,
    pub btree_header: BTreePageHeader,
    pub(crate) buffer: Vec<u8>,
    pub cell_offsets: Vec<u16>,
    // pub cells: Vec<Cell>,
}

impl Page {
    pub fn new(idx: usize, db_header: Option<DbHeader>, b_tree_page: &[u8]) -> Self {
        // let mut db_header = None;
        let btree_header;
        let mut buffer = vec![];
        buffer.extend_from_slice(b_tree_page);

        if idx == 0 {
            // db_header = Some(header.clone());
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
        // let mut cells = Vec::new();
        let mut cell_offsets = vec![0; ncells];
        for i in 0..ncells {
            let offset = header_size + i * 2;
            let num = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
            cell_offsets[i] = num;

            // let cell = Cell::from_bytes(&btree_header.page_type, num as usize, &b_tree_page)
            //     .expect("construct a cell");
            // cells.push(cell);
        }

        Self {
            db_header,
            btree_header,
            buffer: b_tree_page.to_vec(),
            cell_offsets,
            // cells,
        }
    }

    pub fn page_type(&self) -> &PageType {
        &self.btree_header.page_type
    }

    pub fn read_cell(&self, i: u16) -> anyhow::Result<(Option<i64>, Option<Record>)> {
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

                let (rowid, bytes_read) = decode_varint(&self.buffer[idx..idx + 9])
                    .context("decode varint for payload size")?;
                idx += bytes_read;

                // let end = if npayload as usize > self.buffer.len() {
                //     self.buffer.len()
                // } else {
                //     idx + npayload as usize
                // };
                let end = idx + npayload as usize;
                let payload = &self.buffer[idx..end];
                let record = Record::new(payload).context("create new record")?;

                Ok((Some(rowid), Some(record)))
            }
            PageType::LeafIndex => {
                let mut idx = offset;

                let (npayload, bytes_read) =
                    decode_varint(&self.buffer[idx..]).context("decode varint for payload size")?;
                idx += bytes_read;

                let end = idx + npayload as usize;
                let payload = &self.buffer[idx..end];
                let record = Record::new(payload).context("create new record")?;

                Ok((None, Some(record)))
            }
            PageType::InteriorTable => {
                let mut idx = offset;

                let _left_child_pointer = u32::from_be_bytes([
                    self.buffer[idx],
                    self.buffer[idx + 1],
                    self.buffer[idx + 2],
                    self.buffer[idx + 3],
                ]);
                idx += 4;

                let (rowid, _bytes_read) = decode_varint(&self.buffer[idx..idx + 9])
                    .context("decode varint for payload size")?;
                // idx += bytes_read;

                Ok((Some(rowid), None))
            }
            PageType::InteriorIndex => {
                let mut idx = offset;

                let _left_child_pointer = u32::from_be_bytes([
                    self.buffer[idx],
                    self.buffer[idx + 1],
                    self.buffer[idx + 2],
                    self.buffer[idx + 3],
                ]);
                idx += 4;

                let (npayload, bytes_read) =
                    decode_varint(&self.buffer[idx..]).context("decode varint for payload size")?;
                idx += bytes_read;

                let end = idx + npayload as usize;
                let payload = &self.buffer[idx..end];
                let record = Record::new(payload).context("create new record")?;

                Ok((None, Some(record)))
            }
            PageType::PageError => {
                bail!("can not read cell");
            }
        }
    }

    // pub fn read_page_idx(&self, i: u16) -> anyhow::Result<Option<Vec<usize>>> {
    //     if i >= self.btree_header.ncells {
    //         bail!("Cell index out of range");
    //     }

    //     let offset = self.cell_offsets[i as usize] as usize;

    //     match self.btree_header.page_type {
    //         PageType::InteriorTable  => {
    //

    //             Ok(pointer as usize)
    //         }
    //         PageType::InteriorIndex => {
    //         }
    //         _ => todo!(),
    //     }
    // }
}

#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    /// The one-byte flag at offset 0 indicating the b-tree page type
    pub page_type: PageType,

    pub freeblock_offset: u16,
    pub ncells: u16,
    pub cells_start: u16,
    pub nfragemented_free: u8,
    pub right_most_pointer: Option<u32>,
}

impl BTreePageHeader {
    pub fn new(header: &[u8]) -> anyhow::Result<Self> {
        let page_type = PageType::try_from(u8::from_be_bytes([header[0]]))?;

        let right_most_pointer = if matches!(page_type, PageType::InteriorTable)
            || matches!(page_type, PageType::InteriorIndex)
        {
            Some(u32::from_be_bytes([
                header[8], header[9], header[10], header[11],
            ]))
        } else {
            None
        };

        Ok(Self {
            page_type,
            freeblock_offset: u16::from_be_bytes([header[1], header[2]]),
            ncells: u16::from_be_bytes([header[3], header[4]]),
            cells_start: u16::from_be_bytes([header[5], header[6]]),
            nfragemented_free: u8::from_be_bytes([header[7]]),
            right_most_pointer,
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

    /// Any other value for the b-tree page type is an error.
    PageError,
}

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::InteriorIndex),
            5 => Ok(Self::InteriorTable),
            10 => Ok(Self::LeafIndex),
            13 => Ok(Self::LeafTable),
            _ => Ok(Self::PageError),
        }
    }
}
