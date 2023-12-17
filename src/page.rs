use crate::database::DbHeader;

#[derive(Debug, Clone)]
pub struct BTreePage {
    db_header: Option<DbHeader>,
    pub(crate) btree_header: BTreePageHeader,
}

impl BTreePage {
    pub fn new(db_header: Option<DbHeader>, btree_header: BTreePageHeader) -> Self {
        Self {
            db_header,
            btree_header,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    page_type: u8,
    first_freeblock: u16,
    pub(crate) ncells: u16,
    cell_content_area: u16,
    nfragemented_free: u8,
    right_most_pointer: u32,
}

impl BTreePageHeader {
    pub fn new(header: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            page_type: u8::from_be_bytes([header[0]]),
            first_freeblock: u16::from_be_bytes([header[1], header[2]]),
            ncells: u16::from_be_bytes([header[3], header[4]]),
            cell_content_area: u16::from_be_bytes([header[5], header[6]]),
            nfragemented_free: u8::from_be_bytes([header[7]]),
            right_most_pointer: u32::from_be_bytes([header[8], header[9], header[10], header[11]]),
        })
    }
}
