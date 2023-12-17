use std::{fs, path::Path};

use crate::page::{BTreePage, BTreePageHeader};

#[derive(Debug, Clone)]
pub struct Database {
    /// The first 100 bytes of the database file comprise the database file header.
    header: DbHeader,
    pages: Vec<BTreePage>,
}

impl Database {
    pub fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = fs::read(path)?;

        let header = DbHeader::new(&file[0..100])?;
        assert_eq!(file.len() % header.page_size, 0);

        let mut pages = Vec::new();
        for (page_i, b_tree_page) in file.chunks(header.page_size).enumerate() {
            let mut db_header = None;
            let btree_header = BTreePageHeader::new(&b_tree_page[0..12]).unwrap();
            if page_i == 0 {
                db_header = Some(header.clone());
            }

            pages.push(BTreePage::new(db_header, btree_header));
        }

        eprintln!("{:?}", pages);

        Ok(Self { header, pages })
    }

    pub fn page_size(&self) -> usize {
        self.header.page_size
    }

    pub fn tables(&self) -> u16 {
        self.pages[0].btree_header.ncells
    }
}

#[derive(Debug, Clone)]
pub struct DbHeader {
    header_string: String,
    page_size: usize,
}

impl DbHeader {
    pub fn new(header: &[u8]) -> anyhow::Result<Self> {
        let header_string = String::from_utf8(header[0..16].to_vec())?;
        assert_eq!(header_string, "SQLite format 3\0");

        Ok(Self {
            header_string,
            page_size: u16::from_be_bytes([header[16], header[17]]) as usize,
        })
    }
}
