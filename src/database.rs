use std::{fs, path::Path};

use anyhow::Context;

use crate::page::Page;

#[derive(Debug, Clone)]
pub struct Database {
    /// The first 100 bytes of the database file comprise the database file header.
    pub header: DbHeader,
    pub pages: Vec<Page>,
}

impl Database {
    pub fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = fs::read(path)?;

        let (header, _rest) = file.split_at(100);
        let header = DbHeader::new(header)?;
        assert_eq!(file.len() % header.page_size, 0);
        assert_eq!(header.header_string, "SQLite format 3\0");

        let mut pages = vec![];
        for (page_i, b_tree_page) in file.chunks(header.page_size).enumerate() {
            let page = Page::new(page_i, header.clone(), b_tree_page);
            pages.push(page);
        }

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

        Ok(Self {
            header_string,
            page_size: u16::from_be_bytes([header[16], header[17]]) as usize,
        })
    }
}
