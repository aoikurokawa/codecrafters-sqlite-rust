use anyhow::{bail, Context, Result};
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let file_path = &args[1];

            let db = Database::read_file(file_path)?;
            println!("database page size: {}", db.header.page_size);

            // let mut first_page_bytes = vec![0u8; db.header.page_size.into()];
            // file.read_exact(&mut first_page_bytes)?;
            // let b_tree_page_header = BTreePageHeader::new(&mut file)?;
            println!("number of tables: {}", db.pages[0].btree_header.ncells,);
        }
        ".tables" => {}
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

pub struct Database {
    header: DbHeader,
    pages: Vec<BTreePage>,
}

impl Database {
    pub fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = fs::read(path)?;

        // The first 100 bytes of the database file comprise the database file header.
        let header = DbHeader::new(&file[0..100])?;

        assert_eq!(file.len() % header.page_size, 0);

        let mut pages = Vec::new();
        for (page_i, b_tree_page) in file[100..].chunks(header.page_size).enumerate() {
            let mut db_header = None;
            let btree_header = BTreePageHeader::new(b_tree_page).unwrap();
            if page_i == 0 {
                db_header = Some(header.clone());
            }

            pages.push(BTreePage {
                db_header,
                btree_header,
            });
        }

        Ok(Self { header, pages })
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

pub struct BTreePage {
    db_header: Option<DbHeader>,
    btree_header: BTreePageHeader,
}

pub struct BTreePageHeader {
    page_type: u8,
    ncells: u16,
}

impl BTreePageHeader {
    pub fn new(header: &[u8]) -> anyhow::Result<Self> {
        // let mut header = [0; 12];
        // f.read_exact(&mut header).context("read 12 bytes")?;

        Ok(Self {
            page_type: u8::from_be_bytes([header[0]]),
            ncells: u16::from_be_bytes([header[3], header[4]]),
        })
    }
}
