use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::prelude::*;

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
            let mut file = File::open(&args[1])?;

            let header = DbHeader::new(&mut file)?;
            println!("database page size: {}", header.page_size);

            let mut first_page_bytes = vec![0u8; header.page_size.into()];
            file.read_exact(&mut first_page_bytes)?;
            // let b_tree_page_header = BTreePageHeader::new(&mut file)?;
            println!("number of tables: {}", u16::from_be_bytes([first_page_bytes[3], first_page_bytes[4]]));
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

pub struct DbHeader {
    header_string: String,
    page_size: u16,
}

impl DbHeader {
    pub fn new(f: &mut File) -> anyhow::Result<Self> {
        let mut header = [0; 100];
        f.read_exact(&mut header).context("read 100 bytes")?;

        let header_string = String::from_utf8(header[0..16].to_vec())?;
        assert_eq!(header_string, "SQLite format 3\0");

        Ok(Self {
            header_string,
            page_size: u16::from_be_bytes([header[16], header[17]]),
        })
    }
}

pub struct BTreePageHeader {
    page_type: u8,
    ncells: u16,
}

impl BTreePageHeader {
    pub fn new(f: &mut File) -> anyhow::Result<Self> {
        let mut header = [0; 12];
        f.read_exact(&mut header).context("read 12 bytes")?;

        Ok(Self {
            page_type: u8::from_be_bytes([header[0]]),
            ncells: u16::from_be_bytes([header[3], header[4]]),
        })
    }
}
