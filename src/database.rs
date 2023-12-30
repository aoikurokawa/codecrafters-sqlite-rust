use std::{collections::HashSet, fs, path::Path};

use crate::{
    page::{Page, PageType},
    sql::Sql,
};

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

    pub fn read_table(
        &self,
        num: usize,
        select_statement: &Sql,
        fields: Vec<(usize, String)>,
        row_set: &mut HashSet<String>,
        rowid_set: &mut HashSet<i64>,
    ) {
        let mut page_idxes: Vec<usize> = vec![num - 1];
        while let Some(page_idx) = page_idxes.pop() {
            if let Some(page) = self.pages.get(page_idx) {
                let cell_len = page.cell_offsets.len();

                if !select_statement.selection.is_empty() {
                    for i in 0..cell_len {
                        if let Some(page_num_left_child) = page.cells[i].page_number_left_child {
                            page_idxes.push(page_num_left_child as usize - 1);
                        }

                        if let Some(page_num_first_overflow) =
                            page.cells[i].page_number_first_overflow
                        {
                            page_idxes.push(page_num_first_overflow as usize - 1);
                        }

                        if let Some(record) = &page.cells[i].record {
                            select_statement.print_rows(
                                record,
                                &page.cells[i].rowid,
                                &fields,
                                row_set,
                                rowid_set,
                            );
                        }
                    }

                    if let Some(num) = page.btree_header.right_most_pointer {
                        page_idxes.push(num as usize - 1);
                    }
                } else {
                    for i in 0..cell_len {
                        if let Ok((_, Some(record))) = page.read_cell(i as u16) {
                            let mut values = Vec::new();

                            for (field_idx, _field_name) in &fields {
                                values.push(record.columns[*field_idx].data().display());
                            }
                            println!("{}", values.join("|"));
                        }
                    }
                }
            }
        }
    }

    pub fn read_ids_from_table(
        &self,
        num: usize,
        select_statement: &Sql,
        fields: Vec<(usize, String)>,
        row_set: &mut HashSet<String>,
        rowid_set: &mut HashSet<i64>,
        ids: &[i64],
    ) {
        let mut page_idxes: Vec<usize> = vec![num - 1];
        while let Some(page_idx) = page_idxes.pop() {
            if let Some(page) = self.pages.get(page_idx) {
                let cell_len = page.cell_offsets.len();

                if !select_statement.selection.is_empty() {

                    let mut ids = ids;

                    for i in 0..cell_len {
                        if let PageType::InteriorTable = page.page_type() {
                            let page_num_left_child = page.cells[i].page_number_left_child.unwrap();
                            let key = page.cells[i].rowid.unwrap();

                            let split_at = ids.split_at(ids.partition_point(|id| *id < key));
                            let left_ids = split_at.0; // Ids to the left
                            ids = split_at.1; // Ids to the right

                            if !left_ids.is_empty() {
                                page_idxes.push(page_num_left_child as usize - 1);
                            }
                        }

                        if let PageType::LeafTable = page.page_type() {
                            let rowid = &page.cells[i].rowid.unwrap();
                            let record = &page.cells[i].record.clone().unwrap();

                            if ids.binary_search(rowid).is_err() {
                                continue;
                            }

                            select_statement.print_rows(
                                record,
                                &page.cells[i].rowid,
                                &fields,
                                row_set,
                                rowid_set,
                            );
                        }
                    }

                    if ids.is_empty() {
                        break;
                    }

                    if let Some(num) = page.btree_header.right_most_pointer {
                        page_idxes.push(num as usize - 1);
                    }
                } else {
                    for i in 0..cell_len {
                        if let Ok((_, Some(record))) = page.read_cell(i as u16) {
                            let mut values = Vec::new();

                            for (field_idx, _field_name) in &fields {
                                values.push(record.columns[*field_idx].data().display());
                            }
                            println!("{}", values.join("|"));
                        }
                    }
                }
            }
        }
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
