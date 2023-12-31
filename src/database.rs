use std::{
    collections::HashSet,
    fs::{self},
    path::Path,
};

use crate::{
    cell::Cell,
    column::SerialValue,
    page::{Page, PageType},
    record::Record,
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

        // let (header, _rest) = file.split_at(100);
        let header = DbHeader::new(&file[0..100])?;
        assert_eq!(file.len() % header.page_size, 0);
        assert_eq!(header.header_string, "SQLite format 3\0");

        let mut pages = vec![]; // 64023
        for (page_i, b_tree_page) in file.chunks(header.page_size).enumerate() {
            let page = if page_i == 0 {
                Page::new(page_i, Some(header.clone()), b_tree_page)
            } else {
                Page::new(page_i, None, b_tree_page)
            };
            pages.push(page);
        }

        Ok(Self { header, pages })
    }

    pub fn page_size(&self) -> usize {
        self.header.page_size
    }

    pub fn read_index(
        &self,
        num: usize,
        _index_statement: &Sql,
        select_statement: &Sql,
        rowids: &mut HashSet<i64>,
    ) {
        let mut page_idxes: Vec<usize> = vec![num - 1];
        let select_query: Vec<&str> = select_statement
            .selection
            .values()
            .map(|val| val.as_str())
            .collect();

        while let Some(page_idx) = page_idxes.pop() {
            if let Some(page) = self.pages.get(page_idx) {
                let cells: Vec<Cell> = page
                    .cell_offsets
                    .iter()
                    .map(|offset| {
                        Cell::from_bytes(page.page_type(), *offset as usize, &page.buffer)
                            .expect("construct cell")
                    })
                    .collect();
                match page.page_type() {
                    PageType::InteriorIndex => {
                        for cell in cells.iter() {
                            let page_num_left_child = cell.page_number_left_child.unwrap();
                            let record = cell.record.clone().unwrap();

                            if let SerialValue::String(country) = record.columns[0].data() {
                                match country.as_str().cmp(select_query[0]) {
                                    std::cmp::Ordering::Less => {
                                        page_idxes.push(page_num_left_child as usize - 1);
                                    }
                                    std::cmp::Ordering::Greater => {
                                        if let Some(num) = page.btree_header.right_most_pointer {
                                            page_idxes.push(num as usize - 1);
                                        }
                                    }
                                    std::cmp::Ordering::Equal => {
                                        let num = match record.columns[1].data() {
                                            SerialValue::I8(num) => *num as i64,
                                            SerialValue::I16(num) => *num as i64,
                                            SerialValue::I24(num) => *num as i64,
                                            SerialValue::I32(num) => *num as i64,
                                            _ => todo!(),
                                        };

                                        rowids.insert(num);
                                    }
                                }
                            };
                        }
                    }

                    PageType::LeafIndex => {
                        for cell in cells.iter() {
                            let record = cell.record.clone().unwrap();

                            if let SerialValue::String(country) = record.columns[0].data() {
                                if select_query[0] == country {
                                    let num = match record.columns[1].data() {
                                        SerialValue::I8(num) => *num as i64,
                                        SerialValue::I16(num) => *num as i64,
                                        SerialValue::I24(num) => *num as i64,
                                        SerialValue::I32(num) => *num as i64,
                                        _ => todo!(),
                                    };

                                    rowids.insert(num);
                                }
                            };
                        }
                    }
                    _ => {}
                }
            }
        }
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
                let cells: Vec<Cell> = page
                    .cell_offsets
                    .iter()
                    .map(|offset| {
                        Cell::from_bytes(page.page_type(), *offset as usize, &page.buffer)
                            .expect("construct cell")
                    })
                    .collect();
                let cell_len = page.cell_offsets.len();

                if !select_statement.selection.is_empty() {
                    for i in 0..cell_len {
                        if let Some(page_num_left_child) = cells[i].page_number_left_child {
                            page_idxes.push(page_num_left_child as usize - 1);
                        }

                        if let Some(record) = &cells[i].record {
                            select_statement.print_rows(
                                record,
                                &cells[i].rowid,
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
                let cells: Vec<Cell> = page
                    .cell_offsets
                    .iter()
                    .map(|offset| {
                        Cell::from_bytes(page.page_type(), *offset as usize, &page.buffer)
                            .expect("construct cell")
                    })
                    .collect();
                let cell_len = page.cell_offsets.len();

                if !select_statement.selection.is_empty() {
                    match page.page_type() {
                        PageType::InteriorTable => {
                            let mut ids = ids;
                            for i in 0..cell_len {
                                let page_num_left_child = cells[i].page_number_left_child.unwrap();
                                let key = cells[i].rowid.unwrap();

                                let split_at = ids.split_at(ids.partition_point(|id| *id < key));
                                let left_ids = split_at.0; // Ids to the left
                                ids = split_at.1; // Ids to the right

                                if !left_ids.is_empty() {
                                    page_idxes.push(page_num_left_child as usize - 1);
                                }
                            }

                            if ids.is_empty() {
                                continue;
                            }

                            if let Some(num) = page.btree_header.right_most_pointer {
                                page_idxes.push(num as usize - 1);
                            }
                        }
                        PageType::LeafTable => {
                            let records: Vec<(i64, Record)> = cells
                                .iter()
                                .filter(|cell| {
                                    let rowid = cell.rowid.unwrap();
                                    ids.binary_search(&rowid).is_ok()
                                })
                                .map(|cell| (cell.rowid.unwrap(), cell.record.clone().unwrap()))
                                .collect();

                            for (rowid, record) in records {
                                select_statement.print_rows_by_rowid(
                                    &record,
                                    &Some(rowid),
                                    &fields,
                                    row_set,
                                    rowid_set,
                                );
                            }
                        }
                        _ => {}
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
