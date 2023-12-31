use std::{collections::HashSet, time::Instant};

use anyhow::{bail, Result};
use sqlite_starter_rust::{column::SerialValue, database::Database, sql::Sql};

fn main() -> Result<()> {
    let now = Instant::now();
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let file_path = &args[1];
    let db = Database::read_file(file_path)?;
    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size());

            if let Some(first_page) = db.pages.get(0) {
                println!("number of tables: {}", first_page.btree_header.ncells());
            }
        }
        ".tables" => match db.pages.get(0) {
            Some(first_page) => {
                let mut tables = String::new();
                for i in 0..first_page.btree_header.ncells() {
                    if let Ok((_, Some(record))) = first_page.read_cell(i) {
                        match record.columns[0].data() {
                            SerialValue::String(ref str) => {
                                if str != "table" {
                                    continue;
                                }
                            }
                            _ => {}
                        }

                        let tbl_name = match record.columns[2].data() {
                            SerialValue::String(ref str) => {
                                if str == "sqlite_sequence" {
                                    continue;
                                }
                                &str
                            }
                            _ => "",
                        };

                        tables.push_str(&format!("{} ", tbl_name));
                    };
                }
                println!("{tables}");
            }
            None => eprintln!("can not read first page"),
        },
        query if query.to_lowercase().starts_with("select count(*)") => {
            let select_statement = Sql::from_str(query);

            if let Some(first_page) = db.pages.get(0) {
                for i in 0..first_page.btree_header.ncells() {
                    if let Ok((_, Some(record))) = first_page.read_cell(i) {
                        match record.columns[0].data() {
                            SerialValue::String(ref str) => {
                                if str != "table" {
                                    continue;
                                }
                            }
                            _ => {}
                        }

                        match record.columns[2].data() {
                            SerialValue::String(str) => match str.as_str() {
                                "sqlite_sequence" => {
                                    continue;
                                }
                                t_name => {
                                    if select_statement.tbl_name == t_name {
                                        match record.columns[3].data() {
                                            SerialValue::I8(num) => {
                                                // eprintln!("num: {num}");
                                                if let Some(page) = db.pages.get(*num as usize - 1)
                                                {
                                                    let cell_len = page.cell_offsets.len();
                                                    println!("{:?}", cell_len);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            },

                            _ => {}
                        }
                    }
                }
            }
        }
        query if query.to_lowercase().starts_with("select") => {
            let select_statement = Sql::from_str(query);

            if let Some(first_page) = db.pages.get(0) {
                for i in (0..first_page.btree_header.ncells()).rev() {
                    match first_page.read_cell(i)? {
                        (_, Some(record)) => {
                            let mut rowids = HashSet::new();

                            match record.columns[0].data() {
                                SerialValue::String(str) => match str.as_str() {
                                    "index" => {
                                        let index_statement =
                                            Sql::from_str(&record.columns[4].data().display());
                                        if let SerialValue::I8(num) = record.columns[3].data() {
                                            db.read_index(
                                                *num as usize,
                                                &index_statement,
                                                &select_statement,
                                                &mut rowids,
                                            );
                                        }
                                        continue;
                                    }
                                    _ => {}
                                },
                                _ => {}
                            }

                            let mut rowids: Vec<i64> = rowids.into_iter().collect();
                            rowids.sort_unstable();

                            match record.columns[2].data() {
                                SerialValue::String(str) => match str.as_str() {
                                    "sqlite_sequence" => {
                                        continue;
                                    }
                                    t_name => {
                                        if select_statement.tbl_name == t_name {
                                            match record.columns[3].data() {
                                                SerialValue::I8(num) => {
                                                    let create_statement = Sql::from_str(
                                                        &record.columns[4].data().display(),
                                                    );

                                                    let fields = select_statement
                                                        .get_fields(&create_statement);

                                                    let mut row_set = HashSet::new();
                                                    let mut rowid_set = HashSet::new();

                                                    if rowids.is_empty() {
                                                        db.read_table(
                                                            *num as usize,
                                                            &select_statement,
                                                            fields,
                                                            &mut row_set,
                                                            &mut rowid_set,
                                                        );
                                                    } else {
                                                        db.read_ids_from_table(
                                                            *num as usize,
                                                            &select_statement,
                                                            fields,
                                                            &mut row_set,
                                                            &mut rowid_set,
                                                            &rowids,
                                                        );
                                                    }

                                                    row_set
                                                        .iter()
                                                        .for_each(|str| println!("{str}"));
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                },
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
