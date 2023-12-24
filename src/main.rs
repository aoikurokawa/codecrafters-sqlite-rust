use anyhow::{bail, Result};
use sqlite_starter_rust::{column::SerialValue, database::Database};

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
            println!("database page size: {}", db.page_size());

            println!("number of tables: {}", db.tables());
        }
        ".tables" => {
            let file_path = &args[1];

            let db = Database::read_file(file_path)?;
            match db.pages.get(0) {
                Some(first_page) => {
                    let mut tables = String::new();
                    for i in 0..db.tables() {
                        if let Ok(record) = first_page.read_cell(i) {
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
            }
        }
        query if query.to_lowercase().starts_with("select") => {
            let file_path = &args[1];
            let sql_query = nom_sql::parse_query(query).expect("");
            // let target_table = query.split(" ").last().expect("specify table name");

            match sql_query {
                nom_sql::SqlQuery::Select(select) => {
                    let db = Database::read_file(file_path)?;
                    if let Some(first_page) = db.pages.get(0) {
                        for i in 0..db.tables() {
                            if let Ok(record) = first_page.read_cell(i) {
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
                                            for table_name in &select.tables {
                                                // println!("{:?}", target_table);
                                                if table_name.name == t_name {
                                                    match record.columns[3].data() {
                                                        SerialValue::I8(num) => {
                                                            // eprintln!("num: {num}");
                                                            if let Some(page) =
                                                                db.pages.get(*num as usize - 1)
                                                            {
                                                                println!(
                                                                    "{:?}",
                                                                    page.cell_offsets.len()
                                                                );
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        }
                                        _ => {}
                                    },

                                    _ => {}
                                }
                            }

                            // tables.push_str(&format!("{} ", tbl_name));
                        }
                    }
                }
                _ => todo!(),
            }
            // println!("{tables}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
