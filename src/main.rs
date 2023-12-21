use anyhow::{bail, Context, Result};
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
                    for i in 0..db.tables() {
                        eprintln!("reading cell");
                        if let Ok(record) = first_page.read_cell(i) {
                            eprintln!("{:?}", record.columns[0].data());
                            match record.columns[0].data() {
                                SerialValue::String(ref str) => {
                                    if str != "table" {
                                        continue;
                                    }
                                }
                                _ => {}
                            }

                            eprintln!("{:?}", record.columns[2].data());
                            let tbl_name = match record.columns[2].data() {
                                SerialValue::String(ref str) => {
                                    if str != "sqlite_sequence" {
                                        continue;
                                    }
                                    str
                                }
                                _ => "",
                            };

                            eprintln!("{tbl_name}");
                        };
                    }
                }
                None => eprintln!("can not read first page"),
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
