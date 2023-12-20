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
            let first_page = &db.pages[0];
            for i in 0..db.tables() {
                if let Ok(cell) = first_page.read_cell(i) {
                    match cell.record().columns[0].data() {
                        SerialValue::String(ref str) => {
                            if str != "table" {
                                continue;
                            }
                        }
                        _ => {}
                    }

                    let tbl_name = match cell.record().columns[2].data() {
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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
