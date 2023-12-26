use anyhow::{bail, Result};
use sqlite_starter_rust::{column::SerialValue, database::Database, sql::Sql};

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
        ".index" => {
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

                            for i in 0..4 {
                                eprintln!("Column {i}: {:?}", record.columns[i]);
                            }

                            match record.columns[2].data() {
                                SerialValue::String(ref str) => {
                                    if str == "sqlite_sequence" {
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
                                _ => {}
                            };

                            // tables.push_str(&format!("{} ", tbl_name));
                        };
                    }
                    // println!("{tables}");
                }
                None => eprintln!("can not read first page"),
            }
        }
        query if query.to_lowercase().starts_with("select count(*)") => {
            let file_path = &args[1];
            let select_statement = Sql::from_str(query);

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
            let file_path = &args[1];
            let select_statement = Sql::from_str(query);

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
                                    if select_statement.tbl_name == t_name {
                                        match record.columns[3].data() {
                                            SerialValue::I8(num) => {
                                                if let Some(page) = db.pages.get(*num as usize - 1)
                                                {
                                                    let cell_len = page.cell_offsets.len();

                                                    let create_statement = Sql::from_str(
                                                        &record.columns[4].data().display(),
                                                    );

                                                    let fields: Vec<(usize, String)> =
                                                        select_statement
                                                            .field_name
                                                            .clone()
                                                            .into_iter()
                                                            .enumerate()
                                                            .map(|(_i, select_field)| {
                                                                let index = if let Some(index) =
                                                                    create_statement
                                                                        .field_name
                                                                        .iter()
                                                                        .position(|x| {
                                                                            x.as_str()
                                                                                == select_field
                                                                                    .as_str()
                                                                        }) {
                                                                    index
                                                                } else {
                                                                    0
                                                                };

                                                                (index, select_field)
                                                            })
                                                            .collect();

                                                    if !select_statement.selection.is_empty() {
                                                        for i in 0..cell_len {
                                                            let record =
                                                                page.read_cell(i as u16)?;

                                                            let mut values = Vec::new();
                                                            for (key, value) in
                                                                select_statement.selection.iter()
                                                            {
                                                                for (field_idx, field_name) in
                                                                    &fields
                                                                {
                                                                    let candidate_value = record
                                                                        .columns[*field_idx]
                                                                        .data()
                                                                        .display();
                                                                    if candidate_value == *value {
                                                                        let rows: Vec<String> = fields
                                                                            .iter()
                                                                            .map(|(i, _field)| {
                                                                                record.columns[*i]
                                                                                    .data()
                                                                                    .display()
                                                                            })
                                                                            .collect();
                                                                        values.push(rows.join("|"));

                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                            println!("{}", values.join("|"));
                                                        }
                                                    } else {
                                                        for i in 0..cell_len {
                                                            let record =
                                                                page.read_cell(i as u16)?;

                                                            let mut values = Vec::new();
                                                            for (field_idx, _field_name) in &fields
                                                            {
                                                                values.push(
                                                                    record.columns[*field_idx]
                                                                        .data()
                                                                        .display(),
                                                                );
                                                            }
                                                            println!("{}", values.join("|"));
                                                        }
                                                    }
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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
