use std::collections::HashSet;

use anyhow::{bail, Result};
use sqlite_starter_rust::{column::SerialValue, database::Database, page::PageType, sql::Sql};

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

            if let Some(first_page) = db.pages.get(0) {
                println!("number of tables: {}", first_page.btree_header.ncells());
            }
        }
        ".tables" => {
            let file_path = &args[1];

            let db = Database::read_file(file_path)?;
            match db.pages.get(0) {
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
            }
        }
        ".index" => {
            let file_path = &args[1];

            let db = Database::read_file(file_path)?;
            match db.pages.get(0) {
                Some(first_page) => {
                    for i in 0..first_page.btree_header.ncells() {
                        if let Ok((_, Some(record))) = first_page.read_cell(i) {
                            match record.columns[0].data() {
                                SerialValue::String(ref str) => {
                                    if str == "index" {
                                        match record.columns[3].data() {
                                            SerialValue::I8(num) => {
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
                            }
                        };
                    }
                }
                None => eprintln!("can not read first page"),
            }
        }
        query if query.to_lowercase().starts_with("select count(*)") => {
            let file_path = &args[1];
            let select_statement = Sql::from_str(query);

            let db = Database::read_file(file_path)?;
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
            let file_path = &args[1];
            let db = Database::read_file(file_path)?;
            let select_statement = Sql::from_str(query);

            if let Some(first_page) = db.pages.get(0) {
                for i in 0..first_page.btree_header.ncells() {
                    match first_page.read_cell(i)? {
                        (_, Some(record)) => {
                            let mut rowids = Vec::new();

                            match record.columns[0].data() {
                                SerialValue::String(str) => match str.as_str() {
                                    "index" => {
                                        let index_statement =
                                            Sql::from_str(&record.columns[4].data().display());
                                        if let SerialValue::I8(num) = record.columns[3].data() {
                                            let mut page_idxes: Vec<usize> =
                                                vec![*num as usize - 1];

                                            while let Some(page_idx) = page_idxes.pop() {
                                                if let Some(page) = db.pages.get(page_idx) {
                                                    let cell_len = page.cell_offsets.len();

                                                    for i in 0..cell_len {
                                                        // eprintln!("{:?}", page.page_type());

                                                        if let Some(page_num_left_child) =
                                                            page.cells[i].page_number_left_child
                                                        {
                                                            page_idxes
                                                                .push(page_num_left_child as usize);
                                                        }

                                                        if let Some(page_num_first_overflow) =
                                                            page.cells[i].page_number_first_overflow
                                                        {
                                                            page_idxes.push(
                                                                page_num_first_overflow as usize,
                                                            );
                                                        }

                                                        if let Some(row_id) = page.cells[i].rowid {
                                                            index_statement.print_row_id(
                                                                page.cells[i].record.clone(),
                                                                &select_statement,
                                                                &mut rowids,
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                },
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
                                                    let create_statement = Sql::from_str(
                                                        &record.columns[4].data().display(),
                                                    );

                                                    let fields = select_statement
                                                        .get_fields(&create_statement);

                                                    let mut page_idxes: Vec<usize> =
                                                        vec![*num as usize - 1];
                                                    let mut row_set = HashSet::new();
                                                    let mut rowid_set = HashSet::new();

                                                    while let Some(page_idx) = page_idxes.pop() {
                                                        if let Some(page) = db.pages.get(page_idx) {
                                                            let cell_len = page.cell_offsets.len();

                                                            if !select_statement
                                                                .selection
                                                                .is_empty()
                                                            {
                                                                for i in 0..cell_len {
                                                                    if let Some(
                                                                        page_num_left_child,
                                                                    ) = page.cells[i]
                                                                        .page_number_left_child
                                                                    {
                                                                        page_idxes.push(
                                                                            page_num_left_child
                                                                                as usize,
                                                                        );
                                                                    }

                                                                    if let Some(
                                                                        page_num_first_overflow,
                                                                    ) = page.cells[i]
                                                                        .page_number_first_overflow
                                                                    {
                                                                        page_idxes.push(
                                                                            page_num_first_overflow
                                                                                as usize,
                                                                        );
                                                                    }

                                                                    if let Some(record) =
                                                                        &page.cells[i].record
                                                                    {
                                                                        select_statement
                                                                            .print_rows(
                                                                                page,
                                                                                i,
                                                                                &fields,
                                                                                &mut row_set,
                                                                                &mut rowid_set,
                                                                            );
                                                                    }
                                                                }
                                                            } else {
                                                                for i in 0..cell_len {
                                                                    if let Ok((_, Some(record))) =
                                                                        page.read_cell(i as u16)
                                                                    {
                                                                        let mut values = Vec::new();

                                                                        for (
                                                                            field_idx,
                                                                            _field_name,
                                                                        ) in &fields
                                                                        {
                                                                            values.push(
                                                                                record.columns
                                                                                    [*field_idx]
                                                                                    .data()
                                                                                    .display(),
                                                                            );
                                                                        }
                                                                        println!(
                                                                            "{}",
                                                                            values.join("|")
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                        }
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
