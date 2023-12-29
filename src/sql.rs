use std::collections::{HashMap, HashSet};

use sqlparser::{
    ast::{Expr, SelectItem, SetExpr, Statement, TableFactor, Value},
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{column::SerialValue, page::Page};

#[derive(Debug)]
pub struct Sql {
    pub field_name: Vec<String>,
    pub selection: HashMap<String, String>,
    pub tbl_name: String,
}

impl Sql {
    pub fn from_str(query: &str) -> Self {
        let dialect = GenericDialect {};
        let query = Parser::parse_sql(&dialect, query).expect("parse select statement");
        // let target_table = query.split(" ").last().expect("specify table name");

        let mut field_name = Vec::new();
        let mut tbl_name = String::new();
        let mut selection = HashMap::new();

        while field_name.is_empty() && tbl_name.is_empty() {
            match &query[0] {
                Statement::Query(select) => match *select.body.clone() {
                    SetExpr::Select(select) => {
                        for proj in select.projection {
                            match &proj {
                                SelectItem::UnnamedExpr(expr) => match expr {
                                    Expr::Identifier(ident) => {
                                        field_name.push(ident.value.to_string());
                                    }
                                    _ => {}
                                },
                                _ => todo!(),
                            }
                        }
                        if let Some(expr) = &select.selection {
                            let mut key = String::new();
                            let mut value = String::new();
                            match expr {
                                Expr::BinaryOp { left, op: _, right } => {
                                    if let Expr::Identifier(ident) = *left.clone() {
                                        key = ident.value;
                                    }
                                    if let Expr::Value(val) = *right.clone() {
                                        match val {
                                            Value::SingleQuotedString(txt) => {
                                                value = txt.to_string();
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                _ => {}
                            }

                            selection.insert(key, value);
                        }
                        match &select.from[0].relation {
                            TableFactor::Table {
                                name,
                                alias: _,
                                args: _,
                                with_hints: _,
                                version: _,
                                partitions: _,
                            } => {
                                tbl_name = name.0[0].value.to_string();
                            }
                            _ => {}
                        }
                    }
                    _ => todo!(),
                },
                Statement::CreateTable { name, columns, .. } => {
                    field_name = columns
                        .iter()
                        .map(|column_def| column_def.name.value.clone())
                        .collect();
                    tbl_name = name.0[0].value.to_string();
                }
                _ => todo!(),
            }
        }

        Self {
            field_name,
            selection,
            tbl_name,
        }
    }

    pub fn print_rows(
        &self,
        page: &Page,
        i: u16,
        fields: &Vec<(usize, String)>,
        row_set: &mut HashSet<String>,
        _rowid_set: &mut HashSet<i64>,
    ) {
        if let Ok(Some((rowid, record))) = page.read_cell(i) {
            let mut values = Vec::new();
            for (_key, value) in self.selection.iter() {
                for (_column_i, column) in record.columns.iter().enumerate() {
                    if let SerialValue::String(candidate_value) = column.data() {
                        if candidate_value == value {
                            let rows: Vec<String> = fields
                                .iter()
                                .map(|(i, _field)| {
                                    if *i == 0 {
                                        String::new()
                                    } else {
                                        record.columns[*i].data().display()
                                    }
                                })
                                .collect();

                            let con_row = if fields[0].0 == 0 {
                                format!("{rowid}{}", rows.join("|"))
                            } else {
                                rows.join("|")
                            };

                            if !values.contains(&con_row) {
                                values.push(con_row)
                            }
                            break;
                        }
                    }
                }
            }

            if !values.is_empty() {
                // if rowid_set.insert(rowid) {
                row_set.insert(values.join("|"));
                // }
            }
        }
    }
}
