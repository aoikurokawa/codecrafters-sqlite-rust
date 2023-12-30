use std::collections::{HashMap, HashSet};

use sqlparser::{
    ast::{Expr, SelectItem, SetExpr, Statement, TableFactor, Value},
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{column::SerialValue, record::Record};

#[derive(Debug)]
pub struct Sql {
    pub index_name: Option<Vec<String>>,
    pub field_name: Vec<String>,
    pub selection: HashMap<String, String>,
    pub tbl_name: String,
    pub index_column: Option<Vec<String>>,
}

impl Sql {
    pub fn from_str(query: &str) -> Self {
        let dialect = GenericDialect {};
        let query = Parser::parse_sql(&dialect, query).expect("parse select statement");

        let mut index_name = None;
        let mut field_name = Vec::new();
        let mut tbl_name = String::new();
        let mut selection = HashMap::new();
        let mut index_column = None;

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
                Statement::CreateIndex {
                    name,
                    table_name,
                    columns,
                    ..
                } => {
                    if let Some(indexes) = name {
                        let names: Vec<String> =
                            indexes.0.iter().map(|index| index.value.clone()).collect();
                        index_name = Some(names);
                    }
                    tbl_name = table_name.0[0].value.to_string();

                    let mut idx_columns = Vec::new();
                    for column in columns.iter() {
                        if let Expr::Identifier(ident) = &column.expr {
                            idx_columns.push(ident.value.clone());
                        }
                    }
                    index_column = Some(idx_columns);
                }
                _ => todo!(),
            }
        }

        Self {
            index_name,
            field_name,
            selection,
            tbl_name,
            index_column,
        }
    }

    pub fn get_fields(&self, create_statement: &Sql) -> Vec<(usize, String)> {
        self.field_name
            .clone()
            .into_iter()
            .enumerate()
            .map(|(_i, select_field)| {
                let index = if let Some(index) = create_statement
                    .field_name
                    .iter()
                    .position(|x| x.as_str() == select_field.as_str())
                {
                    index
                } else {
                    0
                };

                (index, select_field)
            })
            .collect()
    }

    pub fn print_rows(
        &self,
        record: &Record,
        rowid: &Option<i64>,
        fields: &Vec<(usize, String)>,
        row_set: &mut HashSet<String>,
        _rowid_set: &mut HashSet<i64>,
    ) {
        let mut values = Vec::new();

        for (_key, value) in self.selection.iter() {
            for (column_i, column) in record.columns.iter().enumerate() {
                if column_i == 0 && *column.data() != SerialValue::Null {
                    break;
                }

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
                            format!("{}{}", rowid.unwrap(), rows.join("|"))
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

    pub fn print_rows_by_rowid(
        &self,
        record: &Record,
        rowid: &Option<i64>,
        fields: &Vec<(usize, String)>,
        row_set: &mut HashSet<String>,
        _rowid_set: &mut HashSet<i64>,
    ) {
        let mut values = Vec::new();

        for (column_i, column) in record.columns.iter().enumerate() {
            if column_i == 0 && *column.data() != SerialValue::Null {
                break;
            }

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
                format!("{}{}", rowid.unwrap(), rows.join("|"))
            } else {
                rows.join("|")
            };

            if !values.contains(&con_row) {
                values.push(con_row)
            }
        }

        if !values.is_empty() {
            // if rowid_set.insert(rowid) {
            row_set.insert(values.join("|"));
            // }
        }
    }

    pub fn print_row_id(
        &self,
        record: Option<Record>,
        select_statement: &Sql,
        rowids: &mut HashSet<i64>,
    ) {
        if let Some(record) = record {
            for (_key, value) in select_statement.selection.iter() {
                if let SerialValue::String(country) = record.columns[0].data() {
                    if value == country {
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
            }
        }
    }
}
