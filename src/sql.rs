use sqlparser::{
    ast::{Expr, SelectItem, SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};

pub struct Sql {
    pub field_name: Vec<String>,
    pub tbl_name: String,
}

impl Sql {
    pub fn from_str(query: &str) -> Self {
        let dialect = GenericDialect {};
        let query = Parser::parse_sql(&dialect, query).expect("parse select statement");
        // let target_table = query.split(" ").last().expect("specify table name");

        let mut field_name = Vec::new();
        let mut tbl_name = String::new();

        while field_name.is_empty() && tbl_name.is_empty() {
            match &query[0] {
                Statement::Query(select) => match *select.body.clone() {
                    SetExpr::Select(select) => {
                        match &select.projection[0] {
                            SelectItem::UnnamedExpr(expr) => match expr {
                                Expr::Identifier(ident) => {
                                    field_name.push(ident.value.to_string());
                                }
                                _ => {}
                            },
                            _ => todo!(),
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
                Statement::CreateTable {
                    or_replace,
                    temporary,
                    external,
                    global,
                    if_not_exists,
                    transient,
                    name,
                    columns,
                    constraints,
                    hive_distribution,
                    hive_formats,
                    table_properties,
                    with_options,
                    file_format,
                    location,
                    query,
                    without_rowid,
                    like,
                    clone,
                    engine,
                    comment,
                    auto_increment_offset,
                    default_charset,
                    collation,
                    on_commit,
                    on_cluster,
                    order_by,
                    strict,
                } => {
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
            tbl_name,
        }
    }
}
