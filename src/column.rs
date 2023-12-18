/// Each record consists of a key and optional data
pub struct Column {
    key: ColumnType,
    data: Vec<u8>,
}

pub enum ColumnType {}
