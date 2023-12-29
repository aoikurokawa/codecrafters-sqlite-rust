use anyhow::Context;

use crate::{
    column::{Column, SerialType, SerialValue},
    decode_varint,
};

#[derive(Debug, Clone)]
pub struct Record {
    pub columns: Vec<Column>,
}

impl Record {
    pub fn new(data: &[u8]) -> anyhow::Result<Self> {
        let (header_length, hl_size) = decode_varint(&data[0..]).context("read record header")?;
        let header_length = header_length as usize;
        let mut header_index = hl_size;
        let mut data_index = header_length;

        let mut columns = Vec::new();
        while header_index < header_length {
            let (int, len) = decode_varint(&data[header_index..]).context("read serial type")?;
            header_index += len;

            let serial_type = SerialType::read(int)?;

            let value = match &serial_type {
                SerialType::Null => SerialValue::Null,
                SerialType::I8 => SerialValue::I8(i8::from_be_bytes([data[data_index]])),
                SerialType::I16 => {
                    SerialValue::I16(i16::from_be_bytes([data[data_index], data[data_index + 1]]))
                }
                SerialType::I24 => SerialValue::I24(i32::from_be_bytes([
                    0,
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                ])),
                SerialType::I32 => SerialValue::I32(i32::from_be_bytes([
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                ])),
                SerialType::I48 => SerialValue::I48(i64::from_be_bytes([
                    0,
                    0,
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                    data[data_index + 4],
                    data[data_index + 5],
                ])),
                SerialType::I64 => SerialValue::I64(i64::from_be_bytes([
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                    data[data_index + 4],
                    data[data_index + 5],
                    data[data_index + 6],
                    data[data_index + 7],
                ])),
                SerialType::Float64 => SerialValue::Float64(f64::from_be_bytes([
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                    data[data_index + 4],
                    data[data_index + 5],
                    data[data_index + 6],
                    data[data_index + 7],
                ])),
                SerialType::Zero => SerialValue::Zero,
                SerialType::One => SerialValue::One,
                SerialType::Blob(len) => {
                    SerialValue::Blob((data[data_index..data_index + len]).to_vec())
                }
                SerialType::String(len) => {
                    let val = String::from_utf8(data[data_index..(data_index + len)].to_vec())?;
                    SerialValue::String(val)
                }
            };

            data_index += serial_type.length();

            let column = Column::new(serial_type, value);
            columns.push(column);
        }

        Ok(Self { columns })
    }
}
