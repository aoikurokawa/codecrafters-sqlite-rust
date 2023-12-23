use anyhow::anyhow;

#[derive(Debug, Clone)]
pub enum SerialType {
    /// Value is a NULL
    Null,

    /// Value is an 8-bit twos-complement integer
    I8,

    /// Value is a big-endian 16-bit twos-complement integer
    I16,

    /// Value is a big-endian 24-bit twos-complement integer
    I24,

    /// Value is a big-endian 32-bit twos-complement integer
    I32,

    /// Value is a big-endian 48-bit twos-complement integer
    I48,

    /// Value is a big-endian 64-bit twos-complement integer
    I64,

    /// Value is a big-endian IEEE 754-2008 64-bit floating point number
    Float64,

    /// Value is the integer 0. (Only available for [schema format](https://www.sqlite.org/fileformat2.html#schemaformat) 4 and higher.)
    Zero,

    /// Value is the integer 1. (Only available for [schema format](https://www.sqlite.org/fileformat2.html#schemaformat) 4 and higher.)
    One,

    /// Value is a BLOB that is (N-12)/2 bytes in length
    Blob(usize),

    /// Value is a string in the [text encoding](https://www.sqlite.org/fileformat2.html#enc) and (N-13)/2 bytes in length. The nul terminator is not stored
    String(usize),
}

impl SerialType {
    pub fn read(npayload: i64) -> anyhow::Result<Self> {
        match npayload {
            0 => Ok(Self::Null),
            1 => Ok(Self::I8),
            2 => Ok(Self::I16),
            3 => Ok(Self::I24),
            4 => Ok(Self::I32),
            5 => Ok(Self::I48),
            6 => Ok(Self::I64),
            7 => Ok(Self::Float64),
            8 => Ok(Self::Zero),
            9 => Ok(Self::One),
            n if n >= 12 => {
                if n % 2 == 0 {
                    Ok(Self::Blob(((n - 12) / 2) as usize))
                } else {
                    Ok(Self::String(((n - 13) / 2) as usize))
                }
            }
            npayload => Err(anyhow!("invalid serial type: {}", npayload)),
        }
    }

    pub fn length(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::I8 => 1,
            Self::I16 => 2,
            Self::I24 => 3,
            Self::I32 => 4,
            Self::I48 => 6,
            Self::I64 => 8,
            Self::Float64 => 8,
            Self::Zero => 0,
            Self::One => 0,
            Self::Blob(len) => *len,
            Self::String(len) => *len,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SerialValue {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    Float64(f64),
    Zero,
    One,
    Blob(Vec<u8>),
    String(String),
}

/// Each record consists of a key and optional data
#[derive(Debug, Clone)]
pub struct Column {
    key: SerialType,
    data: SerialValue,
}

impl Column {
    pub fn new(key: SerialType, data: SerialValue) -> Self {
        Self { key, data }
    }

    pub fn key(&self) -> &SerialType {
        &self.key
    }

    pub fn data(&self) -> &SerialValue {
        &self.data
    }
}
