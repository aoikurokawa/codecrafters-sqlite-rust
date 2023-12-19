use anyhow::anyhow;

/// Each record consists of a key and optional data
pub struct Column {
    key: SerialType,
    data: Vec<u8>,
}

#[derive(Debug)]
pub enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    Float64,
    Zero,
    One,
    Blob(usize),
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
            Self::I48 => 5,
            Self::I64 => 6,
            Self::Float64 => 7,
            Self::Zero => 8,
            Self::One => 9,
            Self::Blob(len) => *len,
            Self::String(len) => *len,
        }
    }
}
