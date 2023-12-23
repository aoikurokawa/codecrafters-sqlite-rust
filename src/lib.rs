pub mod cell;
pub mod column;
pub mod database;
pub mod page;
pub mod record;

pub fn decode_varint(bytes: &[u8]) -> anyhow::Result<(i64, usize)> {
    if bytes.is_empty() || bytes.len() > 9 {
        return Err(anyhow::Error::msg(format!(
            "invalid varint: {:?}",
            &bytes[0..9]
        )));
    }

    let mut result = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in bytes.iter().take(8) {
        bytes_read += 1;

        if shift > 64 {
            return Err(anyhow::Error::msg(format!(
                "Varint too long, integer overflow"
            )));
        }

        // (byte & 0x7f):
        // to isolate the lower 7 bits of the byte, effectively removing the high-order bit (the
        // 8th bit) from the byte.
        result |= ((byte & 0x7f) as i64) << shift;
        shift += 7;

        // If the high-order bit of a byte is 0, it signifies the end of the varint, and the
        // function resturns the result
        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }
    }

    // If there are 9 bytes, the function ensures that the ninth byte does not have its high-order
    // bit set ( as per the specification)
    if bytes.len() == 9 {
        if let Some(&last_byte) = bytes.get(8) {
            if last_byte & 0x80 != 0 {
                return Err(anyhow::Error::msg(format!(
                    "invalid varint format: {:?}",
                    &bytes[0..9]
                )));
            }
            result |= (last_byte as i64) << shift;
            bytes_read += 1;
        }
    }

    Ok((result, bytes_read))
    // let mut value = 0;

    // for (i, byte) in bytes.iter().enumerate() {
    //     value = (value << (i * 7)) + (byte & 0b0111_1111) as i64;
    //     if byte & 0b1000_0000 == 0 || i > 9 {
    //         return Ok((value, i + 1usize));
    //     }
    // }

    // return Err(anyhow::Error::msg(format!(
    //     "invalid varint: {:?}",
    //     &bytes[0..9]
    // )));
}

#[cfg(test)]
mod tests {
    use crate::decode_varint;

    #[test]
    fn test_decode_varint() {
        let varint_bytes = vec![0x81, 0x01];

        let (val, _i) = decode_varint(&varint_bytes).unwrap();

        assert_eq!(val, 129);
    }
}
