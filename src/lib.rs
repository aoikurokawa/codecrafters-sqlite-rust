pub mod cell;
pub mod column;
pub mod database;
pub mod page;
pub mod record;
pub mod sql;

pub fn decode_varint(bytes: &[u8]) -> anyhow::Result<(i64, usize)> {
    // if bytes.is_empty() || bytes.len() > 9 {
    //     return Err(anyhow::Error::msg(format!(
    //         "invalid varint: {:?}",
    //         &bytes[0..9]
    //     )));
    // }

    let mut result = 0;
    let mut bytes_read = 0;

    for (i, &byte) in bytes.iter().take(8).enumerate() {
        bytes_read += 1;

        let has_more = i != 8 && (byte & 0b10000000 != 0);

        if has_more {
            result = (result << 7) | ((byte & 0b01111111) as i64);
        } else {
            result = (result << 7) | (byte as i64);
            bytes_read = i + 1;
            break;
        }
    }

    Ok((result, bytes_read))
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
