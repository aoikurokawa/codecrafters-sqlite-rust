pub mod cell;
pub mod database;
pub mod page;

pub fn read_varint(bytes: &[u8]) -> (i64, usize) {
    let mut value = 0;

    for (i, byte) in bytes.iter().enumerate() {
        value = (value << (i * 7)) + (byte & 0b0111_1111) as i64;
        if byte & 0b1000_0000 == 0 || i > 9 {
            return (value, i + 1usize);
        }
    }

    panic!("invalid varint: {:?}", &bytes[0..9]);
}
