pub const ZEROS: [u8; 8] = [0u8; 8];

pub const fn calc_aligned(len: u64) -> u64 {
    len.wrapping_add(7) & !7
}

pub const fn calc_padding(len: u64) -> usize {
    let aligned = calc_aligned(len);
    aligned.wrapping_sub(len) as usize
}
