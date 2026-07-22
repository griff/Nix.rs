mod padded_reader;
mod try_read_bytes_limited;
mod try_read_u64;

#[cfg_attr(
    not(any(feature = "archive", feature = "internal")),
    expect(unused_imports)
)]
pub use padded_reader::PaddedReader;
#[cfg_attr(
    not(any(feature = "daemon-serde", feature = "internal")),
    expect(unused_imports)
)]
pub use try_read_bytes_limited::TryReadBytesLimited;
pub use try_read_u64::TryReadU64;

pub const DEFAULT_BUF_SIZE: usize = 32 * 1024;
pub const RESERVED_BUF_SIZE: usize = DEFAULT_BUF_SIZE / 2;
pub const ZEROS: [u8; 8] = [0u8; 8];

pub const fn calc_aligned(len: u64) -> u64 {
    len.wrapping_add(7) & !7
}

pub const fn calc_padding(len: u64) -> usize {
    let aligned = calc_aligned(len);
    aligned.wrapping_sub(len) as usize
}
