#[cfg(any(feature = "internal", feature = "archive", feature = "daemon-serde"))]
#[cfg_attr(docsrs, doc(cfg(feature = "internal")))]
mod padded_reader;
#[cfg(any(feature = "internal", feature = "archive", feature = "daemon-serde"))]
#[cfg_attr(
    not(any(feature = "internal", feature = "archive")),
    expect(unused_imports)
)]
#[cfg_attr(docsrs, doc(cfg(feature = "internal")))]
pub use padded_reader::PaddedReader;

#[cfg(any(feature = "internal", feature = "archive", feature = "daemon-serde"))]
#[cfg_attr(docsrs, doc(cfg(feature = "internal")))]
pub const ZEROS: [u8; 8] = [0u8; 8];

pub const fn calc_aligned(len: u64) -> u64 {
    len.wrapping_add(7) & !7
}

#[cfg_attr(
    not(any(feature = "internal", feature = "archive", feature = "daemon-serde")),
    expect(dead_code)
)]
pub const fn calc_padding(len: u64) -> usize {
    let aligned = calc_aligned(len);
    aligned.wrapping_sub(len) as usize
}

pub const fn base64_len(len: usize) -> usize {
    ((4 * len / 3) + 3) & !3
}
