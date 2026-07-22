pub const fn base64_len(len: usize) -> usize {
    ((4 * len / 3) + 3) & !3
}
