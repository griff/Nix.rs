
enum ReadState {
    ReadStringSize(u8),
    ReadString(usize),
    ReadContentSize(u8),
    ReadContent(u64),
}

enum State {
    ReadMagic,
    ReadOpen,
    ReadField,
    ReadFieldValue,
}

pin_project! {
    struct NarRead<R> {
        #[pin]
        src: R,
        level: u64,
        scratch_u64: [u8; 8],
        scratch_string: String,
        read_state: ReadState,
        state: State,
    }
}
