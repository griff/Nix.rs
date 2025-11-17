use crate::ByteString;

mod dumper;
mod parser;
pub(crate) mod radix_tree;
mod read_nar;
mod restorer;
mod writer;

pub use dumper::{DumpOptions, DumpedFile, NarDumper, dump};
pub use parser::{NarParser, parse_nar};
pub use read_nar::{NarBytesReader, NarReader};
pub use restorer::{NarRestorer, NarWriteError, RestoreOptions, restore};
pub use writer::NarWriter;

pub const CASE_HACK_SUFFIX: &str = "~nix~case~hack~";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub enum NarEvent<R> {
    File {
        name: ByteString,
        executable: bool,
        size: u64,
        reader: R,
    },
    Symlink {
        name: ByteString,
        target: ByteString,
    },
    StartDirectory {
        name: ByteString,
    },
    EndDirectory,
}
