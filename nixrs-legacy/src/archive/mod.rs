use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::pin;
use tokio_util::codec::FramedWrite;

mod case_hack;
mod dump;
mod encoder;
mod parser;
mod restore;
#[cfg(any(test, feature = "test"))]
pub mod test_data;

pub use case_hack::CaseHackStream;
pub use dump::{dump, All, DumpOptions, Filter};
pub use encoder::NAREncoder;
pub use parser::parse_nar;
pub use restore::{restore, NARRestorer};

pub const NAR_VERSION_MAGIC_1: &str = "nix-archive-1";
pub const CASE_HACK_SUFFIX: &str = "~nix~case~hack~";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub enum NAREvent {
    Magic(Arc<String>),
    RegularNode {
        executable: bool,
        size: u64,
        offset: u64,
    },
    Contents {
        total: u64,
        index: u64,
        buf: Bytes,
    },
    SymlinkNode {
        target: Bytes,
    },
    Directory,
    DirectoryEntry {
        name: Bytes,
    },
    EndDirectoryEntry,
    EndDirectory,
}

pub async fn copy_nar<R, W>(source: R, writer: W) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let parser = parse_nar(source);
    pin!(parser);
    let mut framed = FramedWrite::new(writer, NAREncoder);
    framed.send_all(&mut parser).await
}

pub async fn read_nar<R>(source: R) -> io::Result<Vec<NAREvent>>
where
    R: AsyncRead + Unpin,
{
    parse_nar(source).try_collect().await
}

pub fn write_nar<'e, E>(events: E) -> Bytes
where
    E: IntoIterator<Item = &'e NAREvent>,
{
    let mut buf = BytesMut::new();
    for event in events.into_iter() {
        let encoded = event.encoded_size();
        buf.reserve(encoded);
        let mut temp = buf.split_off(buf.len());
        event.encode_into(&mut temp);
        buf.unsplit(temp);
    }
    buf.freeze()
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::*;
    use ::proptest::prelude::*;
    use bytes::BytesMut;

    pub fn arb_filename() -> impl Strategy<Value = String> {
        "[a-zA-Z 0-9.?=+]+".prop_filter("Not cur and parent dir", |s| s != "." && s != "..")
    }
    pub fn arb_file_component() -> impl Strategy<Value = String> {
        "[a-zA-Z 0-9.?=+]+"
    }
    prop_compose! {
        pub fn arb_path()(prefix in "[a-zA-Z 0-9.?=+][a-zA-Z 0-9.?=+/]{0,250}", last in arb_filename()) -> PathBuf
        {
            let mut ret = PathBuf::from(prefix);
            ret.push(last);
            ret
        }
    }

    #[derive(Clone, Debug)]
    enum NarTree {
        Regular(bool, Vec<u8>),
        Symlink(String),
        Dir(BTreeMap<String, NarTree>),
    }

    impl NarTree {
        fn events(self, mut offset: u64, ls: &mut Vec<NAREvent>) -> u64 {
            match self {
                NarTree::Regular(executable, contents) => {
                    let size = contents.len() as u64;
                    if size > 0 {
                        let e = NAREvent::RegularNode {
                            executable,
                            size,
                            offset,
                        };
                        offset += e.encoded_size() as u64;
                        let e = NAREvent::RegularNode {
                            executable,
                            size,
                            offset,
                        };
                        ls.push(e);
                        let e = NAREvent::Contents {
                            total: size,
                            index: 0,
                            buf: contents.into(),
                        };
                        offset += e.encoded_size() as u64;
                        ls.push(e)
                    } else {
                        let e = NAREvent::RegularNode {
                            executable,
                            size,
                            offset: 0,
                        };
                        offset += e.encoded_size() as u64;
                        ls.push(e);
                    }
                }
                NarTree::Symlink(target) => {
                    let e = NAREvent::SymlinkNode {
                        target: Bytes::from(target),
                    };
                    offset += e.encoded_size() as u64;
                    ls.push(e);
                }
                NarTree::Dir(tree) => {
                    let e = NAREvent::Directory;
                    offset += e.encoded_size() as u64;
                    ls.push(e);
                    for (name, node) in tree {
                        let e = NAREvent::DirectoryEntry {
                            name: Bytes::from(name),
                        };
                        offset += e.encoded_size() as u64;
                        ls.push(e);

                        offset = node.events(offset, ls);

                        let e = NAREvent::EndDirectoryEntry;
                        offset += e.encoded_size() as u64;
                        ls.push(e);
                    }
                    let e = NAREvent::EndDirectory;
                    offset += e.encoded_size() as u64;
                    ls.push(e);
                }
            }
            offset
        }

        fn into_events(self) -> Vec<NAREvent> {
            let mut ret = Vec::new();
            let e = NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.into()));
            let offset = e.encoded_size() as u64;
            ret.push(e);
            let _size = self.events(offset, &mut ret);
            ret
        }
    }

    fn arb_nar_tree(
        depth: u32,
        desired_size: u32,
        expected_branch_size: u32,
    ) -> impl Strategy<Value = NarTree> {
        let leaf = prop_oneof![
            (any::<bool>(), any::<Vec<u8>>()).prop_map(|(e, c)| NarTree::Regular(e, c)),
            arb_path().prop_map(|p| NarTree::Symlink(p.to_str().unwrap().to_owned())),
        ];
        leaf.prop_recursive(depth, desired_size, expected_branch_size, move |inner| {
            prop::collection::btree_map(arb_filename(), inner, 0..expected_branch_size as usize)
                .prop_map(NarTree::Dir)
        })
    }

    pub fn arb_nar_events(
        depth: u32,
        desired_size: u32,
        expected_branch_size: u32,
    ) -> impl Strategy<Value = Vec<super::NAREvent>> {
        arb_nar_tree(depth, desired_size, expected_branch_size).prop_map(|tree| tree.into_events())
    }

    pub fn arb_nar_contents(
        depth: u32,
        desired_size: u32,
        expected_branch_size: u32,
    ) -> impl Strategy<Value = (u64, ring::digest::Digest, Bytes)> {
        arb_nar_events(depth, desired_size, expected_branch_size).prop_map(|events| {
            let mut buf = BytesMut::new();
            let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
            let mut size = 0;
            for event in events {
                let encoded = event.encoded_size();
                size += encoded as u64;
                buf.reserve(encoded);
                let mut temp = buf.split_off(buf.len());
                event.encode_into(&mut temp);
                ctx.update(&temp);
                buf.unsplit(temp);
            }
            (size, ctx.finish(), buf.freeze())
        })
    }
}
