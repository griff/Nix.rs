use crate::ByteString;

mod case_hack;
mod dumper;
mod parser;
pub(crate) mod radix_tree;
mod read_nar;
mod restorer;
pub mod test_data;
mod writer;

pub use case_hack::{CaseHackStream, UncaseHackStream, CASE_HACK_SUFFIX};
pub use dumper::{dump, DumpOptions, DumpedFile, NarDumper};
#[cfg(any(test, feature = "test"))]
pub use parser::read_nar;
pub use parser::{parse_nar, NarParser};
pub use read_nar::{NarBytesReader, NarReader};
pub use restorer::{restore, NarRestorer, NarWriteError, RestoreOptions};
#[cfg(any(test, feature = "test"))]
pub use writer::write_nar;
pub use writer::NarWriter;

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

impl<R> NarEvent<R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    #[cfg(any(test, feature = "test"))]
    pub async fn read_file(self) -> std::io::Result<test_data::TestNarEvent> {
        use tokio::io::AsyncReadExt as _;
        match self {
            NarEvent::StartDirectory { name } => Ok(NarEvent::StartDirectory { name }),
            NarEvent::EndDirectory => Ok(NarEvent::EndDirectory),
            NarEvent::Symlink { name, target } => Ok(NarEvent::Symlink { name, target }),
            NarEvent::File {
                name,
                executable,
                size,
                mut reader,
            } => {
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await?;
                let reader = std::io::Cursor::new(bytes::Bytes::from(buf));
                Ok(NarEvent::File {
                    name,
                    executable,
                    size,
                    reader,
                })
            }
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub mod arbitrary {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;

    use super::*;
    use ::proptest::prelude::*;
    use bytes::Bytes;

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

    #[macro_export]
    macro_rules! pretty_prop_assert_eq {
        ($left:expr , $right:expr,) => ({
            $crate::pretty_prop_assert_eq!($left, $right)
        });
        ($left:expr , $right:expr) => ({
            match (&($left), &($right)) {
                (left_val, right_val) => {
                    ::proptest::prop_assert!(*left_val == *right_val,
                        "assertion failed: `(left == right)`\
                              \n\
                              \n{}\
                              \n",
                              ::pretty_assertions::Comparison::new(left_val, right_val))
                }
            }
        });
        ($left:expr , $right:expr, $($arg:tt)*) => ({
            match (&($left), &($right)) {
                (left_val, right_val) => {
                    ::proptest::prop_assert!(*left_val == *right_val,
                        "assertion failed: `(left == right)`: {}\
                              \n\
                              \n{}\
                              \n",
                               format_args!($($arg)*),
                               ::pretty_assertions::Comparison::new(left_val, right_val))
                }
            }
        });
    }

    #[derive(Clone, Debug)]
    enum NarTree {
        File(bool, Vec<u8>),
        Symlink(String),
        Dir(BTreeMap<String, NarTree>),
    }

    impl NarTree {
        fn events(self, name: String, ls: &mut Vec<NarEvent<Cursor<Bytes>>>) {
            match self {
                NarTree::File(executable, contents) => {
                    let size = contents.len() as u64;
                    let e = NarEvent::File {
                        name: Bytes::from(name),
                        executable,
                        size,
                        reader: Cursor::new(Bytes::from(contents)),
                    };
                    ls.push(e);
                }
                NarTree::Symlink(target) => {
                    let e = NarEvent::Symlink {
                        name: Bytes::from(name),
                        target: Bytes::from(target),
                    };
                    ls.push(e);
                }
                NarTree::Dir(tree) => {
                    let e = NarEvent::StartDirectory {
                        name: Bytes::from(name),
                    };
                    ls.push(e);
                    for (name, node) in tree {
                        node.events(name, ls);
                    }
                    let e = NarEvent::EndDirectory;
                    ls.push(e);
                }
            }
        }

        fn into_events(self) -> Vec<NarEvent<Cursor<Bytes>>> {
            let mut ret = Vec::new();
            self.events(String::new(), &mut ret);
            ret
        }
    }

    fn arb_nar_tree(
        depth: u32,
        desired_size: u32,
        expected_branch_size: u32,
    ) -> impl Strategy<Value = NarTree> {
        let leaf = prop_oneof![
            (any::<bool>(), any::<Vec<u8>>()).prop_map(|(e, c)| NarTree::File(e, c)),
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
    ) -> impl Strategy<Value = Vec<super::NarEvent<Cursor<Bytes>>>> {
        arb_nar_tree(depth, desired_size, expected_branch_size).prop_map(|tree| tree.into_events())
    }

    pub fn arb_nar_contents(
        depth: u32,
        desired_size: u32,
        expected_branch_size: u32,
    ) -> impl Strategy<Value = Bytes> {
        arb_nar_events(depth, desired_size, expected_branch_size)
            .prop_map(|events| write_nar(events.iter()))
    }
}
