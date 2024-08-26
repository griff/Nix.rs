use std::collections::btree_map::IntoIter;
use std::collections::BTreeMap;
use std::fs::{FileType, Metadata};
use std::future::Future;
use std::io;
#[cfg(unix)]
use std::os::unix::prelude::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_stream::try_stream;
use bstr::{ByteSlice, ByteVec};
use bytes::{Bytes, BytesMut};
use futures::future::Ready;
use futures::Stream;
use tokio::fs::File;
use tokio::fs::{read_dir, read_link, symlink_metadata};
use tokio::io::AsyncReadExt;
use tracing::debug;
use tracing::trace;

use super::{NAREvent, CASE_HACK_SUFFIX, NAR_VERSION_MAGIC_1};

struct Item {
    path: PathBuf,
    file_type: FileType,
    metadata: Option<Metadata>,
}

enum Entry {
    Entry(Bytes, Item),
    Single(Item),
}

impl Entry {
    fn path(&self) -> &Path {
        match self {
            Entry::Entry(_, i) => &i.path,
            Entry::Single(i) => &i.path,
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Process {
    Done,
    Single(Item),
    Dir(IntoIter<Vec<u8>, Item>),
}

impl Iterator for Process {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::replace(self, Process::Done) {
            Process::Done => None,
            Process::Single(item) => {
                *self = Process::Done;
                Some(Entry::Single(item))
            }
            Process::Dir(mut it) => {
                let (name, item) = it.next()?;
                *self = Process::Dir(it);
                Some(Entry::Entry(Bytes::from(name), item))
            }
        }
    }
}

pub trait Filter {
    type Future: Future<Output = bool>;
    fn run(&self, path: &Path) -> Self::Future;
}

pub struct All;
impl Filter for All {
    type Future = Ready<bool>;
    fn run(&self, _path: &Path) -> Self::Future {
        futures::future::ready(true)
    }
}
impl<T, Fut> Filter for T
where
    T: Fn(&Path) -> Fut,
    Fut: Future<Output = bool>,
{
    type Future = Fut;
    fn run(&self, path: &Path) -> Self::Future {
        (self)(path)
    }
}

pub struct DumpOptions<F> {
    use_case_hack: bool,
    filter: F,
}

impl DumpOptions<All> {
    pub fn new() -> DumpOptions<All> {
        #[cfg(target_os = "macos")]
        let use_case_hack = true;
        #[cfg(not(target_os = "macos"))]
        let use_case_hack = false;
        DumpOptions {
            use_case_hack,
            filter: All,
        }
    }
}

impl Default for DumpOptions<All> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> DumpOptions<F> {
    pub fn filter<Fut>(&mut self, filter: F) -> &mut Self
    where
        F: Fn(&Path) -> Fut,
        Fut: Future<Output = bool>,
    {
        self.filter = filter;
        self
    }

    pub fn use_case_hack(mut self, use_case_hack: bool) -> Self {
        self.use_case_hack = use_case_hack;
        self
    }
}
impl<F> DumpOptions<F> {
    pub fn dump<Fut, P>(self, path: P) -> impl Stream<Item = io::Result<NAREvent>>
    where
        P: Into<PathBuf>,
        F: Filter<Future = Fut>,
        Fut: Future<Output = bool>,
    {
        dump_inner(path.into(), self)
    }
}

pub fn dump<P: Into<PathBuf>>(path: P) -> impl Stream<Item = io::Result<NAREvent>> {
    DumpOptions::new().dump(path)
}

fn dump_inner<F, Fut>(
    path: PathBuf,
    options: DumpOptions<F>,
) -> impl Stream<Item = io::Result<NAREvent>>
where
    F: Filter<Future = Fut>,
    Fut: Future<Output = bool>,
{
    try_stream! {
        let mut offset = 0;
        let first = NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned()));
        trace!("Magic {} {}", offset, first.encoded_size());
        offset += first.encoded_size() as u64;
        yield first;
        let metadata = symlink_metadata(&path).await?;
        let mut proc = Process::Single(Item{
            path,
            file_type: metadata.file_type(),
            metadata: Some(metadata),
        });

        let mut depth = 0;
        let mut proc_stack : Vec<Process> = Vec::new();
        let mut was_dir = false;
        let mut buf = BytesMut::with_capacity(65536);
        let cut_off = 65536 / 4;
        loop {
            while let Some(item) = proc.next() {
                if !options.filter.run(item.path()).await {
                    continue;
                }
                let (item, mut close) = match item {
                    Entry::Single(item) => (item, false),
                    Entry::Entry(name, item) => {
                        depth += 1;
                        let event = NAREvent::DirectoryEntry { name: name.clone() };
                        trace!("{}DirEntry {} {} {}", " ".repeat(depth), bstr::BStr::new(&name), offset, event.encoded_size());
                        offset += event.encoded_size() as u64;
                        yield event;
                        (item, true)
                    }
                };
                let path = item.path;
                let file_type = item.file_type;
                if file_type.is_symlink() {
                    let target_p = read_link(&path).await?;
                    let target = target_p.to_str().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData,
                            format!("Target for {:?} is not UTF-8 {:?}", path, target_p))
                    })?.to_owned().into();
                    let event = NAREvent::SymlinkNode { target };
                    trace!("{}Symlink {} {}", " ".repeat(depth), offset, event.encoded_size());
                    offset += event.encoded_size() as u64;
                    yield event;
                } else if file_type.is_file() {
                    let meta = if let Some(m) = item.metadata {
                        m
                    } else {
                        symlink_metadata(&path).await?
                    };
                    let size = meta.len();
                    let executable;
                    #[cfg(unix)]
                    {
                        let mode = meta.permissions().mode();
                        executable = mode & 0o100 == 0o100;
                    }
                    #[cfg(not(unix))]
                    {
                        executable = false;
                    }

                    if size == 0 {
                        let event = NAREvent::RegularNode { executable, size: 0, offset: 0 };
                        trace!("{}Regular {} {}", " ".repeat(depth), offset, event.encoded_size());
                        offset += event.encoded_size() as u64;
                        yield event;
                    } else {
                        let event = NAREvent::RegularNode { executable, size, offset };
                        trace!("{}Regular {} {}", " ".repeat(depth), offset, event.encoded_size());
                        offset += event.encoded_size() as u64;
                        let event = NAREvent::RegularNode { executable, size, offset };
                        yield event;

                        let mut index = 0;
                        let mut source = File::open(&path).await?;
                        while index < size {
                            if buf.capacity() as u64 > size - index {
                                drop(buf.split_off((size - index) as usize));
                            }
                            if buf.capacity() as u64 != size - index && buf.capacity() - buf.len() < cut_off {
                                buf.reserve(cut_off);
                            }
                            source.read_buf(&mut buf).await?;
                            let data = buf.split().freeze();
                            let new_index = index + data.len() as u64;
                            let event = NAREvent::Contents {
                                total: size,
                                index,
                                buf: data
                            };
                            trace!("{}Contents {} {}", " ".repeat(depth), offset, event.encoded_size());
                            offset += event.encoded_size() as u64;
                            yield event;
                            index = new_index;
                        }
                    }
                } else if file_type.is_dir() {
                    let parent_path = path;
                    was_dir = true;
                    let event = NAREvent::Directory;
                    trace!("{}Dir {:?} {} {}", " ".repeat(depth), parent_path, offset, event.encoded_size());
                    depth += 1;
                    offset += event.encoded_size() as u64;
                    yield event;

                    let mut unhacked = BTreeMap::new();
                    let mut rd = read_dir(&parent_path).await?;
                    while let Some(entry) = rd.next_entry().await? {
                        let mut name = Vec::from_os_string(entry.file_name())
                            .map_err(|s| io::Error::new(io::ErrorKind::Other, format!("filename {:?} not valid UTF-8", s) ))?;
                        let file_type = entry.file_type().await?;
                        let path = entry.path();
                        let item = Item {
                            path, file_type, metadata: None,
                        };
                        if options.use_case_hack {
                            if let Some(pos) = name.rfind(CASE_HACK_SUFFIX) {
                                debug!("removing case hack suffix from '{:?}'", entry.path());
                                name = name[..pos].to_owned();
                                if unhacked.contains_key(&name) {
                                    let name_s = String::from_utf8_lossy(&name);
                                    Err(io::Error::new(io::ErrorKind::Other,
                                        format!("file name collision in between '{:?}' and '{:?}'",
                                            parent_path.join(name_s.as_ref()),
                                            entry.path())))?;
                                    return;
                                }
                            }
                        }
                        unhacked.insert(name, item);
                    }
                    let next = Process::Dir(unhacked.into_iter());
                    if let Process::Done = proc {
                        proc = next;
                    } else {
                        proc_stack.push(std::mem::replace(&mut proc, next));
                    }
                    close = false;
                } else {
                    Err(io::Error::new(io::ErrorKind::Other,
                        format!("unsupported file type {:?}", file_type)))?;
                    return;
                }
                if close {
                    depth -= 1;
                    let event = NAREvent::EndDirectoryEntry;
                    trace!("{}End DirEntry closing {} {}", " ".repeat(depth), offset, event.encoded_size());
                    offset += event.encoded_size() as u64;
                    yield event;
                }
            }
            if was_dir {
                depth -= 1;
                let event =  NAREvent::EndDirectory;
                trace!("{}End Dir {} {}", " ".repeat(depth), offset, event.encoded_size());
                offset += event.encoded_size() as u64;
                yield event;
            }
            if let Some(p) = proc_stack.pop() {
                depth -= 1;
                let event = NAREvent::EndDirectoryEntry;
                trace!("{}DirEntry pop {} {}", " ".repeat(depth), offset, event.encoded_size());
                offset += event.encoded_size() as u64;
                yield event;

                proc = p;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::archive::test_data;

    use super::*;

    #[tokio::test]
    async fn test_dump_dir() {
        let s = DumpOptions::new()
            .use_case_hack(true)
            .dump("test-data/nar")
            .try_collect::<Vec<NAREvent>>()
            .await
            .unwrap();
        assert_eq!(s, test_data::dir_example());
    }

    #[tokio::test]
    async fn test_dump_text_file() {
        let s = dump("test-data/nar/testing.txt")
            .try_collect::<Vec<NAREvent>>()
            .await
            .unwrap();
        assert_eq!(s, test_data::text_file());
    }

    #[tokio::test]
    async fn test_dump_exec_file() {
        let s = dump("test-data/nar/dir/more/Deep~nix~case~hack~1")
            .try_collect::<Vec<NAREvent>>()
            .await
            .unwrap();
        assert_eq!(s, test_data::exec_file());
    }

    #[tokio::test]
    async fn test_dump_empty_file() {
        let s = dump("test-data/nar/dir/more/deep/empty.keep")
            .try_collect::<Vec<NAREvent>>()
            .await
            .unwrap();
        assert_eq!(s, test_data::empty_file());
    }

    #[tokio::test]
    async fn test_dump_symlink() {
        let s = dump("test-data/nar/dir/more/deep/loop")
            .try_collect::<Vec<NAREvent>>()
            .await
            .unwrap();
        assert_eq!(s, test_data::symlink());
    }
}
