use std::io;
use std::sync::Arc;
use std::vec::IntoIter;

use async_stream::try_stream;
use bytes::BytesMut;
use futures::Stream;
use log::trace;
use nixrs_legacy::archive::{NAREvent, NAR_VERSION_MAGIC_1};
use tokio::io::AsyncReadExt;
use tvix_castore::blobservice::BlobService;
use tvix_castore::directoryservice::DirectoryService;
use tvix_castore::proto::{self, NamedNode};

#[derive(Debug)]
enum Process {
    DoneSingle,
    DoneDir,
    Single(proto::node::Node),
    Dir(IntoIter<proto::node::Node>),
}

impl Process {
    fn is_dir(&self) -> bool {
        matches!(self, Self::Dir(_) | Self::DoneDir)
    }
}

impl Iterator for Process {
    type Item = proto::node::Node;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::replace(self, Process::DoneSingle) {
            Process::DoneSingle => None,
            Process::Single(node) => {
                *self = Process::DoneSingle;
                Some(node)
            }
            Process::DoneDir => None,
            Process::Dir(mut it) => {
                if let Some(node) = it.next() {
                    *self = Process::Dir(it);
                    Some(node)
                } else {
                    *self = Self::DoneDir;
                    None
                }
            }
        }
    }
}

pub fn nar_source(
    blob_service: Arc<dyn BlobService>,
    directory_service: Arc<dyn DirectoryService>,
    node: proto::node::Node,
) -> impl Stream<Item = Result<NAREvent, io::Error>> {
    try_stream! {
        let mut offset = 0;
        let first = NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned()));
        trace!("Magic {} {}", offset, first.encoded_size());
        offset += first.encoded_size() as u64;
        yield first;
        //ret.push(first);

        let mut depth = 0;
        let mut buf = BytesMut::with_capacity(65536);
        let cut_off = 65536 / 4;
        let mut dir_stack = Vec::new();
        let mut cur_process = Process::Single(node);

        loop {
            while let Some(node) = cur_process.next() {
                match node {
                    proto::node::Node::Symlink(symlink_node) => {
                        if !dir_stack.is_empty() {
                            let name = symlink_node.name;
                            let event = NAREvent::DirectoryEntry { name: name.clone() };
                            trace!("{}DirEntry {} {} {}", " ".repeat(depth), bstr::BStr::new(&name), offset, event.encoded_size());
                            depth += 1;
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);
                        }
                        let target = symlink_node.target;
                        let event = NAREvent::SymlinkNode { target };
                        trace!("{}Symlink {} {}", " ".repeat(depth), offset, event.encoded_size());
                        offset += event.encoded_size() as u64;
                        yield event;
                        //ret.push(event);
                        if !dir_stack.is_empty() {
                            depth -= 1;
                            let event =  NAREvent::EndDirectoryEntry;
                            trace!("{}End DirEntry {} {}", " ".repeat(depth), offset, event.encoded_size());
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);
                        }
                    }
                    proto::node::Node::File(file_node) => {
                        if !dir_stack.is_empty() {
                            let name = file_node.name;
                            let event = NAREvent::DirectoryEntry { name: name.clone() };
                            trace!("{}DirEntry {} {} {}", " ".repeat(depth), bstr::BStr::new(&name), offset, event.encoded_size());
                            depth += 1;
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);
                        }
                        let executable = file_node.executable;
                        let size = file_node.size as u64;

                        if size == 0 {
                            let event = NAREvent::RegularNode { executable, size: 0, offset: 0 };
                            trace!("{}Regular {} {}", " ".repeat(depth), offset, event.encoded_size());
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);
                        } else {
                            let event = NAREvent::RegularNode { executable, size, offset };
                            trace!("{}Regular {} {}", " ".repeat(depth), offset, event.encoded_size());
                            offset += event.encoded_size() as u64;
                            let event = NAREvent::RegularNode { executable, size, offset };
                            yield event;
                            //ret.push(event);

                            let digest = file_node.digest.try_into()
                                .map_err(|err| {
                                    tvix_castore::Error::InvalidRequest(format!("Invalid digest {:?}", err))
                                })?;
                            let mut index = 0;
                            let blob_service = blob_service.clone();
                            let mut source = blob_service.open_read(&digest).await?
                                .ok_or_else( || {
                                    tvix_castore::Error::InvalidRequest(format!("Missing blob {}", digest))
                                })?;
                            while index < size {
                                if buf.capacity() as u64 > size - index {
                                    drop(buf.split_off((size - index) as usize));
                                }
                                if buf.capacity() as u64 != size - index && buf.capacity() - buf.len() < cut_off {
                                    buf.reserve(cut_off);
                                }
                                source.read_buf(&mut buf).await.map_err(|err| {
                                    tvix_castore::Error::StorageError(format!("IO Error {:?}", err))
                                })?;
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
                                //ret.push(event);
                                index = new_index;
                            }
                        }
                        if !dir_stack.is_empty() {
                            let event =  NAREvent::EndDirectoryEntry;
                            depth -= 1;
                            trace!("{}End DirEntry {} {}", " ".repeat(depth), offset, event.encoded_size());
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);
                        }
                    }
                    proto::node::Node::Directory(dir_node) => {
                        if !dir_stack.is_empty() {
                            let name = dir_node.name;
                            let event = NAREvent::DirectoryEntry { name: name.clone() };
                            trace!("{}DirEntry {} {} {}", " ".repeat(depth), bstr::BStr::new(&name), offset, event.encoded_size());
                            depth += 1;
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);
                        }
                        let digest = dir_node.digest.try_into()
                            .map_err(|err| {
                                tvix_castore::Error::InvalidRequest(format!("Invalid digest {:?}", err))
                            })?;
                        let directory_service = directory_service.clone();
                        let dir = directory_service.get(&digest).await?
                            .ok_or_else( || {
                                tvix_castore::Error::InvalidRequest(format!("Missing directory {}", digest))
                            })?;
                        let dir_map = dir.directories.into_iter().map(proto::node::Node::Directory);
                        let file_map = dir.files.into_iter().map(proto::node::Node::File);
                        let sym_map = dir.symlinks.into_iter().map(proto::node::Node::Symlink);
                        let mut nodes = dir_map.chain(file_map).chain(sym_map).collect::<Vec<_>>();
                        nodes.sort_by_key(|n| {
                            n.get_name().to_vec()
                        });
                        let event = NAREvent::Directory;
                        trace!("{}Dir {} {}", " ".repeat(depth), offset, event.encoded_size());
                        depth += 1;
                        offset += event.encoded_size() as u64;
                        yield event;
                        //ret.push(event);
                        if nodes.is_empty() {
                            let event =  NAREvent::EndDirectory;
                            depth -= 1;
                            trace!("{}End Dir {} {}", " ".repeat(depth), offset, event.encoded_size());
                            offset += event.encoded_size() as u64;
                            yield event;
                            //ret.push(event);

                            if !dir_stack.is_empty() {
                                let event = NAREvent::EndDirectoryEntry;
                                depth -= 1;
                                trace!("{}DirEntry {} {}", " ".repeat(depth), offset, event.encoded_size());
                                offset += event.encoded_size() as u64;
                                yield event;
                                //ret.push(event);
                            }
                        } else {
                            dir_stack.push(cur_process);
                            cur_process = Process::Dir(nodes.into_iter());
                        }
                    }
                }
            }
            if cur_process.is_dir() {
                let event =  NAREvent::EndDirectory;
                depth -= 1;
                trace!("{}End Dir {} {}", " ".repeat(depth), offset, event.encoded_size());
                offset += event.encoded_size() as u64;
                yield event;
                //ret.push(event);
            }
            if let Some(old) = dir_stack.pop() {
                if !dir_stack.is_empty() {
                    let event = NAREvent::EndDirectoryEntry;
                    depth -= 1;
                    trace!("{}DirEntry pop {} {}", " ".repeat(depth), offset, event.encoded_size());
                    offset += event.encoded_size() as u64;
                    yield event;
                    //ret.push(event);
                }
                cur_process = old;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{stream::iter, StreamExt, TryStreamExt};
    use nixrs_legacy::{
        archive::{proptest::arb_nar_events, test_data as nixrs_test_data},
        pretty_prop_assert_eq,
    };
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use tvix_castore::{blobservice::MemoryBlobService, directoryservice::MemoryDirectoryService};

    use super::*;
    use crate::nar::{store_nar, NARStoreError};

    macro_rules! test_source {
        ($events:expr) => {
            let blob_service = Arc::new(MemoryBlobService::default());
            let directory_service = Arc::new(MemoryDirectoryService::default());
            let events =
                iter($events.into_iter()).map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
            let root_node = store_nar(blob_service.clone(), directory_service.clone(), events)
                .await
                .unwrap()
                .node
                .unwrap();

            let s = nar_source(blob_service, directory_service, root_node)
                .try_collect::<Vec<NAREvent>>()
                .await
                .unwrap();
            assert_eq!(s, $events);
        };
    }

    #[tokio::test]
    async fn test_source_text_file() {
        test_source!(nixrs_test_data::text_file());
    }

    #[tokio::test]
    async fn test_source_exec_file() {
        test_source!(nixrs_test_data::exec_file());
    }

    #[tokio::test]
    async fn test_source_empty_file() {
        test_source!(nixrs_test_data::empty_file());
    }

    #[tokio::test]
    async fn test_source_empty_file_in_dir() {
        let _ = env_logger::builder().is_test(true).try_init();
        test_source!(nixrs_test_data::empty_file_in_dir());
    }

    #[tokio::test]
    async fn test_source_empty_dir() {
        let _ = env_logger::builder().is_test(true).try_init();
        test_source!(nixrs_test_data::empty_dir());
    }

    #[tokio::test]
    async fn test_source_empty_dir_in_dir() {
        let _ = env_logger::builder().is_test(true).try_init();
        test_source!(nixrs_test_data::empty_dir_in_dir());
    }

    #[tokio::test]
    async fn test_source_symlink() {
        test_source!(nixrs_test_data::symlink());
    }

    #[tokio::test]
    async fn test_source_dir_example() {
        test_source!(nixrs_test_data::dir_example());
    }

    #[test]
    fn proptest_store_source() {
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        proptest!(|(events in arb_nar_events(8, 256, 10))| {
            r.block_on(async {
                let blob_service = Arc::new(MemoryBlobService::default());
                let directory_service = Arc::new(MemoryDirectoryService::default());
                let events_s = iter(events.clone().into_iter())
                    .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
                let root_node = store_nar(blob_service.clone(), directory_service.clone(), events_s).await.unwrap().node.unwrap();

                let s = nar_source(blob_service, directory_service, root_node)
                    .try_collect::<Vec<NAREvent>>()
                    .await
                    .unwrap();
                pretty_prop_assert_eq!(&s, &events);
                Ok(())
            })?;

        });
    }
}
