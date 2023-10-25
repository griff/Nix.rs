use std::future::Future;
use std::io;
use std::mem::take;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use bstr::ByteSlice;
use bytes::Bytes;
use futures::{Sink, Stream, StreamExt, TryFutureExt};
use log::trace;
use nixrs::archive::NAREvent;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tvix_castore::blobservice::{BlobService, BlobWriter};
use tvix_castore::directoryservice::{DirectoryPutter, DirectoryService};
use tvix_castore::{proto, B3Digest};

pub async fn store_nar<S, U>(
    blob_service: Arc<dyn BlobService>,
    directory_service: Arc<dyn DirectoryService>,
    stream: S,
) -> Result<proto::Node, NARStoreError>
where
    S: Stream<Item = U> + Send,
    U: Into<Result<NAREvent, NARStoreError>>,
{
    let directory_putter = directory_service.put_multiple_start();
    let mut storer = NARStorer::new(blob_service, directory_putter);
    let event_s = stream.map(|item| item.into());
    event_s.forward(&mut storer).await?;
    Ok(storer.root_node())
}

#[derive(Error, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum NARStoreErrorKind {
    #[error("creating directory '{0:?}'")]
    CreateDirectory(PathBuf),
    #[error("writing file '{0:?}'")]
    WriteFile(PathBuf),
    #[error("invalid stream order")]
    InvalidStreamOrder,
    #[error("Unknown I/O error")]
    UnknownIOError,
    #[error("Join error")]
    JoinError,
}

#[derive(Error, Debug)]
#[error("{kind}")]
pub struct NARStoreError {
    kind: NARStoreErrorKind,
    #[source]
    source: Option<Box<dyn std::error::Error + Send>>,
}

impl NARStoreError {
    pub fn create_directory_error(path: PathBuf, err: tvix_castore::Error) -> Self {
        Self {
            kind: NARStoreErrorKind::CreateDirectory(path),
            source: Some(Box::new(err)),
        }
    }

    pub fn write_file_error(path: PathBuf, err: io::Error) -> Self {
        Self {
            kind: NARStoreErrorKind::WriteFile(path),
            source: Some(Box::new(err)),
        }
    }

    pub fn file_close_error(path: PathBuf, err: tvix_castore::Error) -> Self {
        Self {
            kind: NARStoreErrorKind::WriteFile(path),
            source: Some(Box::new(err)),
        }
    }

    pub fn invalid_stream_order() -> Self {
        NARStoreError {
            kind: NARStoreErrorKind::InvalidStreamOrder,
            source: None,
        }
    }

    pub fn join_error() -> Self {
        NARStoreError {
            kind: NARStoreErrorKind::JoinError,
            source: None,
        }
    }
}

impl From<io::Error> for NARStoreError {
    fn from(source: io::Error) -> Self {
        Self {
            kind: NARStoreErrorKind::UnknownIOError,
            source: Some(Box::new(source)),
        }
    }
}

type WorkingFut = dyn Future<Output = Result<Box<dyn DirectoryPutter>, NARStoreError>> + Send;
type WritingFut = dyn Future<Output = Result<(Box<dyn DirectoryPutter>, PathBuf, Box<dyn BlobWriter>), NARStoreError>>
    + Send;
type ClosingFut =
    dyn Future<Output = Result<(Box<dyn DirectoryPutter>, B3Digest), NARStoreError>> + Send;

#[derive(Default)]
enum State {
    Ready(Box<dyn DirectoryPutter>),
    Working(Pin<Box<WorkingFut>>),
    FileReady(Box<dyn DirectoryPutter>, PathBuf, Box<dyn BlobWriter>),
    FileWriting(Pin<Box<WritingFut>>),
    FileClosing(Pin<Box<ClosingFut>>),
    FileDone(Box<dyn DirectoryPutter>, B3Digest),
    #[default]
    Invalid,
}

impl State {
    pub fn is_ready(&self) -> bool {
        matches!(
            self,
            State::Ready(_) | State::FileReady(_, _, _) | Self::FileDone(_, _)
        )
    }

    pub fn take_putter(&mut self) -> Result<Box<dyn DirectoryPutter>, NARStoreError> {
        match take(self) {
            Self::Invalid => panic!("State is invalid"),
            Self::FileWriting(_) => panic!("State is writing"),
            Self::Working(_) => panic!("State is working"),
            Self::FileClosing(_) => panic!("State is closing"),
            Self::Ready(putter) => Ok(putter),
            Self::FileReady(_, _, _) => Err(NARStoreError::invalid_stream_order()),
            Self::FileDone(_, _) => Err(NARStoreError::invalid_stream_order()),
        }
    }

    pub fn take_digest(&mut self) -> Result<B3Digest, NARStoreError> {
        match take(self) {
            Self::Invalid => panic!("State is invalid"),
            Self::FileWriting(_) => panic!("State is writing"),
            Self::Working(_) => panic!("State is working"),
            Self::FileClosing(_) => panic!("State is closing"),
            Self::Ready(_) => panic!("State is ready"),
            Self::FileReady(_, _, _) => Err(NARStoreError::invalid_stream_order()),
            Self::FileDone(putter, digest) => {
                *self = Self::Ready(putter);
                Ok(digest)
            }
        }
    }
}

impl Future for State {
    type Output = Result<(), NARStoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this {
            State::Invalid => panic!("Polling invalid state"),
            State::Ready(_) => Poll::Ready(Ok(())),
            State::FileReady(_, _, _) => Poll::Ready(Ok(())),
            State::FileDone(_, _) => Poll::Ready(Ok(())),
            State::Working(f) => {
                let putter = ready!(f.as_mut().poll(cx))?;
                *this = State::Ready(putter);
                Poll::Ready(Ok(()))
            }
            State::FileWriting(f) => {
                let (putter, path, file) = ready!(f.as_mut().poll(cx))?;
                *this = State::FileReady(putter, path, file);
                Poll::Ready(Ok(()))
            }
            State::FileClosing(f) => {
                let (putter, digest) = ready!(f.as_mut().poll(cx))?;
                *this = State::FileDone(putter, digest);
                Poll::Ready(Ok(()))
            }
        }
    }
}

pub struct NARStorer {
    node: proto::Node,
    directories: Vec<proto::Directory>,
    directory_nodes: Vec<proto::DirectoryNode>,
    current_name: Option<Bytes>,
    current_path: PathBuf,
    blob_service: Arc<dyn BlobService>,
    writing: State,
}

impl NARStorer {
    pub fn new(
        blob_service: Arc<dyn BlobService>,
        directory_putter: Box<dyn DirectoryPutter>,
    ) -> NARStorer {
        NARStorer {
            blob_service,
            node: Default::default(),
            current_name: None,
            current_path: PathBuf::new(),
            directories: Vec::new(),
            directory_nodes: Vec::new(),
            writing: State::Ready(directory_putter),
        }
    }

    pub fn root_node(&self) -> proto::Node {
        self.node.clone()
    }
}

impl Sink<NAREvent> for NARStorer {
    type Error = NARStoreError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().writing).poll(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: NAREvent) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if !this.writing.is_ready() {
            panic!("Sending when not ready");
        }
        match item {
            NAREvent::Magic(_) => (),
            NAREvent::EndDirectory => {
                if let Some(value) = this.directories.pop() {
                    if let Some(proto::node::Node::Directory(dir_node)) = this.node.node.as_mut() {
                        dir_node.size = value.size();
                        dir_node.digest = value.digest().into();
                    } else {
                        return Err(NARStoreError::invalid_stream_order());
                    }
                    trace!("Write directory {:?}", value);
                    this.current_path.pop();
                    let current_path = this.current_path.clone();
                    let mut directory_putter = this.writing.take_putter()?;
                    let fut = async move {
                        directory_putter.put(value).await.map_err(|err| {
                            NARStoreError::create_directory_error(current_path, err)
                        })?;
                        Ok(directory_putter)
                    };
                    /*
                    let fut = this
                        .tokio_handle
                        .spawn_blocking(move || {
                            directory_putter
                                .put(value)
                                .map_err(|err| NARStoreError::create_directory_error(current_path, err))?;
                            Ok(directory_putter)
                        })
                        .map(unwind_join);
                    */
                    this.writing = State::Working(Box::pin(fut));
                } else {
                    return Err(NARStoreError::invalid_stream_order());
                }
            }
            NAREvent::EndDirectoryEntry => {
                if let Some(dir) = this.directories.last_mut() {
                    match this.node.node.take() {
                        Some(proto::node::Node::Directory(dir_node)) => {
                            dir.directories.push(dir_node);
                        }
                        Some(proto::node::Node::File(mut file_node)) => {
                            file_node.digest = this.writing.take_digest()?.into();
                            dir.files.push(file_node);
                        }
                        Some(proto::node::Node::Symlink(symlink_node)) => {
                            dir.symlinks.push(symlink_node)
                        }
                        None => {
                            return Err(NARStoreError::invalid_stream_order());
                        }
                    }
                } else {
                    return Err(NARStoreError::invalid_stream_order());
                }
                if let Some(dir_node) = this.directory_nodes.pop() {
                    this.node.node = Some(proto::node::Node::Directory(dir_node));
                }
            }
            NAREvent::DirectoryEntry { name } => {
                let name_os = name.to_os_str_lossy();
                this.current_path.push(&name_os);
                this.current_name = Some(name);
                if let Some(proto::node::Node::Directory(dir_node)) = this.node.node.take() {
                    this.directory_nodes.push(dir_node);
                } else {
                    return Err(NARStoreError::invalid_stream_order());
                }
            }
            NAREvent::Directory => {
                let mut entry = proto::DirectoryNode::default();
                if let Some(name) = this.current_name.take() {
                    entry.name = name;
                } else if !this.directories.is_empty() {
                    return Err(NARStoreError::invalid_stream_order());
                }
                this.node.node = Some(proto::node::Node::Directory(entry));
                let dir = proto::Directory::default();
                this.directories.push(dir);
            }
            NAREvent::SymlinkNode { target } => {
                let mut entry = proto::SymlinkNode {
                    target,
                    ..Default::default()
                };
                if let Some(name) = this.current_name.take() {
                    entry.name = name;
                } else if !this.directories.is_empty() {
                    return Err(NARStoreError::invalid_stream_order());
                }
                this.node.node = Some(proto::node::Node::Symlink(entry));
            }
            NAREvent::RegularNode {
                offset: _,
                executable,
                size,
            } => {
                let mut entry = proto::FileNode {
                    executable,
                    size: size as u32,
                    ..Default::default()
                };
                if let Some(name) = this.current_name.take() {
                    entry.name = name;
                } else if !this.directories.is_empty() {
                    return Err(NARStoreError::invalid_stream_order());
                }
                this.node.node = Some(proto::node::Node::File(entry));

                let putter = this.writing.take_putter()?;
                let blob_service = this.blob_service.clone();
                let current_path = this.current_path.clone();
                let fut =
                    async move { Ok((putter, current_path, blob_service.open_write().await)) };
                if size == 0 {
                    let fut = fut.and_then(move |(putter, path, mut file)| async move {
                        if let Err(err) = file.flush().await {
                            return Err(NARStoreError::write_file_error(path, err));
                        }
                        let digest = file
                            .close()
                            .await
                            .map_err(|err| NARStoreError::file_close_error(path, err))?;
                        Ok((putter, digest))
                    });
                    this.writing = State::FileClosing(Box::pin(fut));
                } else {
                    this.writing = State::FileWriting(Box::pin(fut));
                }
            }
            NAREvent::Contents { total, index, buf } => {
                if let State::FileReady(putter, path, mut file) = take(&mut this.writing) {
                    let last = index + buf.len() as u64 == total;
                    let fut = async move {
                        match file.write_all(&buf).await {
                            Ok(_) => Ok((putter, path, file)),
                            Err(err) => Err(NARStoreError::write_file_error(path, err)),
                        }
                    };
                    if last {
                        let fut = fut.and_then(move |(putter, path, mut file)| async move {
                            if let Err(err) = file.flush().await {
                                return Err(NARStoreError::write_file_error(path, err));
                            }
                            let digest = file
                                .close()
                                .await
                                .map_err(|err| NARStoreError::file_close_error(path, err))?;
                            Ok((putter, digest))
                        });
                        this.writing = State::FileClosing(Box::pin(fut));
                    } else {
                        this.writing = State::FileWriting(Box::pin(fut));
                    }
                } else {
                    let path = this.current_path.clone();
                    return Err(NARStoreError::write_file_error(
                        path,
                        io::Error::new(io::ErrorKind::NotConnected, "no open filed found"),
                    ));
                }
            }
        }
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_ready(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match Pin::new(&mut this.writing).poll(cx) {
            Poll::Ready(_) => {
                if let Some(proto::node::Node::File(file_node)) = this.node.node.as_mut() {
                    file_node.digest = this.writing.take_digest()?.into();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{stream::iter, StreamExt};

    use nixrs::archive::test_data as nixrs_test_data;
    use tokio::io::AsyncReadExt;
    use tvix_castore::{blobservice::MemoryBlobService, directoryservice::MemoryDirectoryService};

    use super::*;
    use crate::nar::test_data;

    macro_rules! assert_blob_data {
        ($blob_service:expr, $blob_data:expr) => {
            for (digest, contents) in $blob_data.into_iter() {
                if let Some(mut r) = $blob_service.open_read(&digest).await.unwrap() {
                    let mut actual_contents = Vec::new();
                    r.read_to_end(&mut actual_contents).await.unwrap();
                    assert_eq!(&contents, &actual_contents, "Digest {}", digest);
                } else {
                    panic!("Missing blob for digest {} expected {:?}", digest, contents);
                }
            }
        };
    }
    macro_rules! assert_directory_data {
        ($directory_service:expr, $dir_data:expr) => {
            let directory_service = $directory_service;
            for (digest, dir) in $dir_data.into_iter() {
                if let Some(actual) = directory_service.get(&digest).await.unwrap() {
                    assert_eq!(&dir, &actual, "Digest {}", digest);
                } else {
                    panic!("Missing directory for digest {} expected {:?}", digest, dir);
                }
            }
        };
    }

    #[tokio::test]
    async fn test_store_text_file() {
        let blob_service = Arc::new(MemoryBlobService::default());
        let directory_service = Arc::new(MemoryDirectoryService::default());
        let events = iter(nixrs_test_data::text_file().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
        let root_node = store_nar(blob_service.clone(), directory_service, events)
            .await
            .unwrap();

        let (expected, blob_data) = test_data::text_file();
        assert_eq!(root_node, expected);
        assert_blob_data!(blob_service, vec![blob_data]);
    }

    #[tokio::test]
    async fn test_store_exec_file() {
        let blob_service = Arc::new(MemoryBlobService::default());
        let directory_service = Arc::new(MemoryDirectoryService::default());
        let events = iter(nixrs_test_data::exec_file().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
        let root_node = store_nar(blob_service.clone(), directory_service, events)
            .await
            .unwrap();

        let (expected, blob_data) = test_data::exec_file();
        assert_eq!(root_node, expected);
        assert_blob_data!(blob_service, vec![blob_data]);
    }

    #[tokio::test]
    async fn test_store_empty_file() {
        let blob_service = Arc::new(MemoryBlobService::default());
        let directory_service = Arc::new(MemoryDirectoryService::default());
        let events = iter(nixrs_test_data::empty_file().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
        let root_node = store_nar(blob_service.clone(), directory_service, events)
            .await
            .unwrap();

        let (expected, blob_data) = test_data::empty_file();
        assert_eq!(root_node, expected);
        assert_blob_data!(blob_service, vec![blob_data]);
    }

    #[tokio::test]
    async fn empty_file_in_dir() {
        let blob_service = Arc::new(MemoryBlobService::default());
        let directory_service = Arc::new(MemoryDirectoryService::default());
        let events = iter(nixrs_test_data::empty_file_in_dir().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
        let root_node = store_nar(blob_service.clone(), directory_service.clone(), events)
            .await
            .unwrap();

        let (expected_root, dir_data, blob_data) = test_data::empty_file_in_dir();
        assert_blob_data!(blob_service, blob_data);
        assert_directory_data!(directory_service, dir_data);
        assert_eq!(root_node, expected_root);
    }

    #[tokio::test]
    async fn test_store_symlink() {
        let blob_service = Arc::new(MemoryBlobService::default());
        let directory_service = Arc::new(MemoryDirectoryService::default());
        let events = iter(nixrs_test_data::symlink().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
        let root_node = store_nar(blob_service, directory_service, events)
            .await
            .unwrap();

        let expected = proto::SymlinkNode {
            name: b"".to_vec().into(),
            target: b"../deep".to_vec().into(),
        };
        assert_eq!(
            root_node,
            proto::Node {
                node: Some(proto::node::Node::Symlink(expected))
            }
        );
    }

    #[tokio::test]
    async fn test_store_dir() {
        let blob_service = Arc::new(MemoryBlobService::default());
        let directory_service = Arc::new(MemoryDirectoryService::default());
        let events = iter(nixrs_test_data::dir_example().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARStoreError>);
        let root_node = store_nar(blob_service.clone(), directory_service.clone(), events)
            .await
            .unwrap();

        let (expected_root, dir_data, blob_data) = test_data::dir_example();
        assert_blob_data!(blob_service, blob_data);
        assert_directory_data!(directory_service, dir_data);
        assert_eq!(root_node, expected_root);
    }
}
