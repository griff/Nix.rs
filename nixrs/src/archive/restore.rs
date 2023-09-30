use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bstr::ByteSlice;
use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use thiserror::Error;
use tokio::fs::create_dir;
use tokio::fs::symlink;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::ready;

use super::{CaseHackStream, NAREvent};

pub async fn restore<S, U, P>(stream: S, path: P) -> Result<(), NARWriteError>
where
    S: Stream<Item = U>,
    U: Into<Result<NAREvent, NARWriteError>>,
    P: Into<PathBuf>,
{
    let restorer = NARRestorer::new(path);
    let event_s = stream.map(|item| item.into());
    #[cfg(target_os = "macos")]
    {
        let hack = CaseHackStream::new(event_s);
        hack.forward(restorer).await
    }
    #[cfg(not(target_os = "macos"))]
    {
        event_s.forward(restorer).await
    }
}

#[derive(Error, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum NARWriteErrorKind {
    #[error("creating directory '{0:?}'")]
    CreateDirectory(PathBuf),
    #[error("creating symlink '{0:?}'")]
    CreateSymlink(PathBuf),
    #[error("creating file '{0:?}'")]
    CreateFile(PathBuf),
    #[error("writing file '{0:?}'")]
    WriteFile(PathBuf),
    #[error("path contains invalid UTF-8 '{0:?}'")]
    PathUTF8(PathBuf),
}

#[derive(Error, Debug)]
#[error("{kind}")]
pub struct NARWriteError {
    kind: NARWriteErrorKind,
    #[source]
    source: io::Error,
}

impl NARWriteError {
    pub fn new(kind: NARWriteErrorKind, source: io::Error) -> Self {
        NARWriteError { kind, source }
    }
    pub fn path_utf8_error(path: PathBuf, err: bstr::Utf8Error) -> Self {
        Self::new(
            NARWriteErrorKind::PathUTF8(path),
            io::Error::new(io::ErrorKind::InvalidData, err),
        )
    }
    pub fn create_dir_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NARWriteErrorKind::CreateDirectory(path), err)
    }
    pub fn create_symlink_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NARWriteErrorKind::CreateSymlink(path), err)
    }
    pub fn create_file_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NARWriteErrorKind::CreateFile(path), err)
    }
    pub fn write_file_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NARWriteErrorKind::WriteFile(path), err)
    }
}

enum State {
    Ready,
    Working(Pin<Box<dyn Future<Output = Result<(), NARWriteError>>>>),
    FileReady(PathBuf, File),
    FileWriting(Pin<Box<dyn Future<Output = Result<(PathBuf, File), NARWriteError>>>>),
    Invalid,
}

impl State {
    pub fn is_ready(&self) -> bool {
        match self {
            State::Ready | State::FileReady(_, _) => true,
            _ => false,
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, State::Invalid)
    }
}

impl Future for State {
    type Output = Result<(), NARWriteError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this {
            State::Invalid => panic!("Polling invalid state"),
            State::Ready => Poll::Ready(Ok(())),
            State::FileReady(_, _) => Poll::Ready(Ok(())),
            State::Working(f) => {
                ready!(f.as_mut().poll(cx))?;
                *this = State::Ready;
                Poll::Ready(Ok(()))
            }
            State::FileWriting(f) => {
                let (path, file) = ready!(f.as_mut().poll(cx))?;
                *this = State::FileReady(path, file);
                Poll::Ready(Ok(()))
            }
        }
    }
}

pub struct NARRestorer {
    path: PathBuf,
    writing: State,
}

impl NARRestorer {
    pub fn new<P: Into<PathBuf>>(path: P) -> NARRestorer {
        NARRestorer {
            path: path.into(),
            writing: State::Ready,
        }
    }
}

impl Sink<NAREvent> for NARRestorer {
    type Error = NARWriteError;

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
            NAREvent::EndDirectory => (),
            NAREvent::EndDirectoryEntry => {
                this.path.pop();
            }
            NAREvent::DirectoryEntry { name } => {
                let name_os = name.to_os_str().map_err(|err| {
                    let lossy = name.to_os_str_lossy();
                    let path = this.path.join(lossy);
                    NARWriteError::path_utf8_error(path, err)
                })?;
                this.path.push(name_os);
            }
            NAREvent::Directory => {
                let path = this.path.clone();
                this.writing = State::Working(Box::pin(async move {
                    if let Err(err) = create_dir(&path).await {
                        Err(NARWriteError::create_dir_error(path, err))
                    } else {
                        Ok(())
                    }
                }));
            }
            NAREvent::SymlinkNode { target } => {
                let target_os = target.to_os_str().map_err(|err| {
                    let lossy = target.to_os_str_lossy().into_owned();
                    let path = PathBuf::from(lossy);
                    NARWriteError::path_utf8_error(path, err)
                })?;
                let src = PathBuf::from(target_os);
                let path = this.path.clone();
                this.writing = State::Working(Box::pin(async move {
                    if let Err(err) = symlink(src, &path).await {
                        Err(NARWriteError::create_symlink_error(path, err))
                    } else {
                        Ok(())
                    }
                }));
            }
            NAREvent::RegularNode {
                offset: _,
                executable,
                size,
            } => {
                let path = this.path.clone();
                let fut = async move {
                    let mut options = OpenOptions::new();
                    options.write(true);
                    options.create_new(true);
                    #[cfg(unix)]
                    {
                        //options.custom_flags(libc::O_CLOEXEC);
                        if executable {
                            options.mode(0o777);
                        } else {
                            options.mode(0o666);
                        }
                    }
                    match options.open(&path).await {
                        Ok(file) => Ok((path, file)),
                        Err(err) => Err(NARWriteError::create_file_error(path, err)),
                    }
                };
                if size == 0 {
                    this.writing =
                        State::Working(Box::pin(fut.and_then(|(path, mut file)| async move {
                            file.shutdown()
                                .await
                                .map_err(|err| NARWriteError::create_file_error(path, err))
                        })))
                } else {
                    /*
                    let fut = fut.and_then(move |(path, file)| async move {
                        match file.set_len(size).await {
                            Ok(_) => Ok((path, file)),
                            Err(err) => {
                                Err(NARWriteError::create_file_error(path, err))
                            }
                        }
                    });
                     */
                    this.writing = State::FileWriting(Box::pin(fut));
                }
            }
            NAREvent::Contents { total, index, buf } => {
                if let State::FileReady(path, mut file) = this.writing.take() {
                    let last = index + buf.len() as u64 == total;
                    let fut = async move {
                        match file.write_all(&buf).await {
                            Ok(_) => Ok((path, file)),
                            Err(err) => Err(NARWriteError::write_file_error(path, err)),
                        }
                    };
                    if last {
                        let fut = fut.and_then(|(path, mut file)| async move {
                            if let Err(err) = file.sync_all().await {
                                return Err(NARWriteError::write_file_error(path, err));
                            }
                            file.shutdown()
                                .await
                                .map_err(|err| NARWriteError::write_file_error(path, err))
                        });
                        this.writing = State::Working(Box::pin(fut));
                    } else {
                        this.writing = State::FileWriting(Box::pin(fut));
                    }
                } else {
                    let path = this.path.clone();
                    return Err(NARWriteError::write_file_error(
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
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::iter;
    use futures::{StreamExt, TryStreamExt};
    use pretty_assertions::assert_eq;
    use proptest::proptest;
    use tempfile::{tempdir, Builder};

    use crate::archive::proptest::arb_nar_events;
    use crate::archive::{dump, test_data};
    use crate::pretty_prop_assert_eq;

    use super::*;

    #[tokio::test]
    async fn test_restore_dir() {
        let dir = Builder::new().prefix("test_restore_dir").tempdir().unwrap();
        let path = dir.path().join("output");

        let events = iter(test_data::dir_example().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARWriteError>);
        restore(events, &path).await.unwrap();

        let s = dump(path).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::dir_example());
    }

    #[tokio::test]
    async fn test_restore_text_file() {
        let dir = Builder::new()
            .prefix("test_restore_text_file")
            .tempdir()
            .unwrap();
        let path = dir.path().join("output");

        let events = iter(test_data::text_file().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARWriteError>);
        restore(events, &path).await.unwrap();

        let s = dump(path).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::text_file());
    }

    #[tokio::test]
    async fn test_restore_exec_file() {
        let dir = Builder::new()
            .prefix("test_restore_exec_file")
            .tempdir()
            .unwrap();
        let path = dir.path().join("output");

        let events = iter(test_data::exec_file().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARWriteError>);
        restore(events, &path).await.unwrap();

        let s = dump(path).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::exec_file());
    }

    #[tokio::test]
    async fn test_restore_empty_file() {
        let dir = Builder::new()
            .prefix("test_restore_empty_file")
            .tempdir()
            .unwrap();
        let path = dir.path().join("output");

        let events = iter(test_data::empty_file().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARWriteError>);
        restore(events, &path).await.unwrap();

        let s = dump(path).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::empty_file());
    }

    #[tokio::test]
    async fn test_restore_symlink() {
        let dir = Builder::new()
            .prefix("test_restore_symlink")
            .tempdir()
            .unwrap();
        let path = dir.path().join("output");

        let events = iter(test_data::symlink().into_iter())
            .map(|e| Ok(e) as Result<NAREvent, NARWriteError>);
        restore(events, &path).await.unwrap();

        let s = dump(path).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::symlink());
    }

    #[test]
    fn test_restore_dump() {
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        proptest!(|(events in arb_nar_events(8, 256, 10))| {
            r.block_on(async {
                let dir = tempdir()?;
                let path = dir.path().join("output");

                let event_s = iter(events.clone().into_iter())
                    .map(|e| Ok(e) as Result<NAREvent, NARWriteError> );
                restore(event_s, &path).await.unwrap();

                let s = dump(path)
                    .try_collect::<Vec<NAREvent>>().await?;
                pretty_prop_assert_eq!(&s, &events);
                Ok(())
            })?;

        });
    }
}
