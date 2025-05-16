use std::fs::{create_dir, OpenOptions};
use std::future::Future;
use std::io::{self, BufRead as _, Write as _};
#[cfg(unix)]
use std::os::unix::fs::{symlink, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{ready, Poll};

use bstr::ByteSlice as _;
use derive_more::Display;
use futures::{Sink, Stream};
use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::io::AsyncBufRead;
use tokio::task::{spawn_blocking, JoinHandle};
use tokio_util::io::SyncIoBridge;
use tracing::trace;

use super::{CaseHackStream, NarEvent};

#[derive(Display, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum NarWriteOperation {
    #[display(fmt = "creating directory")]
    CreateDirectory,
    #[display(fmt = "creating symlink")]
    CreateSymlink,
    #[display(fmt = "creating file")]
    CreateFile,
    #[display(fmt = "path contains invalid UTF-8")]
    PathUTF8,
    #[display(fmt = "Could not join state")]
    JoinError,
}

#[derive(Error, Debug)]
#[error("{operation} {path}: {source}")]
pub struct NarWriteError {
    operation: NarWriteOperation,
    path: PathBuf,
    #[source]
    source: io::Error,
}

impl NarWriteError {
    pub fn new(operation: NarWriteOperation, path: PathBuf, source: io::Error) -> Self {
        Self {
            operation,
            path,
            source,
        }
    }
    pub fn path_utf8_error(path: PathBuf, err: bstr::Utf8Error) -> Self {
        Self::new(
            NarWriteOperation::PathUTF8,
            path,
            io::Error::new(io::ErrorKind::InvalidData, err),
        )
    }
    pub fn create_dir_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NarWriteOperation::CreateDirectory, path, err)
    }
    pub fn create_symlink_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NarWriteOperation::CreateSymlink, path, err)
    }
    pub fn create_file_error(path: PathBuf, err: io::Error) -> Self {
        Self::new(NarWriteOperation::CreateFile, path, err)
    }
}

pin_project! {
    pub struct NarRestorer {
        root: PathBuf,
        path: PathBuf,
        #[pin]
        state: Option<JoinHandle<Result<(), NarWriteError>>>,
    }
}

impl NarRestorer {
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        Self {
            root: path.clone(),
            path,
            state: None,
        }
    }
}

fn join_name(path: &Path, name: &[u8]) -> Result<PathBuf, NarWriteError> {
    if name.is_empty() {
        Ok(path.to_owned())
    } else {
        let name_os = name.to_os_str().map_err(|err| {
            let lossy = name.to_os_str_lossy();
            let path = path.join(lossy);
            NarWriteError::path_utf8_error(path, err)
        })?;
        Ok(path.join(name_os))
    }
}

impl<R> Sink<NarEvent<R>> for NarRestorer
where
    R: AsyncBufRead + Send + 'static,
{
    type Error = NarWriteError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        if let Some(state) = this.state.as_mut().as_pin_mut() {
            ready!(state.poll(cx)).map_err(|_| {
                NarWriteError::new(
                    NarWriteOperation::JoinError,
                    this.root.clone(),
                    io::Error::new(io::ErrorKind::Other, "background task failed"),
                )
            })??;
        }
        this.state.set(None);
        Poll::Ready(Ok(()))
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: NarEvent<R>,
    ) -> Result<(), Self::Error> {
        if self.state.is_some() {
            panic!("Sending when not ready!");
        }
        match item {
            NarEvent::File {
                name,
                executable,
                size: _,
                reader,
            } => {
                let path = join_name(&self.path, &name)?;
                let mut options = OpenOptions::new();
                options.write(true);
                options.create_new(true);
                #[cfg(unix)]
                {
                    if executable {
                        options.mode(0o777);
                    } else {
                        options.mode(0o666);
                    }
                }
                let handle = spawn_blocking(move || {
                    let reader = pin!(reader);
                    let mut reader = SyncIoBridge::new(reader);
                    let mut writer = options
                        .open(&path)
                        .map_err(|err| NarWriteError::create_file_error(path.clone(), err))?;
                    loop {
                        trace!("Writing to file {:?}", path);
                        let buf = reader
                            .fill_buf()
                            .map_err(|err| NarWriteError::create_file_error(path.clone(), err))?;
                        if buf.is_empty() {
                            break;
                        }
                        let amt = buf.len();
                        writer
                            .write_all(buf)
                            .map_err(|err| NarWriteError::create_file_error(path.clone(), err))?;
                        reader.consume(amt);
                    }
                    writer
                        .flush()
                        .map_err(|err| NarWriteError::create_file_error(path.clone(), err))?;
                    Ok(())
                });
                self.state = Some(handle);
            }
            NarEvent::Symlink { name, target } => {
                let path = join_name(&self.path, &name)?;
                let target_os = target
                    .to_os_str()
                    .map_err(|err| {
                        let lossy = target.to_os_str_lossy().into_owned();
                        let path = PathBuf::from(lossy);
                        NarWriteError::path_utf8_error(path, err)
                    })?
                    .to_owned();
                self.state = Some(spawn_blocking(move || {
                    #[cfg(unix)]
                    {
                        symlink(target_os, &path)
                            .map_err(|err| NarWriteError::create_symlink_error(path, err))
                    }
                }));
            }
            NarEvent::StartDirectory { name } => {
                let path = join_name(&self.path, &name)?;
                self.path = path;
                let path = self.path.clone();
                self.state = Some(spawn_blocking(|| {
                    let path = path;
                    create_dir(&path).map_err(|err| NarWriteError::create_dir_error(path, err))
                }));
            }
            NarEvent::EndDirectory => {
                self.path.pop();
            }
        }
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<NarEvent<R>>>::poll_ready(self, cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<NarEvent<R>>>::poll_ready(self, cx)
    }
}

pub struct RestoreOptions {
    use_case_hack: bool,
}

impl RestoreOptions {
    pub fn new() -> Self {
        #[cfg(target_os = "macos")]
        let use_case_hack = true;
        #[cfg(not(target_os = "macos"))]
        let use_case_hack = false;
        Self { use_case_hack }
    }

    pub fn use_case_hack(mut self, use_case_hack: bool) -> Self {
        self.use_case_hack = use_case_hack;
        self
    }

    pub async fn restore<S, U, R, P>(self, stream: S, path: P) -> Result<(), NarWriteError>
    where
        S: Stream<Item = U>,
        U: Into<Result<NarEvent<R>, NarWriteError>>,
        P: Into<PathBuf>,
        R: AsyncBufRead + Send + 'static,
    {
        use futures::stream::StreamExt as _;
        let restorer = NarRestorer::new(path);
        let event_s = stream.map(|item| item.into());
        if self.use_case_hack {
            let hack = CaseHackStream::new(event_s);
            hack.forward(restorer).await
        } else {
            event_s.forward(restorer).await
        }
    }
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn restore<S, U, R, P>(stream: S, path: P) -> Result<(), NarWriteError>
where
    S: Stream<Item = U>,
    U: Into<Result<NarEvent<R>, NarWriteError>>,
    P: Into<PathBuf>,
    R: AsyncBufRead + Send + 'static,
{
    RestoreOptions::new().restore(stream, path).await
}

#[cfg(test)]
mod unittests {
    use super::*;
    use crate::archive::{test_data, NarEvent};
    use futures::stream::{iter, StreamExt as _};
    use rstest::rstest;
    use tempfile::Builder;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case::text_file(test_data::text_file())]
    #[case::exec_file(test_data::exec_file())]
    #[case::empty_file(test_data::empty_file())]
    #[case::empty_file_in_dir(test_data::empty_file_in_dir())]
    #[case::empty_dir(test_data::empty_dir())]
    #[case::empty_dir_in_dir(test_data::empty_dir_in_dir())]
    #[case::symlink(test_data::symlink())]
    #[case::dir_example(test_data::dir_example())]
    async fn test_restore(#[case] events: test_data::TestNarEvents) {
        use futures::TryStreamExt as _;

        use crate::archive::dump;

        let dir = Builder::new().prefix("test_restore").tempdir().unwrap();
        let path = dir.path().join("output");

        let events_s = iter(events.clone().into_iter())
            .map(|e| Ok(e) as Result<test_data::TestNarEvent, NarWriteError>);
        restore(events_s, &path).await.unwrap();

        let s = dump(path)
            .and_then(NarEvent::read_file)
            .try_collect::<test_data::TestNarEvents>()
            .await
            .unwrap();
        assert_eq!(s, events);
    }
}

#[cfg(test)]
mod proptests {
    use futures::stream::iter;
    use futures::{StreamExt as _, TryStreamExt as _};
    use proptest::proptest;
    use tempfile::tempdir;

    use crate::archive::arbitrary::arb_nar_events;
    use crate::archive::{dump, restore, test_data, NarEvent, NarWriteError};
    use crate::pretty_prop_assert_eq;

    #[test]
    fn proptest_restore_dump() {
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        proptest!(|(events in arb_nar_events(8, 256, 10))| {
            r.block_on(async {
                let dir = tempdir()?;
                let path = dir.path().join("output");

                let event_s = iter(events.clone().into_iter())
                    .map(|e| Ok(e) as Result<test_data::TestNarEvent, NarWriteError> );
                restore(event_s, &path).await.unwrap();

                let s = dump(path)
                    .and_then(NarEvent::read_file)
                    .try_collect::<test_data::TestNarEvents>().await?;
                pretty_prop_assert_eq!(&s, &events);
                Ok(())
            })?;

        });
    }
}
