use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::AtomicU64;

use async_stream::stream;
use futures::future::Either;
use futures::io::Cursor;
use futures::Stream;
use tokio::io::{copy_buf, AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tracing::{debug, info, trace};

use super::de::{NixDeserialize, NixRead as _, NixReader, NixReaderBuilder};
use super::logger::{
    DriveResult, FutureResult, ProcessStderr, ResultLog, ResultLogExt, ResultProcess,
};
use super::ser::{NixWrite, NixWriter, NixWriterBuilder};
use super::types::AddToStoreItem;
use super::wire::logger::RawLogMessage;
use super::wire::types::Operation;
use super::wire::types2::{BuildMode, DerivedPath};
use super::wire::{CLIENT_MAGIC, SERVER_MAGIC};
use super::{
    DaemonError, DaemonErrorKind, DaemonResult, DaemonResultExt as _, DaemonStore,
    HandshakeDaemonStore, LogMessage, ProtocolVersion, TrustLevel,
};
use crate::archive::NarReader;
use crate::daemon::wire::{write_add_multiple_to_store_stream, FramedWriter};
use crate::io::{AsyncBufReadCompat, BytesReader};
use crate::store_path::{StoreDir, StorePath, StorePathSet};

pub struct DaemonClientBuilder {
    store_dir: StoreDir,
    host: Option<String>,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    reader_builder: NixReaderBuilder,
    writer_builder: NixWriterBuilder,
}

impl Default for DaemonClientBuilder {
    fn default() -> Self {
        Self {
            store_dir: Default::default(),
            host: Default::default(),
            min_version: ProtocolVersion::min(),
            max_version: ProtocolVersion::max(),
            reader_builder: Default::default(),
            writer_builder: Default::default(),
        }
    }
}

impl DaemonClientBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_store_dir(mut self, store_dir: &StoreDir) -> Self {
        self.store_dir = store_dir.clone();
        self
    }

    pub fn set_host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    pub fn clear_host(mut self) -> Self {
        self.host = None;
        self
    }

    pub fn set_min_version<P: Into<ProtocolVersion>>(mut self, min_version: P) -> Self {
        let min_version = min_version.into();
        assert!(
            min_version >= ProtocolVersion::min(),
            "Only protocols later than {} are supported",
            ProtocolVersion::min()
        );
        self.min_version = min_version;
        self
    }

    pub fn set_max_version<P: Into<ProtocolVersion>>(mut self, max_version: P) -> Self {
        let max_version = max_version.into();
        assert!(
            max_version <= ProtocolVersion::max(),
            "Only protocols up to {} are supported",
            ProtocolVersion::max()
        );
        self.max_version = max_version;
        self
    }

    pub fn build<R, W>(self, reader: R, writer: W) -> DaemonHandshakeClient<R, W>
    where
        R: AsyncRead,
    {
        let reader = self
            .reader_builder
            .set_store_dir(&self.store_dir)
            .build_buffered(reader);
        let writer = self
            .writer_builder
            .set_store_dir(&self.store_dir)
            .build(writer);
        let host = self.host.unwrap_or("unknown".to_string());
        let min_version = self.min_version;
        let max_version = self.max_version;
        DaemonHandshakeClient {
            host,
            reader,
            writer,
            min_version,
            max_version,
        }
    }

    pub async fn build_unix<P>(
        self,
        path: P,
    ) -> DaemonResult<DaemonHandshakeClient<OwnedReadHalf, OwnedWriteHalf>>
    where
        P: AsRef<Path>,
    {
        let stream = UnixStream::connect(path).await?;
        let (reader, writer) = stream.into_split();
        Ok(self.build(reader, writer))
    }

    pub async fn build_daemon(
        self,
    ) -> DaemonResult<DaemonHandshakeClient<OwnedReadHalf, OwnedWriteHalf>> {
        self.build_unix("/nix/var/nix/daemon-socket/socket").await
    }

    pub fn connect<R, W>(
        self,
        reader: R,
        writer: W,
    ) -> impl ResultLog<Output = DaemonResult<DaemonClient<R, W>>>
    where
        R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
        W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
    {
        self.build(reader, writer).handshake()
    }

    pub fn connect_unix<P>(
        self,
        path: P,
    ) -> impl ResultLog<Output = DaemonResult<DaemonClient<OwnedReadHalf, OwnedWriteHalf>>>
    where
        P: AsRef<Path> + Send,
    {
        FutureResult::new(async move { Ok(self.build_unix(path).await?.handshake()) })
    }

    pub fn connect_daemon(
        self,
    ) -> impl ResultLog<Output = DaemonResult<DaemonClient<OwnedReadHalf, OwnedWriteHalf>>> {
        FutureResult::new(async move { Ok(self.build_daemon().await?.handshake()) })
    }
}

#[derive(Debug)]
pub struct DaemonHandshakeClient<R, W> {
    host: String,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    reader: NixReader<BytesReader<R>>,
    writer: NixWriter<W>,
}

impl<R, W> HandshakeDaemonStore for DaemonHandshakeClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    type Store = DaemonClient<R, W>;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        FutureResult::new(async move {
            let mut reader = self.reader;
            let mut writer = self.writer;
            let mut daemon_nix_version = None;
            let mut remote_trusts_us = TrustLevel::Unknown;

            // Send the magic greeting, check for the reply.
            writer
                .write_number(CLIENT_MAGIC)
                .await
                .with_field("clientMagic")?;
            writer.flush().await.with_field("clientMagic")?;

            let magic = reader.read_number().await.with_field("serverMagic")?;
            if magic != SERVER_MAGIC {
                return Err(DaemonErrorKind::WrongMagic(magic)).with_field("serverMagic");
            }

            let server_version: ProtocolVersion =
                reader.read_value().await.with_field("protocolVersion")?;
            let version = server_version.min(self.max_version);
            if version < self.min_version {
                return Err(DaemonErrorKind::UnsupportedVersion(version))
                    .with_field("protocolVersion");
            }
            writer
                .write_value(&version)
                .await
                .with_field("clientVersion")?;
            reader.set_version(version);
            writer.set_version(version);
            info!(
                ?version,
                ?server_version,
                "Client Version is {}, server version is {}",
                version,
                server_version
            );

            if version.minor() >= 14 {
                // Obsolete CPU Affinity
                writer.write_value(&false).await.with_field("sendCpu")?;
            }

            if version.minor() >= 11 {
                // Obsolete reserved space
                writer
                    .write_value(&false)
                    .await
                    .with_field("reserveSpace")?;
            }

            if version.minor() >= 33 {
                writer.flush().await?;
                let version = reader.read_value().await.with_field("nixVersion")?;
                info!(version, "Nix Version {:?}", version);
                daemon_nix_version = Some(version);
            }

            if version.minor() >= 35 {
                remote_trusts_us = reader.read_value().await.with_field("trusted")?;
            }

            writer.flush().await?;

            let host = self.host;

            Ok(ProcessStderr::new(reader)
                .result_fn(move |result, reader, _, _, _| async move {
                    result?;
                    let id = CLIENT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(DaemonClient {
                        id,
                        host,
                        reader,
                        writer,
                        daemon_nix_version,
                        remote_trusts_us,
                    })
                })
                .stream())
        })
    }
}

static CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct DaemonClient<R, W> {
    id: u64,
    host: String,
    reader: NixReader<BytesReader<R>>,
    writer: NixWriter<W>,
    daemon_nix_version: Option<String>,
    remote_trusts_us: TrustLevel,
}

impl DaemonClient<Cursor<Vec<u8>>, Cursor<Vec<u8>>> {
    pub fn builder() -> DaemonClientBuilder {
        DaemonClientBuilder::new()
    }
}

impl<R, W> DaemonClient<R, W> {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn daemon_nix_version(&self) -> Option<&str> {
        self.daemon_nix_version.as_deref()
    }
}

impl<R, W> DaemonClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    pub async fn connect(
        store_dir: &StoreDir,
        host: String,
        reader: R,
        writer: W,
    ) -> impl ResultLog<Output = DaemonResult<Self>> {
        DaemonClient::builder()
            .set_store_dir(store_dir)
            .set_host(host)
            .build(reader, writer)
            .handshake()
    }

    pub async fn close(&mut self) -> Result<(), DaemonError> {
        self.writer.shutdown().await?;
        Ok(())
    }

    fn process_stderr<T>(&mut self) -> impl ResultLog<Output = DaemonResult<T>> + '_
    where
        T: NixDeserialize + Send + 'static,
    {
        FutureResult::new(async {
            self.writer.flush().await?;
            Ok(ProcessStderr::new(&mut self.reader).stream())
        })
    }

    /*
    async fn process_stderr_source<SR>(&mut self, source: SR) -> Result<(), DaemonError>
    where
        SR: AsyncBufRead + fmt::Debug + Unpin + Send,
    {
        self.writer.flush().await?;
        ProcessStderr::new(
            &mut self.reader,
        )
        .with_source(&mut self.writer, source)
        .result()
        .await
    }
     */
}

impl<R, W> DaemonStore for DaemonClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    fn trust_level(&self) -> TrustLevel {
        self.remote_trusts_us
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a super::ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        FutureResult::new(async {
            self.writer.write_value(&Operation::SetOptions).await?;
            self.writer.write_value(options).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::SetOptions))
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + 'a {
        FutureResult::new(async {
            self.writer.write_value(&Operation::IsValidPath).await?;
            self.writer.write_value(path).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::IsValidPath))
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        FutureResult::new(async move {
            self.writer.write_value(&Operation::QueryValidPaths).await?;
            self.writer.write_value(paths).await?;
            if (27..).contains(&self.writer.version().minor()) {
                self.writer.write_value(&substitute).await?;
            }
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::QueryValidPaths))
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<super::UnkeyedValidPathInfo>>> + 'a {
        FutureResult::new(async {
            self.writer.write_value(&Operation::QueryPathInfo).await?;
            self.writer.write_value(path).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::QueryPathInfo))
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl Stream<Item = LogMessage>
           + Future<Output = Result<impl AsyncBufRead + 's, DaemonError>>
           + Send
           + 's {
        let reader = &mut self.reader;
        let writer = &mut self.writer;
        let (sender, receiver) = oneshot::channel();
        ResultProcess {
            stream: stream! {
                let w = async {
                    writer.write_value(&Operation::NarFromPath).await?;
                    writer.write_value(path).await?;
                    writer.flush().await?;
                    Ok(())
                }.await;
                let result;
                if w.is_err() {
                    result = w;
                } else {
                    loop {
                        let msg = reader.read_value::<RawLogMessage>().await;
                        match msg {
                            Ok(RawLogMessage::Next(msg)) => {
                                yield LogMessage::Next(msg);
                            }
                            Ok(RawLogMessage::Result(result)) => {
                                yield LogMessage::Result(result);
                            }
                            Ok(RawLogMessage::StartActivity(act)) => {
                                yield LogMessage::StartActivity(act);
                            }
                            Ok(RawLogMessage::StopActivity(act)) => {
                                yield LogMessage::StopActivity(act);
                            }
                            Ok(RawLogMessage::Read(_len)) => {
                            }
                            Ok(RawLogMessage::Write(_buf)) => {
                            }
                            Ok(RawLogMessage::Last) => {
                                result = Ok(());
                                break;
                            }
                            Ok(RawLogMessage::Error(err)) => {
                                result = Err(err.into());
                                break;
                            }
                            Err(err) => {
                                result = Err(DaemonError::from(err));
                                break;
                            }
                        }
                    }
                }
                let _ = sender.send((reader, result));
            },
            result: async {
                let (reader, result) = receiver.await.unwrap();
                match result {
                    Ok(_) => Ok(NarReader::new(AsyncBufReadCompat::new(reader))),
                    Err(err) => Err(err.fill_operation(Operation::NarFromPath)),
                }
            },
        }
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        FutureResult::new(async move {
            self.writer.write_value(&Operation::BuildPaths).await?;
            self.writer.write_value(&paths).await?;
            self.writer.write_value(&mode).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::BuildPaths))
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<super::wire::types2::KeyedBuildResult>>> + Send + 'a
    {
        FutureResult::new(async move {
            self.writer
                .write_value(&Operation::BuildPathsWithResults)
                .await?;
            self.writer.write_value(&drvs).await?;
            self.writer.write_value(&mode).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::BuildPathsWithResults))
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv_path: &'a crate::store_path::StorePath,
        drv: &'a super::wire::types2::BasicDerivation,
        build_mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<super::wire::types2::BuildResult>> + 'a {
        FutureResult::new(async move {
            self.writer.write_value(&Operation::BuildDerivation).await?;
            self.writer.write_value(drv_path).await?;
            self.writer.write_value(drv).await?;
            self.writer.write_value(&build_mode).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::BuildDerivation))
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<super::wire::types2::QueryMissingResult>> + 'a {
        FutureResult::new(async move {
            self.writer.write_value(&Operation::QueryMissing).await?;
            self.writer.write_value(&paths).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::QueryMissing))
    }

    fn add_to_store_nar<'s, 'r, 'i, AR>(
        &'s mut self,
        info: &'i super::wire::types2::ValidPathInfo,
        source: AR,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        AR: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        Box::pin(
            FutureResult::new(async move {
                self.writer.write_value(&Operation::AddToStoreNar).await?;
                self.writer.write_value(info).await?;
                self.writer.write_value(&repair).await?;
                self.writer.write_value(&dont_check_sigs).await?;
                if self.writer.version().minor() >= 23 {
                    Ok(Either::Left(Either::Left(Box::pin(DriveResult {
                        result: ProcessStderr::new(&mut self.reader).stream(),
                        driver: async {
                            let mut source = source;
                            let mut framed = FramedWriter::new(&mut self.writer);
                            trace!("client:add_to_store_nar:driver: copy_buf");
                            copy_buf(&mut source, &mut framed).await?;
                            framed.shutdown().await?;
                            trace!("client:add_to_store_nar:driver: flush");
                            self.writer.flush().await?;
                            trace!("client:add_to_store_nar:driver: done");
                            Ok(()) as DaemonResult<()>
                        },
                        driving: true,
                        drive_err: None,
                    }))))
                } else if self.writer.version().minor() >= 21 {
                    Ok(Either::Left(Either::Right(Box::pin(
                        ProcessStderr::new(&mut self.reader)
                            .with_source(&mut self.writer, source)
                            .stream(),
                    ))))
                } else {
                    Ok(Either::Right(Box::pin(DriveResult {
                        result: ProcessStderr::new(&mut self.reader).stream(),
                        driver: async {
                            let mut reader = NarReader::new(source);
                            copy_buf(&mut reader, &mut self.writer).await?;
                            self.writer.flush().await?;
                            Ok(()) as DaemonResult<()>
                        },
                        driving: true,
                        drive_err: None,
                    })))
                }
            })
            .map_err(|err| err.fill_operation(Operation::AddToStoreNar)),
        )
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, SR>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        S: Stream<Item = Result<AddToStoreItem<SR>, DaemonError>> + Send + 'i,
        SR: AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        FutureResult::new(async move {
            self.writer
                .write_value(&Operation::AddMultipleToStore)
                .await?;
            self.writer.write_value(&repair).await?;
            self.writer.write_value(&dont_check_sigs).await?;

            info!(self.id, "add_multiple_to_store {}", self.id);
            let driver = async {
                let id = self.id;
                let version = self.writer.version();
                let mut writer = NixWriter::builder()
                    .set_version(version)
                    .build(FramedWriter::new(&mut self.writer));

                debug!(id, "Write write stream");
                write_add_multiple_to_store_stream(&mut writer, stream).await?;
                debug!(id, "Write writer shutdown");
                writer.shutdown().await?;
                debug!(id, "Write self.writer flush");
                self.writer.flush().await?;
                debug!(id, "Write done");
                Ok(()) as DaemonResult<()>
            };
            Ok(DriveResult::new(
                ProcessStderr::new(&mut self.reader).stream(),
                driver,
            ))
        })
        .map_err(|err| err.fill_operation(Operation::AddMultipleToStore))
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + '_ {
        FutureResult::new(async move {
            self.writer
                .write_value(&Operation::QueryAllValidPaths)
                .await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::QueryMissing))
    }
}
