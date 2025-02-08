use std::fmt;
use std::path::Path;

use futures::io::Cursor;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;

use super::de::{NixDeserialize, NixRead as _, NixReader, NixReaderBuilder};
use super::logger::{FutureResult, LoggerResult, LoggerResultExt, ProcessStderr};
use super::ser::{NixWrite as _, NixWriter, NixWriterBuilder};
use super::wire::types::Operation;
use super::wire::{CLIENT_MAGIC, SERVER_MAGIC};
use super::{
    DaemonError, DaemonErrorKind, DaemonResult, DaemonResultExt as _, DaemonStore,
    HandshakeDaemonStore, ProtocolVersion, TrustLevel,
};
use crate::archive::copy_nar;
use crate::store_path::StoreDir;

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

    pub fn set_store_dir(&mut self, store_dir: &StoreDir) -> &mut Self {
        self.store_dir = store_dir.clone();
        self
    }

    pub fn set_host(&mut self, host: String) -> &mut Self {
        self.host = Some(host);
        self
    }

    pub fn clear_host(&mut self) -> &mut Self {
        self.host = None;
        self
    }

    pub fn set_min_version<P: Into<ProtocolVersion>>(&mut self, min_version: P) -> &mut Self {
        let min_version = min_version.into();
        assert!(
            min_version >= ProtocolVersion::min(),
            "Only protocols later than {} are supported",
            ProtocolVersion::min()
        );
        self.min_version = min_version;
        self
    }

    pub fn set_max_version<P: Into<ProtocolVersion>>(&mut self, max_version: P) -> &mut Self {
        let max_version = max_version.into();
        assert!(
            max_version <= ProtocolVersion::max(),
            "Only protocols up to {} are supported",
            ProtocolVersion::max()
        );
        self.max_version = max_version;
        self
    }

    pub fn build<R, W>(self, reader: R, writer: W) -> DaemonHandshakeClient<R, W> {
        let reader = self
            .reader_builder
            .set_store_dir(&self.store_dir)
            .build(reader);
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
    ) -> impl LoggerResult<DaemonClient<R, W>, DaemonError>
    where
        R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
        W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
    {
        self.build(reader, writer).handshake()
    }

    pub fn connect_unix<P>(
        self,
        path: P,
    ) -> impl LoggerResult<DaemonClient<OwnedReadHalf, OwnedWriteHalf>, DaemonError>
    where
        P: AsRef<Path> + Send,
    {
        FutureResult::new(async move { Ok(self.build_unix(path).await?.handshake()) })
    }

    pub fn connect_daemon(
        self,
    ) -> impl LoggerResult<DaemonClient<OwnedReadHalf, OwnedWriteHalf>, DaemonError> {
        FutureResult::new(async move { Ok(self.build_daemon().await?.handshake()) })
    }
}

#[derive(Debug)]
pub struct DaemonHandshakeClient<R, W> {
    host: String,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    reader: NixReader<R>,
    writer: NixWriter<W>,
}

impl<R, W> HandshakeDaemonStore for DaemonHandshakeClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    type Store = DaemonClient<R, W>;

    fn handshake(self) -> impl LoggerResult<Self::Store, DaemonError> {
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
            eprintln!(
                "Client Version is {}, server version is {}",
                version, server_version
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
                eprintln!("Nix Version {}", version);
                daemon_nix_version = Some(version);
            }

            if version.minor() >= 35 {
                remote_trusts_us = reader.read_value().await.with_field("trusted")?;
            }

            writer.flush().await?;

            let host = self.host;

            Ok(
                ProcessStderr::new(reader).result_fn(move |result, reader, _, _, _| async move {
                    result?;
                    Ok(DaemonClient {
                        host,
                        reader,
                        writer,
                        daemon_nix_version,
                        remote_trusts_us,
                    })
                }),
            )
        })
    }
}

#[derive(Debug)]
pub struct DaemonClient<R, W> {
    host: String,
    reader: NixReader<R>,
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
    //#[instrument(skip(store_dir, reader, writer))]
    pub async fn connect(
        store_dir: &StoreDir,
        host: String,
        reader: R,
        writer: W,
    ) -> impl LoggerResult<Self, DaemonError> {
        let mut b = DaemonClient::builder();
        b.set_store_dir(store_dir).set_host(host);
        b.build(reader, writer).handshake()
    }

    pub async fn close(&mut self) -> Result<(), DaemonError> {
        self.writer.shutdown().await?;
        Ok(())
    }

    fn process_stderr<'a, T>(&'a mut self) -> impl LoggerResult<T, DaemonError> + 'a
    where
        T: NixDeserialize + Send + 'static,
    {
        FutureResult::new(async {
            self.writer.flush().await?;
            Ok(ProcessStderr::new(&mut self.reader))
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
    ) -> impl LoggerResult<(), DaemonError> + 'a {
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
    ) -> impl LoggerResult<bool, DaemonError> + 'a {
        FutureResult::new(async {
            self.writer.write_value(&Operation::IsValidPath).await?;
            self.writer.write_value(path).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::IsValidPath))
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
        substitute: bool,
    ) -> impl LoggerResult<crate::store_path::StorePathSet, DaemonError> + 'a {
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
    ) -> impl LoggerResult<Option<super::UnkeyedValidPathInfo>, DaemonError> + 'a {
        FutureResult::new(async {
            self.writer.write_value(&Operation::QueryPathInfo).await?;
            self.writer.write_value(path).await?;
            Ok(self.process_stderr())
        })
        .map_err(|err| err.fill_operation(Operation::QueryPathInfo))
    }

    fn nar_from_path<'a, 'p, 'r, NW>(
        &'a mut self,
        path: &'p crate::store_path::StorePath,
        mut sink: NW,
    ) -> impl LoggerResult<(), DaemonError> + 'r
    where
        NW: AsyncWrite + Unpin + Send + 'r,
        'a: 'r,
        'p: 'r,
    {
        FutureResult::new(async {
            self.writer.write_value(&Operation::NarFromPath).await?;
            self.writer.write_value(path).await?;
            self.writer.flush().await?;
            Ok(ProcessStderr::new(&mut self.reader).result_fn(
                |result, reader, _, _, _| async move {
                    result?;
                    eprintln!("Copying NAR from client");
                    copy_nar(reader, &mut sink).await?;
                    sink.shutdown().await?;
                    eprintln!("Copied NAR from client");
                    Ok(())
                },
            ))
        })
        .map_err(|err| err.fill_operation(Operation::NarFromPath))
    }
}
