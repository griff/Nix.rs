use futures::future::TryFutureExt;
use tokio::io::{copy, simplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::try_join;

use crate::daemon::wire::logger::RawLogMessage;
use crate::daemon::wire::types::Operation;
use crate::daemon::wire::types2::{AddToStoreRequest, BaseStorePath};
use crate::daemon::{DaemonErrorKind, DaemonResultExt, PROTOCOL_VERSION};
use crate::store_path::StorePath;

use super::de::{NixRead, NixReader};
use super::logger::{LocalLoggerResult, LoggerResult};
use super::ser::{NixWrite, NixWriter};
use super::types::{LocalDaemonStore, LocalHandshakeDaemonStore};
use super::wire::types2::Request;
use super::wire::{CLIENT_MAGIC, SERVER_MAGIC};
use super::{
    DaemonError, DaemonResult, DaemonStore, HandshakeDaemonStore, ProtocolVersion, TrustLevel,
    NIX_VERSION,
};

pub struct RecoverableError {
    pub can_recover: bool,
    pub source: DaemonError,
}

trait RecoverExt<T> {
    fn recover(self) -> Result<T, RecoverableError>;
}

impl<T, E> RecoverExt<T> for Result<T, E>
where
    E: Into<DaemonError>,
{
    fn recover(self) -> Result<T, RecoverableError> {
        self.map_err(|source| RecoverableError {
            can_recover: true,
            source: source.into(),
        })
    }
}

impl<T> From<T> for RecoverableError
where
    T: Into<DaemonError>,
{
    fn from(source: T) -> Self {
        RecoverableError {
            can_recover: false,
            source: source.into(),
        }
    }
}

pub struct Builder {
    store_trust: TrustLevel,
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    nix_version: Option<String>,
}

impl Builder {
    pub fn new() -> Builder {
        Default::default()
    }

    pub async fn serve_connection<'s, R, W, S>(
        &'s self,
        reader: R,
        writer: W,
        store: S,
    ) -> DaemonResult<()>
    where
        R: AsyncRead + Send + Unpin + 's,
        W: AsyncWrite + Send + Unpin + 's,
        S: HandshakeDaemonStore + Send + 's,
    {
        let reader = NixReader::new(reader);
        let writer = NixWriter::new(writer);
        let mut conn = DaemonConnection {
            store_trust: self.store_trust,
            reader,
            writer,
        };
        let nix_version = self.nix_version.as_deref().unwrap_or(NIX_VERSION);
        conn.handshake(self.min_version, self.max_version, nix_version)
            .await?;
        eprintln!("Server handshake done!");
        let store_result = store.handshake();
        let store = conn
            .process_logs(store_result)
            .await
            .map_err(|e| e.source)?;
        conn.writer.flush().await?;
        eprintln!("Server handshake logs done!");
        conn.process_requests(store).await?;
        eprintln!("Server processed all requests!");
        Ok(())
    }

    pub async fn local_serve_connection<'s, R, W, S>(
        &'s self,
        reader: R,
        writer: W,
        store: S,
    ) -> DaemonResult<()>
    where
        R: AsyncRead + Send + Unpin + 's,
        W: AsyncWrite + Send + Unpin + 's,
        S: LocalHandshakeDaemonStore + Send + 's,
    {
        let reader = NixReader::new(reader);
        let writer = NixWriter::new(writer);
        let mut conn = DaemonConnection {
            store_trust: self.store_trust,
            reader,
            writer,
        };
        let nix_version = self.nix_version.as_deref().unwrap_or(NIX_VERSION);
        conn.handshake(self.min_version, self.max_version, nix_version)
            .await?;
        eprintln!("Server handshake done!");
        let store_result = store.handshake();
        let store = conn
            .local_process_logs(store_result)
            .await
            .map_err(|e| e.source)?;
        conn.writer.flush().await?;
        eprintln!("Server handshake logs done!");
        conn.local_process_requests(store).await?;
        eprintln!("Server processed all requests!");
        Ok(())
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            store_trust: TrustLevel::NotTrusted,
            min_version: ProtocolVersion::min(),
            max_version: ProtocolVersion::max(),
            nix_version: None,
        }
    }
}

pub struct DaemonConnection<R, W> {
    store_trust: TrustLevel,
    reader: NixReader<R>,
    writer: NixWriter<W>,
}

impl<R, W> DaemonConnection<R, W>
where
    R: AsyncReadExt + Send + Unpin,
    W: AsyncWriteExt + Send + Unpin,
{
    pub async fn handshake<'s>(
        &'s mut self,
        min_version: ProtocolVersion,
        max_version: ProtocolVersion,
        nix_version: &'s str,
    ) -> Result<ProtocolVersion, DaemonError> {
        assert!(
            min_version.major() == 1 && min_version.minor() >= 21,
            "Only Nix 2.3 and later is supported"
        );
        assert!(
            max_version <= PROTOCOL_VERSION,
            "Only protocols up to {} is supported",
            PROTOCOL_VERSION
        );

        let client_magic = self.reader.read_number().await.with_field("clientMagic")?;
        if client_magic != CLIENT_MAGIC {
            return Err(DaemonErrorKind::WrongMagic(client_magic)).with_field("clientMagic");
        }

        self.writer
            .write_number(SERVER_MAGIC)
            .await
            .with_field("serverMagic")?;
        self.writer
            .write_value(&max_version)
            .await
            .with_field("protocolVersion")?;
        self.writer.flush().await?;

        let client_version: ProtocolVersion =
            self.reader.read_value().await.with_field("clientVersion")?;
        let version = client_version.min(max_version);
        if version < min_version {
            return Err(DaemonErrorKind::UnsupportedVersion(version)).with_field("clientVersion");
        }
        self.reader.set_version(version);
        self.writer.set_version(version);
        eprintln!(
            "Server Version is {}, Client version is {}",
            version, client_version
        );

        if version.minor() >= 14 {
            // Obsolete CPU Affinity
            if self.reader.read_value().await.with_field("sendCpu")? {
                let _cpu_affinity = self.reader.read_number().await.with_field("cpuAffinity")?;
            }
        }

        if version.minor() >= 11 {
            // Obsolete reserved space
            let _reserve_space: bool = self.reader.read_value().await.with_field("reserveSpace")?;
        }

        if version.minor() >= 33 {
            self.writer
                .write_value(nix_version)
                .await
                .with_field("nixVersion")?;
        }

        if version.minor() >= 35 {
            self.writer
                .write_value(&self.store_trust)
                .await
                .with_field("trusted")?;
        }

        self.writer.flush().await?;
        Ok(version)
    }

    pub async fn process_logs<'s, T: Send + 's>(
        &'s mut self,
        mut logs: impl LoggerResult<T, DaemonError> + 's,
    ) -> Result<T, RecoverableError> {
        while let Some(msg) = logs.next().await {
            self.writer.write_value(&msg.recover()?).await?;
        }
        // TODO: Test this recover
        let value = logs.result().await.recover()?;
        self.writer.write_value(&RawLogMessage::Last).await?;
        Ok(value)
    }

    pub async fn process_requests<'s, S>(&'s mut self, mut store: S) -> Result<(), DaemonError>
    where
        S: DaemonStore + 's,
    {
        while let Some(request) = self.reader.try_read_value::<Request>().await? {
            let op = request.operation();
            eprintln!("Server got operation {}", op);
            if let Err(mut err) = self.process_request(&mut store, request).await {
                err.source = err.source.fill_operation(op);
                if err.can_recover {
                    self.writer
                        .write_value(&RawLogMessage::Error(err.source.into()))
                        .await?;
                } else {
                    return Err(err.source);
                }
            }
            eprintln!("Server flush");
            self.writer.flush().await?;
        }
        eprintln!("Server handled all requests");
        Ok(())
    }

    fn store_nar_from_path<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        path: &'p StorePath,
        sink: NW,
    ) -> impl LoggerResult<(), DaemonError> + 'r
    where
        S: DaemonStore + 's,
        NW: AsyncWrite + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.nar_from_path(path, sink)
    }

    async fn nar_from_path<'s, 't, S>(
        &'s mut self,
        store: &'t mut S,
        path: StorePath,
    ) -> Result<(), RecoverableError>
    where
        S: DaemonStore + 't,
    {
        // FUTUREWORK: Fix that this whole implementation allocates 2 buffers

        let (mut reader, sink) = simplex(10_000);
        let mut logs = Self::store_nar_from_path(store, &path, sink);

        while let Some(msg) = logs.next().await {
            self.writer.write_value(&msg.recover()?).await?;
        }

        self.writer.write_value(&RawLogMessage::Last).await?;
        try_join!(
            async move {
                eprintln!("Copying NAR from server");
                let ret = copy(&mut reader, &mut self.writer)
                    .map_err(DaemonError::from)
                    .await;
                eprintln!("Copied {:?} NAR from server", ret);
                ret
            },
            logs.result().map_err(DaemonError::from)
        )?;
        Ok(())
    }

    pub async fn process_request<'s, S>(
        &'s mut self,
        mut store: S,
        request: Request,
    ) -> Result<(), RecoverableError>
    where
        S: DaemonStore + 's,
    {
        use Request::*;
        let op = request.operation();
        match request {
            SetOptions(options) => {
                let logs = store.set_options(&options);
                self.process_logs(logs).await?;
            }
            IsValidPath(path) => {
                let logs = store.is_valid_path(&path);
                let value = self.process_logs(logs).await?;
                self.writer.write_value(&value).await?;
            }
            QueryValidPaths(req) => {
                let logs = store.query_valid_paths(&req.paths, req.substitute);
                let value = self.process_logs(logs).await?;
                self.writer.write_value(&value).await?;
            }
            QueryPathInfo(path) => {
                let logs = store.query_path_info(&path);
                let value = self.process_logs(logs).await?;
                self.writer.write_value(&value).await?;
            }
            NarFromPath(path) => {
                self.nar_from_path(&mut store, path).await?;
            }
            QueryReferrers(_path) => {
                /*
                ### Outputs
                referrers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryReferrers,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddToStore(req) => {
                match req {
                    AddToStoreRequest::Protocol25(_post25_req) => {
                        /*
                        #### Inputs
                        - name :: [StorePathName][se-StorePathName]
                        - camStr :: [ContentAddressMethodWithAlgo][se-ContentAddressMethodWithAlgo]
                        - refs :: [Set][se-Set] of [StorePath][se-StorePath]
                        - repairBool :: [Bool64][se-Bool64]
                        - [Framed][se-Framed] NAR dump

                        #### Outputs
                        info :: [ValidPathInfo][se-ValidPathInfo]
                         */
                    }
                    AddToStoreRequest::ProtocolPre25(_pre25_req) => {
                        /*
                        #### Inputs
                        - baseName :: [StorePathName][se-StorePathName]
                        - fixed :: [Bool64][se-Bool64]
                        - recursive :: [FileIngestionMethod][se-FileIngestionMethod]
                        - hashAlgo :: [HashAlgorithm][se-HashAlgorithm]
                        - NAR dump

                        If fixed is `true`, hashAlgo is forced to `sha256` and recursive is forced to
                        `NixArchive`.

                        Only `Flat` and `NixArchive` values are supported for the recursive input
                        parameter.

                        #### Outputs
                        path :: [StorePath][se-StorePath]
                         */
                    }
                }
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddToStore,
                ))
                .with_operation(op)?;
            }
            BuildPaths(_req) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::BuildPaths,
                ))
                .with_operation(op)
                .recover()?;
            }
            EnsurePath(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::EnsurePath,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddTempRoot(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddTempRoot,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddIndirectRoot(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddIndirectRoot,
                ))
                .with_operation(op)
                .recover()?;
            }
            FindRoots => {
                /*
                ### Outputs
                roots :: [Map][se-Map] of [Path][se-Path] to [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::FindRoots,
                ))
                .with_operation(op)
                .recover()?;
            }
            CollectGarbage(_req) => {
                /*
                ### Outputs
                - pathsDeleted :: [Set][se-Set] of [Path][se-Path]
                - bytesFreed :: [UInt64][se-UInt64]
                - 0 :: [UInt64][se-UInt64] (hardcoded, obsolete and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::CollectGarbage,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryAllValidPaths => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryAllValidPaths,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryPathFromHashPart(_hash) => {
                /*
                ### Outputs
                path :: [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryPathFromHashPart,
                ))
                .with_operation(op)
                .recover()?;
            }
            QuerySubstitutablePaths(_paths) => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QuerySubstitutablePaths,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryValidDerivers(_path) => {
                /*
                ### Outputs
                derivers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryValidDerivers,
                ))
                .with_operation(op)
                .recover()?;
            }
            OptimiseStore => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::OptimiseStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            VerifyStore(_req) => {
                /*
                ### Outputs
                errors :: [Bool][se-Bool]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::VerifyStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            BuildDerivation(_req) => {
                /*
                ### Outputs
                buildResult :: [BuildResult][se-BuildResult]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::BuildDerivation,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddSignatures(_req) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddSignatures,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddToStoreNar(_req) => {
                /*
                ### Inputs
                #### If protocol version is 1.23 or newer
                [Framed][se-Framed] NAR dump

                #### If protocol version is between 1.21 and 1.23
                NAR dump sent using [`STDERR_READ`](./logging.md#stderr_read)

                #### If protocol version is older than 1.21
                NAR dump sent raw on stream

                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddToStoreNar,
                ))
                .with_operation(op)?;
            }
            QueryMissing(_paths) => {
                /*
                ### Outputs
                - willBuild :: [Set][se-Set] of [StorePath][se-StorePath]
                - willSubstitute :: [Set][se-Set] of [StorePath][se-StorePath]
                - unknown :: [Set][se-Set] of [StorePath][se-StorePath]
                - downloadSize :: [UInt64][se-UInt64]
                - narSize :: [UInt64][se-UInt64]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryMissing,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryDerivationOutputMap(_path) => {
                /*
                ### Outputs
                outputs :: [Map][se-Map] of [OutputName][se-OutputName] to [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDerivationOutputMap,
                ))
                .with_operation(op)
                .recover()?;
            }
            RegisterDrvOutput(_req) => {
                /*
                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::RegisterDrvOutput,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryRealisation(_output_id) => {
                /*
                ### Outputs
                #### If protocol is 1.31 or newer
                realisations :: [Set][se-Set] of [Realisation][se-Realisation]

                #### If protocol is older than 1.31
                outPaths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryRealisation,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddMultipleToStore(_req) => {
                /*
                ### Inputs
                - repair :: [Bool64][se-Bool64]
                - dontCheckSigs :: [Bool64][se-Bool64]
                - [Framed][se-Framed] stream of [add multiple NAR dump][se-AddMultipleToStore]

                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddMultipleToStore,
                ))
                .with_operation(op)?;
            }
            AddBuildLog(BaseStorePath(_path)) => {
                /*
                ### Inputs
                - path :: [BaseStorePath][se-BaseStorePath]
                - [Framed][se-Framed] stream of log lines

                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddBuildLog,
                ))
                .with_operation(op)?;
            }
            BuildPathsWithResults(_req) => {
                /*
                ### Outputs
                results :: [List][se-List] of [KeyedBuildResult][se-KeyedBuildResult]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::BuildPathsWithResults,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddPermRoot(_req) => {
                /*
                ### Outputs
                gcRoot :: [Path][se-Path]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddPermRoot,
                ))
                .with_operation(op)
                .recover()?;
            }

            // Obsolete Nix 2.5.0 Protocol 1.32
            SyncWithGC => {
                /*
                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::SyncWithGC,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.25
            AddTextToStore(_req) => {
                /*
                ### Outpus
                path :: [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddTextToStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.22*
            QueryDerivationOutputs(_path) => {
                /*
                ### Outputs
                derivationOutputs :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDerivationOutputs,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.21
            QueryDerivationOutputNames(_path) => {
                /*
                ### Outputs
                names :: [Set][se-Set] of [OutputName][se-OutputName]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDerivationOutputNames,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0, Protocol 1.19*
            QuerySubstitutablePathInfos(_req) => {
                /*
                ### Outputs
                infos :: [Map][se-Map] of [StorePath][se-StorePath] to [SubstitutablePathInfo][se-SubstitutablePathInfo]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QuerySubstitutablePathInfos,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.17
            ExportPath(_path) => {
                /*
                ### Outputs
                Uses [`STDERR_WRITE`](./logging.md#stderr_write) to send dump in
                [export format][se-ExportFormat]

                After dump it outputs.

                1 :: [Int][se-Int] (hardcoded)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::ExportPath,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.17
            ImportPaths => {
                /*
                ### Inputs
                [List of NAR dumps][se-ImportPaths] coming from one or more ExportPath operations.

                ### Outputs
                importedPaths :: [List][se-List] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::ImportPaths,
                ))
                .with_operation(op)?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryPathHash(_path) => {
                /*
                ### Outputs
                hash :: [NARHash][se-NARHash]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryPathHash,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryReferences(_path) => {
                /*
                ### Outputs
                references :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryReferences,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryDeriver(_path) => {
                /*
                ### Outputs
                deriver :: [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDeriver,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 1.2 Protocol 1.12
            HasSubstitutes(_paths) => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::HasSubstitutes,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 1.2 Protocol 1.12
            QuerySubstitutablePathInfo(_path) => {
                /*
                ### Outputs
                found :: [Bool][se-Bool]

                #### If found is true
                - info :: [SubstitutablePathInfo][se-SubstitutablePathInfo]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QuerySubstitutablePathInfo,
                ))
                .with_operation(op)
                .recover()?;
            }
        }
        Ok(())
    }


    pub async fn local_process_logs<'s, T: Send + 's>(
        &'s mut self,
        mut logs: impl LocalLoggerResult<T, DaemonError> + 's,
    ) -> Result<T, RecoverableError> {
        while let Some(msg) = logs.next().await {
            self.writer.write_value(&msg.recover()?).await?;
        }
        // TODO: Test this recover
        let value = logs.result().await.recover()?;
        self.writer.write_value(&RawLogMessage::Last).await?;
        Ok(value)
    }

    pub async fn local_process_requests<'s, S>(&'s mut self, mut store: S) -> Result<(), DaemonError>
    where
        S: LocalDaemonStore + 's,
    {
        while let Some(request) = self.reader.try_read_value::<Request>().await? {
            let op = request.operation();
            eprintln!("Server got operation {}", op);
            if let Err(mut err) = self.local_process_request(&mut store, request).await {
                err.source = err.source.fill_operation(op);
                if err.can_recover {
                    self.writer
                        .write_value(&RawLogMessage::Error(err.source.into()))
                        .await?;
                } else {
                    return Err(err.source);
                }
            }
            eprintln!("Server flush");
            self.writer.flush().await?;
        }
        eprintln!("Server handled all requests");
        Ok(())
    }

    fn local_store_nar_from_path<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        path: &'p StorePath,
        sink: NW,
    ) -> impl LocalLoggerResult<(), DaemonError> + 'r
    where
        S: LocalDaemonStore + 's,
        NW: AsyncWrite + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        store.nar_from_path(path, sink)
    }

    async fn local_nar_from_path<'s, 't, S>(
        &'s mut self,
        store: &'t mut S,
        path: StorePath,
    ) -> Result<(), RecoverableError>
    where
        S: LocalDaemonStore + 't,
    {
        // FUTUREWORK: Fix that this whole implementation allocates 2 buffers

        let (mut reader, sink) = simplex(10_000);
        let mut logs = Self::local_store_nar_from_path(store, &path, sink);

        while let Some(msg) = logs.next().await {
            self.writer.write_value(&msg.recover()?).await?;
        }

        self.writer.write_value(&RawLogMessage::Last).await?;
        try_join!(
            async move {
                eprintln!("Copying NAR from server");
                let ret = copy(&mut reader, &mut self.writer)
                    .map_err(DaemonError::from)
                    .await;
                eprintln!("Copied {:?} NAR from server", ret);
                ret
            },
            logs.result().map_err(DaemonError::from)
        )?;
        Ok(())
    }

    pub async fn local_process_request<'s, S>(
        &'s mut self,
        mut store: S,
        request: Request,
    ) -> Result<(), RecoverableError>
    where
        S: LocalDaemonStore + 's,
    {
        use Request::*;
        let op = request.operation();
        match request {
            SetOptions(options) => {
                let logs = store.set_options(&options);
                self.local_process_logs(logs).await?;
            }
            IsValidPath(path) => {
                let logs = store.is_valid_path(&path);
                let value = self.local_process_logs(logs).await?;
                self.writer.write_value(&value).await?;
            }
            QueryValidPaths(req) => {
                let logs = store.query_valid_paths(&req.paths, req.substitute);
                let value = self.local_process_logs(logs).await?;
                self.writer.write_value(&value).await?;
            }
            QueryPathInfo(path) => {
                let logs = store.query_path_info(&path);
                let value = self.local_process_logs(logs).await?;
                self.writer.write_value(&value).await?;
            }
            NarFromPath(path) => {
                self.local_nar_from_path(&mut store, path).await?;
            }
            QueryReferrers(_path) => {
                /*
                ### Outputs
                referrers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryReferrers,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddToStore(req) => {
                match req {
                    AddToStoreRequest::Protocol25(_post25_req) => {
                        /*
                        #### Inputs
                        - name :: [StorePathName][se-StorePathName]
                        - camStr :: [ContentAddressMethodWithAlgo][se-ContentAddressMethodWithAlgo]
                        - refs :: [Set][se-Set] of [StorePath][se-StorePath]
                        - repairBool :: [Bool64][se-Bool64]
                        - [Framed][se-Framed] NAR dump

                        #### Outputs
                        info :: [ValidPathInfo][se-ValidPathInfo]
                         */
                    }
                    AddToStoreRequest::ProtocolPre25(_pre25_req) => {
                        /*
                        #### Inputs
                        - baseName :: [StorePathName][se-StorePathName]
                        - fixed :: [Bool64][se-Bool64]
                        - recursive :: [FileIngestionMethod][se-FileIngestionMethod]
                        - hashAlgo :: [HashAlgorithm][se-HashAlgorithm]
                        - NAR dump

                        If fixed is `true`, hashAlgo is forced to `sha256` and recursive is forced to
                        `NixArchive`.

                        Only `Flat` and `NixArchive` values are supported for the recursive input
                        parameter.

                        #### Outputs
                        path :: [StorePath][se-StorePath]
                         */
                    }
                }
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddToStore,
                ))
                .with_operation(op)?;
            }
            BuildPaths(_req) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::BuildPaths,
                ))
                .with_operation(op)
                .recover()?;
            }
            EnsurePath(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::EnsurePath,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddTempRoot(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddTempRoot,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddIndirectRoot(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddIndirectRoot,
                ))
                .with_operation(op)
                .recover()?;
            }
            FindRoots => {
                /*
                ### Outputs
                roots :: [Map][se-Map] of [Path][se-Path] to [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::FindRoots,
                ))
                .with_operation(op)
                .recover()?;
            }
            CollectGarbage(_req) => {
                /*
                ### Outputs
                - pathsDeleted :: [Set][se-Set] of [Path][se-Path]
                - bytesFreed :: [UInt64][se-UInt64]
                - 0 :: [UInt64][se-UInt64] (hardcoded, obsolete and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::CollectGarbage,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryAllValidPaths => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryAllValidPaths,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryPathFromHashPart(_hash) => {
                /*
                ### Outputs
                path :: [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryPathFromHashPart,
                ))
                .with_operation(op)
                .recover()?;
            }
            QuerySubstitutablePaths(_paths) => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QuerySubstitutablePaths,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryValidDerivers(_path) => {
                /*
                ### Outputs
                derivers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryValidDerivers,
                ))
                .with_operation(op)
                .recover()?;
            }
            OptimiseStore => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::OptimiseStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            VerifyStore(_req) => {
                /*
                ### Outputs
                errors :: [Bool][se-Bool]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::VerifyStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            BuildDerivation(_req) => {
                /*
                ### Outputs
                buildResult :: [BuildResult][se-BuildResult]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::BuildDerivation,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddSignatures(_req) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddSignatures,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddToStoreNar(_req) => {
                /*
                ### Inputs
                #### If protocol version is 1.23 or newer
                [Framed][se-Framed] NAR dump

                #### If protocol version is between 1.21 and 1.23
                NAR dump sent using [`STDERR_READ`](./logging.md#stderr_read)

                #### If protocol version is older than 1.21
                NAR dump sent raw on stream

                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddToStoreNar,
                ))
                .with_operation(op)?;
            }
            QueryMissing(_paths) => {
                /*
                ### Outputs
                - willBuild :: [Set][se-Set] of [StorePath][se-StorePath]
                - willSubstitute :: [Set][se-Set] of [StorePath][se-StorePath]
                - unknown :: [Set][se-Set] of [StorePath][se-StorePath]
                - downloadSize :: [UInt64][se-UInt64]
                - narSize :: [UInt64][se-UInt64]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryMissing,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryDerivationOutputMap(_path) => {
                /*
                ### Outputs
                outputs :: [Map][se-Map] of [OutputName][se-OutputName] to [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDerivationOutputMap,
                ))
                .with_operation(op)
                .recover()?;
            }
            RegisterDrvOutput(_req) => {
                /*
                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::RegisterDrvOutput,
                ))
                .with_operation(op)
                .recover()?;
            }
            QueryRealisation(_output_id) => {
                /*
                ### Outputs
                #### If protocol is 1.31 or newer
                realisations :: [Set][se-Set] of [Realisation][se-Realisation]

                #### If protocol is older than 1.31
                outPaths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryRealisation,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddMultipleToStore(_req) => {
                /*
                ### Inputs
                - repair :: [Bool64][se-Bool64]
                - dontCheckSigs :: [Bool64][se-Bool64]
                - [Framed][se-Framed] stream of [add multiple NAR dump][se-AddMultipleToStore]

                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddMultipleToStore,
                ))
                .with_operation(op)?;
            }
            AddBuildLog(BaseStorePath(_path)) => {
                /*
                ### Inputs
                - path :: [BaseStorePath][se-BaseStorePath]
                - [Framed][se-Framed] stream of log lines

                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddBuildLog,
                ))
                .with_operation(op)?;
            }
            BuildPathsWithResults(_req) => {
                /*
                ### Outputs
                results :: [List][se-List] of [KeyedBuildResult][se-KeyedBuildResult]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::BuildPathsWithResults,
                ))
                .with_operation(op)
                .recover()?;
            }
            AddPermRoot(_req) => {
                /*
                ### Outputs
                gcRoot :: [Path][se-Path]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddPermRoot,
                ))
                .with_operation(op)
                .recover()?;
            }

            // Obsolete Nix 2.5.0 Protocol 1.32
            SyncWithGC => {
                /*
                ### Outputs
                Nothing
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::SyncWithGC,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.25
            AddTextToStore(_req) => {
                /*
                ### Outpus
                path :: [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddTextToStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.22*
            QueryDerivationOutputs(_path) => {
                /*
                ### Outputs
                derivationOutputs :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDerivationOutputs,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.21
            QueryDerivationOutputNames(_path) => {
                /*
                ### Outputs
                names :: [Set][se-Set] of [OutputName][se-OutputName]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDerivationOutputNames,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0, Protocol 1.19*
            QuerySubstitutablePathInfos(_req) => {
                /*
                ### Outputs
                infos :: [Map][se-Map] of [StorePath][se-StorePath] to [SubstitutablePathInfo][se-SubstitutablePathInfo]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QuerySubstitutablePathInfos,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.17
            ExportPath(_path) => {
                /*
                ### Outputs
                Uses [`STDERR_WRITE`](./logging.md#stderr_write) to send dump in
                [export format][se-ExportFormat]

                After dump it outputs.

                1 :: [Int][se-Int] (hardcoded)
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::ExportPath,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.17
            ImportPaths => {
                /*
                ### Inputs
                [List of NAR dumps][se-ImportPaths] coming from one or more ExportPath operations.

                ### Outputs
                importedPaths :: [List][se-List] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::ImportPaths,
                ))
                .with_operation(op)?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryPathHash(_path) => {
                /*
                ### Outputs
                hash :: [NARHash][se-NARHash]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryPathHash,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryReferences(_path) => {
                /*
                ### Outputs
                references :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryReferences,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryDeriver(_path) => {
                /*
                ### Outputs
                deriver :: [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QueryDeriver,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 1.2 Protocol 1.12
            HasSubstitutes(_paths) => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::HasSubstitutes,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 1.2 Protocol 1.12
            QuerySubstitutablePathInfo(_path) => {
                /*
                ### Outputs
                found :: [Bool][se-Bool]

                #### If found is true
                - info :: [SubstitutablePathInfo][se-SubstitutablePathInfo]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::QuerySubstitutablePathInfo,
                ))
                .with_operation(op)
                .recover()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {}
