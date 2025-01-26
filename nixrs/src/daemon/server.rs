use futures::future::TryFutureExt;
use tokio::io::{copy, simplex, split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::try_join;

use crate::daemon::wire::logger::RawLogMessage;
use crate::daemon::wire::types::Operation;
use crate::daemon::wire::types2::{AddToStoreRequest, BaseStorePath};
use crate::daemon::PROTOCOL_VERSION;

use super::de::{NixRead, NixReader};
use super::logger::LoggerResult;
use super::ser::{NixWrite, NixWriter};
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
        Builder {
            store_trust: TrustLevel::NotTrusted,
            min_version: ProtocolVersion::min(),
            max_version: ProtocolVersion::max(),
            nix_version: None,
        }
    }

    pub async fn serve_connection<I, S>(&self, io: I, store: S) -> DaemonResult<()>
    where
        I: AsyncRead + AsyncWrite + Send + Unpin,
        S: HandshakeDaemonStore,
    {
        let (reader, writer) = split(io);
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
    pub async fn handshake(
        &mut self,
        min_version: ProtocolVersion,
        max_version: ProtocolVersion,
        nix_version: &str,
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

        let client_magic = self.reader.read_number().await?;
        if client_magic != CLIENT_MAGIC {
            return Err(DaemonError::WrongMagic(client_magic));
        }

        self.writer.write_number(SERVER_MAGIC).await?;
        self.writer.write_value(&max_version).await?;
        self.writer.flush().await?;

        let client_version: ProtocolVersion = self.reader.read_value().await?;
        let version = client_version.min(max_version);
        if version < min_version {
            return Err(DaemonError::UnsupportedVersion(version));
        }
        self.reader.set_version(version);
        self.writer.set_version(version);

        if version.minor() >= 14 {
            // Obsolete CPU Affinity
            if self.reader.read_value().await? {
                let _cpu_affinity = self.reader.read_number().await?;
            }
        }

        if version.minor() >= 11 {
            // Obsolete reserved space
            let _reserve_space: bool = self.reader.read_value().await?;
        }

        if version.minor() >= 33 {
            self.writer.write_value(nix_version).await?;
        }

        if version.minor() >= 35 {
            self.writer.write_value(&self.store_trust).await?;
        }

        self.writer.flush().await?;
        Ok(version)
    }

    pub async fn process_logs<T>(
        &mut self,
        mut logs: impl LoggerResult<T, DaemonError>,
    ) -> Result<T, RecoverableError> {
        while let Some(msg) = logs.next().await {
            self.writer.write_value(&msg.recover()?).await?;
        }
        // TODO: Test this recover
        let value = logs.result().await.recover()?;
        self.writer.write_value(&RawLogMessage::Last).await?;
        Ok(value)
    }

    pub async fn process_requests<S>(&mut self, mut store: S) -> Result<(), DaemonError>
    where
        S: DaemonStore,
    {
        while let Some(request) = self.reader.try_read_value().await? {
            if let Err(err) = self.process_request(&mut store, request).await {
                if err.can_recover {
                    self.writer
                        .write_value(&RawLogMessage::Error(err.source.into()))
                        .await?;
                } else {
                    return Err(err.source);
                }
            }
            self.writer.flush().await?;
        }
        Ok(())
    }

    pub async fn process_request<S>(
        &mut self,
        mut store: S,
        request: Request,
    ) -> Result<(), RecoverableError>
    where
        S: DaemonStore,
    {
        use Request::*;
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
                // FUTUREWORK: Fix that this whole implementation allocates 2 buffers
                let (mut reader, sink) = simplex(10_000);
                let mut logs = store.nar_from_path(&path, sink);
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
            }
            QueryReferrers(_path) => {
                /*
                ### Outputs
                referrers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */

                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryReferrers,
                ))
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
                Err(DaemonError::UnimplementedOperation(Operation::AddToStore))?;
            }
            BuildPaths(_req) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonError::UnimplementedOperation(Operation::BuildPaths)).recover()?;
            }
            EnsurePath(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonError::UnimplementedOperation(Operation::EnsurePath)).recover()?;
            }
            AddTempRoot(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */

                Err(DaemonError::UnimplementedOperation(Operation::AddTempRoot)).recover()?;
            }
            AddIndirectRoot(_path) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::AddIndirectRoot,
                ))
                .recover()?;
            }
            FindRoots => {
                /*
                ### Outputs
                roots :: [Map][se-Map] of [Path][se-Path] to [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(Operation::FindRoots)).recover()?;
            }
            CollectGarbage(_req) => {
                /*
                ### Outputs
                - pathsDeleted :: [Set][se-Set] of [Path][se-Path]
                - bytesFreed :: [UInt64][se-UInt64]
                - 0 :: [UInt64][se-UInt64] (hardcoded, obsolete and ignored by client)
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::CollectGarbage,
                ))
                .recover()?;
            }
            QueryAllValidPaths => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryAllValidPaths,
                ))
                .recover()?;
            }
            QueryPathFromHashPart(_hash) => {
                /*
                ### Outputs
                path :: [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryPathFromHashPart,
                ))
                .recover()?;
            }
            QuerySubstitutablePaths(_paths) => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QuerySubstitutablePaths,
                ))
                .recover()?;
            }
            QueryValidDerivers(_path) => {
                /*
                ### Outputs
                derivers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryValidDerivers,
                ))
                .recover()?;
            }
            OptimiseStore => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::OptimiseStore,
                ))
                .recover()?;
            }
            VerifyStore(_req) => {
                /*
                ### Outputs
                errors :: [Bool][se-Bool]
                 */
                Err(DaemonError::UnimplementedOperation(Operation::VerifyStore)).recover()?;
            }
            BuildDerivation(_req) => {
                /*
                ### Outputs
                buildResult :: [BuildResult][se-BuildResult]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::BuildDerivation,
                ))
                .recover()?;
            }
            AddSignatures(_req) => {
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::AddSignatures,
                ))
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
                Err(DaemonError::UnimplementedOperation(
                    Operation::AddToStoreNar,
                ))?;
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
                Err(DaemonError::UnimplementedOperation(Operation::QueryMissing)).recover()?;
            }
            QueryDerivationOutputMap(_path) => {
                /*
                ### Outputs
                outputs :: [Map][se-Map] of [OutputName][se-OutputName] to [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryDerivationOutputMap,
                ))
                .recover()?;
            }
            RegisterDrvOutput(_req) => {
                /*
                ### Outputs
                Nothing
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::RegisterDrvOutput,
                ))
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
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryRealisation,
                ))
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
                Err(DaemonError::UnimplementedOperation(
                    Operation::AddMultipleToStore,
                ))?;
            }
            AddBuildLog(BaseStorePath(_path)) => {
                /*
                ### Inputs
                - path :: [BaseStorePath][se-BaseStorePath]
                - [Framed][se-Framed] stream of log lines

                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                Err(DaemonError::UnimplementedOperation(Operation::AddBuildLog))?;
            }
            BuildPathsWithResults(_req) => {
                /*
                ### Outputs
                results :: [List][se-List] of [KeyedBuildResult][se-KeyedBuildResult]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::BuildPathsWithResults,
                ))
                .recover()?;
            }
            AddPermRoot(_req) => {
                /*
                ### Outputs
                gcRoot :: [Path][se-Path]
                 */
                Err(DaemonError::UnimplementedOperation(Operation::AddPermRoot)).recover()?;
            }

            // Obsolete Nix 2.5.0 Protocol 1.32
            SyncWithGC => {
                /*
                ### Outputs
                Nothing
                 */
                Err(DaemonError::UnimplementedOperation(Operation::SyncWithGC)).recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.25
            AddTextToStore(_req) => {
                /*
                ### Outpus
                path :: [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::AddTextToStore,
                ))
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.22*
            QueryDerivationOutputs(_path) => {
                /*
                ### Outputs
                derivationOutputs :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryDerivationOutputs,
                ))
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.21
            QueryDerivationOutputNames(_path) => {
                /*
                ### Outputs
                names :: [Set][se-Set] of [OutputName][se-OutputName]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryDerivationOutputNames,
                ))
                .recover()?;
            }
            // Obsolete Nix 2.0, Protocol 1.19*
            QuerySubstitutablePathInfos(_req) => {
                /*
                ### Outputs
                infos :: [Map][se-Map] of [StorePath][se-StorePath] to [SubstitutablePathInfo][se-SubstitutablePathInfo]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QuerySubstitutablePathInfos,
                ))
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
                Err(DaemonError::UnimplementedOperation(Operation::ExportPath)).recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.17
            ImportPaths => {
                /*
                ### Inputs
                [List of NAR dumps][se-ImportPaths] coming from one or more ExportPath operations.

                ### Outputs
                importedPaths :: [List][se-List] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(Operation::ImportPaths))?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryPathHash(_path) => {
                /*
                ### Outputs
                hash :: [NARHash][se-NARHash]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryPathHash,
                ))
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryReferences(_path) => {
                /*
                ### Outputs
                references :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::QueryReferences,
                ))
                .recover()?;
            }
            // Obsolete Nix 2.0 Protocol 1.16
            QueryDeriver(_path) => {
                /*
                ### Outputs
                deriver :: [OptStorePath][se-OptStorePath]
                 */
                Err(DaemonError::UnimplementedOperation(Operation::QueryDeriver)).recover()?;
            }
            // Obsolete Nix 1.2 Protocol 1.12
            HasSubstitutes(_paths) => {
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                Err(DaemonError::UnimplementedOperation(
                    Operation::HasSubstitutes,
                ))
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
                Err(DaemonError::UnimplementedOperation(
                    Operation::QuerySubstitutablePathInfo,
                ))
                .recover()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {}
