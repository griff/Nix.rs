use std::fmt::Debug;
use std::ops::Deref;
use std::pin::pin;

use futures::future::TryFutureExt;
use futures::{FutureExt, Stream, StreamExt as _};
use tokio::io::{
    copy, simplex, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt,
};
use tokio::{select, try_join};
use tracing::{debug, error, info, info_span, instrument, trace, Instrument};

use crate::archive::NarReader;
use crate::daemon::wire::logger::RawLogMessage;
use crate::daemon::wire::types::Operation;
use crate::daemon::wire::types2::{AddToStoreRequest, BaseStorePath};
use crate::daemon::wire::IgnoredOne;
use crate::daemon::wire::{parse_add_multiple_to_store, FramedReader, StderrReader};
use crate::daemon::{DaemonErrorKind, DaemonResultExt, PROTOCOL_VERSION};
use crate::io::{AsyncBufReadCompat, BytesReader};
use crate::store_path::StorePath;

use super::de::{NixRead, NixReader};
use super::logger::LocalLoggerResult;
use super::ser::{NixWrite, NixWriter};
use super::types::{AddToStoreItem, LocalDaemonStore, LocalHandshakeDaemonStore};
use super::wire::types2::{Request, ValidPathInfo};
use super::wire::{CLIENT_MAGIC, SERVER_MAGIC};
use super::{
    DaemonError, DaemonResult, DaemonStore, HandshakeDaemonStore, LogMessage, ProtocolVersion,
    ResultLog, TrustLevel, NIX_VERSION,
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
        R: AsyncRead + Debug + Send + Unpin + 's,
        W: AsyncWrite + Debug + Send + Unpin + 's,
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
        trace!("Server handshake done!");
        let store_result = store.handshake();
        let store = conn
            .process_logs(store_result)
            .await
            .map_err(|e| e.source)?;
        conn.writer.flush().await?;
        trace!("Server handshake logs done!");
        conn.process_requests(store).await?;
        trace!("Server processed all requests!");
        Ok(())
    }

    pub async fn local_serve_connection<'s, R, W, S>(
        &'s self,
        reader: R,
        writer: W,
        store: S,
    ) -> DaemonResult<()>
    where
        R: AsyncRead + Debug + Send + Unpin + 's,
        W: AsyncWrite + Debug + Send + Unpin + 's,
        S: LocalHandshakeDaemonStore + Debug + Send + 's,
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
        trace!("Server handshake done!");
        let store_result = store.handshake();
        let store = conn
            .local_process_logs(store_result)
            .await
            .map_err(|e| e.source)?;
        conn.writer.flush().await?;
        trace!("Server handshake logs done!");
        conn.local_process_requests(store).await?;
        trace!("Server processed all requests!");
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

async fn write_log<W>(writer: &mut NixWriter<W>, msg: LogMessage) -> Result<(), RecoverableError>
where
    W: AsyncWrite + Send + Unpin,
{
    match &msg {
        LogMessage::Next(raw_msg) => {
            let msg = String::from_utf8_lossy(raw_msg);
            debug!("log_message: {}", msg);
        }
        LogMessage::StartActivity(activity) => {
            let text = String::from_utf8_lossy(&activity.text);
            debug!(id=activity.act, level=?activity.level, type=?activity.activity_type,
                ?text,
                parent=activity.parent,
                "start_activity: {:?} {:?}: {}", activity.activity_type, activity.fields, text);
        }
        LogMessage::StopActivity(activity) => {
            debug!(id = activity, "stop_activity: {}", activity);
        }
        LogMessage::Result(result) => {
            debug!(
                id = result.act,
                "log_result: {} {:?} {:?}", result.act, result.result_type, result.fields,
            );
        }
    }
    writer.write_value(&msg).await?;
    writer.flush().await?;
    Ok(())
}

async fn process_logs<'s, T: Send + 's, W: AsyncWrite + Send + Unpin>(
    writer: &'s mut NixWriter<W>,
    logs: impl ResultLog<T, DaemonError> + 's,
) -> Result<T, RecoverableError> {
    let mut logs = pin!(logs);
    while let Some(msg) = logs.next().await {
        write_log(writer, msg).await?;
    }
    match logs.await {
        Err(source) => {
            error!("result_error: {:?}", source);
            Err(RecoverableError {
                can_recover: true,
                source,
            })
        }
        Ok(value) => {
            writer.write_value(&RawLogMessage::Last).await?;
            writer.flush().await?;
            Ok(value)
        }
    }
}

struct BoxedStore<S>(S);
impl<S> DaemonStore for BoxedStore<S>
where
    S: DaemonStore,
{
    fn trust_level(&self) -> TrustLevel {
        self.0.trust_level()
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a super::ClientOptions,
    ) -> impl ResultLog<(), DaemonError> + Send + 'a {
        Box::pin(self.0.set_options(options))
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<bool, DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.is_valid_path(path));
        trace!("IsValidPath Size {}", size_of_val(&ret));
        ret
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<crate::store_path::StorePathSet, DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.query_valid_paths(paths, substitute));
        trace!("QueryValidPaths Size {}", size_of_val(&ret));
        ret
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Option<super::UnkeyedValidPathInfo>, DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.query_path_info(path));
        trace!("QueryPathInfo Size {}", size_of_val(&ret));
        ret
    }

    fn nar_from_path<'s, 'p, 'r, W>(
        &'s mut self,
        path: &'p StorePath,
        sink: W,
    ) -> impl ResultLog<(), DaemonError> + Send + 'r
    where
        W: AsyncWrite + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        let ret = Box::pin(self.0.nar_from_path(path, sink));
        trace!("NarFromPath Size {}", size_of_val(&ret));
        ret
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [super::wire::types2::DerivedPath],
        mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<(), DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.build_paths(paths, mode));
        trace!("BuildPaths Size {}", size_of_val(&ret));
        ret
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv_path: &'a StorePath,
        drv: &'a super::wire::types2::BasicDerivation,
        build_mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<super::wire::types2::BuildResult, DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.build_derivation(drv_path, drv, build_mode));
        trace!("BuildDerivation Size {}", size_of_val(&ret));
        ret
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [super::wire::types2::DerivedPath],
    ) -> impl ResultLog<super::wire::types2::QueryMissingResult, DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.query_missing(paths));
        trace!("QueryMissing Size {}", size_of_val(&ret));
        ret
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<(), DaemonError> + Send + 'r
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        let ret = Box::pin(
            self.0
                .add_to_store_nar(info, source, repair, dont_check_sigs),
        );
        trace!("AddToStoreNar Size {}", size_of_val(ret.deref()));
        ret
    }

    fn add_multiple_to_store<'s, 'i, 'r, ST, STR>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: ST,
    ) -> impl ResultLog<(), DaemonError> + Send + 'r
    where
        ST: Stream<Item = Result<AddToStoreItem<STR>, DaemonError>> + Send + 'i,
        STR: AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        let ret = Box::pin(
            self.0
                .add_multiple_to_store(repair, dont_check_sigs, stream),
        );
        trace!("AddMultipleToStore Size {}", size_of_val(ret.deref()));
        ret
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [super::wire::types2::DerivedPath],
        mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<Vec<super::wire::types2::KeyedBuildResult>, DaemonError> + Send + 'a {
        let ret = Box::pin(self.0.build_paths_with_results(drvs, mode));
        trace!("BuildPathsWithResults Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<crate::store_path::StorePathSet, DaemonError> + Send + '_ {
        let ret = Box::pin(self.0.query_all_valid_paths());
        trace!("QueryAllValidPaths Size {}", size_of_val(&ret));
        ret
    }
}

pub struct DaemonConnection<R, W> {
    store_trust: TrustLevel,
    reader: NixReader<BytesReader<R>>,
    writer: NixWriter<W>,
}

impl<R, W> DaemonConnection<R, W>
where
    R: AsyncRead + Send + Unpin + Debug,
    W: AsyncWrite + Send + Unpin + Debug,
{
    #[instrument(skip(self))]
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
        info!(
            ?version,
            ?client_version,
            "Server Version is {}, Client version is {}",
            version,
            client_version
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

    #[instrument(level = "trace", skip_all)]
    pub async fn process_logs<'s, T: Send + 's>(
        &'s mut self,
        logs: impl ResultLog<T, DaemonError> + Send + 's,
    ) -> Result<T, RecoverableError> {
        process_logs(&mut self.writer, logs).await
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn process_requests<'s, S>(&'s mut self, store: S) -> Result<(), DaemonError>
    where
        S: DaemonStore + 's,
    {
        let mut store = BoxedStore(store);
        loop {
            trace!("server buffer is {:?}", self.reader.get_ref().filled());
            let fut = self.reader.try_read_value::<Request>().boxed();
            trace!("Request Size {}", size_of_val(fut.deref()));
            let res = fut.await?;
            if res.is_none() {
                break;
            }
            let request = res.unwrap();
            let op = request.operation();
            let span = request.span();
            async {
                trace!("Server got operation {}", op);
                let req = self.process_request(&mut store, request);
                if let Err(mut err) = req.await {
                    error!(error = ?err.source, recover=err.can_recover, "Error processing request");
                    err.source = err.source.fill_operation(op);
                    if err.can_recover {
                        self.writer
                            .write_value(&RawLogMessage::Error(err.source.into()))
                            .await?;
                    } else {
                        return Err(err.source);
                    }
                }
                trace!("Server flush");
                self.writer.flush().await?;
                Ok(())
            }
            .instrument(span).await?;
        }
        trace!("Server handled all requests");
        Ok(())
    }

    fn add_to_store_nar<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        info: &'p ValidPathInfo,
        source: NW,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<(), DaemonError> + Send + 'r
    where
        S: DaemonStore + 's,
        NW: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.add_to_store_nar(info, source, repair, dont_check_sigs)
    }

    fn add_multiple_to_store<'s, 'r, S, ST, STR>(
        store: &'s mut S,
        repair: bool,
        dont_check_sigs: bool,
        stream: ST,
    ) -> impl ResultLog<(), DaemonError> + Send + 'r
    where
        S: DaemonStore + 's,
        ST: Stream<Item = Result<AddToStoreItem<STR>, DaemonError>> + Send + 'r,
        STR: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
    {
        store.add_multiple_to_store(repair, dont_check_sigs, stream)
    }

    fn store_nar_from_path<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        path: &'p StorePath,
        sink: NW,
    ) -> impl ResultLog<(), DaemonError> + 'r
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
        let logs = Self::store_nar_from_path(store, &path, sink);

        let mut logs = pin!(logs);
        while let Some(msg) = logs.next().await {
            write_log(&mut self.writer, msg).await?;
        }

        self.writer.write_value(&RawLogMessage::Last).await?;
        try_join!(
            async move {
                let _ = info_span!("copy_nar_from_path").enter();
                let ret = copy(&mut reader, &mut self.writer)
                    .map_err(DaemonError::from)
                    .await;
                match ret {
                    Err(err) => {
                        error!("NAR Copy failed {:?}", err);
                        Err(err)
                    }
                    Ok(bytes) => {
                        info!(bytes, "Copied {} bytes", bytes);
                        Ok(())
                    }
                }
            },
            logs.map_err(DaemonError::from)
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
            BuildPaths(req) => {
                let logs = store.build_paths(&req.paths, req.mode);
                self.process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
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
            BuildDerivation(req) => {
                let logs = store.build_derivation(&req.drv_path, &req.drv, req.build_mode);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                buildResult :: [BuildResult][se-BuildResult]
                 */
                self.writer.write_value(&value).await?;
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
            AddToStoreNar(req) => {
                /*
                ### Inputs
                 */
                if self.reader.version().minor() >= 23 {
                    /*
                    #### If protocol version is 1.23 or newer
                    [Framed][se-Framed] NAR dump
                     */
                    trace!("DaemonConnection: Add to store");
                    let buf_reader = AsyncBufReadCompat::new(&mut self.reader);
                    let mut framed = FramedReader::new(buf_reader);
                    trace!("DaemonConnection: Add to store: Framed");
                    let logs = Self::add_to_store_nar(
                        &mut store,
                        &req.path_info,
                        &mut framed,
                        req.repair,
                        req.dont_check_sigs,
                    );
                    trace!("DaemonConnection: Add to store: Logs");
                    let res: Result<(), RecoverableError> = async {
                        let mut logs = pin!(logs);
                        trace!("DaemonConnection: Add to store: get log");
                        while let Some(msg) = logs.next().await {
                            trace!("DaemonConnection: Add to store: got log");
                            write_log(&mut self.writer, msg).await?;
                        }
                        trace!("DaemonConnection: Add to store: get result");
                        logs.await.recover()?;
                        self.writer.write_value(&RawLogMessage::Last).await?;
                        Ok(())
                    }
                    .await;
                    trace!("DaemonConnection: Add to store: drain reader");
                    let err = framed.drain_all().await;
                    trace!("DaemonConnection: Add to store: done");
                    res?;
                    err?;
                } else if self.reader.version().minor() >= 21 {
                    /*
                    #### If protocol version is between 1.21 and 1.23
                    NAR dump sent using [`STDERR_READ`](./logging.md#stderr_read)
                     */
                    let (mut receiver, reader) = StderrReader::new(&mut self.reader);
                    let mut reader = NarReader::new(reader);
                    let logs = Self::add_to_store_nar(
                        &mut store,
                        &req.path_info,
                        &mut reader,
                        req.repair,
                        req.dont_check_sigs,
                    );
                    let res: Result<(), RecoverableError> = async {
                        let mut logs = pin!(logs);
                        loop {
                            select! {
                                log = logs.next() => {
                                    if let Some(msg) = log {
                                        write_log(&mut self.writer, msg).await?;
                                    } else {
                                        break;
                                    }
                                }
                                read_msg = receiver.recv() => {
                                    if let Some(read) = read_msg {
                                        self.writer.write_value(&RawLogMessage::Read(read)).await?;
                                        self.writer.flush().await?;
                                    }
                                }
                            }
                        }
                        logs.await.recover()?;
                        self.writer.write_value(&RawLogMessage::Last).await?;
                        Ok(())
                    }
                    .await;
                    let err: DaemonResult<()> = async {
                        loop {
                            let len = reader.fill_buf().await?.len();
                            if len == 0 {
                                break;
                            }
                            reader.consume(len);
                        }
                        Ok(())
                    }
                    .await;
                    res?;
                    err?;
                } else {
                    /*
                    #### If protocol version is older than 1.21
                    NAR dump sent raw on stream
                     */
                    let buf_reader = AsyncBufReadCompat::new(&mut self.reader);
                    let mut reader = NarReader::new(buf_reader);
                    let logs = Self::add_to_store_nar(
                        &mut store,
                        &req.path_info,
                        &mut reader,
                        req.repair,
                        req.dont_check_sigs,
                    );
                    let res: Result<(), RecoverableError> = async {
                        let mut logs = pin!(logs);
                        while let Some(msg) = logs.next().await {
                            write_log(&mut self.writer, msg).await?
                        }
                        logs.await.recover()?;
                        self.writer.write_value(&RawLogMessage::Last).await?;
                        Ok(())
                    }
                    .await;
                    let err: DaemonResult<()> = async {
                        loop {
                            let len = reader.fill_buf().await?.len();
                            if len == 0 {
                                break;
                            }
                            reader.consume(len);
                        }
                        Ok(())
                    }
                    .await;
                    res?;
                    err?;
                }
                /*
                ### Outputs
                Nothing
                 */
            }
            QueryMissing(paths) => {
                let logs = store.query_missing(&paths);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                - willBuild :: [Set][se-Set] of [StorePath][se-StorePath]
                - willSubstitute :: [Set][se-Set] of [StorePath][se-StorePath]
                - unknown :: [Set][se-Set] of [StorePath][se-StorePath]
                - downloadSize :: [UInt64][se-UInt64]
                - narSize :: [UInt64][se-UInt64]
                 */
                self.writer.write_value(&value).await?;
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
            AddMultipleToStore(req) => {
                /*
                ### Inputs
                - [Framed][se-Framed] stream of [add multiple NAR dump][se-AddMultipleToStore]
                */
                let builder = NixReader::builder().set_version(self.reader.version());
                let buf_reader = AsyncBufReadCompat::new(&mut self.reader);
                let mut framed = FramedReader::new(buf_reader);
                let source = builder.build_buffered(&mut framed);
                let stream = parse_add_multiple_to_store(source).await?;
                debug!("DaemonConnection: Add multiple to store: call store");
                let logs = Self::add_multiple_to_store(
                    &mut store,
                    req.repair,
                    req.dont_check_sigs,
                    stream,
                );
                debug!("DaemonConnection: Add multiple to store: Logs");
                let res: Result<(), RecoverableError> = async {
                    let mut logs = pin!(logs);
                    debug!("DaemonConnection: Add to store: get log");
                    while let Some(msg) = logs.next().await {
                        debug!("DaemonConnection: Add multiple to store: got log {:?}", msg);
                        write_log(&mut self.writer, msg).await?;
                    }
                    debug!("DaemonConnection: Add multiple to store: get result");
                    logs.await.recover()?;
                    debug!("DaemonConnection: Add multiple to store: write result");
                    self.writer.write_value(&RawLogMessage::Last).await?;
                    Ok(())
                }
                .await;
                debug!("DaemonConnection: Add to store: drain reader");
                let err = framed.drain_all().await;
                debug!("DaemonConnection: Add multiple to store: done");
                res?;
                err?;
                /*
                ### Outputs
                Nothing
                 */
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
            BuildPathsWithResults(req) => {
                let logs = store.build_paths_with_results(&req.drvs, req.mode);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                results :: [List][se-List] of [KeyedBuildResult][se-KeyedBuildResult]
                 */
                self.writer.write_value(&value).await?;
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

    pub async fn local_process_requests<'s, S>(
        &'s mut self,
        mut store: S,
    ) -> Result<(), DaemonError>
    where
        S: LocalDaemonStore + 's,
    {
        while let Some(request) = self.reader.try_read_value::<Request>().await? {
            let op = request.operation();
            info!("Server got operation {}", op);
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
            info!("Server flush");
            self.writer.flush().await?;
        }
        info!("Server handled all requests");
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
                info!("Copying NAR from server");
                let ret = copy(&mut reader, &mut self.writer)
                    .map_err(DaemonError::from)
                    .await;
                info!("Copied {:?} NAR from server", ret);
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
