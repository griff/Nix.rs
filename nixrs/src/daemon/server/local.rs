use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Deref as _,
    pin::pin,
};

use futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _};
use tokio::io::AsyncBufRead;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, copy_buf},
    select,
};
use tracing::{debug, error, info, instrument, trace};
use tracing_futures::Instrument;

use crate::archive::NarReader;
use crate::daemon::de::{NixRead as _, NixReader};
use crate::daemon::local::{LocalDaemonStore, LocalHandshakeDaemonStore};
use crate::daemon::ser::NixWriter;
use crate::daemon::server::{Builder, DaemonConnection, RecoverableError, process_logs, write_log};
use crate::daemon::wire::logger::RawLogMessage;
use crate::daemon::wire::types::Operation;
use crate::daemon::{
    DaemonError, DaemonErrorKind, DaemonResult, DaemonResultExt as _, NIX_VERSION, ResultLog,
};
use crate::{
    daemon::{
        AddToStoreItem,
        ser::NixWrite as _,
        server::RecoverExt as _,
        wire::{
            FramedReader, IgnoredOne, StderrReader, parse_add_multiple_to_store,
            types2::{
                AddToStoreRequest, BaseStorePath, RegisterDrvOutputRequest, Request, ValidPathInfo,
            },
        },
    },
    io::AsyncBufReadCompat,
    realisation::Realisation,
    store_path::{ContentAddressMethodAlgorithm, StorePath, StorePathSet},
};

impl Builder {
    pub async fn local_serve_connection<'s, R, W, S>(
        &'s self,
        reader: R,
        writer: W,
        store: S,
    ) -> DaemonResult<()>
    where
        R: AsyncRead + fmt::Debug + Send + Unpin + 's,
        W: AsyncWrite + fmt::Debug + Send + Unpin + 's,
        S: LocalHandshakeDaemonStore + fmt::Debug + 's,
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

impl<R, W> DaemonConnection<R, W>
where
    R: AsyncRead + Send + Unpin + fmt::Debug,
    W: AsyncWrite + Send + Unpin + fmt::Debug,
{
    #[instrument(level = "trace", skip_all)]
    pub async fn local_process_logs<'s, T: 's>(
        &'s mut self,
        logs: impl ResultLog<Output = DaemonResult<T>> + 's,
    ) -> Result<T, RecoverableError> {
        let value = process_logs(&mut self.writer, logs).await?;
        self.writer.write_value(&RawLogMessage::Last).await?;
        Ok(value)
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn local_process_requests<'s, S>(
        &'s mut self,
        mut store: S,
    ) -> Result<(), DaemonError>
    where
        S: LocalDaemonStore + 's,
    {
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
                debug!("Server got operation {}", op);
                let req = self.local_process_request(&mut store, request);
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
        debug!("Server handled all requests");
        store.shutdown().await
    }

    fn local_add_ca_to_store<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        name: &'p str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'p StorePathSet,
        repair: bool,
        source: NW,
    ) -> impl ResultLog<Output = DaemonResult<ValidPathInfo>> + 'r
    where
        S: LocalDaemonStore + 's,
        NW: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.add_ca_to_store(name, cam, refs, repair, source)
    }

    fn local_add_to_store_nar<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        info: &'p ValidPathInfo,
        source: NW,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        S: LocalDaemonStore + 's,
        NW: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.add_to_store_nar(info, source, repair, dont_check_sigs)
    }

    fn local_add_multiple_to_store<'s, 'r, S, ST, STR>(
        store: &'s mut S,
        repair: bool,
        dont_check_sigs: bool,
        stream: ST,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        S: LocalDaemonStore + 's,
        ST: Stream<Item = Result<AddToStoreItem<STR>, DaemonError>> + Send + 'r,
        STR: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
    {
        store.add_multiple_to_store(repair, dont_check_sigs, stream)
    }

    fn local_store_nar_from_path<'s, S>(
        store: &'s mut S,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + 's>> + 's
    where
        S: LocalDaemonStore + 's,
    {
        store.nar_from_path(path)
    }

    async fn local_nar_from_path<'s, 't, S>(
        &'s mut self,
        store: &'t mut S,
        path: StorePath,
    ) -> Result<(), RecoverableError>
    where
        S: LocalDaemonStore + 't,
    {
        let logs = Self::local_store_nar_from_path(store, &path);

        let mut logs = pin!(logs);
        while let Some(msg) = logs.next().await {
            write_log(&mut self.writer, msg).await?;
        }

        let mut reader = pin!(logs.await?);
        self.writer.write_value(&RawLogMessage::Last).await?;
        let ret = copy_buf(&mut reader, &mut self.writer)
            .map_err(DaemonError::from)
            .await;
        match ret {
            Err(err) => {
                error!("NAR Copy failed {:?}", err);
                Err(err.into())
            }
            Ok(bytes) => {
                info!(bytes, "Copied {} bytes", bytes);
                Ok(())
            }
        }
    }

    fn local_add_build_log<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        path: &'p StorePath,
        source: NW,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        S: LocalDaemonStore + 's,
        NW: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.add_build_log(path, source)
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
            QueryReferrers(path) => {
                let logs = store.query_referrers(&path);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                referrers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            AddToStore(AddToStoreRequest::Protocol25(req)) => {
                /*
                #### Inputs
                - name :: [StorePathName][se-StorePathName]
                - camStr :: [ContentAddressMethodWithAlgo][se-ContentAddressMethodWithAlgo]
                - refs :: [Set][se-Set] of [StorePath][se-StorePath]
                - repairBool :: [Bool64][se-Bool64]
                - [Framed][se-Framed] NAR dump
                */
                let buf_reader = AsyncBufReadCompat::new(&mut self.reader);
                let mut framed = FramedReader::new(buf_reader);
                let logs = Self::local_add_ca_to_store(
                    &mut store,
                    &req.name,
                    req.cam,
                    &req.refs,
                    req.repair,
                    &mut framed,
                );
                let res = process_logs(&mut self.writer, logs).await;
                let err = framed.drain_all().await;
                let value = res?;
                err?;
                self.writer.write_value(&RawLogMessage::Last).await?;
                /*
                #### Outputs
                info :: [ValidPathInfo][se-ValidPathInfo]
                */
                self.writer.write_value(&value).await?;
            }
            AddToStore(AddToStoreRequest::ProtocolPre25(_req)) => {
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
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddToStore,
                ))
                .with_operation(op)?;
            }
            BuildPaths(req) => {
                let logs = store.build_paths(&req.paths, req.mode);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            EnsurePath(path) => {
                let logs = store.ensure_path(&path);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            AddTempRoot(path) => {
                let logs = store.add_temp_root(&path);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            AddIndirectRoot(path) => {
                let logs = store.add_indirect_root(&path);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            FindRoots => {
                let logs = store.find_roots();
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                roots :: [Map][se-Map] of [Path][se-Path] to [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            CollectGarbage(req) => {
                let logs = store.collect_garbage(
                    req.action,
                    &req.paths_to_delete,
                    req.ignore_liveness,
                    req.max_freed,
                );
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                - pathsDeleted :: [Set][se-Set] of [Path][se-Path]
                - bytesFreed :: [UInt64][se-UInt64]
                - 0 :: [UInt64][se-UInt64] (hardcoded, obsolete and ignored by client)
                 */
                self.writer.write_value(&value).await?;
            }
            QueryAllValidPaths => {
                let logs = store.query_all_valid_paths();
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            QueryPathFromHashPart(hash) => {
                let logs = store.query_path_from_hash_part(&hash);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                path :: [OptStorePath][se-OptStorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            QuerySubstitutablePaths(paths) => {
                let logs = store.query_substitutable_paths(&paths);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            QueryValidDerivers(path) => {
                let logs = store.query_valid_derivers(&path);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                derivers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            OptimiseStore => {
                let logs = store.optimise_store();
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            VerifyStore(req) => {
                let logs = store.verify_store(req.check_contents, req.repair);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                errors :: [Bool][se-Bool]
                 */
                self.writer.write_value(&value).await?;
            }
            BuildDerivation(req) => {
                let logs = store.build_derivation(&req.drv, req.mode);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                buildResult :: [BuildResult][se-BuildResult]
                 */
                self.writer.write_value(&value).await?;
            }
            AddSignatures(req) => {
                let logs = store.add_signatures(&req.path, &req.signatures);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
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
                    let logs = Self::local_add_to_store_nar(
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
                        Ok(())
                    }
                    .await;
                    trace!("DaemonConnection: Add to store: drain reader");
                    let err = framed.drain_all().await;
                    trace!("DaemonConnection: Add to store: done");
                    res?;
                    err?;
                    self.writer.write_value(&RawLogMessage::Last).await?;
                } else if self.reader.version().minor() >= 21 {
                    /*
                    #### If protocol version is between 1.21 and 1.23
                    NAR dump sent using [`STDERR_READ`](./logging.md#stderr_read)
                     */
                    let (mut receiver, reader) = StderrReader::new(&mut self.reader);
                    let mut reader = NarReader::new(reader);
                    let logs = Self::local_add_to_store_nar(
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
                    let logs = Self::local_add_to_store_nar(
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
                let value = self.local_process_logs(logs).await?;
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
            QueryDerivationOutputMap(path) => {
                let logs = store.query_derivation_output_map(&path);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                outputs :: [Map][se-Map] of [OutputName][se-OutputName] to [OptStorePath][se-OptStorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            RegisterDrvOutput(RegisterDrvOutputRequest::Post31(realisation)) => {
                let logs = store.register_drv_output(&realisation);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                Nothing
                 */
            }
            RegisterDrvOutput(RegisterDrvOutputRequest::Pre31 {
                output_id,
                output_path,
            }) => {
                let realisation = Realisation {
                    id: output_id,
                    out_path: output_path,
                    signatures: BTreeSet::new(),
                    dependent_realisations: BTreeMap::new(),
                };
                let logs = store.register_drv_output(&realisation);
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                Nothing
                 */
            }
            QueryRealisation(output_id) => {
                let logs = store.query_realisation(&output_id);
                let value = self.local_process_logs(logs).await?;
                /*
                  ### Outputs
                */
                if self.reader.version().minor() >= 31 {
                    /*
                    #### If protocol is 1.31 or newer
                    realisations :: [Set][se-Set] of [Realisation][se-Realisation]
                    */
                    self.writer.write_value(&value).await?;
                } else {
                    /*
                    #### If protocol is older than 1.31
                    outPaths :: [Set][se-Set] of [StorePath][se-StorePath]
                     */
                    let out_paths: BTreeSet<StorePath> =
                        value.into_iter().map(|r| r.out_path).collect();
                    self.writer.write_value(&out_paths).await?;
                }
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
                trace!("DaemonConnection: Add multiple to store: call store");
                let logs = Self::local_add_multiple_to_store(
                    &mut store,
                    req.repair,
                    req.dont_check_sigs,
                    stream,
                );
                trace!("DaemonConnection: Add multiple to store: Logs");
                let res: Result<(), RecoverableError> = async {
                    let mut logs = pin!(logs);
                    trace!("DaemonConnection: Add to store: get log");
                    while let Some(msg) = logs.next().await {
                        trace!("DaemonConnection: Add multiple to store: got log {:?}", msg);
                        write_log(&mut self.writer, msg).await?;
                    }
                    trace!("DaemonConnection: Add multiple to store: get result");
                    logs.await.recover()?;
                    trace!("DaemonConnection: Add multiple to store: write result");
                    self.writer.write_value(&RawLogMessage::Last).await?;
                    Ok(())
                }
                .await;
                trace!("DaemonConnection: Add to store: drain reader");
                let err = framed.drain_all().await;
                trace!("DaemonConnection: Add multiple to store: done");
                res?;
                err?;
                /*
                ### Outputs
                Nothing
                 */
            }
            AddBuildLog(BaseStorePath(path)) => {
                /*
                ### Inputs
                - path :: [BaseStorePath][se-BaseStorePath]
                - [Framed][se-Framed] stream of log lines
                */
                let buf_reader = AsyncBufReadCompat::new(&mut self.reader);
                let mut framed = FramedReader::new(buf_reader);
                let logs = Self::local_add_build_log(&mut store, &path, &mut framed);
                let res = process_logs(&mut self.writer, logs).await;
                let err = framed.drain_all().await;
                res?;
                err?;
                self.writer.write_value(&RawLogMessage::Last).await?;

                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            BuildPathsWithResults(req) => {
                let logs = store.build_paths_with_results(&req.paths, req.mode);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                results :: [List][se-List] of [KeyedBuildResult][se-KeyedBuildResult]
                 */
                self.writer.write_value(&value).await?;
            }
            AddPermRoot(req) => {
                let logs = store.add_perm_root(&req.store_path, &req.gc_root);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                gcRoot :: [Path][se-Path]
                 */
                self.writer.write_value(&value).await?;
            }

            // Obsolete Nix 2.5.0 Protocol 1.32
            SyncWithGC => {
                let logs = store.sync_with_gc();
                self.local_process_logs(logs).await?;
                /*
                ### Outputs
                Nothing
                 */
            }
            // Obsolete Nix 2.4 Protocol 1.25
            AddTextToStore(_req) => {
                //let logs = store.add_ca_to_store(&req.path, req.gc_root);
                //let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                path :: [StorePath][se-StorePath]
                 */
                Err(DaemonErrorKind::UnimplementedOperation(
                    Operation::AddTextToStore,
                ))
                .with_operation(op)
                .recover()?;
            }
            // Obsolete Nix 2.4 Protocol 1.22*
            QueryDerivationOutputs(path) => {
                let logs = store.query_derivation_outputs(&path);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                derivationOutputs :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            // Obsolete Nix 2.4 Protocol 1.21
            QueryDerivationOutputNames(path) => {
                let logs = store.query_derivation_output_names(&path);
                let value = self.local_process_logs(logs).await?;
                /*
                ### Outputs
                names :: [Set][se-Set] of [OutputName][se-OutputName]
                 */
                self.writer.write_value(&value).await?;
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
