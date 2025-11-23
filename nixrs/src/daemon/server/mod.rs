use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::pin::{Pin, pin};

use futures::future::TryFutureExt;
use futures::{FutureExt, Stream, StreamExt as _};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, copy_buf};
use tokio::select;
use tracing::{Instrument, debug, error, info, instrument, trace};

use crate::archive::NarReader;
use crate::daemon::de::{NixRead, NixReader};
use crate::daemon::ser::{NixWrite, NixWriter};
use crate::daemon::wire::logger::RawLogMessage;
use crate::daemon::wire::types::{
    AddToStoreRequest, BaseStorePath, Operation, RegisterDrvOutputRequest, Request,
};
use crate::daemon::wire::{
    CLIENT_MAGIC, FramedReader, IgnoredOne, SERVER_MAGIC, StderrReader, parse_add_multiple_to_store,
};
use crate::daemon::{
    AddToStoreItem, CollectGarbageResponse, DaemonError, DaemonErrorKind, DaemonPath, DaemonResult,
    DaemonResultExt, DaemonStore, GCAction, HandshakeDaemonStore, NIX_VERSION, PROTOCOL_VERSION,
    ProtocolVersion, ResultLog, TrustLevel, ValidPathInfo,
};
use crate::derivation::BasicDerivation;
use crate::derived_path::{DerivedPath, OutputName};
use crate::io::{AsyncBufReadCompat, BytesReader};
use crate::log::LogMessage;
use crate::realisation::{DrvOutput, Realisation};
use crate::signature::Signature;
use crate::store_path::{
    ContentAddressMethodAlgorithm, HasStoreDir, StorePath, StorePathHash, StorePathSet,
};

mod local;

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

    pub fn set_min_version<V: Into<ProtocolVersion>>(&mut self, version: V) -> &mut Self {
        let version = version.into();
        assert!(
            version >= ProtocolVersion::min(),
            "min version must be at least {}",
            ProtocolVersion::min()
        );
        self.min_version = version;
        self
    }

    pub fn set_max_version<V: Into<ProtocolVersion>>(&mut self, version: V) -> &mut Self {
        let version = version.into();
        assert!(
            version <= ProtocolVersion::max(),
            "max version must not be after {}",
            ProtocolVersion::max()
        );
        self.max_version = version;
        self
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
        LogMessage::Message(raw_msg) => {
            let msg = String::from_utf8_lossy(&raw_msg.text);
            trace!("log_message: {}", msg);
        }
        LogMessage::StartActivity(activity) => {
            let text = String::from_utf8_lossy(&activity.text);
            trace!(id=activity.id, level=?activity.level, type=?activity.activity_type,
                ?text,
                parent=activity.parent,
                "start_activity: {:?} {:?}: {}", activity.activity_type, activity.fields, text);
        }
        LogMessage::StopActivity(activity) => {
            trace!(id = activity.id, "stop_activity: {}", activity.id);
        }
        LogMessage::Result(result) => {
            trace!(
                id = result.id,
                "log_result: {} {:?} {:?}", result.id, result.result_type, result.fields,
            );
        }
    }
    writer.write_value(&msg).await?;
    writer.flush().await?;
    Ok(())
}

async fn process_logs<'s, T, W>(
    writer: &'s mut NixWriter<W>,
    logs: impl ResultLog<Output = DaemonResult<T>> + 's,
) -> Result<T, RecoverableError>
where
    T: 's,
    W: AsyncWrite + Send + Unpin,
{
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
        Ok(value) => Ok(value),
    }
}

struct BoxedStore<S>(S);

impl<S: HasStoreDir> HasStoreDir for BoxedStore<S> {
    fn store_dir(&self) -> &crate::store_path::StoreDir {
        self.0.store_dir()
    }
}

#[forbid(clippy::missing_trait_methods)]
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
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        Box::pin(self.0.set_options(options))
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + 'a {
        let ret = Box::pin(self.0.is_valid_path(path));
        trace!("IsValidPath Size {}", size_of_val(&ret));
        ret
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + 'a {
        let ret = Box::pin(self.0.query_valid_paths(paths, substitute));
        trace!("QueryValidPaths Size {}", size_of_val(&ret));
        ret
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<super::UnkeyedValidPathInfo>>> + Send + 'a
    {
        let ret = Box::pin(self.0.query_path_info(path));
        trace!("QueryPathInfo Size {}", size_of_val(&ret));
        ret
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + use<S>>> + Send + 's {
        let ret = Box::pin(self.0.nar_from_path(path));
        trace!("NarFromPath Size {}", size_of_val(&ret));
        ret
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let ret = Box::pin(self.0.build_paths(paths, mode));
        trace!("BuildPaths Size {}", size_of_val(&ret));
        ret
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<super::BuildResult>> + Send + 'a {
        let ret = Box::pin(self.0.build_derivation(drv, mode));
        trace!("BuildDerivation Size {}", size_of_val(&ret));
        ret
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<super::QueryMissingResult>> + Send + 'a {
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
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
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
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        ST: Stream<Item = Result<AddToStoreItem<STR>, DaemonError>> + Send + 'i,
        STR: AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        let ret = self
            .0
            .add_multiple_to_store(repair, dont_check_sigs, stream);
        trace!("AddMultipleToStore Size {}", size_of_val(ret.deref()));
        ret
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<super::KeyedBuildResult>>> + Send + 'a {
        let ret = Box::pin(self.0.build_paths_with_results(drvs, mode));
        trace!("BuildPathsWithResults Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + '_ {
        let ret = Box::pin(self.0.query_all_valid_paths());
        trace!("QueryAllValidPaths Size {}", size_of_val(&ret));
        ret
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<StorePath>>> + Send + 'a {
        let ret = Box::pin(self.0.query_referrers(path));
        trace!("QueryReferrers Size {}", size_of_val(ret.deref()));
        ret
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let ret = Box::pin(self.0.ensure_path(path));
        trace!("EnsurePath Size {}", size_of_val(ret.deref()));
        ret
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let ret = Box::pin(self.0.add_temp_root(path));
        trace!("AddTempRoot Size {}", size_of_val(ret.deref()));
        ret
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let ret = Box::pin(self.0.add_indirect_root(path));
        trace!("AddIndirectRoot Size {}", size_of_val(ret.deref()));
        ret
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + Send + '_ {
        let ret = Box::pin(self.0.find_roots());
        trace!("FindRoots Size {}", size_of_val(ret.deref()));
        ret
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + Send + 'a {
        let ret =
            Box::pin(
                self.0
                    .collect_garbage(action, paths_to_delete, ignore_liveness, max_freed),
            );
        trace!("CollectGarbage Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + Send + 'a {
        let ret = Box::pin(self.0.query_path_from_hash_part(hash));
        trace!("QueryPathFromHashPart Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let ret = Box::pin(self.0.query_substitutable_paths(paths));
        trace!("QuerySubstitutablePaths Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let ret = Box::pin(self.0.query_valid_derivers(path));
        trace!("QueryValidDerivers Size {}", size_of_val(ret.deref()));
        ret
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        let ret = Box::pin(self.0.optimise_store());
        trace!("OptimiseStore Size {}", size_of_val(ret.deref()));
        ret
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + '_ {
        let ret = Box::pin(self.0.verify_store(check_contents, repair));
        trace!("VerifyStore Size {}", size_of_val(ret.deref()));
        ret
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let ret = Box::pin(self.0.add_signatures(path, signatures));
        trace!("AddSignatures Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + Send + 'a
    {
        let ret = Box::pin(self.0.query_derivation_output_map(path));
        trace!("QueryDerivationOutputMap Size {}", size_of_val(ret.deref()));
        ret
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let ret = Box::pin(self.0.register_drv_output(realisation));
        trace!("RegisterDrvOutput Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<Realisation>>> + Send + 'a {
        let ret = Box::pin(self.0.query_realisation(output_id));
        trace!("QueryRealisation Size {}", size_of_val(ret.deref()));
        ret
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        let ret = self.0.add_build_log(path, source);
        trace!("AddBuildLog Size {}", size_of_val(ret.deref()));
        ret
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a {
        let ret = Box::pin(self.0.add_perm_root(path, gc_root));
        trace!("AddPermRoot Size {}", size_of_val(ret.deref()));
        ret
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        let ret = Box::pin(self.0.sync_with_gc());
        trace!("SyncWithGC Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let ret = Box::pin(self.0.query_derivation_outputs(path));
        trace!("QueryDerivationOutputs Size {}", size_of_val(ret.deref()));
        ret
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + Send + 'a {
        let ret = Box::pin(self.0.query_derivation_output_names(path));
        trace!(
            "QueryDerivationOutputNames Size {}",
            size_of_val(ret.deref())
        );
        ret
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<ValidPathInfo>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        let ret = self.0.add_ca_to_store(name, cam, refs, repair, source);
        trace!("AddToStore Size {}", size_of_val(ret.deref()));
        ret
    }

    fn shutdown(&mut self) -> impl Future<Output = DaemonResult<()>> + Send + '_ {
        let ret = Box::pin(self.0.shutdown());
        trace!("Shutdown Size {}", size_of_val(&ret));
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
        debug!(
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
        logs: impl ResultLog<Output = DaemonResult<T>> + Send + 's,
    ) -> Result<T, RecoverableError> {
        let value = process_logs(&mut self.writer, logs).await?;
        self.writer.write_value(&RawLogMessage::Last).await?;
        Ok(value)
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
                debug!("Server got operation {}", op);
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
        debug!("Server handled all requests");
        store.shutdown().await
    }

    fn add_ca_to_store<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        name: &'p str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'p StorePathSet,
        repair: bool,
        source: NW,
    ) -> impl ResultLog<Output = DaemonResult<ValidPathInfo>> + Send + 'r
    where
        S: DaemonStore + 's,
        NW: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.add_ca_to_store(name, cam, refs, repair, source)
    }

    fn add_to_store_nar<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        info: &'p ValidPathInfo,
        source: NW,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
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
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        S: DaemonStore + 's,
        ST: Stream<Item = Result<AddToStoreItem<STR>, DaemonError>> + Send + 'r,
        STR: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
    {
        store.add_multiple_to_store(repair, dont_check_sigs, stream)
    }

    fn store_nar_from_path<'s, S>(
        store: &'s mut S,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + 's>> + Send + 's
    where
        S: DaemonStore + 's,
    {
        store.nar_from_path(path)
    }

    async fn nar_from_path<'s, 't, S>(
        &'s mut self,
        store: &'t mut S,
        path: StorePath,
    ) -> Result<(), RecoverableError>
    where
        S: DaemonStore + 't,
    {
        let logs = Self::store_nar_from_path(store, &path);

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

    fn add_build_log<'s, 'p, 'r, NW, S>(
        store: &'s mut S,
        path: &'p StorePath,
        source: NW,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        S: DaemonStore + 's,
        NW: AsyncBufRead + Unpin + Send + 'r,
        's: 'r,
        'p: 'r,
    {
        store.add_build_log(path, source)
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
            QueryReferrers(path) => {
                let logs = store.query_referrers(&path);
                let value = self.process_logs(logs).await?;
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
                let logs = Self::add_ca_to_store(
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
                self.process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            EnsurePath(path) => {
                let logs = store.ensure_path(&path);
                self.process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            AddTempRoot(path) => {
                let logs = store.add_temp_root(&path);
                self.process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            AddIndirectRoot(path) => {
                let logs = store.add_indirect_root(&path);
                self.process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            FindRoots => {
                let logs = store.find_roots();
                let value = self.process_logs(logs).await?;
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
                let value = self.process_logs(logs).await?;
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
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            QueryPathFromHashPart(hash) => {
                let logs = store.query_path_from_hash_part(&hash);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                path :: [OptStorePath][se-OptStorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            QuerySubstitutablePaths(paths) => {
                let logs = store.query_substitutable_paths(&paths);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                paths :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            QueryValidDerivers(path) => {
                let logs = store.query_valid_derivers(&path);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                derivers :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            OptimiseStore => {
                let logs = store.optimise_store();
                self.process_logs(logs).await?;
                /*
                ### Outputs
                1 :: [Int][se-Int] (hardcoded and ignored by client)
                 */
                self.writer.write_value(&IgnoredOne).await?;
            }
            VerifyStore(req) => {
                let logs = store.verify_store(req.check_contents, req.repair);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                errors :: [Bool][se-Bool]
                 */
                self.writer.write_value(&value).await?;
            }
            BuildDerivation(req) => {
                let logs = store.build_derivation(&req.drv, req.mode);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                buildResult :: [BuildResult][se-BuildResult]
                 */
                self.writer.write_value(&value).await?;
            }
            AddSignatures(req) => {
                let logs = store.add_signatures(&req.path, &req.signatures);
                self.process_logs(logs).await?;
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
            QueryDerivationOutputMap(path) => {
                let logs = store.query_derivation_output_map(&path);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                outputs :: [Map][se-Map] of [OutputName][se-OutputName] to [OptStorePath][se-OptStorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            RegisterDrvOutput(RegisterDrvOutputRequest::Post31(realisation)) => {
                let logs = store.register_drv_output(&realisation);
                self.process_logs(logs).await?;
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
                self.process_logs(logs).await?;
                /*
                ### Outputs
                Nothing
                 */
            }
            QueryRealisation(output_id) => {
                let logs = store.query_realisation(&output_id);
                let value = self.process_logs(logs).await?;
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
                let logs = Self::add_multiple_to_store(
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
                let logs = Self::add_build_log(&mut store, &path, &mut framed);
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
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                results :: [List][se-List] of [KeyedBuildResult][se-KeyedBuildResult]
                 */
                self.writer.write_value(&value).await?;
            }
            AddPermRoot(req) => {
                let logs = store.add_perm_root(&req.store_path, &req.gc_root);
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                gcRoot :: [Path][se-Path]
                 */
                self.writer.write_value(&value).await?;
            }

            // Obsolete Nix 2.5.0 Protocol 1.32
            SyncWithGC => {
                let logs = store.sync_with_gc();
                self.process_logs(logs).await?;
                /*
                ### Outputs
                Nothing
                 */
            }
            // Obsolete Nix 2.4 Protocol 1.25
            AddTextToStore(_req) => {
                //let logs = store.add_ca_to_store(&req.path, req.gc_root);
                //let value = self.process_logs(logs).await?;
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
                let value = self.process_logs(logs).await?;
                /*
                ### Outputs
                derivationOutputs :: [Set][se-Set] of [StorePath][se-StorePath]
                 */
                self.writer.write_value(&value).await?;
            }
            // Obsolete Nix 2.4 Protocol 1.21
            QueryDerivationOutputNames(path) => {
                let logs = store.query_derivation_output_names(&path);
                let value = self.process_logs(logs).await?;
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
