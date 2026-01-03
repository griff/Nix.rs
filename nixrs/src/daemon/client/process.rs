use std::collections::{BTreeMap, BTreeSet};
use std::pin::{Pin, pin};

use bytes::Bytes;
use futures::{SinkExt, StreamExt as _};
use tokio::io::{AsyncBufRead, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tracing::warn;

use crate::daemon::client::compat::CompatAddPermRoot;
use crate::daemon::client::{DaemonClient, DaemonHandshakeClient};
use crate::daemon::{
    DaemonResult, DaemonStore, HandshakeDaemonStore, LogSender, ResultLog, ResultLogExt as _,
    make_result,
};
use crate::log::{LogMessage, Message};
use crate::store_path::{HasStoreDir, StorePath};

async fn read_logs<T>(
    result: impl ResultLog<Output = DaemonResult<T>>,
    sender: &mut LogSender,
    lines: &mut Lines<BufReader<ChildStderr>>,
) -> DaemonResult<T> {
    let mut s = pin!(result);
    loop {
        tokio::select! {
            next_msg = s.next() => {
                if let Some(msg) = next_msg {
                    sender.send(msg).await?;
                } else {
                    break;
                }
            },
            line_res = lines.next_line() => {
                match line_res {
                    Ok(Some(line)) => {
                        let msg = LogMessage::Message(Message {
                            level: crate::log::Verbosity::Debug,
                            text: Bytes::copy_from_slice(line.as_bytes()),
                        });
                        sender.send(msg).await?;
                    }
                    Ok(_) => {
                        warn!("Stderr already completed!");
                    },
                    Err(err) => {
                        let err_message = format!("Could not read stderr: {err}");
                        let msg = LogMessage::Message(Message {
                            level: crate::log::Verbosity::Error,
                            text: Bytes::from(err_message),
                        });
                        sender.send(msg).await?;
                    }
                }
            }
        }
    }
    s.await
}

pub struct ChildHandshakeStore<CP> {
    pub(crate) store: DaemonHandshakeClient<ChildStdout, ChildStdin, CP>,
    pub(crate) stderr: Lines<BufReader<ChildStderr>>,
    pub(crate) child: Child,
}

impl<CP> HasStoreDir for ChildHandshakeStore<CP> {
    fn store_dir(&self) -> &crate::store_path::StoreDir {
        self.store.store_dir()
    }
}

impl<CP> HandshakeDaemonStore for ChildHandshakeStore<CP>
where
    CP: CompatAddPermRoot<DaemonClient<ChildStdout, ChildStdin, CP>> + Clone + Send,
{
    type Store = ChildStore<CP>;

    fn handshake(
        mut self,
    ) -> impl crate::daemon::ResultLog<Output = DaemonResult<Self::Store>> + Send {
        make_result(move |mut sender| async move {
            let result = self.store.handshake();

            let store = read_logs(result, &mut sender, &mut self.stderr).await?;
            Ok(ChildStore {
                store,
                stderr: self.stderr,
                child: self.child,
            })
        })
    }
}

pub struct ChildStore<CP> {
    store: DaemonClient<ChildStdout, ChildStdin, CP>,
    stderr: Lines<BufReader<ChildStderr>>,
    child: Child,
}

impl<CP> HasStoreDir for ChildStore<CP> {
    fn store_dir(&self) -> &crate::store_path::StoreDir {
        self.store.store_dir()
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<CP> DaemonStore for ChildStore<CP>
where
    CP: CompatAddPermRoot<DaemonClient<ChildStdout, ChildStdin, CP>> + Clone + Send,
{
    fn trust_level(&self) -> crate::daemon::TrustLevel {
        self.store.trust_level()
    }

    fn shutdown(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        make_result(move |mut sender| async move {
            let result = self.store.shutdown();

            read_logs(result, &mut sender, &mut self.stderr).await?;
            self.child.kill().await?;
            self.child.wait().await?;
            while let Some(line) = self.stderr.next_line().await? {
                let msg = LogMessage::Message(Message {
                    level: crate::log::Verbosity::Debug,
                    text: Bytes::from(line),
                });
                sender.send(msg).await?;
            }
            Ok(())
        })
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a crate::daemon::ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.set_options(options);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.is_valid_path(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_valid_paths(paths, substitute);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<crate::daemon::UnkeyedValidPathInfo>>> + Send + 'a
    {
        make_result(move |mut sender| async move {
            let result = self.store.query_path_info(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + use<CP>>> + Send + 's {
        make_result(move |mut sender| async move {
            let result = self.store.nar_from_path(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: crate::daemon::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.build_paths(drvs, mode);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: crate::daemon::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<crate::daemon::KeyedBuildResult>>> + Send + 'a
    {
        make_result(move |mut sender| async move {
            let result = self.store.build_paths_with_results(drvs, mode);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a crate::derivation::BasicDerivation,
        mode: crate::daemon::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<crate::daemon::BuildResult>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.build_derivation(drv, mode);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [crate::derived_path::DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<crate::daemon::QueryMissingResult>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_missing(paths);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i crate::daemon::ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: tokio::io::AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        make_result(move |mut sender| async move {
            let result = self
                .store
                .add_to_store_nar(info, source, repair, dont_check_sigs);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
        .boxed_result()
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        S: futures::Stream<
                Item = Result<crate::daemon::AddToStoreItem<R>, crate::daemon::DaemonError>,
            > + Send
            + 'i,
        R: tokio::io::AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        make_result(move |mut sender| async move {
            let result = self
                .store
                .add_multiple_to_store(repair, dont_check_sigs, stream);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
        .boxed_result()
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + '_ {
        make_result(move |mut sender| async move {
            let result = self.store.query_all_valid_paths();
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_referrers(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.ensure_path(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.add_temp_root(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a crate::daemon::DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.add_indirect_root(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<crate::daemon::DaemonPath, StorePath>>> + Send + '_
    {
        make_result(move |mut sender| async move {
            let result = self.store.find_roots();
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: crate::daemon::GCAction,
        paths_to_delete: &'a crate::store_path::StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<crate::daemon::CollectGarbageResponse>> + Send + 'a
    {
        make_result(move |mut sender| async move {
            let result =
                self.store
                    .collect_garbage(action, paths_to_delete, ignore_liveness, max_freed);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a crate::store_path::StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_path_from_hash_part(hash);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_substitutable_paths(paths);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_valid_derivers(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        make_result(move |mut sender| async move {
            let result = self.store.optimise_store();
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + '_ {
        make_result(move |mut sender| async move {
            let result = self.store.verify_store(check_contents, repair);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [crate::signature::Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.add_signatures(path, signatures);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<
        Output = DaemonResult<BTreeMap<crate::derived_path::OutputName, Option<StorePath>>>,
    > + Send
    + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_derivation_output_map(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a crate::realisation::Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.register_drv_output(realisation);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a crate::realisation::DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<Option<crate::realisation::Realisation>>> + Send + 'a
    {
        make_result(move |mut sender| async move {
            let result = self.store.query_realisation(output_id);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: tokio::io::AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        make_result(move |mut sender| async move {
            let result = self.store.add_build_log(path, source);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
        .boxed_result()
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a crate::daemon::DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<crate::daemon::DaemonPath>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.add_perm_root(path, gc_root);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        make_result(move |mut sender| async move {
            let result = self.store.sync_with_gc();
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + 'a {
        make_result(move |mut sender| async move {
            let result = self.store.query_derivation_outputs(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<crate::derived_path::OutputName>>> + Send + 'a
    {
        make_result(move |mut sender| async move {
            let result = self.store.query_derivation_output_names(path);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: crate::store_path::ContentAddressMethodAlgorithm,
        refs: &'a crate::store_path::StorePathSet,
        repair: bool,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<crate::daemon::ValidPathInfo>> + Send + 'r>>
    where
        R: tokio::io::AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        make_result(move |mut sender| async move {
            let result = self.store.add_ca_to_store(name, cam, refs, repair, source);
            read_logs(result, &mut sender, &mut self.stderr).await
        })
        .boxed_result()
    }
}
