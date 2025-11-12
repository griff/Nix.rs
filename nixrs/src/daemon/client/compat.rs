use std::future::ready;

use bstr::ByteSlice as _;
use tokio::fs::canonicalize;

use crate::daemon::wire::types::Operation;
use crate::daemon::{
    DaemonError, DaemonPath, DaemonResult, DaemonStore, FutureResultExt as _, ResultLog,
    ResultLogExt as _, make_result,
};
use crate::store_path::StorePath;

pub trait CompatAddPermRoot<S> {
    fn add_perm_root<'a>(
        self,
        store: &'a mut S,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a;
}

pub trait LocalCompatAddPermRoot<S> {
    fn add_perm_root(
        self,
        store: &mut S,
        path: &StorePath,
        gc_root: &DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>>;
}

impl<S> CompatAddPermRoot<S> for () {
    fn add_perm_root(
        self,
        _store: &mut S,
        _path: &StorePath,
        _gc_root: &DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send {
        ready(Err(DaemonError::unimplemented(Operation::AddPermRoot))).empty_logs()
    }
}

impl<S> LocalCompatAddPermRoot<S> for () {
    fn add_perm_root(
        self,
        _store: &mut S,
        _path: &StorePath,
        _gc_root: &DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> {
        ready(Err(DaemonError::unimplemented(Operation::AddPermRoot))).empty_logs()
    }
}

pub struct LocalFSAddPermRoot {}

impl<S> CompatAddPermRoot<S> for LocalFSAddPermRoot
where
    S: DaemonStore + Send,
{
    fn add_perm_root<'a>(
        self,
        store: &'a mut S,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a {
        make_result(move |mut sender| async move {
            let gc_root = gc_root.to_os_str().map_err(DaemonError::custom)?;
            let gc_root = canonicalize(gc_root).await?;

            // check that gc_root is not in store
            if gc_root.starts_with(store.store_dir().to_path()) {
                return Err(DaemonError::custom(format!(
                    "creating a garbage collection root ({gc_root:?}) in theNix store is forbidden"
                )));
            }

            // addTempRoot(path)
            store.add_temp_root(path).forward_logs(&mut sender).await?;

            // Ensure that if gc_root exists it is a link that link points to the store
            // make symlink from gc_root to path

            // addIndirectRoot(gc_root)
            let n = <[u8]>::from_os_str(gc_root.as_os_str()).ok_or_else(|| {
                DaemonError::custom(format!(
                    "garbage collection root {gc_root:?} not valid UTF-8"
                ))
            })?;
            let gc_root = DaemonPath::copy_from_slice(n);
            store
                .add_indirect_root(&gc_root)
                .forward_logs(&mut sender)
                .await?;

            Ok(gc_root) as DaemonResult<DaemonPath>
        })
    }
}
