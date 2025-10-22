use std::borrow::Borrow;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::ready;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use capnp::Error as CapError;
use capnp::capability::Promise;
use capnp::traits::{FromPointerBuilder as _, SetterInput};
use capnp_rpc::{new_client, new_future_client, pry};
use futures::TryFutureExt as _;
use futures::future::{select_all, try_join, try_join_all};
use nixrs::daemon::UnkeyedValidPathInfo;
use nixrs::daemon::wire::types2::ValidPathInfo;
use nixrs::store_path::StorePath;
use tokio::sync::watch;
use tokio::task::spawn_local;
use tracing::warn;

use crate::capnp::nix_daemon_capnp::nix_daemon;
use crate::capnp::nix_types_capnp;
use crate::capnp::nixrs_capnp::{
    lookup_params, remote_store_path, store_path_access, store_path_store,
};
use crate::convert::{BuildFrom, ReadFrom, ReadInto};
use crate::nixrs::DaemonNar;

#[derive(Clone)]
pub struct RemoteStorePath {
    pub store_path: Rc<StorePath>,
    pub client: store_path_access::Client,
}
impl<'de> serde::Deserialize<'de> for RemoteStorePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let store_path = StorePath::deserialize(deserializer)?;
        Ok(RemoteStorePath {
            store_path: Rc::new(store_path),
            client: new_future_client(ready(Err(capnp::Error::disconnected(
                "No such client".into(),
            )))),
        })
    }
}
impl serde::Serialize for RemoteStorePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.store_path.serialize(serializer)
    }
}

impl fmt::Debug for RemoteStorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorePathAccess")
            .field("store_path", &self.store_path)
            .finish()
    }
}

impl std::hash::Hash for RemoteStorePath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.store_path, state);
    }
}

impl PartialEq for RemoteStorePath {
    fn eq(&self, other: &Self) -> bool {
        self.store_path == other.store_path
    }
}

impl Eq for RemoteStorePath {}

impl PartialOrd for RemoteStorePath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RemoteStorePath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.store_path.cmp(&other.store_path)
    }
}

impl Borrow<StorePath> for RemoteStorePath {
    fn borrow(&self) -> &StorePath {
        &self.store_path
    }
}

impl<'b> BuildFrom<RemoteStorePath> for nix_types_capnp::store_path::Builder<'b> {
    fn build_from(&mut self, input: &RemoteStorePath) -> Result<(), CapError> {
        self.build_from(&*input.store_path)
    }
}

impl<'b> BuildFrom<RemoteStorePath> for remote_store_path::Builder<'b> {
    fn build_from(&mut self, input: &RemoteStorePath) -> Result<(), CapError> {
        self.set_store_path(&*input.store_path)?;
        self.set_access(input.client.clone());
        Ok(())
    }
}

impl SetterInput<remote_store_path::Owned> for &'_ RemoteStorePath {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = remote_store_path::Builder::init_pointer(builder, 0);
        builder.set_store_path(&*input.store_path)?;
        builder.set_access(input.client.clone());
        Ok(())
    }
}

impl<'r> ReadFrom<remote_store_path::Reader<'r>> for RemoteStorePath {
    fn read_from(reader: remote_store_path::Reader<'r>) -> Result<Self, CapError> {
        let store_path = Rc::new(reader.get_store_path()?.read_into()?);
        let client = reader.get_access()?;
        Ok(RemoteStorePath { store_path, client })
    }
}

impl RemoteStorePath {
    pub fn daemon_path(store: nix_daemon::Client, store_path: StorePath) -> Self {
        let store_path = Rc::new(store_path);
        let client = new_client(DaemonStorePathAccess::new(store, store_path.clone()));
        RemoteStorePath { store_path, client }
    }

    pub fn from_store_path(
        store_path: StorePath,
        client: &store_path_store::Client,
    ) -> capnp::Result<Self> {
        let mut req = client.lookup_request();
        req.get().init_params().set_by_store_path(&store_path)?;
        let client = req.send().pipeline.get_path().get_access();
        let store_path = Rc::new(store_path);
        Ok(RemoteStorePath { store_path, client })
    }

    pub async fn load(client: store_path_access::Client) -> Result<Self, CapError> {
        let res = client.get_store_path_request().send().promise.await?;
        let store_path = Rc::new(res.get()?.get_path()?.read_into()?);
        Ok(Self { store_path, client })
    }

    pub async fn compute_closure(&self) -> Result<RemoteStorePathSet, CapError> {
        let mut ret = RemoteStorePathSet::new();
        ret.insert_closure(self.clone()).await?;
        Ok(ret)
    }
}

#[derive(Clone, Default)]
pub struct RemoteStorePathSet(BTreeSet<RemoteStorePath>);
impl RemoteStorePathSet {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn insert_closure(&mut self, store_path: RemoteStorePath) -> capnp::Result<bool> {
        if !self.contains(&store_path) {
            let mut pending = vec![store_path.client.get_references_request().send().promise];
            self.insert(store_path);
            while !pending.is_empty() {
                let (res, _, mut new_pending) = select_all(pending).await;
                for reader in res?.get()?.get_references()? {
                    let access: RemoteStorePath = reader.read_into()?;
                    if !self.contains(&access) {
                        new_pending.push(access.client.get_references_request().send().promise);
                        self.insert(access);
                    }
                }
                pending = new_pending;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn toposort(&self) -> capnp::Result<Vec<RemoteStorePath>> {
        let mut refs = BTreeMap::new();
        let mut rrefs: BTreeMap<RemoteStorePath, BTreeSet<RemoteStorePath>> = BTreeMap::new();
        let mut roots = BTreeSet::new();
        let mut infos = BTreeMap::new();
        let edges = try_join_all(self.iter().map(|access| {
            access
                .client
                .info_request()
                .send()
                .promise
                .map_ok(|refs| (access.clone(), refs))
        }))
        .await?;
        for (remote_store_path, info_resp) in edges {
            let info_r = info_resp.get()?.get_info()?;
            let info: ValidPathInfo = info_r.read_into()?;
            infos.insert(remote_store_path.clone(), info);
            let mut arefs: BTreeSet<RemoteStorePath> = info_r.get_references()?.read_into()?;
            arefs.remove(&remote_store_path);
            let edges: BTreeSet<RemoteStorePath> = arefs.intersection(self).cloned().collect();
            if edges.is_empty() {
                roots.insert(remote_store_path);
            } else {
                for edge in &edges {
                    rrefs
                        .entry(edge.clone())
                        .or_default()
                        .insert(remote_store_path.clone());
                }
                refs.insert(remote_store_path, edges);
            }
        }

        let mut sorted = Vec::with_capacity(self.len());
        while !roots.is_empty() {
            let n = roots.pop_first().unwrap();
            sorted.push(n.clone());
            if let Some(edges) = rrefs.get(&n) {
                for edge in edges {
                    if let Entry::Occupied(mut oci) = refs.entry(edge.clone()) {
                        let references = oci.get_mut();
                        references.remove(&n);
                        if references.is_empty() {
                            oci.remove_entry();
                            roots.insert(edge.clone());
                        }
                    }
                }
            }
        }
        if refs.is_empty() {
            Ok(sorted)
        } else {
            Err(CapError::failed("detected cycle in store paths".into()))
        }
    }

    pub async fn copy_to(&self, receiver: &store_path_store::Client) -> capnp::Result<()> {
        let mut res = Vec::with_capacity(self.len());
        for path in self.iter() {
            let mut req = receiver.add_request();
            req.get().set_path(path)?;
            res.push(req.send().promise);
        }
        try_join_all(res).await?;
        Ok(())
    }
}

impl Deref for RemoteStorePathSet {
    type Target = BTreeSet<RemoteStorePath>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RemoteStorePathSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl BuildFrom<RemoteStorePathSet> for capnp::struct_list::Builder<'_, remote_store_path::Owned> {
    fn build_from(&mut self, input: &RemoteStorePathSet) -> Result<(), CapError> {
        self.build_from(&input.0)
    }
}

impl BuildFrom<RemoteStorePathSet>
    for capnp::struct_list::Builder<'_, nix_types_capnp::store_path::Owned>
{
    fn build_from(&mut self, input: &RemoteStorePathSet) -> Result<(), CapError> {
        self.build_from(&input.0)
    }
}

pub struct DaemonStorePathAccess {
    store: nix_daemon::Client,
    store_path: Rc<StorePath>,
    info_sender: Option<watch::Sender<Result<UnkeyedValidPathInfo, CapError>>>,
    info_receiver: watch::Receiver<Result<UnkeyedValidPathInfo, CapError>>,
}

impl DaemonStorePathAccess {
    pub fn new(store: nix_daemon::Client, store_path: Rc<StorePath>) -> Self {
        let (sender, receiver) = watch::channel(Err(CapError::disconnected("No such info".into())));
        Self {
            store,
            store_path,
            info_sender: Some(sender),
            info_receiver: receiver,
        }
    }

    fn send_for_info(&mut self) {
        if let Some(sender) = self.info_sender.take() {
            let mut req = self.store.query_path_info_request();
            let store_path = self.store_path.clone();

            spawn_local(async move {
                if let Err(err) = req.get().set_path(&*store_path) {
                    let _ = sender.send(Err(err));
                    return;
                }
                tokio::select! {
                    _ = sender.closed() => {
                        warn!(%store_path, "Canceling info requets because all recivers were dropped");
                    },
                    res = req.send().promise => {
                        let value = res.and_then(|res| {
                            let r = res.get()?;
                            if r.has_info() {
                                let info = r.get_info()?.read_into()?;
                                Ok(info)
                            } else {
                                Err(CapError::failed(format!("info for {store_path} is missing")))
                            }
                        });
                        let _ = sender.send(value);
                    }
                }
            });
        }
    }

    fn with_info<F>(&mut self, f: F) -> Promise<(), CapError>
    where
        F: FnOnce(&UnkeyedValidPathInfo) -> Result<(), CapError> + 'static,
    {
        if matches!(self.info_receiver.has_changed(), Ok(true) | Err(_)) {
            let info = self.info_receiver.borrow();
            match info.as_ref() {
                Ok(info) => f(info).into(),
                Err(err) => Promise::err(err.clone()),
            }
        } else {
            self.send_for_info();
            let mut receiver = self.info_receiver.clone();
            let store_path = self.store_path.clone();
            Promise::from_future(async move {
                receiver.changed().await.map_err(|err| {
                    CapError::failed(format!("Could not get info for {store_path}: {err}"))
                })?;
                let info = receiver.borrow();
                match info.as_ref() {
                    Ok(info) => f(info),
                    Err(err) => Err(err.clone()),
                }
            })
        }
    }
}

impl store_path_access::Server for DaemonStorePathAccess {
    fn get_store_path(
        &mut self,
        _params: store_path_access::GetStorePathParams,
        mut result: store_path_access::GetStorePathResults,
    ) -> Promise<(), CapError> {
        pry!(result.get().set_path(&*self.store_path));
        Promise::ok(())
    }

    fn get_deriver(
        &mut self,
        _params: store_path_access::GetDeriverParams,
        mut result: store_path_access::GetDeriverResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        self.with_info(move |info| {
            if let Some(deriver) = info.deriver.as_ref() {
                let remote_store_path = RemoteStorePath::daemon_path(store, deriver.clone());
                result.get().set_deriver(&remote_store_path)?;
            }
            Ok(())
        })
    }

    fn get_references(
        &mut self,
        _params: store_path_access::GetReferencesParams,
        mut result: store_path_access::GetReferencesResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        self.with_info(move |info| {
            let mut b = result.get().init_references(info.references.len() as u32);
            for (index, store_path) in info.references.iter().enumerate() {
                let remote_store_path =
                    RemoteStorePath::daemon_path(store.clone(), store_path.clone());
                b.reborrow()
                    .get(index as u32)
                    .build_from(&remote_store_path)?;
            }
            Ok(())
        })
    }

    fn get_registration_time(
        &mut self,
        _params: store_path_access::GetRegistrationTimeParams,
        mut result: store_path_access::GetRegistrationTimeResults,
    ) -> Promise<(), CapError> {
        self.with_info(move |info| {
            result.get().set_time(info.registration_time);
            Ok(())
        })
    }

    fn get_size(
        &mut self,
        _params: store_path_access::GetSizeParams,
        mut result: store_path_access::GetSizeResults,
    ) -> Promise<(), CapError> {
        self.with_info(move |info| {
            result.get().set_size(info.nar_size);
            Ok(())
        })
    }

    fn is_ultimate(
        &mut self,
        _params: store_path_access::IsUltimateParams,
        mut result: store_path_access::IsUltimateResults,
    ) -> Promise<(), CapError> {
        self.with_info(move |info| {
            result.get().set_trusted(info.ultimate);
            Ok(())
        })
    }

    fn get_signatures(
        &mut self,
        _params: store_path_access::GetSignaturesParams,
        mut result: store_path_access::GetSignaturesResults,
    ) -> Promise<(), CapError> {
        self.with_info(move |info| {
            let mut b = result.get().init_signatures(info.signatures.len() as u32);
            for (index, signature) in info.signatures.iter().enumerate() {
                let mut ib = b.reborrow().get(index as u32);
                ib.set_key(signature.name());
                ib.set_hash(signature.signature_bytes());
            }
            Ok(())
        })
    }

    fn info(
        &mut self,
        _params: store_path_access::InfoParams,
        mut result: store_path_access::InfoResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        self.with_info(move |info| {
            let b = result.get();
            let mut builder = b.init_info();
            if let Some(deriver) = info.deriver.as_ref() {
                let remote_store_path =
                    RemoteStorePath::daemon_path(store.clone(), deriver.clone());
                builder.set_deriver(&remote_store_path)?;
            }
            builder.set_nar_hash(info.nar_hash.digest_bytes());
            let mut b = builder
                .reborrow()
                .init_references(info.references.len() as u32);
            for (index, store_path) in info.references.iter().enumerate() {
                let remote_store_path =
                    RemoteStorePath::daemon_path(store.clone(), store_path.clone());
                b.reborrow()
                    .get(index as u32)
                    .build_from(&remote_store_path)?;
            }
            builder.set_registration_time(info.registration_time);
            builder.set_nar_size(info.nar_size);
            builder.set_ultimate(info.ultimate);
            builder
                .reborrow()
                .init_signatures(info.signatures.len() as u32)
                .build_from(&info.signatures)?;
            if let Some(ca) = info.ca.as_ref() {
                builder.set_ca(ca)?;
            }
            Ok(())
        })
    }

    fn nar(
        &mut self,
        _params: store_path_access::NarParams,
        mut result: store_path_access::NarResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        let store_path = self.store_path.clone();
        self.with_info(move |info| {
            let client = new_client(DaemonNar {
                store,
                store_path,
                nar_hash: info.nar_hash,
                nar_size: info.nar_size,
            });
            result.get().set_nar(client);
            Ok(())
        })
    }
}

pub struct DaemonStorePathStore {
    store: nix_daemon::Client,
}

impl DaemonStorePathStore {
    pub fn new(store: nix_daemon::Client) -> Self {
        Self { store }
    }
}

impl store_path_store::Server for DaemonStorePathStore {
    fn list(
        &mut self,
        _params: store_path_store::ListParams,
        mut result: store_path_store::ListResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        Promise::from_future(async move {
            let paths = store.query_all_valid_paths_request().send().promise.await?;
            let r = paths.get()?.get_paths()?;
            let mut b = result.get().init_paths(r.len());
            for (index, item) in r.iter().enumerate() {
                let store_path = item.read_into()?;
                let remote_store_path = RemoteStorePath::daemon_path(store.clone(), store_path);
                b.reborrow()
                    .get(index as u32)
                    .build_from(&remote_store_path)?;
            }
            Ok(())
        })
    }

    fn lookup(
        &mut self,
        params: store_path_store::LookupParams,
        mut result: store_path_store::LookupResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        Promise::from_future(async move {
            match params.get()?.get_params()?.which()? {
                lookup_params::Which::ByStorePath(store_path) => {
                    let store_path = store_path?.read_into()?;
                    let mut req = store.is_valid_path_request();
                    req.get().set_path(&store_path)?;
                    let resp = req.send().promise.await?;
                    if resp.get()?.get_valid() {
                        let remote_store_path = RemoteStorePath::daemon_path(store, store_path);
                        result.get().set_path(&remote_store_path)?;
                    }
                }
                lookup_params::Which::ByHash(hash) => {
                    let mut req = store.query_path_from_hash_part_request();
                    req.get().set_hash(hash?);
                    let resp = req.send().promise.await?;
                    let r = resp.get()?;
                    if r.has_path() {
                        let store_path: StorePath = r.get_path()?.read_into()?;
                        let remote_store_path = RemoteStorePath::daemon_path(store, store_path);
                        result.get().set_path(&remote_store_path)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn add(
        &mut self,
        params: store_path_store::AddParams,
        _result: store_path_store::AddResults,
    ) -> Promise<(), CapError> {
        let store = self.store.clone();
        Promise::from_future(async move {
            let rp = params.get()?;
            let remote_store_path: RemoteStorePath = rp.get_path()?.read_into()?;
            // ComputeFSClosure
            let mut closure = RemoteStorePathSet::new();
            closure.insert_closure(remote_store_path).await?;

            // Remove already valid
            let mut req = store.query_valid_paths_request();
            let mut b = req.get();
            b.reborrow()
                .init_paths(closure.len() as u32)
                .build_from(&closure)?;
            b.set_substitute(rp.get_substitute());
            let valid_res = req.send().promise.await?;
            for v in valid_res.get()?.get_valid_set()? {
                let store_path: StorePath = v.read_into()?;
                closure.remove(&store_path);
            }

            // Toposort missing
            let sorted = closure.toposort().await?;

            // AddMultipleToStore
            let mut req = store.add_multiple_to_store_request();
            let mut b = req.get();
            b.set_count(sorted.len() as u16);
            b.set_dont_check_sigs(rp.get_dont_check_sigs());
            b.set_repair(rp.get_repair());
            let res = req.send();
            let stream = res.pipeline.get_stream();
            let mut work = Vec::with_capacity(sorted.len());
            for item in sorted {
                let info: ValidPathInfo = item
                    .client
                    .info_request()
                    .send()
                    .promise
                    .await?
                    .get()?
                    .get_info()?
                    .read_into()?;
                let mut req = stream.add_request();
                req.get().set_info(&info)?;
                let res = req.send();
                let bs = res.pipeline.get_stream();
                let mut write = item
                    .client
                    .nar_request()
                    .send()
                    .pipeline
                    .get_nar()
                    .write_to_request();
                write.get().set_stream(bs);
                work.push(try_join(res.promise, write.send().promise));
            }
            try_join(res.promise, try_join_all(work)).await?;
            Ok(())
        })
    }
}
